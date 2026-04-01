"""Program-memory curation assistant.

Drafts provenance-grounded update suggestions against the checked-in
program-memory substrate.  Every output is a *draft* that requires
explicit human adjudication before it can be merged into the canonical
dataset -- no silent auto-merge ever happens.

Typical flow
-------------
1. Load the current ``ProgramMemoryDataset`` (v2 CSVs).
2. Run a coverage audit to surface gaps and uncertainty.
3. Optionally load a harvest batch to incorporate machine-suggested
   additions.
4. Call ``build_curation_draft`` to produce a ``CurationDraft`` --
   a list of ``CurationDraftItem`` records, each carrying:
   - a provenance chain back to checked-in evidence,
   - an uncertainty assessment,
   - and a human-facing rationale.
5. Persist the draft with ``write_curation_draft`` for downstream
   human review.

The assistant **never** applies changes to the canonical dataset.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from scz_target_engine.io import write_json
from scz_target_engine.program_memory._helpers import clean_text
from scz_target_engine.program_memory.coverage import (
    ProgramMemoryCoverageAudit,
    ProgramMemoryCoverageGap,
    ProgramMemoryCoverageSummary,
    build_program_memory_coverage_audit,
)
from scz_target_engine.program_memory.harvest import ProgramMemoryHarvestBatch
from scz_target_engine.program_memory.loaders import load_program_memory_dataset
from scz_target_engine.program_memory.models import ProgramMemoryDataset


CURATION_ASSISTANT_SCHEMA_VERSION = "curation-assistant-draft-v1"

# Draft-item action vocabulary.  These are *suggestions to the human
# reviewer*, not commands.
ACTION_ADD_EVENT = "suggest_add_event"
ACTION_ADD_HYPOTHESIS = "suggest_add_directionality_hypothesis"
ACTION_UPGRADE_CONFIDENCE = "suggest_upgrade_confidence"
ACTION_RESOLVE_FAILURE_SCOPE = "suggest_resolve_failure_scope"
ACTION_ADD_SUPPORTING_EVENTS = "suggest_add_supporting_event_ids"
ACTION_FLAG_FOR_REVIEW = "flag_for_human_review"

DRAFT_ACTIONS = frozenset(
    {
        ACTION_ADD_EVENT,
        ACTION_ADD_HYPOTHESIS,
        ACTION_UPGRADE_CONFIDENCE,
        ACTION_RESOLVE_FAILURE_SCOPE,
        ACTION_ADD_SUPPORTING_EVENTS,
        ACTION_FLAG_FOR_REVIEW,
    }
)


@dataclass(frozen=True)
class CurationDraftItem:
    """A single suggestion for the human curator.

    Every item carries provenance references back to checked-in records
    or harvest suggestions so the reviewer can verify the basis before
    deciding.
    """

    item_id: str
    action: str
    dimension: str
    scope_value: str
    rationale: str
    provenance_event_ids: tuple[str, ...]
    provenance_hypothesis_ids: tuple[str, ...]
    provenance_gap_codes: tuple[str, ...]
    uncertainty_codes: tuple[str, ...]
    harvest_suggestion_ids: tuple[str, ...]
    confidence_assessment: str
    requires_human_review: bool

    def __post_init__(self) -> None:
        if not self.item_id:
            raise ValueError("curation draft items require item_id")
        if self.action not in DRAFT_ACTIONS:
            raise ValueError(
                f"unsupported curation draft action {self.action!r}"
            )
        if not self.requires_human_review:
            raise ValueError(
                "curation draft items must always require human review"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "item_id": self.item_id,
            "action": self.action,
            "dimension": self.dimension,
            "scope_value": self.scope_value,
            "rationale": self.rationale,
            "provenance_event_ids": list(self.provenance_event_ids),
            "provenance_hypothesis_ids": list(self.provenance_hypothesis_ids),
            "provenance_gap_codes": list(self.provenance_gap_codes),
            "uncertainty_codes": list(self.uncertainty_codes),
            "harvest_suggestion_ids": list(self.harvest_suggestion_ids),
            "confidence_assessment": self.confidence_assessment,
            "requires_human_review": self.requires_human_review,
        }


@dataclass(frozen=True)
class CurationDraftRequest:
    """Parameters that control what the assistant examines."""

    target: str = ""
    target_class: str = ""
    domain: str = ""
    failure_scope: str = ""
    include_harvest: bool = True


@dataclass(frozen=True)
class CurationDraft:
    """The complete assistant output -- a set of draft items plus metadata.

    Human reviewers should treat every item as a suggestion, not an
    instruction.  No item may be merged into the canonical program-memory
    dataset without explicit curator sign-off.
    """

    schema_version: str
    dataset_dir: str
    request: CurationDraftRequest
    items: tuple[CurationDraftItem, ...]
    audit_summary: dict[str, object]

    def __post_init__(self) -> None:
        if self.schema_version != CURATION_ASSISTANT_SCHEMA_VERSION:
            raise ValueError(
                "unsupported curation draft schema_version "
                f"{self.schema_version!r}"
            )
        item_ids: set[str] = set()
        for item in self.items:
            if item.item_id in item_ids:
                raise ValueError(
                    f"duplicate curation draft item_id {item.item_id!r}"
                )
            item_ids.add(item.item_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "dataset_dir": self.dataset_dir,
            "request": {
                "target": self.request.target,
                "target_class": self.request.target_class,
                "domain": self.request.domain,
                "failure_scope": self.request.failure_scope,
                "include_harvest": self.request.include_harvest,
            },
            "item_count": len(self.items),
            "items": [item.to_dict() for item in self.items],
            "audit_summary": self.audit_summary,
            "trust_boundary": (
                "DRAFT ONLY -- every item requires explicit human "
                "adjudication before merge"
            ),
        }


def build_curation_draft(
    dataset_or_path: ProgramMemoryDataset | Path,
    *,
    request: CurationDraftRequest | None = None,
    harvest: ProgramMemoryHarvestBatch | None = None,
) -> CurationDraft:
    """Build a curation draft from the current program-memory substrate.

    The draft inspects coverage gaps, confidence levels, and optionally
    pending harvest suggestions.  It **never** modifies the underlying
    dataset.
    """
    if request is None:
        request = CurationDraftRequest()
    dataset = _coerce_dataset(dataset_or_path)
    dataset_dir = _resolve_dataset_dir(dataset_or_path)
    audit = build_program_memory_coverage_audit(
        dataset,
        require_program_universe=dataset.requires_program_universe,
    )

    items: list[CurationDraftItem] = []
    item_counter = 0

    # Phase 1: gap-driven suggestions
    for gap in _filter_gaps(audit, request):
        item_counter += 1
        items.append(_gap_to_draft_item(gap, audit, item_counter))

    # Phase 2: confidence-upgrade suggestions
    for summary in _filter_summaries(audit, request):
        upgrade_items = _confidence_upgrade_items(
            summary, dataset, item_counter
        )
        for item in upgrade_items:
            item_counter += 1
            items.append(item)

    # Phase 3: harvest-sourced suggestions (if provided)
    if harvest is not None and request.include_harvest:
        for suggestion_item in _harvest_to_draft_items(
            harvest, audit, item_counter, request
        ):
            item_counter += 1
            items.append(suggestion_item)

    audit_summary = _build_audit_summary(audit, request)
    return CurationDraft(
        schema_version=CURATION_ASSISTANT_SCHEMA_VERSION,
        dataset_dir=dataset_dir,
        request=request,
        items=tuple(items),
        audit_summary=audit_summary,
    )


def write_curation_draft(
    path: Path,
    draft: CurationDraft,
) -> None:
    """Persist a curation draft as JSON for human review."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, draft.to_dict())


# ---- internal helpers ----


def _coerce_dataset(
    dataset_or_path: ProgramMemoryDataset | Path,
) -> ProgramMemoryDataset:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return dataset_or_path
    return load_program_memory_dataset(Path(dataset_or_path))


def _resolve_dataset_dir(
    dataset_or_path: ProgramMemoryDataset | Path,
) -> str:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return "<loaded_dataset>"
    from scz_target_engine.program_memory.loaders import (
        resolve_program_memory_v2_dir,
    )

    resolved = resolve_program_memory_v2_dir(Path(dataset_or_path))
    if resolved is not None:
        return str(resolved)
    return str(dataset_or_path)

def _filter_gaps(
    audit: ProgramMemoryCoverageAudit,
    request: CurationDraftRequest,
) -> Sequence[ProgramMemoryCoverageGap]:
    if not _has_scope_filter(request):
        return audit.gaps
    return [gap for gap in audit.gaps if _matches_scope(gap.dimension, gap.scope_value, request)]


def _filter_summaries(
    audit: ProgramMemoryCoverageAudit,
    request: CurationDraftRequest,
) -> Sequence[ProgramMemoryCoverageSummary]:
    if not _has_scope_filter(request):
        return audit.summaries
    return [
        s
        for s in audit.summaries
        if _matches_scope(s.dimension, s.scope_value, request)
    ]


def _has_scope_filter(request: CurationDraftRequest) -> bool:
    return bool(
        clean_text(request.target)
        or clean_text(request.target_class)
        or clean_text(request.domain)
        or clean_text(request.failure_scope)
    )


def _matches_scope(
    dimension: str,
    scope_value: str,
    request: CurationDraftRequest,
) -> bool:
    if dimension == "target" and clean_text(request.target):
        return scope_value.upper() == clean_text(request.target).upper()
    if dimension == "target_class" and clean_text(request.target_class):
        return scope_value.casefold() == clean_text(request.target_class).casefold()
    if dimension == "domain" and clean_text(request.domain):
        return scope_value.casefold() == clean_text(request.domain).casefold()
    if dimension == "failure_scope" and clean_text(request.failure_scope):
        return scope_value.casefold() == clean_text(request.failure_scope).casefold()
    if not _has_scope_filter(request):
        return True
    return False


def _gap_to_draft_item(
    gap: ProgramMemoryCoverageGap,
    audit: ProgramMemoryCoverageAudit,
    counter: int,
) -> CurationDraftItem:
    action = _gap_code_to_action(gap.gap_code)
    summary = _find_summary(audit, gap.dimension, gap.scope_value)
    uncertainty_codes = summary.uncertainty_codes if summary else ()
    return CurationDraftItem(
        item_id=f"draft-gap-{counter}",
        action=action,
        dimension=gap.dimension,
        scope_value=gap.scope_value,
        rationale=gap.explanation,
        provenance_event_ids=gap.related_event_ids,
        provenance_hypothesis_ids=gap.related_hypothesis_ids,
        provenance_gap_codes=(gap.gap_code,),
        uncertainty_codes=uncertainty_codes,
        harvest_suggestion_ids=(),
        confidence_assessment=_gap_confidence_note(gap),
        requires_human_review=True,
    )


def _gap_code_to_action(gap_code: str) -> str:
    mapping = {
        "no_checked_in_event_history": ACTION_ADD_EVENT,
        "single_event_history": ACTION_ADD_EVENT,
        "single_asset_history": ACTION_ADD_EVENT,
        "no_directionality_hypothesis": ACTION_ADD_HYPOTHESIS,
        "unsupported_directionality_hypothesis": ACTION_ADD_SUPPORTING_EVENTS,
        "unresolved_failure_scope": ACTION_RESOLVE_FAILURE_SCOPE,
        "no_high_confidence_event": ACTION_UPGRADE_CONFIDENCE,
        "low_confidence_directionality": ACTION_UPGRADE_CONFIDENCE,
        "scope_not_yet_adjudicated": ACTION_RESOLVE_FAILURE_SCOPE,
        "no_checked_in_history": ACTION_ADD_EVENT,
        "one_sided_outcome_history": ACTION_ADD_EVENT,
        "unresolved_scope_bucket": ACTION_RESOLVE_FAILURE_SCOPE,
    }
    return mapping.get(gap_code, ACTION_FLAG_FOR_REVIEW)


def _gap_confidence_note(gap: ProgramMemoryCoverageGap) -> str:
    if gap.gap_reason_category == "curation_incomplete":
        return (
            "This gap is identifiable from existing checked-in evidence. "
            "A curator should verify whether new source material is needed."
        )
    return (
        "This gap may require additional source evidence. "
        "The assistant cannot confirm provenance for new records."
    )


def _confidence_upgrade_items(
    summary: ProgramMemoryCoverageSummary,
    dataset: ProgramMemoryDataset,
    base_counter: int,
) -> list[CurationDraftItem]:
    items: list[CurationDraftItem] = []
    if (
        summary.dimension == "target"
        and summary.low_confidence_hypothesis_count > 0
    ):
        matching_hypotheses = [
            h
            for h in dataset.directionality_hypotheses
            if h.entity_label.upper() == summary.scope_value.upper()
            and h.confidence == "low"
        ]
        for hypothesis in matching_hypotheses:
            items.append(
                CurationDraftItem(
                    item_id=f"draft-conf-{base_counter + len(items) + 1}",
                    action=ACTION_UPGRADE_CONFIDENCE,
                    dimension="target",
                    scope_value=summary.scope_value,
                    rationale=(
                        f"Directionality hypothesis {hypothesis.hypothesis_id!r} "
                        f"for {summary.scope_value} is currently low-confidence. "
                        "Consider whether additional checked-in evidence or "
                        "source review could support an upgrade."
                    ),
                    provenance_event_ids=hypothesis.supporting_event_ids,
                    provenance_hypothesis_ids=(hypothesis.hypothesis_id,),
                    provenance_gap_codes=(),
                    uncertainty_codes=(
                        summary.uncertainty_codes
                        if summary.uncertainty_codes
                        else ("low_confidence_directionality",)
                    ),
                    harvest_suggestion_ids=(),
                    confidence_assessment=(
                        "Low-confidence hypothesis found. Upgrade requires "
                        "curator-verified additional evidence."
                    ),
                    requires_human_review=True,
                )
            )
    return items


def _harvest_to_draft_items(
    harvest: ProgramMemoryHarvestBatch,
    audit: ProgramMemoryCoverageAudit,
    base_counter: int,
    request: CurationDraftRequest,
) -> list[CurationDraftItem]:
    items: list[CurationDraftItem] = []
    for suggestion in harvest.suggestions:
        action = (
            ACTION_ADD_EVENT
            if suggestion.suggestion_kind == "event"
            else ACTION_ADD_HYPOTHESIS
        )
        scope_value = ""
        dimension = ""
        if suggestion.asset is not None:
            scope_value = " / ".join(suggestion.asset.target_symbols)
            dimension = "target"
        elif suggestion.directionality_hypothesis is not None:
            scope_value = suggestion.directionality_hypothesis.entity_label.upper()
            dimension = "target"

        if _has_scope_filter(request) and not _harvest_matches_scope(
            suggestion, request
        ):
            continue

        items.append(
            CurationDraftItem(
                item_id=f"draft-harvest-{base_counter + len(items) + 1}",
                action=action,
                dimension=dimension,
                scope_value=scope_value,
                rationale=(
                    f"Harvest suggestion {suggestion.suggestion_id!r} "
                    f"(machine confidence: {suggestion.machine_confidence}) "
                    f"from extractor {suggestion.extractor_name!r}. "
                    f"Evidence: {suggestion.evidence_excerpt or 'none provided'}"
                ),
                provenance_event_ids=(),
                provenance_hypothesis_ids=(),
                provenance_gap_codes=(),
                uncertainty_codes=_harvest_uncertainty_codes(suggestion),
                harvest_suggestion_ids=(suggestion.suggestion_id,),
                confidence_assessment=(
                    f"Machine confidence: {suggestion.machine_confidence}. "
                    "Requires curator verification of provenance and accuracy."
                ),
                requires_human_review=True,
            )
        )
    return items


def _harvest_matches_scope(
    suggestion: object,
    request: CurationDraftRequest,
) -> bool:
    """Check whether a harvest suggestion falls within the requested scope."""
    from scz_target_engine.program_memory.extract import ProgramMemorySuggestion

    if not isinstance(suggestion, ProgramMemorySuggestion):
        return False

    target_filter = clean_text(request.target).upper()
    target_class_filter = clean_text(request.target_class).casefold()
    domain_filter = clean_text(request.domain).casefold()

    if target_filter:
        symbols: tuple[str, ...] = ()
        if suggestion.asset is not None:
            symbols = suggestion.asset.target_symbols
        elif suggestion.directionality_hypothesis is not None:
            symbols = (suggestion.directionality_hypothesis.entity_label.upper(),)
        if target_filter not in symbols:
            return False

    if target_class_filter:
        if suggestion.asset is not None:
            if suggestion.asset.target_class.casefold() != target_class_filter:
                return False
        else:
            return False

    if domain_filter:
        if suggestion.event is not None:
            if suggestion.event.domain.casefold() != domain_filter:
                return False
        else:
            return False

    return True


def _harvest_uncertainty_codes(suggestion: object) -> tuple[str, ...]:
    from scz_target_engine.program_memory.extract import ProgramMemorySuggestion

    if not isinstance(suggestion, ProgramMemorySuggestion):
        return ("untyped_suggestion",)
    codes: list[str] = []
    if suggestion.machine_confidence in ("low", "unscored"):
        codes.append("low_machine_confidence")
    if not suggestion.evidence_excerpt:
        codes.append("missing_evidence_excerpt")
    return tuple(codes) if codes else ("pending_review",)


def _find_summary(
    audit: ProgramMemoryCoverageAudit,
    dimension: str,
    scope_value: str,
) -> ProgramMemoryCoverageSummary | None:
    for summary in audit.summaries:
        if summary.dimension == dimension and summary.scope_value == scope_value:
            return summary
    return None


def _build_audit_summary(
    audit: ProgramMemoryCoverageAudit,
    request: CurationDraftRequest,
) -> dict[str, object]:
    scoped = _has_scope_filter(request)
    gaps = (
        [g for g in audit.gaps if _matches_scope(g.dimension, g.scope_value, request)]
        if scoped
        else list(audit.gaps)
    )
    summaries = (
        [s for s in audit.summaries if _matches_scope(s.dimension, s.scope_value, request)]
        if scoped
        else list(audit.summaries)
    )
    gap_reason_counts: dict[str, int] = {}
    for gap in gaps:
        gap_reason_counts[gap.gap_reason_category] = (
            gap_reason_counts.get(gap.gap_reason_category, 0) + 1
        )
    coverage_band_counts: dict[str, int] = {}
    for summary in summaries:
        coverage_band_counts[summary.coverage_band] = (
            coverage_band_counts.get(summary.coverage_band, 0) + 1
        )
    result: dict[str, object] = {
        "asset_count": audit.asset_count,
        "event_count": audit.event_count,
        "directionality_hypothesis_count": audit.directionality_hypothesis_count,
        "total_gap_count": len(gaps),
        "gap_reason_counts": dict(sorted(gap_reason_counts.items())),
        "coverage_band_counts": dict(sorted(coverage_band_counts.items())),
    }
    if scoped:
        result["scoped"] = True
    return result
