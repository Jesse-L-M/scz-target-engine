from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import write_csv, write_json
from scz_target_engine.program_memory._helpers import clean_text, encode_string_list
from scz_target_engine.program_memory.analogs import NONFAILURE_TAXONOMY
from scz_target_engine.program_memory.counterfactuals import FAILURE_SCOPE_BY_TAXONOMY
from scz_target_engine.program_memory.loaders import (
    load_program_memory_dataset,
    resolve_program_memory_v2_dir,
)
from scz_target_engine.program_memory.models import (
    ProgramMemoryAsset,
    ProgramMemoryDataset,
    ProgramMemoryDirectionalityHypothesis,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
    ProgramMemoryUniverseRow,
)
from scz_target_engine.sources.clinicaltrials import (
    build_clinicaltrials_study_url,
    normalize_nct_id,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


DIMENSION_ORDER = ("target", "target_class", "domain", "failure_scope")
FAILURE_SCOPE_ORDER = (
    "nonfailure",
    "unresolved",
    "molecule",
    "target_class",
    "target",
    "population",
    "endpoint",
)

PROGRAM_MEMORY_COVERAGE_DENOMINATOR_SUMMARY_FIELDNAMES = [
    "stage_bucket",
    "modality",
    "domain",
    "coverage_state",
    "coverage_reason",
    "program_count",
    "mapped_event_count",
    "program_universe_ids_json",
]

PROGRAM_MEMORY_COVERAGE_DENOMINATOR_GAP_FIELDNAMES = [
    "program_universe_id",
    "asset_id",
    "asset_name",
    "asset_lineage_id",
    "target",
    "target_class",
    "target_class_lineage_id",
    "modality",
    "domain",
    "population",
    "regimen",
    "stage_bucket",
    "coverage_state",
    "coverage_reason",
    "coverage_confidence",
    "mapped_event_ids_json",
    "duplicate_of_program_universe_id",
    "discovery_source_type",
    "discovery_source_id",
    "source_candidate_url",
    "notes",
]

PROGRAM_MEMORY_COVERAGE_SCOPE_SUMMARY_FIELDNAMES = [
    "dimension",
    "scope_value",
    "coverage_band",
    "event_count",
    "asset_count",
    "failure_event_count",
    "nonfailure_event_count",
    "unresolved_event_count",
    "high_confidence_event_count",
    "medium_confidence_event_count",
    "low_confidence_event_count",
    "directionality_hypothesis_count",
    "supported_directionality_hypothesis_count",
    "low_confidence_hypothesis_count",
    "gap_codes_json",
    "uncertainty_codes_json",
    "explanation",
]

PROGRAM_MEMORY_COVERAGE_SCOPE_GAP_FIELDNAMES = [
    "dimension",
    "scope_value",
    "gap_code",
    "gap_reason_category",
    "related_event_ids_json",
    "related_hypothesis_ids_json",
    "explanation",
]

PROGRAM_MEMORY_COVERAGE_SUMMARY_FIELDNAMES = (
    PROGRAM_MEMORY_COVERAGE_SCOPE_SUMMARY_FIELDNAMES
)
PROGRAM_MEMORY_COVERAGE_GAP_FIELDNAMES = (
    PROGRAM_MEMORY_COVERAGE_SCOPE_GAP_FIELDNAMES
)

PROGRAM_MEMORY_COVERAGE_EVIDENCE_FIELDNAMES = [
    "dimension",
    "scope_value",
    "record_kind",
    "record_id",
    "event_id",
    "hypothesis_id",
    "relation",
    "asset_id",
    "asset_lineage_id",
    "molecule",
    "sponsor",
    "target",
    "target_symbol",
    "target_class",
    "target_class_lineage_id",
    "mechanism",
    "modality",
    "domain",
    "population",
    "mono_or_adjunct",
    "phase",
    "event_type",
    "event_date",
    "failure_reason_taxonomy",
    "failure_scope",
    "confidence",
    "source_tiers_json",
    "source_urls_json",
    "supporting_event_ids_json",
    "notes",
]

PROGRAM_MEMORY_COVERAGE_MANIFEST_SCHEMA_VERSION = "program-memory-coverage-manifest-v1"
PROGRAM_MEMORY_COVERAGE_STATES = (
    "included",
    "unresolved",
    "excluded",
    "duplicate",
    "out_of_scope",
)
PROGRAM_MEMORY_COVERAGE_REASONS = {
    "included": ("checked_in_event_history",),
    "unresolved": (
        "ctgov_candidate_pending_adjudication",
        "needs_direct_source_recovery",
        "needs_alias_resolution",
    ),
    "excluded": (
        "follow_on_supporting_study",
        "insufficient_interventional_signal",
    ),
    "duplicate": (
        "asset_alias_duplicate",
        "registry_alias_duplicate",
    ),
    "out_of_scope": (
        "non_schizophrenia_indication",
        "non_molecular_intervention",
    ),
}
PROGRAM_MEMORY_COVERAGE_CONFIDENCE_LEVELS = ("high", "medium", "low")
PROGRAM_MEMORY_EVENT_STAGE_BUCKETS = {
    "phase_2": "phase_2",
    "phase_3": "phase_3_or_registration",
    "registration": "phase_3_or_registration",
    "approved": "approved",
}
PROGRAM_MEMORY_STAGE_BUCKET_RANKS = {
    "phase_2": 1,
    "phase_3_or_registration": 2,
    "approved": 3,
    "postapproval_supporting": 4,
}
PROGRAM_MEMORY_ALLOWED_STAGE_BUCKETS = tuple(PROGRAM_MEMORY_STAGE_BUCKET_RANKS)


@dataclass(frozen=True)
class ProgramMemoryCoverageEvidence:
    dimension: str
    scope_value: str
    record_kind: str
    record_id: str
    relation: str
    asset_id: str = ""
    asset_lineage_id: str = ""
    molecule: str = ""
    sponsor: str = ""
    target: str = ""
    target_symbol: str = ""
    target_class: str = ""
    target_class_lineage_id: str = ""
    mechanism: str = ""
    modality: str = ""
    domain: str = ""
    population: str = ""
    mono_or_adjunct: str = ""
    phase: str = ""
    event_type: str = ""
    event_date: str = ""
    event_id: str = ""
    hypothesis_id: str = ""
    failure_reason_taxonomy: str = ""
    failure_scope: str = ""
    confidence: str = ""
    source_tiers: tuple[str, ...] = ()
    source_urls: tuple[str, ...] = ()
    supporting_event_ids: tuple[str, ...] = ()
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "dimension": self.dimension,
            "scope_value": self.scope_value,
            "record_kind": self.record_kind,
            "record_id": self.record_id,
            "event_id": self.event_id,
            "hypothesis_id": self.hypothesis_id,
            "relation": self.relation,
            "asset_id": self.asset_id,
            "asset_lineage_id": self.asset_lineage_id,
            "molecule": self.molecule,
            "sponsor": self.sponsor,
            "target": self.target,
            "target_symbol": self.target_symbol,
            "target_class": self.target_class,
            "target_class_lineage_id": self.target_class_lineage_id,
            "mechanism": self.mechanism,
            "modality": self.modality,
            "domain": self.domain,
            "population": self.population,
            "mono_or_adjunct": self.mono_or_adjunct,
            "phase": self.phase,
            "event_type": self.event_type,
            "event_date": self.event_date,
            "failure_reason_taxonomy": self.failure_reason_taxonomy,
            "failure_scope": self.failure_scope,
            "confidence": self.confidence,
            "source_tiers": list(self.source_tiers),
            "source_urls": list(self.source_urls),
            "supporting_event_ids": list(self.supporting_event_ids),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ProgramMemoryCoverageGap:
    dimension: str
    scope_value: str
    gap_code: str
    gap_reason_category: str
    explanation: str
    related_event_ids: tuple[str, ...]
    related_hypothesis_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "dimension": self.dimension,
            "scope_value": self.scope_value,
            "gap_code": self.gap_code,
            "gap_reason_category": self.gap_reason_category,
            "explanation": self.explanation,
            "related_event_ids": list(self.related_event_ids),
            "related_hypothesis_ids": list(self.related_hypothesis_ids),
        }


@dataclass(frozen=True)
class ProgramMemoryCoverageSummary:
    dimension: str
    scope_value: str
    coverage_band: str
    event_count: int
    asset_count: int
    failure_event_count: int
    nonfailure_event_count: int
    unresolved_event_count: int
    high_confidence_event_count: int
    medium_confidence_event_count: int
    low_confidence_event_count: int
    directionality_hypothesis_count: int
    supported_directionality_hypothesis_count: int
    low_confidence_hypothesis_count: int
    gap_codes: tuple[str, ...]
    uncertainty_codes: tuple[str, ...]
    explanation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "dimension": self.dimension,
            "scope_value": self.scope_value,
            "coverage_band": self.coverage_band,
            "event_count": self.event_count,
            "asset_count": self.asset_count,
            "failure_event_count": self.failure_event_count,
            "nonfailure_event_count": self.nonfailure_event_count,
            "unresolved_event_count": self.unresolved_event_count,
            "high_confidence_event_count": self.high_confidence_event_count,
            "medium_confidence_event_count": self.medium_confidence_event_count,
            "low_confidence_event_count": self.low_confidence_event_count,
            "directionality_hypothesis_count": self.directionality_hypothesis_count,
            "supported_directionality_hypothesis_count": (
                self.supported_directionality_hypothesis_count
            ),
            "low_confidence_hypothesis_count": self.low_confidence_hypothesis_count,
            "gap_codes": list(self.gap_codes),
            "uncertainty_codes": list(self.uncertainty_codes),
            "explanation": self.explanation,
        }


@dataclass(frozen=True)
class ProgramMemoryCoverageFocusReport:
    request: dict[str, str]
    matched_summaries: tuple[ProgramMemoryCoverageSummary, ...]
    matched_gaps: tuple[ProgramMemoryCoverageGap, ...]
    matched_evidence: tuple[ProgramMemoryCoverageEvidence, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "request": dict(self.request),
            "matched_summaries": [summary.to_dict() for summary in self.matched_summaries],
            "matched_gaps": [gap.to_dict() for gap in self.matched_gaps],
            "matched_evidence": [evidence.to_dict() for evidence in self.matched_evidence],
        }


@dataclass(frozen=True)
class ProgramMemoryCoverageAudit:
    dataset_dir: str
    asset_count: int
    event_count: int
    directionality_hypothesis_count: int
    coverage_manifest: dict[str, object]
    denominator_summary_rows: tuple[dict[str, object], ...]
    denominator_gap_rows: tuple[dict[str, object], ...]
    summaries: tuple[ProgramMemoryCoverageSummary, ...]
    gaps: tuple[ProgramMemoryCoverageGap, ...]
    evidence_rows: tuple[ProgramMemoryCoverageEvidence, ...]

    def to_dict(self) -> dict[str, object]:
        coverage_summary_rows = materialize_program_memory_coverage_summary_rows(self)
        coverage_gap_rows = materialize_program_memory_coverage_gap_rows(self)
        denominator_summary_rows = (
            materialize_program_memory_coverage_denominator_summary_rows(self)
        )
        denominator_gap_rows = (
            materialize_program_memory_coverage_denominator_gap_rows(self)
        )
        grouped_summaries = {
            dimension: [
                summary.to_dict()
                for summary in self.summaries
                if summary.dimension == dimension
            ]
            for dimension in DIMENSION_ORDER
        }
        coverage_band_counts: dict[str, int] = defaultdict(int)
        for summary in self.summaries:
            coverage_band_counts[summary.coverage_band] += 1
        gap_reason_counts: dict[str, int] = defaultdict(int)
        for gap in self.gaps:
            gap_reason_counts[gap.gap_reason_category] += 1
        return {
            "dataset_dir": self.dataset_dir,
            "asset_count": self.asset_count,
            "event_count": self.event_count,
            "directionality_hypothesis_count": self.directionality_hypothesis_count,
            "coverage_manifest": dict(self.coverage_manifest),
            "coverage_band_counts": dict(sorted(coverage_band_counts.items())),
            "gap_reason_counts": dict(sorted(gap_reason_counts.items())),
            "coverage_by_dimension": grouped_summaries,
            "coverage_summary_rows": coverage_summary_rows,
            "coverage_gap_rows": coverage_gap_rows,
            "denominator_summary_rows": denominator_summary_rows,
            "denominator_gap_rows": denominator_gap_rows,
            "gap_rows": [gap.to_dict() for gap in self.gaps],
            "scope_summary_rows": coverage_summary_rows,
            "scope_gap_rows": coverage_gap_rows,
            "evidence_rows": [evidence.to_dict() for evidence in self.evidence_rows],
        }


@dataclass(frozen=True)
class _EventContext:
    asset: ProgramMemoryAsset
    event: ProgramMemoryEvent
    provenance: ProgramMemoryProvenance


def build_program_memory_coverage_audit(
    dataset_or_path: ProgramMemoryDataset | Path,
    *,
    require_program_universe: bool = False,
) -> ProgramMemoryCoverageAudit:
    dataset = _coerce_dataset(dataset_or_path)
    require_program_universe = require_program_universe or dataset.requires_program_universe
    dataset_dir = _resolve_dataset_dir(dataset_or_path)
    assets_by_id = {asset.asset_id: asset for asset in dataset.assets}
    provenances_by_event_id = {
        provenance.event_id: provenance for provenance in dataset.provenances
    }
    ordered_event_contexts = [
        _EventContext(
            asset=assets_by_id[event.asset_id],
            event=event,
            provenance=provenances_by_event_id[event.event_id],
        )
        for event in sorted(
            dataset.events,
            key=lambda item: (item.sort_order, item.event_date, item.event_id),
        )
    ]
    event_contexts_by_id = {
        context.event.event_id: context for context in ordered_event_contexts
    }
    ordered_hypotheses = list(
        sorted(
            dataset.directionality_hypotheses,
            key=lambda item: (item.sort_order, item.entity_label.lower(), item.hypothesis_id),
        )
    )

    target_events: dict[str, list[_EventContext]] = defaultdict(list)
    target_class_events: dict[str, list[_EventContext]] = defaultdict(list)
    domain_events: dict[str, list[_EventContext]] = defaultdict(list)
    failure_scope_events: dict[str, list[_EventContext]] = defaultdict(list)
    target_hypotheses: dict[str, list[ProgramMemoryDirectionalityHypothesis]] = defaultdict(
        list
    )

    evidence_rows: list[ProgramMemoryCoverageEvidence] = []
    for context in ordered_event_contexts:
        failure_scope = _map_failure_scope(context.event.failure_reason_taxonomy)
        for target_symbol in context.asset.target_symbols:
            target_events[target_symbol].append(context)
            evidence_rows.append(
                _event_evidence_row(
                    dimension="target",
                    scope_value=target_symbol,
                    relation="target_symbol_event",
                    context=context,
                    target_symbol=target_symbol,
                    failure_scope=failure_scope,
                )
            )
        target_class_events[context.asset.target_class].append(context)
        evidence_rows.append(
            _event_evidence_row(
                dimension="target_class",
                scope_value=context.asset.target_class,
                relation="target_class_event",
                context=context,
                target_symbol="",
                failure_scope=failure_scope,
            )
        )
        domain_events[context.event.domain].append(context)
        evidence_rows.append(
            _event_evidence_row(
                dimension="domain",
                scope_value=context.event.domain,
                relation="domain_event",
                context=context,
                target_symbol="",
                failure_scope=failure_scope,
            )
        )
        failure_scope_events[failure_scope].append(context)
        evidence_rows.append(
            _event_evidence_row(
                dimension="failure_scope",
                scope_value=failure_scope,
                relation="failure_scope_event",
                context=context,
                target_symbol="",
                failure_scope=failure_scope,
            )
        )

    for hypothesis in ordered_hypotheses:
        scope_value = hypothesis.entity_label.upper()
        target_hypotheses[scope_value].append(hypothesis)
        evidence_rows.append(
            _hypothesis_evidence_row(
                hypothesis=hypothesis,
                scope_value=scope_value,
                provenances_by_event_id=provenances_by_event_id,
            )
        )

    summaries: list[ProgramMemoryCoverageSummary] = []
    gaps: list[ProgramMemoryCoverageGap] = []
    total_failure_event_count = sum(
        1
        for context in ordered_event_contexts
        if context.event.failure_reason_taxonomy != NONFAILURE_TAXONOMY
    )

    target_values = sorted(set(target_events) | set(target_hypotheses))
    for scope_value in target_values:
        summary, summary_gaps = _build_summary_for_scope(
            dimension="target",
            scope_value=scope_value,
            events=target_events.get(scope_value, []),
            hypotheses=target_hypotheses.get(scope_value, []),
            total_failure_event_count=total_failure_event_count,
        )
        summaries.append(summary)
        gaps.extend(summary_gaps)

    for scope_value in sorted(target_class_events):
        summary, summary_gaps = _build_summary_for_scope(
            dimension="target_class",
            scope_value=scope_value,
            events=target_class_events[scope_value],
            hypotheses=[],
            total_failure_event_count=total_failure_event_count,
        )
        summaries.append(summary)
        gaps.extend(summary_gaps)

    for scope_value in sorted(domain_events):
        summary, summary_gaps = _build_summary_for_scope(
            dimension="domain",
            scope_value=scope_value,
            events=domain_events[scope_value],
            hypotheses=[],
            total_failure_event_count=total_failure_event_count,
        )
        summaries.append(summary)
        gaps.extend(summary_gaps)

    available_failure_scopes = set(failure_scope_events)
    for scope_value in FAILURE_SCOPE_ORDER:
        if scope_value not in available_failure_scopes and scope_value != "nonfailure":
            failure_scope_events.setdefault(scope_value, [])
    if "nonfailure" not in available_failure_scopes:
        failure_scope_events.setdefault("nonfailure", [])
    for scope_value in FAILURE_SCOPE_ORDER:
        summary, summary_gaps = _build_summary_for_scope(
            dimension="failure_scope",
            scope_value=scope_value,
            events=failure_scope_events.get(scope_value, []),
            hypotheses=[],
            total_failure_event_count=total_failure_event_count,
        )
        summaries.append(summary)
        gaps.extend(summary_gaps)

    ordered_summaries = sorted(summaries, key=_summary_sort_key)
    ordered_gaps = sorted(gaps, key=_gap_sort_key)
    ordered_evidence_rows = sorted(evidence_rows, key=_evidence_sort_key)
    ordered_program_universe_rows = (
        _validate_and_order_program_universe(
            dataset=dataset,
            dataset_dir=dataset_dir,
            event_contexts_by_id=event_contexts_by_id,
        )
        if dataset.program_universe_rows or require_program_universe
        else ()
    )
    denominator_summary_rows = tuple(
        _build_denominator_summary_rows(ordered_program_universe_rows)
    )
    denominator_gap_rows = tuple(_build_denominator_gap_rows(ordered_program_universe_rows))
    return ProgramMemoryCoverageAudit(
        dataset_dir=dataset_dir,
        asset_count=len(dataset.assets),
        event_count=len(dataset.events),
        directionality_hypothesis_count=len(dataset.directionality_hypotheses),
        coverage_manifest=_build_coverage_manifest(
            dataset_dir=dataset_dir,
            program_universe_rows=ordered_program_universe_rows,
            event_count=len(dataset.events),
        ),
        denominator_summary_rows=denominator_summary_rows,
        denominator_gap_rows=denominator_gap_rows,
        summaries=tuple(ordered_summaries),
        gaps=tuple(ordered_gaps),
        evidence_rows=tuple(ordered_evidence_rows),
    )


def build_program_memory_coverage_focus_report(
    audit: ProgramMemoryCoverageAudit,
    *,
    target: str = "",
    target_class: str = "",
    domain: str = "",
    failure_scope: str = "",
) -> ProgramMemoryCoverageFocusReport:
    requested: dict[str, str] = {}
    requested_pairs: set[tuple[str, str]] = set()
    if target:
        normalized = target.upper()
        requested["target"] = normalized
        requested_pairs.add(("target", normalized))
    if target_class:
        requested["target_class"] = target_class
        requested_pairs.add(("target_class", target_class))
    if domain:
        requested["domain"] = domain
        requested_pairs.add(("domain", domain))
    if failure_scope:
        requested["failure_scope"] = failure_scope
        requested_pairs.add(("failure_scope", failure_scope))

    if not requested_pairs:
        return ProgramMemoryCoverageFocusReport(
            request={},
            matched_summaries=(),
            matched_gaps=(),
            matched_evidence=(),
        )

    matched_summaries = tuple(
        summary
        for summary in audit.summaries
        if (summary.dimension, summary.scope_value) in requested_pairs
    )
    matched_gaps = tuple(
        gap
        for gap in audit.gaps
        if (gap.dimension, gap.scope_value) in requested_pairs
    )
    matched_evidence = tuple(
        evidence
        for evidence in audit.evidence_rows
        if (evidence.dimension, evidence.scope_value) in requested_pairs
    )
    return ProgramMemoryCoverageFocusReport(
        request=requested,
        matched_summaries=matched_summaries,
        matched_gaps=matched_gaps,
        matched_evidence=matched_evidence,
    )


def materialize_program_memory_coverage_denominator_summary_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return [dict(row) for row in audit.denominator_summary_rows]


def materialize_program_memory_coverage_denominator_gap_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return [dict(row) for row in audit.denominator_gap_rows]


def materialize_program_memory_coverage_summary_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return [
        {
            "dimension": summary.dimension,
            "scope_value": summary.scope_value,
            "coverage_band": summary.coverage_band,
            "event_count": summary.event_count,
            "asset_count": summary.asset_count,
            "failure_event_count": summary.failure_event_count,
            "nonfailure_event_count": summary.nonfailure_event_count,
            "unresolved_event_count": summary.unresolved_event_count,
            "high_confidence_event_count": summary.high_confidence_event_count,
            "medium_confidence_event_count": summary.medium_confidence_event_count,
            "low_confidence_event_count": summary.low_confidence_event_count,
            "directionality_hypothesis_count": summary.directionality_hypothesis_count,
            "supported_directionality_hypothesis_count": (
                summary.supported_directionality_hypothesis_count
            ),
            "low_confidence_hypothesis_count": summary.low_confidence_hypothesis_count,
            "gap_codes_json": encode_string_list(summary.gap_codes),
            "uncertainty_codes_json": encode_string_list(summary.uncertainty_codes),
            "explanation": summary.explanation,
        }
        for summary in audit.summaries
    ]


def materialize_program_memory_coverage_scope_summary_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return materialize_program_memory_coverage_summary_rows(audit)


def materialize_program_memory_coverage_gap_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return [
        {
            "dimension": gap.dimension,
            "scope_value": gap.scope_value,
            "gap_code": gap.gap_code,
            "gap_reason_category": gap.gap_reason_category,
            "related_event_ids_json": encode_string_list(gap.related_event_ids),
            "related_hypothesis_ids_json": encode_string_list(
                gap.related_hypothesis_ids
            ),
            "explanation": gap.explanation,
        }
        for gap in audit.gaps
    ]


def materialize_program_memory_coverage_scope_gap_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return materialize_program_memory_coverage_gap_rows(audit)


def materialize_program_memory_coverage_evidence_rows(
    audit: ProgramMemoryCoverageAudit,
) -> list[dict[str, object]]:
    return [
        {
            "dimension": evidence.dimension,
            "scope_value": evidence.scope_value,
            "record_kind": evidence.record_kind,
            "record_id": evidence.record_id,
            "event_id": evidence.event_id,
            "hypothesis_id": evidence.hypothesis_id,
            "relation": evidence.relation,
            "asset_id": evidence.asset_id,
            "asset_lineage_id": evidence.asset_lineage_id,
            "molecule": evidence.molecule,
            "sponsor": evidence.sponsor,
            "target": evidence.target,
            "target_symbol": evidence.target_symbol,
            "target_class": evidence.target_class,
            "target_class_lineage_id": evidence.target_class_lineage_id,
            "mechanism": evidence.mechanism,
            "modality": evidence.modality,
            "domain": evidence.domain,
            "population": evidence.population,
            "mono_or_adjunct": evidence.mono_or_adjunct,
            "phase": evidence.phase,
            "event_type": evidence.event_type,
            "event_date": evidence.event_date,
            "failure_reason_taxonomy": evidence.failure_reason_taxonomy,
            "failure_scope": evidence.failure_scope,
            "confidence": evidence.confidence,
            "source_tiers_json": encode_string_list(evidence.source_tiers),
            "source_urls_json": encode_string_list(evidence.source_urls),
            "supporting_event_ids_json": encode_string_list(
                evidence.supporting_event_ids
            ),
            "notes": evidence.notes,
        }
        for evidence in audit.evidence_rows
    ]


def write_program_memory_coverage_outputs(
    output_dir: Path,
    audit: ProgramMemoryCoverageAudit,
    *,
    target: str = "",
    target_class: str = "",
    domain: str = "",
    failure_scope: str = "",
) -> ProgramMemoryCoverageFocusReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    focus_path = output_dir / "coverage_focus.json"
    write_json(output_dir / "coverage_audit.json", audit.to_dict())
    write_json(output_dir / "coverage_manifest.json", audit.coverage_manifest)
    write_csv(
        output_dir / "coverage_summary.csv",
        materialize_program_memory_coverage_summary_rows(audit),
        PROGRAM_MEMORY_COVERAGE_SUMMARY_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_gaps.csv",
        materialize_program_memory_coverage_gap_rows(audit),
        PROGRAM_MEMORY_COVERAGE_GAP_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_denominator_summary.csv",
        materialize_program_memory_coverage_denominator_summary_rows(audit),
        PROGRAM_MEMORY_COVERAGE_DENOMINATOR_SUMMARY_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_denominator_gaps.csv",
        materialize_program_memory_coverage_denominator_gap_rows(audit),
        PROGRAM_MEMORY_COVERAGE_DENOMINATOR_GAP_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_scope_summary.csv",
        materialize_program_memory_coverage_scope_summary_rows(audit),
        PROGRAM_MEMORY_COVERAGE_SCOPE_SUMMARY_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_scope_gaps.csv",
        materialize_program_memory_coverage_scope_gap_rows(audit),
        PROGRAM_MEMORY_COVERAGE_SCOPE_GAP_FIELDNAMES,
    )
    write_csv(
        output_dir / "coverage_evidence.csv",
        materialize_program_memory_coverage_evidence_rows(audit),
        PROGRAM_MEMORY_COVERAGE_EVIDENCE_FIELDNAMES,
    )
    focus_report = build_program_memory_coverage_focus_report(
        audit,
        target=target,
        target_class=target_class,
        domain=domain,
        failure_scope=failure_scope,
    )
    if focus_report.request:
        write_json(focus_path, focus_report.to_dict())
    elif focus_path.exists():
        focus_path.unlink()
    return focus_report


def _validate_and_order_program_universe(
    *,
    dataset: ProgramMemoryDataset,
    dataset_dir: str,
    event_contexts_by_id: dict[str, _EventContext],
) -> tuple[ProgramMemoryUniverseRow, ...]:
    if not dataset.program_universe_rows:
        raise ValueError(
            "program_universe.csv is required for denominator coverage-audit"
            + (f": {dataset_dir}" if dataset_dir else "")
        )

    event_ids = {event.event_id for event in dataset.events}
    rows_by_id: dict[str, ProgramMemoryUniverseRow] = {}
    grain_owner_by_key: dict[tuple[str, ...], str] = {}

    for row in dataset.program_universe_rows:
        _validate_program_universe_row(
            row,
            event_ids,
            event_contexts_by_id=event_contexts_by_id,
        )

    ordered_rows = tuple(
        sorted(dataset.program_universe_rows, key=_program_universe_sort_key)
    )

    for row in ordered_rows:
        rows_by_id[row.program_universe_id] = row
        if row.coverage_state == "duplicate":
            continue
        grain_key = _program_opportunity_key(row)
        existing = grain_owner_by_key.get(grain_key)
        if existing is not None:
            raise ValueError(
                "program universe duplicates require explicit duplicate coverage_state: "
                f"{row.program_universe_id!r} collides with {existing!r}"
            )
        grain_owner_by_key[grain_key] = row.program_universe_id

    for row in ordered_rows:
        if row.coverage_state != "duplicate":
            continue
        canonical_row = rows_by_id.get(row.duplicate_of_program_universe_id)
        if canonical_row is None:
            raise ValueError(
                "program universe duplicate rows must reference an existing canonical row: "
                f"{row.program_universe_id!r} -> {row.duplicate_of_program_universe_id!r}"
            )
        if canonical_row.coverage_state == "duplicate":
            raise ValueError(
                "program universe duplicates cannot point to another duplicate row: "
                f"{row.program_universe_id!r} -> {canonical_row.program_universe_id!r}"
            )
        if _program_opportunity_key(row) != _program_opportunity_key(canonical_row):
            raise ValueError(
                "program universe duplicate rows must preserve the canonical program-opportunity grain: "
                f"{row.program_universe_id!r}"
            )

    return ordered_rows


def _validate_program_universe_row(
    row: ProgramMemoryUniverseRow,
    event_ids: set[str],
    *,
    event_contexts_by_id: dict[str, _EventContext],
) -> None:
    if row.coverage_state not in PROGRAM_MEMORY_COVERAGE_STATES:
        raise ValueError(
            f"unsupported program universe coverage_state {row.coverage_state!r}"
        )
    allowed_reasons = PROGRAM_MEMORY_COVERAGE_REASONS[row.coverage_state]
    if row.coverage_reason not in allowed_reasons:
        raise ValueError(
            "unsupported coverage_reason for program universe row "
            f"{row.program_universe_id!r}: {row.coverage_state!r} / {row.coverage_reason!r}"
        )
    if row.coverage_confidence not in PROGRAM_MEMORY_COVERAGE_CONFIDENCE_LEVELS:
        raise ValueError(
            "unsupported coverage_confidence for program universe row "
            f"{row.program_universe_id!r}: {row.coverage_confidence!r}"
        )
    if row.stage_bucket not in PROGRAM_MEMORY_ALLOWED_STAGE_BUCKETS:
        raise ValueError(
            "unsupported stage_bucket for program universe row "
            f"{row.program_universe_id!r}: {row.stage_bucket!r}"
        )
    required_fields = {
        "program_universe_id": row.program_universe_id,
        "asset_name": row.asset_name,
        "asset_lineage_id": row.asset_lineage_id,
        "target_class": row.target_class,
        "target_class_lineage_id": row.target_class_lineage_id,
        "modality": row.modality,
        "domain": row.domain,
        "population": row.population,
        "regimen": row.regimen,
        "stage_bucket": row.stage_bucket,
        "discovery_source_type": row.discovery_source_type,
    }
    missing = [field_name for field_name, value in required_fields.items() if not value]
    if missing:
        raise ValueError(
            f"program universe row {row.program_universe_id!r} is missing required fields {missing}"
        )

    if row.discovery_source_type == "clinicaltrials_gov":
        study_id = normalize_nct_id(row.discovery_source_id)
        expected_url = build_clinicaltrials_study_url(study_id)
        if row.source_candidate_url != expected_url:
            raise ValueError(
                "ClinicalTrials.gov program universe rows must use the canonical study URL: "
                f"{row.program_universe_id!r}"
            )
    elif not row.source_candidate_url:
        raise ValueError(
            f"program universe row {row.program_universe_id!r} requires source_candidate_url"
        )

    duplicate_event_ids = _find_duplicate_string_values(row.mapped_event_ids)
    if duplicate_event_ids:
        raise ValueError(
            "program universe row repeats mapped_event_ids "
            f"{duplicate_event_ids!r} for {row.program_universe_id!r}"
        )
    unknown_event_ids = [
        event_id for event_id in row.mapped_event_ids if event_id not in event_ids
    ]
    if unknown_event_ids:
        raise ValueError(
            "program universe row references unknown mapped_event_ids "
            f"{unknown_event_ids} for {row.program_universe_id!r}"
        )

    if row.coverage_state == "included":
        if not row.mapped_event_ids:
            raise ValueError(
                f"included program universe row {row.program_universe_id!r} must map to checked-in event_ids"
            )
        if row.duplicate_of_program_universe_id:
            raise ValueError(
                f"included program universe row {row.program_universe_id!r} cannot set duplicate_of_program_universe_id"
            )
        _validate_included_program_universe_mapped_events(
            row,
            tuple(event_contexts_by_id[event_id] for event_id in row.mapped_event_ids),
        )
        return

    if row.mapped_event_ids:
        raise ValueError(
            f"non-included program universe row {row.program_universe_id!r} cannot carry mapped_event_ids"
        )
    if row.coverage_state == "duplicate":
        if not row.duplicate_of_program_universe_id:
            raise ValueError(
                f"duplicate program universe row {row.program_universe_id!r} must set duplicate_of_program_universe_id"
            )
        return
    if row.duplicate_of_program_universe_id:
        raise ValueError(
            f"non-duplicate program universe row {row.program_universe_id!r} cannot set duplicate_of_program_universe_id"
        )


def _validate_included_program_universe_mapped_events(
    row: ProgramMemoryUniverseRow,
    mapped_event_contexts: tuple[_EventContext, ...],
) -> None:
    mismatch_details: list[str] = []
    has_stage_aligned_event = False
    expected_stage_bucket = clean_text(row.stage_bucket)
    for context in mapped_event_contexts:
        mismatch_reasons = _program_universe_mismatch_reasons(row, context)
        if mismatch_reasons:
            mismatch_details.append(
                f"{context.event.event_id!r} ({', '.join(mismatch_reasons)})"
            )
        if _event_stage_bucket(context.event.phase) == expected_stage_bucket:
            has_stage_aligned_event = True

    if mismatch_details:
        raise ValueError(
            "included program universe row "
            f"{row.program_universe_id!r} has mapped_event_ids that do not match "
            "the canonical program-opportunity grain: "
            f"{'; '.join(mismatch_details)}"
        )
    if not has_stage_aligned_event:
        raise ValueError(
            "included program universe row "
            f"{row.program_universe_id!r} must include at least one mapped event "
            f"aligned to stage_bucket {row.stage_bucket!r}"
        )


def _program_universe_mismatch_reasons(
    row: ProgramMemoryUniverseRow,
    context: _EventContext,
) -> list[str]:
    mismatch_reasons: list[str] = []
    comparable_fields = (
        ("asset_lineage_id", row.asset_lineage_id, context.asset.asset_lineage_id),
        (
            "target_class_lineage_id",
            row.target_class_lineage_id,
            context.asset.target_class_lineage_id,
        ),
        ("modality", row.modality, context.asset.modality),
        ("domain", row.domain, context.event.domain),
        ("population", row.population, context.event.population),
        ("regimen", row.regimen, context.event.mono_or_adjunct),
    )
    for field_name, row_value, event_value in comparable_fields:
        if _normalize_program_memory_text(row_value) != _normalize_program_memory_text(
            event_value
        ):
            mismatch_reasons.append(field_name)

    event_stage_bucket = _event_stage_bucket(context.event.phase)
    if _stage_bucket_rank(event_stage_bucket) > _stage_bucket_rank(row.stage_bucket):
        mismatch_reasons.append("stage_bucket")
    return mismatch_reasons


def _normalize_program_memory_text(value: str) -> str:
    return clean_text(value).casefold()


def _event_stage_bucket(phase: str) -> str:
    cleaned_phase = clean_text(phase)
    return PROGRAM_MEMORY_EVENT_STAGE_BUCKETS.get(cleaned_phase, cleaned_phase)


def _stage_bucket_rank(stage_bucket: str) -> int:
    return PROGRAM_MEMORY_STAGE_BUCKET_RANKS.get(clean_text(stage_bucket), 0)


def _find_duplicate_string_values(values: tuple[str, ...]) -> list[str]:
    duplicates: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
            continue
        seen.add(value)
    return duplicates


def _build_coverage_manifest(
    *,
    dataset_dir: str,
    program_universe_rows: tuple[ProgramMemoryUniverseRow, ...],
    event_count: int,
) -> dict[str, object]:
    coverage_state_counts: dict[str, int] = defaultdict(int)
    coverage_reason_counts: dict[str, int] = defaultdict(int)
    discovery_source_counts: dict[str, int] = defaultdict(int)
    for row in program_universe_rows:
        coverage_state_counts[row.coverage_state] += 1
        coverage_reason_counts[row.coverage_reason] += 1
        discovery_source_counts[row.discovery_source_type] += 1

    unique_in_scope_programs = {
        _program_opportunity_key(row)
        for row in program_universe_rows
        if row.coverage_state in {"included", "unresolved"}
    }
    mapped_event_count = sum(
        len(row.mapped_event_ids)
        for row in program_universe_rows
        if row.coverage_state == "included"
    )
    return {
        "schema_version": PROGRAM_MEMORY_COVERAGE_MANIFEST_SCHEMA_VERSION,
        "dataset_dir": dataset_dir,
        "row_grain": (
            "one row per program opportunity keyed by asset_lineage_id, "
            "target_class_lineage_id, modality, domain, population, regimen, and stage_bucket"
        ),
        "program_universe_row_count": len(program_universe_rows),
        "unique_in_scope_program_count": len(unique_in_scope_programs),
        "checked_in_event_count": event_count,
        "mapped_event_count": mapped_event_count,
        "coverage_state_counts": dict(sorted(coverage_state_counts.items())),
        "coverage_reason_counts": dict(sorted(coverage_reason_counts.items())),
        "discovery_source_type_counts": dict(sorted(discovery_source_counts.items())),
        "allowed_coverage_states": list(PROGRAM_MEMORY_COVERAGE_STATES),
        "allowed_coverage_reasons": {
            state: list(PROGRAM_MEMORY_COVERAGE_REASONS[state])
            for state in PROGRAM_MEMORY_COVERAGE_STATES
        },
        "source_cut_rules": [
            "program_universe.csv defines the explicit denominator at program-opportunity grain",
            "data/curated/program_history/v2 events remain the authoritative included-event substrate",
            "ClinicalTrials.gov supports denominator discovery and provenance but never replaces adjudicated checked-in events",
            "included denominator rows must map to checked-in event_ids",
            "duplicate rows must point to one canonical program_universe_id",
        ],
    }


def _build_denominator_summary_rows(
    program_universe_rows: tuple[ProgramMemoryUniverseRow, ...],
) -> list[dict[str, object]]:
    grouped_rows: dict[tuple[str, str, str, str, str], list[ProgramMemoryUniverseRow]] = (
        defaultdict(list)
    )
    for row in program_universe_rows:
        grouped_rows[
            (
                row.stage_bucket,
                row.modality,
                row.domain,
                row.coverage_state,
                row.coverage_reason,
            )
        ].append(row)

    summary_rows: list[dict[str, object]] = []
    for key in sorted(grouped_rows):
        rows = sorted(grouped_rows[key], key=_program_universe_sort_key)
        summary_rows.append(
            {
                "stage_bucket": key[0],
                "modality": key[1],
                "domain": key[2],
                "coverage_state": key[3],
                "coverage_reason": key[4],
                "program_count": len(rows),
                "mapped_event_count": sum(
                    len(row.mapped_event_ids) for row in rows
                ),
                "program_universe_ids_json": encode_string_list(
                    [row.program_universe_id for row in rows]
                ),
            }
        )
    return summary_rows


def _build_denominator_gap_rows(
    program_universe_rows: tuple[ProgramMemoryUniverseRow, ...],
) -> list[dict[str, object]]:
    return [
        {
            "program_universe_id": row.program_universe_id,
            "asset_id": row.asset_id,
            "asset_name": row.asset_name,
            "asset_lineage_id": row.asset_lineage_id,
            "target": row.target,
            "target_class": row.target_class,
            "target_class_lineage_id": row.target_class_lineage_id,
            "modality": row.modality,
            "domain": row.domain,
            "population": row.population,
            "regimen": row.regimen,
            "stage_bucket": row.stage_bucket,
            "coverage_state": row.coverage_state,
            "coverage_reason": row.coverage_reason,
            "coverage_confidence": row.coverage_confidence,
            "mapped_event_ids_json": encode_string_list(row.mapped_event_ids),
            "duplicate_of_program_universe_id": row.duplicate_of_program_universe_id,
            "discovery_source_type": row.discovery_source_type,
            "discovery_source_id": row.discovery_source_id,
            "source_candidate_url": row.source_candidate_url,
            "notes": row.notes,
        }
        for row in program_universe_rows
        if row.coverage_state != "included"
    ]


def _program_opportunity_key(row: ProgramMemoryUniverseRow) -> tuple[str, ...]:
    return (
        row.asset_lineage_id,
        row.target_class_lineage_id,
        row.modality,
        row.domain,
        row.population,
        row.regimen,
        row.stage_bucket,
    )


def _program_universe_sort_key(
    row: ProgramMemoryUniverseRow,
) -> tuple[object, ...]:
    return (
        PROGRAM_MEMORY_COVERAGE_STATES.index(row.coverage_state),
        row.stage_bucket,
        row.domain,
        row.asset_lineage_id,
        row.population,
        row.regimen,
        row.coverage_reason,
        row.program_universe_id,
    )


def _coerce_dataset(dataset_or_path: ProgramMemoryDataset | Path) -> ProgramMemoryDataset:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return dataset_or_path
    return load_program_memory_dataset(
        Path(dataset_or_path),
        validate_program_universe=False,
    )


def _resolve_dataset_dir(dataset_or_path: ProgramMemoryDataset | Path) -> str:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return ""
    resolved = resolve_program_memory_v2_dir(Path(dataset_or_path))
    if resolved is None:
        return _normalize_dataset_dir(Path(dataset_or_path))
    return _normalize_dataset_dir(resolved)


def _normalize_dataset_dir(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _build_summary_for_scope(
    *,
    dimension: str,
    scope_value: str,
    events: list[_EventContext],
    hypotheses: list[ProgramMemoryDirectionalityHypothesis],
    total_failure_event_count: int,
) -> tuple[ProgramMemoryCoverageSummary, list[ProgramMemoryCoverageGap]]:
    event_count = len(events)
    asset_count = len({context.asset.asset_id for context in events})
    failure_event_count = sum(
        1
        for context in events
        if context.event.failure_reason_taxonomy != NONFAILURE_TAXONOMY
    )
    nonfailure_event_count = event_count - failure_event_count
    unresolved_event_count = sum(
        1 for context in events if context.event.failure_reason_taxonomy == "unresolved"
    )
    high_confidence_event_count = sum(
        1 for context in events if context.event.confidence == "high"
    )
    medium_confidence_event_count = sum(
        1 for context in events if context.event.confidence == "medium"
    )
    low_confidence_event_count = sum(
        1 for context in events if context.event.confidence == "low"
    )
    directionality_hypothesis_count = len(hypotheses)
    supported_directionality_hypothesis_count = sum(
        1 for hypothesis in hypotheses if hypothesis.supporting_event_ids
    )
    low_confidence_hypothesis_count = sum(
        1 for hypothesis in hypotheses if hypothesis.confidence == "low"
    )

    gaps = _build_gap_rows(
        dimension=dimension,
        scope_value=scope_value,
        events=events,
        hypotheses=hypotheses,
        total_failure_event_count=total_failure_event_count,
        high_confidence_event_count=high_confidence_event_count,
        failure_event_count=failure_event_count,
        nonfailure_event_count=nonfailure_event_count,
        unresolved_event_count=unresolved_event_count,
        supported_directionality_hypothesis_count=(
            supported_directionality_hypothesis_count
        ),
        low_confidence_hypothesis_count=low_confidence_hypothesis_count,
    )
    uncertainty_codes = _build_uncertainty_codes(
        events=events,
        hypotheses=hypotheses,
        failure_event_count=failure_event_count,
        nonfailure_event_count=nonfailure_event_count,
        unresolved_event_count=unresolved_event_count,
        medium_confidence_event_count=medium_confidence_event_count,
        composite_event_count=sum(1 for context in events if _is_composite_event(context)),
    )
    summary = ProgramMemoryCoverageSummary(
        dimension=dimension,
        scope_value=scope_value,
        coverage_band=_determine_coverage_band(
            dimension=dimension,
            event_count=event_count,
            hypothesis_count=directionality_hypothesis_count,
            gaps=gaps,
        ),
        event_count=event_count,
        asset_count=asset_count,
        failure_event_count=failure_event_count,
        nonfailure_event_count=nonfailure_event_count,
        unresolved_event_count=unresolved_event_count,
        high_confidence_event_count=high_confidence_event_count,
        medium_confidence_event_count=medium_confidence_event_count,
        low_confidence_event_count=low_confidence_event_count,
        directionality_hypothesis_count=directionality_hypothesis_count,
        supported_directionality_hypothesis_count=(
            supported_directionality_hypothesis_count
        ),
        low_confidence_hypothesis_count=low_confidence_hypothesis_count,
        gap_codes=tuple(gap.gap_code for gap in gaps),
        uncertainty_codes=uncertainty_codes,
        explanation=_build_summary_explanation(
            dimension=dimension,
            scope_value=scope_value,
            event_count=event_count,
            asset_count=asset_count,
            directionality_hypothesis_count=directionality_hypothesis_count,
            supported_directionality_hypothesis_count=(
                supported_directionality_hypothesis_count
            ),
            gaps=gaps,
            uncertainty_codes=uncertainty_codes,
        ),
    )
    return summary, gaps


def _build_gap_rows(
    *,
    dimension: str,
    scope_value: str,
    events: list[_EventContext],
    hypotheses: list[ProgramMemoryDirectionalityHypothesis],
    total_failure_event_count: int,
    high_confidence_event_count: int,
    failure_event_count: int,
    nonfailure_event_count: int,
    unresolved_event_count: int,
    supported_directionality_hypothesis_count: int,
    low_confidence_hypothesis_count: int,
) -> list[ProgramMemoryCoverageGap]:
    asset_count = len({context.asset.asset_id for context in events})
    related_event_ids = tuple(context.event.event_id for context in events)
    related_hypothesis_ids = tuple(hypothesis.hypothesis_id for hypothesis in hypotheses)
    gaps: list[ProgramMemoryCoverageGap] = []

    def add_gap(code: str, category: str, explanation: str) -> None:
        gap = ProgramMemoryCoverageGap(
            dimension=dimension,
            scope_value=scope_value,
            gap_code=code,
            gap_reason_category=category,
            explanation=explanation,
            related_event_ids=related_event_ids,
            related_hypothesis_ids=related_hypothesis_ids,
        )
        if gap not in gaps:
            gaps.append(gap)

    if dimension == "target":
        if not events:
            add_gap(
                "no_checked_in_event_history",
                "history_sparse",
                (
                    f"{scope_value} has no checked-in clinical event rows yet, so target "
                    "coverage is still history-sparse."
                ),
            )
        elif len(events) == 1:
            add_gap(
                "single_event_history",
                "history_sparse",
                (
                    f"{scope_value} is anchored by only one checked-in clinical event, "
                    "so target coverage is still thin."
                ),
            )
        if events and asset_count == 1 and len(events) > 1:
            add_gap(
                "single_asset_history",
                "history_sparse",
                (
                    f"{scope_value} is still concentrated in one asset lineage, so the "
                    "target history is not yet broad."
                ),
            )
        if events and not hypotheses:
            add_gap(
                "no_directionality_hypothesis",
                "curation_incomplete",
                (
                    f"{scope_value} appears in checked-in clinical history, but the v2 "
                    "substrate does not yet carry a target-level directionality hypothesis."
                ),
            )
        if hypotheses and supported_directionality_hypothesis_count == 0:
            add_gap(
                "unsupported_directionality_hypothesis",
                "history_sparse",
                (
                    f"{scope_value} has a directionality record, but it does not yet "
                    "point to checked-in supporting event_ids."
                ),
            )
        if unresolved_event_count:
            add_gap(
                "unresolved_failure_scope",
                "curation_incomplete",
                (
                    f"{scope_value} includes checked-in misses that remain explicitly "
                    "unresolved rather than defended at molecule-, target-, or class-level scope."
                ),
            )
        if events and high_confidence_event_count == 0:
            add_gap(
                "no_high_confidence_event",
                "curation_incomplete",
                (
                    f"{scope_value} has clinical history, but none of the checked-in "
                    "events is a high-confidence adjudication yet."
                ),
            )
        if low_confidence_hypothesis_count:
            add_gap(
                "low_confidence_directionality",
                "curation_incomplete",
                (
                    f"{scope_value} directionality remains low-confidence in the "
                    "checked-in substrate."
                ),
            )
        return gaps

    if not events:
        if dimension == "failure_scope" and total_failure_event_count > 0 and scope_value != "nonfailure":
            add_gap(
                "scope_not_yet_adjudicated",
                "curation_incomplete",
                (
                    f"No checked-in events currently land in the {scope_value} failure "
                    "scope even though the repository carries other failure history."
                ),
            )
        else:
            add_gap(
                "no_checked_in_history",
                "history_sparse",
                (
                    f"{scope_value} has no checked-in rows in the current program-memory "
                    "substrate."
                ),
            )
        return gaps

    if len(events) == 1:
        add_gap(
            "single_event_history",
            "history_sparse",
            (
                f"{scope_value} is represented by only one checked-in event, so the "
                "history is still thin."
            ),
        )
    if asset_count == 1 and len(events) > 1:
        add_gap(
            "single_asset_history",
            "history_sparse",
            (
                f"{scope_value} has multiple events, but they all sit on one asset "
                "lineage."
            ),
        )
    if (
        dimension != "failure_scope"
        and len(events) > 1
        and (failure_event_count == 0 or nonfailure_event_count == 0)
    ):
        add_gap(
            "one_sided_outcome_history",
            "history_sparse",
            (
                f"{scope_value} currently has only one outcome direction in checked-in "
                "history, so the coverage is not outcome-complete."
            ),
        )
    if unresolved_event_count:
        add_gap(
            "unresolved_failure_scope",
            "curation_incomplete",
            (
                f"{scope_value} includes checked-in misses that are still stored as "
                "`unresolved`."
            ),
        )
    if dimension == "failure_scope" and scope_value == "unresolved":
        add_gap(
            "unresolved_scope_bucket",
            "curation_incomplete",
            (
                "This bucket explicitly tracks failures whose scope is not yet defended "
                "as molecule, target, target-class, population, or endpoint baggage."
            ),
        )
    if high_confidence_event_count == 0:
        add_gap(
            "no_high_confidence_event",
            "curation_incomplete",
            (
                f"{scope_value} has checked-in rows, but none is high-confidence yet."
            ),
        )
    return gaps


def _build_uncertainty_codes(
    *,
    events: list[_EventContext],
    hypotheses: list[ProgramMemoryDirectionalityHypothesis],
    failure_event_count: int,
    nonfailure_event_count: int,
    unresolved_event_count: int,
    medium_confidence_event_count: int,
    composite_event_count: int,
) -> tuple[str, ...]:
    codes: list[str] = []
    if failure_event_count and nonfailure_event_count:
        codes.append("mixed_history")
    if unresolved_event_count:
        codes.append("unresolved_failure_scope")
    if medium_confidence_event_count:
        codes.append("medium_confidence_events")
    if composite_event_count:
        codes.append("composite_mechanism_history")
    if any(hypothesis.confidence == "medium" for hypothesis in hypotheses):
        codes.append("medium_confidence_directionality")
    return tuple(codes)


def _determine_coverage_band(
    *,
    dimension: str,
    event_count: int,
    hypothesis_count: int,
    gaps: list[ProgramMemoryCoverageGap],
) -> str:
    if dimension == "target":
        if event_count == 0:
            return "thin" if hypothesis_count else "missing"
        if any(gap.gap_reason_category == "history_sparse" for gap in gaps):
            return "thin"
        if gaps:
            return "partial"
        return "strong"

    if event_count == 0:
        return "missing"
    if any(gap.gap_reason_category == "history_sparse" for gap in gaps):
        return "thin"
    if gaps:
        return "partial"
    return "strong"


def _build_summary_explanation(
    *,
    dimension: str,
    scope_value: str,
    event_count: int,
    asset_count: int,
    directionality_hypothesis_count: int,
    supported_directionality_hypothesis_count: int,
    gaps: list[ProgramMemoryCoverageGap],
    uncertainty_codes: tuple[str, ...],
) -> str:
    if dimension == "target":
        base = (
            f"{scope_value} has {event_count} checked-in events across {asset_count} "
            f"assets and {directionality_hypothesis_count} target-level hypotheses "
            f"({supported_directionality_hypothesis_count} supported by event_ids)."
        )
    else:
        base = (
            f"{scope_value} has {event_count} checked-in events across {asset_count} assets."
        )

    if not gaps and not uncertainty_codes:
        return base + " Coverage is relatively complete inside the current checked-in slice."

    details: list[str] = []
    if gaps:
        details.append(
            "Gaps: " + ", ".join(gap.gap_code for gap in gaps[:3])
        )
    if uncertainty_codes:
        details.append(
            "Uncertainty: " + ", ".join(uncertainty_codes[:3])
        )
    return base + " " + " ".join(details)


def _event_evidence_row(
    *,
    dimension: str,
    scope_value: str,
    relation: str,
    context: _EventContext,
    target_symbol: str,
    failure_scope: str,
) -> ProgramMemoryCoverageEvidence:
    return ProgramMemoryCoverageEvidence(
        dimension=dimension,
        scope_value=scope_value,
        record_kind="event",
        record_id=context.event.event_id,
        event_id=context.event.event_id,
        relation=relation,
        asset_id=context.asset.asset_id,
        asset_lineage_id=context.asset.asset_lineage_id,
        molecule=context.asset.molecule,
        sponsor=context.event.sponsor,
        target=context.asset.target,
        target_symbol=target_symbol,
        target_class=context.asset.target_class,
        target_class_lineage_id=context.asset.target_class_lineage_id,
        mechanism=context.asset.mechanism,
        modality=context.asset.modality,
        domain=context.event.domain,
        population=context.event.population,
        mono_or_adjunct=context.event.mono_or_adjunct,
        phase=context.event.phase,
        event_type=context.event.event_type,
        event_date=context.event.event_date,
        failure_reason_taxonomy=context.event.failure_reason_taxonomy,
        failure_scope=failure_scope,
        confidence=context.event.confidence,
        source_tiers=(context.provenance.source_tier,),
        source_urls=(context.provenance.source_url,),
        supporting_event_ids=(context.event.event_id,),
        notes=context.event.notes,
    )


def _hypothesis_evidence_row(
    *,
    hypothesis: ProgramMemoryDirectionalityHypothesis,
    scope_value: str,
    provenances_by_event_id: dict[str, ProgramMemoryProvenance],
) -> ProgramMemoryCoverageEvidence:
    source_tiers: list[str] = []
    source_urls: list[str] = []
    for event_id in hypothesis.supporting_event_ids:
        provenance = provenances_by_event_id.get(event_id)
        if provenance is None:
            continue
        source_tiers.append(provenance.source_tier)
        source_urls.append(provenance.source_url)
    return ProgramMemoryCoverageEvidence(
        dimension="target",
        scope_value=scope_value,
        record_kind="directionality_hypothesis",
        record_id=hypothesis.hypothesis_id,
        hypothesis_id=hypothesis.hypothesis_id,
        relation="directionality_hypothesis",
        target=scope_value,
        target_symbol=scope_value,
        mechanism=hypothesis.modality_hypothesis,
        confidence=hypothesis.confidence,
        source_tiers=tuple(source_tiers),
        source_urls=tuple(source_urls),
        supporting_event_ids=hypothesis.supporting_event_ids,
        notes=hypothesis.ambiguity or hypothesis.evidence_basis,
    )


def _map_failure_scope(failure_reason_taxonomy: str) -> str:
    return FAILURE_SCOPE_BY_TAXONOMY.get(failure_reason_taxonomy, "unresolved")


def _is_composite_event(context: _EventContext) -> bool:
    return len(context.asset.target_symbols) > 1 or "combination" in context.asset.modality


def _summary_sort_key(
    summary: ProgramMemoryCoverageSummary,
) -> tuple[int, int, str]:
    dimension_index = DIMENSION_ORDER.index(summary.dimension)
    if summary.dimension == "failure_scope":
        scope_index = FAILURE_SCOPE_ORDER.index(summary.scope_value)
        return (dimension_index, scope_index, summary.scope_value)
    return (dimension_index, 0, summary.scope_value)


def _gap_sort_key(gap: ProgramMemoryCoverageGap) -> tuple[int, int, str, str]:
    dimension_index = DIMENSION_ORDER.index(gap.dimension)
    scope_index = 0
    if gap.dimension == "failure_scope":
        scope_index = FAILURE_SCOPE_ORDER.index(gap.scope_value)
    return (dimension_index, scope_index, gap.scope_value, gap.gap_code)


def _evidence_sort_key(
    evidence: ProgramMemoryCoverageEvidence,
) -> tuple[int, int, str, str, str]:
    dimension_index = DIMENSION_ORDER.index(evidence.dimension)
    scope_index = 0
    if evidence.dimension == "failure_scope":
        scope_index = FAILURE_SCOPE_ORDER.index(evidence.scope_value)
    return (
        dimension_index,
        scope_index,
        evidence.scope_value,
        evidence.record_kind,
        evidence.record_id,
    )
