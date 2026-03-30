from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.program_memory._helpers import clean_text
from scz_target_engine.program_memory.loaders import (
    load_program_memory_dataset,
    resolve_program_memory_v2_dir,
)
from scz_target_engine.program_memory.models import (
    ProgramMemoryAsset,
    ProgramMemoryDataset,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
)


NONFAILURE_TAXONOMY = "not_applicable_nonfailure"
POPULATION_STOPWORDS = {
    "a",
    "adult",
    "adults",
    "after",
    "and",
    "during",
    "for",
    "in",
    "of",
    "on",
    "or",
    "stable",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True)
class InterventionProposal:
    entity_id: str = ""
    target_symbol: str = ""
    target_class: str = ""
    molecule: str = ""
    domain: str = ""
    population: str = ""
    modality: str = ""
    mono_or_adjunct: str = ""
    phase: str = ""


@dataclass(frozen=True)
class AnalogReason:
    dimension: str
    relation: str
    proposal_value: str
    record_value: str
    explanation: str


@dataclass(frozen=True)
class UncertaintyFlag:
    code: str
    explanation: str


@dataclass(frozen=True)
class CheckedInRecordRef:
    dataset_dir: str
    asset_id: str
    event_id: str
    compatibility_program_id: str
    source_tier: str
    source_url: str


@dataclass(frozen=True)
class ProgramMemoryAnalog:
    match_tier: str
    asset_id: str
    event_id: str
    sponsor: str
    molecule: str
    target: str
    target_symbols: tuple[str, ...]
    target_class: str
    mechanism: str
    modality: str
    population: str
    domain: str
    mono_or_adjunct: str
    phase: str
    event_type: str
    event_date: str
    primary_outcome_result: str
    failure_reason_taxonomy: str
    confidence: str
    notes: str
    record_ref: CheckedInRecordRef
    match_reasons: tuple[AnalogReason, ...]
    context_gaps: tuple[AnalogReason, ...]
    uncertainty_flags: tuple[UncertaintyFlag, ...]

    @property
    def is_failure(self) -> bool:
        return self.failure_reason_taxonomy != NONFAILURE_TAXONOMY

    @property
    def is_nonfailure(self) -> bool:
        return not self.is_failure

    def has_match(self, dimension: str, relation: str | None = None) -> bool:
        for reason in self.match_reasons:
            if reason.dimension != dimension:
                continue
            if relation is None or reason.relation == relation:
                return True
        return False


@dataclass(frozen=True)
class AnalogSearchSummary:
    matched_event_count: int
    failure_event_count: int
    nonfailure_event_count: int
    exact_target_match_count: int
    target_class_match_count: int
    molecule_match_count: int


@dataclass(frozen=True)
class AnalogSearchResult:
    proposal: InterventionProposal
    inferred_target_classes: tuple[str, ...]
    matched_analogs: tuple[ProgramMemoryAnalog, ...]
    summary: AnalogSearchSummary
    uncertainty_flags: tuple[UncertaintyFlag, ...]


def retrieve_program_memory_analogs(
    dataset_or_path: ProgramMemoryDataset | Path,
    proposal: InterventionProposal,
    *,
    limit: int | None = None,
) -> AnalogSearchResult:
    _validate_proposal(proposal)
    dataset = _coerce_dataset(dataset_or_path)
    dataset_dir = _resolve_dataset_dir(dataset_or_path)
    inferred_target_classes = _infer_target_classes(dataset, proposal)
    assets_by_id = {
        asset.asset_id: asset
        for asset in dataset.assets
    }
    provenance_by_event_id = {
        provenance.event_id: provenance
        for provenance in dataset.provenances
    }

    analogs: list[ProgramMemoryAnalog] = []
    ordered_events = sorted(
        dataset.events,
        key=lambda event: (event.sort_order, event.event_date, event.event_id),
    )
    for event in ordered_events:
        asset = assets_by_id[event.asset_id]
        match_reasons = _build_match_reasons(
            proposal,
            asset,
            event,
            inferred_target_classes,
        )
        if not _has_biological_anchor(match_reasons):
            continue
        provenance = provenance_by_event_id[event.event_id]
        analogs.append(
            ProgramMemoryAnalog(
                match_tier=_determine_match_tier(match_reasons),
                asset_id=asset.asset_id,
                event_id=event.event_id,
                sponsor=event.sponsor,
                molecule=asset.molecule,
                target=asset.target,
                target_symbols=asset.target_symbols,
                target_class=asset.target_class,
                mechanism=asset.mechanism,
                modality=asset.modality,
                population=event.population,
                domain=event.domain,
                mono_or_adjunct=event.mono_or_adjunct,
                phase=event.phase,
                event_type=event.event_type,
                event_date=event.event_date,
                primary_outcome_result=event.primary_outcome_result,
                failure_reason_taxonomy=event.failure_reason_taxonomy,
                confidence=event.confidence,
                notes=event.notes,
                record_ref=CheckedInRecordRef(
                    dataset_dir=dataset_dir,
                    asset_id=asset.asset_id,
                    event_id=event.event_id,
                    compatibility_program_id=event.event_id,
                    source_tier=provenance.source_tier,
                    source_url=provenance.source_url,
                ),
                match_reasons=tuple(match_reasons),
                context_gaps=tuple(_build_context_gaps(proposal, asset, event)),
                uncertainty_flags=tuple(
                    _build_analog_uncertainty_flags(asset, event, match_reasons)
                ),
            )
        )

    analogs.sort(key=_analog_sort_key, reverse=True)
    if limit is not None:
        analogs = analogs[:limit]

    summary = AnalogSearchSummary(
        matched_event_count=len(analogs),
        failure_event_count=sum(1 for analog in analogs if analog.is_failure),
        nonfailure_event_count=sum(1 for analog in analogs if analog.is_nonfailure),
        exact_target_match_count=sum(
            1 for analog in analogs if analog.has_match("target_symbol")
        ),
        target_class_match_count=sum(
            1 for analog in analogs if analog.has_match("target_class")
        ),
        molecule_match_count=sum(
            1 for analog in analogs if analog.has_match("molecule")
        ),
    )
    return AnalogSearchResult(
        proposal=proposal,
        inferred_target_classes=inferred_target_classes,
        matched_analogs=tuple(analogs),
        summary=summary,
        uncertainty_flags=tuple(
            _build_search_uncertainty_flags(proposal, inferred_target_classes, analogs)
        ),
    )


def _coerce_dataset(dataset_or_path: ProgramMemoryDataset | Path) -> ProgramMemoryDataset:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return dataset_or_path
    return load_program_memory_dataset(Path(dataset_or_path))


def _resolve_dataset_dir(dataset_or_path: ProgramMemoryDataset | Path) -> str:
    if isinstance(dataset_or_path, ProgramMemoryDataset):
        return "<loaded_dataset>"
    path = Path(dataset_or_path)
    resolved = resolve_program_memory_v2_dir(path)
    if resolved is not None:
        return str(resolved)
    return str(path)


def _validate_proposal(proposal: InterventionProposal) -> None:
    if not any(
        clean_text(value)
        for value in (
            proposal.target_symbol,
            proposal.target_class,
            proposal.molecule,
        )
    ):
        raise ValueError(
            "intervention proposals require at least one of target_symbol, "
            "target_class, or molecule"
        )


def _infer_target_classes(
    dataset: ProgramMemoryDataset,
    proposal: InterventionProposal,
) -> tuple[str, ...]:
    target_classes: list[str] = []
    provided_target_class = clean_text(proposal.target_class)
    if provided_target_class:
        target_classes.append(provided_target_class)

    proposal_symbol = _normalize_target_symbol(proposal.target_symbol)
    if proposal_symbol:
        for asset in dataset.assets:
            if proposal_symbol in asset.target_symbols and asset.target_class:
                target_classes.append(asset.target_class)
    return tuple(_dedupe_preserve_order(target_classes))


def _build_match_reasons(
    proposal: InterventionProposal,
    asset: ProgramMemoryAsset,
    event: ProgramMemoryEvent,
    inferred_target_classes: tuple[str, ...],
) -> list[AnalogReason]:
    reasons: list[AnalogReason] = []
    proposal_molecule = clean_text(proposal.molecule)
    if proposal_molecule and _normalize_text(proposal_molecule) == _normalize_text(
        asset.molecule
    ):
        reasons.append(
            AnalogReason(
                dimension="molecule",
                relation="exact_match",
                proposal_value=proposal_molecule,
                record_value=asset.molecule,
                explanation=(
                    f"Proposal molecule {proposal_molecule} matches checked-in "
                    f"molecule {asset.molecule}."
                ),
            )
        )

    proposal_symbol = _normalize_target_symbol(proposal.target_symbol)
    if proposal_symbol and proposal_symbol in asset.target_symbols:
        reasons.append(
            AnalogReason(
                dimension="target_symbol",
                relation="exact_match",
                proposal_value=proposal_symbol,
                record_value=proposal_symbol,
                explanation=(
                    f"Proposal target symbol {proposal_symbol} matches checked-in "
                    f"target {asset.target}."
                ),
            )
        )

    normalized_target_classes = {
        _normalize_text(target_class): target_class
        for target_class in inferred_target_classes
    }
    asset_target_class_key = _normalize_text(asset.target_class)
    if asset_target_class_key and asset_target_class_key in normalized_target_classes:
        relation = "exact_match"
        proposal_value = normalized_target_classes[asset_target_class_key]
        explanation = (
            f"Proposal target class {proposal_value} matches checked-in "
            f"target class {asset.target_class}."
        )
        if not clean_text(proposal.target_class) and proposal_symbol:
            relation = "inferred_from_target_symbol"
            proposal_value = proposal_symbol
            explanation = (
                f"Proposal target symbol {proposal_symbol} maps to checked-in "
                f"target class {asset.target_class}, which this event shares."
            )
        reasons.append(
            AnalogReason(
                dimension="target_class",
                relation=relation,
                proposal_value=proposal_value,
                record_value=asset.target_class,
                explanation=explanation,
            )
        )

    if clean_text(proposal.domain) and _normalize_text(proposal.domain) == _normalize_text(
        event.domain
    ):
        reasons.append(
            AnalogReason(
                dimension="domain",
                relation="exact_match",
                proposal_value=proposal.domain,
                record_value=event.domain,
                explanation=(
                    f"Proposal domain {proposal.domain} matches checked-in domain "
                    f"{event.domain}."
                ),
            )
        )

    if clean_text(proposal.modality) and _normalize_text(
        proposal.modality
    ) == _normalize_text(asset.modality):
        reasons.append(
            AnalogReason(
                dimension="modality",
                relation="exact_match",
                proposal_value=proposal.modality,
                record_value=asset.modality,
                explanation=(
                    f"Proposal modality {proposal.modality} matches checked-in "
                    f"modality {asset.modality}."
                ),
            )
        )

    if clean_text(proposal.mono_or_adjunct) and _normalize_text(
        proposal.mono_or_adjunct
    ) == _normalize_text(event.mono_or_adjunct):
        reasons.append(
            AnalogReason(
                dimension="mono_or_adjunct",
                relation="exact_match",
                proposal_value=proposal.mono_or_adjunct,
                record_value=event.mono_or_adjunct,
                explanation=(
                    f"Proposal regimen {proposal.mono_or_adjunct} matches checked-in "
                    f"regimen {event.mono_or_adjunct}."
                ),
            )
        )

    if clean_text(proposal.phase) and _normalize_text(proposal.phase) == _normalize_text(
        event.phase
    ):
        reasons.append(
            AnalogReason(
                dimension="phase",
                relation="exact_match",
                proposal_value=proposal.phase,
                record_value=event.phase,
                explanation=(
                    f"Proposal phase {proposal.phase} matches checked-in phase "
                    f"{event.phase}."
                ),
            )
        )

    proposal_population = clean_text(proposal.population)
    if proposal_population:
        population_relation = _population_relation(proposal_population, event.population)
        if population_relation == "exact_match":
            reasons.append(
                AnalogReason(
                    dimension="population",
                    relation="exact_match",
                    proposal_value=proposal_population,
                    record_value=event.population,
                    explanation=(
                        "Proposal population matches the checked-in event population."
                    ),
                )
            )
        elif population_relation == "partial_overlap":
            reasons.append(
                AnalogReason(
                    dimension="population",
                    relation="partial_overlap",
                    proposal_value=proposal_population,
                    record_value=event.population,
                    explanation=(
                        "Proposal population overlaps the checked-in event population "
                        "but does not fully match it."
                    ),
                )
            )
    return reasons


def _build_context_gaps(
    proposal: InterventionProposal,
    asset: ProgramMemoryAsset,
    event: ProgramMemoryEvent,
) -> list[AnalogReason]:
    gaps: list[AnalogReason] = []
    if clean_text(proposal.domain) and _normalize_text(proposal.domain) != _normalize_text(
        event.domain
    ):
        gaps.append(
            AnalogReason(
                dimension="domain",
                relation="different_context",
                proposal_value=proposal.domain,
                record_value=event.domain,
                explanation=(
                    f"Proposal domain {proposal.domain} differs from checked-in "
                    f"domain {event.domain}."
                ),
            )
        )

    if clean_text(proposal.modality) and _normalize_text(
        proposal.modality
    ) != _normalize_text(asset.modality):
        gaps.append(
            AnalogReason(
                dimension="modality",
                relation="different_context",
                proposal_value=proposal.modality,
                record_value=asset.modality,
                explanation=(
                    f"Proposal modality {proposal.modality} differs from checked-in "
                    f"modality {asset.modality}."
                ),
            )
        )

    if clean_text(proposal.mono_or_adjunct) and _normalize_text(
        proposal.mono_or_adjunct
    ) != _normalize_text(event.mono_or_adjunct):
        gaps.append(
            AnalogReason(
                dimension="mono_or_adjunct",
                relation="different_context",
                proposal_value=proposal.mono_or_adjunct,
                record_value=event.mono_or_adjunct,
                explanation=(
                    f"Proposal regimen {proposal.mono_or_adjunct} differs from "
                    f"checked-in regimen {event.mono_or_adjunct}."
                ),
            )
        )

    if clean_text(proposal.phase) and _normalize_text(proposal.phase) != _normalize_text(
        event.phase
    ):
        gaps.append(
            AnalogReason(
                dimension="phase",
                relation="different_context",
                proposal_value=proposal.phase,
                record_value=event.phase,
                explanation=(
                    f"Proposal phase {proposal.phase} differs from checked-in phase "
                    f"{event.phase}."
                ),
            )
        )

    proposal_population = clean_text(proposal.population)
    if proposal_population:
        population_relation = _population_relation(proposal_population, event.population)
        if population_relation == "different_context":
            gaps.append(
                AnalogReason(
                    dimension="population",
                    relation="different_context",
                    proposal_value=proposal_population,
                    record_value=event.population,
                    explanation=(
                        "Proposal population does not match the checked-in event "
                        "population."
                    ),
                )
            )
        elif population_relation == "partial_overlap":
            gaps.append(
                AnalogReason(
                    dimension="population",
                    relation="not_identical",
                    proposal_value=proposal_population,
                    record_value=event.population,
                    explanation=(
                        "Proposal population only partially overlaps the checked-in "
                        "event population."
                    ),
                )
            )
    return gaps


def _build_analog_uncertainty_flags(
    asset: ProgramMemoryAsset,
    event: ProgramMemoryEvent,
    match_reasons: list[AnalogReason],
) -> list[UncertaintyFlag]:
    flags: list[UncertaintyFlag] = []
    if event.failure_reason_taxonomy == "unresolved":
        flags.append(
            UncertaintyFlag(
                code="unresolved_failure_scope",
                explanation=(
                    "This checked-in miss is explicitly recorded as unresolved rather "
                    "than as a defended molecule-, target-, or class-level failure."
                ),
            )
        )
    if event.confidence == "medium":
        flags.append(
            UncertaintyFlag(
                code="medium_curator_confidence",
                explanation=(
                    "The checked-in row is medium-confidence rather than a high-"
                    "confidence adjudication."
                ),
            )
        )
    elif event.confidence == "low":
        flags.append(
            UncertaintyFlag(
                code="low_curator_confidence",
                explanation=(
                    "The checked-in row is low-confidence and should be treated as "
                    "tentative history."
                ),
            )
        )
    if not any(reason.dimension == "target_symbol" for reason in match_reasons):
        flags.append(
            UncertaintyFlag(
                code="class_level_analog_only",
                explanation=(
                    "This analog is connected through target-class history rather than "
                    "an exact checked-in target-symbol match."
                ),
            )
        )
    if len(asset.target_symbols) > 1 or "combination" in asset.modality:
        flags.append(
            UncertaintyFlag(
                code="composite_mechanism_analog",
                explanation=(
                    "This analog carries multiple targets or a combination modality, "
                    "so it is not a clean single-node replay."
                ),
            )
        )
    return flags


def _build_search_uncertainty_flags(
    proposal: InterventionProposal,
    inferred_target_classes: tuple[str, ...],
    analogs: list[ProgramMemoryAnalog],
) -> list[UncertaintyFlag]:
    flags: list[UncertaintyFlag] = []
    if not analogs:
        flags.append(
            UncertaintyFlag(
                code="no_checked_in_analogs",
                explanation=(
                    "The checked-in program-memory substrate does not yet contain a "
                    "biologically anchored analog for this proposal."
                ),
            )
        )
        return flags

    if len(analogs) < 2:
        flags.append(
            UncertaintyFlag(
                code="sparse_history",
                explanation=(
                    "Only one checked-in analog matched, so replay reasoning is still "
                    "history-sparse."
                ),
            )
        )
    if clean_text(proposal.target_symbol) and not any(
        analog.has_match("target_symbol")
        for analog in analogs
    ):
        flags.append(
            UncertaintyFlag(
                code="no_exact_target_history",
                explanation=(
                    "The proposal target has no exact checked-in target-symbol analog; "
                    "only class-level history is available."
                ),
            )
        )
    if analogs and not inferred_target_classes and clean_text(proposal.target_symbol):
        flags.append(
            UncertaintyFlag(
                code="target_class_not_inferred",
                explanation=(
                    "The proposal target did not map to a checked-in target class, so "
                    "class-level analog expansion was unavailable."
                ),
            )
        )
    if any(analog.is_failure for analog in analogs) and any(
        analog.is_nonfailure for analog in analogs
    ):
        flags.append(
            UncertaintyFlag(
                code="mixed_history",
                explanation=(
                    "Checked-in history contains both failure and nonfailure analogs."
                ),
            )
        )
    return flags


def _determine_match_tier(match_reasons: list[AnalogReason]) -> str:
    if any(reason.dimension == "molecule" for reason in match_reasons):
        return "molecule"
    if any(reason.dimension == "target_symbol" for reason in match_reasons):
        return "exact_target"
    return "target_class"


def _has_biological_anchor(match_reasons: list[AnalogReason]) -> bool:
    return any(
        reason.dimension in {"molecule", "target_symbol", "target_class"}
        for reason in match_reasons
    )


def _analog_sort_key(analog: ProgramMemoryAnalog) -> tuple[int, int, int, int, int, int, str]:
    return (
        1 if analog.has_match("molecule") else 0,
        1 if analog.has_match("target_symbol") else 0,
        1 if analog.has_match("domain", "exact_match") else 0,
        1 if analog.has_match("mono_or_adjunct", "exact_match") else 0,
        1 if analog.has_match("modality", "exact_match") else 0,
        1 if analog.has_match("population", "exact_match") else 0,
        analog.event_date,
    )


def _normalize_text(value: str) -> str:
    return clean_text(value).casefold()


def _normalize_target_symbol(value: str) -> str:
    return clean_text(value).upper()


def _population_relation(proposal_population: str, event_population: str) -> str:
    if _normalize_text(proposal_population) == _normalize_text(event_population):
        return "exact_match"
    proposal_tokens = _population_tokens(proposal_population)
    event_tokens = _population_tokens(event_population)
    if proposal_tokens and event_tokens and proposal_tokens.intersection(event_tokens):
        return "partial_overlap"
    return "different_context"


def _population_tokens(value: str) -> set[str]:
    return {
        token
        for token in _normalize_text(value).replace("-", " ").split()
        if token and token not in POPULATION_STOPWORDS
    }


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = clean_text(value)
        key = _normalize_text(cleaned)
        if not cleaned or key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped
