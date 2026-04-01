from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProgramMemoryAsset:
    asset_id: str
    molecule: str
    target: str
    target_symbols: tuple[str, ...]
    target_class: str
    mechanism: str
    modality: str
    asset_lineage_id: str = ""
    asset_aliases: tuple[str, ...] = ()
    target_class_lineage_id: str = ""
    target_class_aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProgramMemoryEvent:
    event_id: str
    asset_id: str
    sponsor: str
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
    sort_order: int


@dataclass(frozen=True)
class ProgramMemoryProvenance:
    event_id: str
    source_tier: str
    source_url: str


@dataclass(frozen=True)
class ProgramMemoryDirectionalityHypothesis:
    hypothesis_id: str
    entity_id: str
    entity_label: str
    desired_perturbation_direction: str
    modality_hypothesis: str
    preferred_modalities: tuple[str, ...]
    confidence: str
    ambiguity: str
    evidence_basis: str
    supporting_event_ids: tuple[str, ...]
    contradiction_conditions: tuple[str, ...]
    falsification_conditions: tuple[str, ...]
    open_risks: tuple[str, ...]
    sort_order: int


@dataclass(frozen=True)
class ProgramMemoryDataset:
    assets: tuple[ProgramMemoryAsset, ...]
    events: tuple[ProgramMemoryEvent, ...]
    provenances: tuple[ProgramMemoryProvenance, ...]
    directionality_hypotheses: tuple[ProgramMemoryDirectionalityHypothesis, ...]
    program_universe_rows: tuple["ProgramMemoryUniverseRow", ...] = ()
    requires_program_universe: bool = False


@dataclass(frozen=True)
class ProgramMemoryUniverseRow:
    program_universe_id: str
    asset_id: str
    asset_name: str
    asset_lineage_id: str
    asset_aliases: tuple[str, ...]
    target: str
    target_symbols: tuple[str, ...]
    target_class: str
    target_class_lineage_id: str
    target_class_aliases: tuple[str, ...]
    mechanism: str
    modality: str
    domain: str
    population: str
    regimen: str
    stage_bucket: str
    coverage_state: str
    coverage_reason: str
    coverage_confidence: str
    mapped_event_ids: tuple[str, ...]
    duplicate_of_program_universe_id: str
    discovery_source_type: str
    discovery_source_id: str
    source_candidate_url: str
    notes: str


@dataclass(frozen=True)
class ProgramHistoryEvent:
    program_id: str
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
    date: str
    primary_outcome_result: str
    failure_reason_taxonomy: str
    source_tier: str
    source_url: str
    confidence: str
    notes: str


@dataclass(frozen=True)
class DirectionalityHypothesis:
    entity_id: str
    entity_label: str
    desired_perturbation_direction: str
    modality_hypothesis: str
    preferred_modalities: tuple[str, ...]
    confidence: str
    ambiguity: str
    evidence_basis: str
    supporting_program_ids: tuple[str, ...]
    contradiction_conditions: tuple[str, ...]
    falsification_conditions: tuple[str, ...]
    open_risks: tuple[str, ...]
