from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

from scz_target_engine.io import read_json, write_csv, write_json
from scz_target_engine.program_memory._helpers import clean_text, encode_string_list
from scz_target_engine.program_memory.extract import (
    PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION,
    PROGRAM_MEMORY_EVENT_SUGGESTION,
    ProgramMemorySuggestion,
    parse_program_memory_asset,
    parse_program_memory_directionality_hypothesis,
    parse_program_memory_event,
    parse_program_memory_provenance,
)
from scz_target_engine.program_memory.harvest import ProgramMemoryHarvestBatch
from scz_target_engine.program_memory.models import (
    ProgramMemoryAsset,
    ProgramMemoryDataset,
    ProgramMemoryDirectionalityHypothesis,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
)


PROGRAM_MEMORY_ADJUDICATION_SCHEMA_VERSION = "program-memory-adjudication-v1"
PROGRAM_MEMORY_ACCEPT_DECISION = "accept"
PROGRAM_MEMORY_REJECT_DECISION = "reject"
PROGRAM_MEMORY_EDIT_DECISION = "edit"
PROGRAM_MEMORY_ADJUDICATION_DECISIONS = {
    PROGRAM_MEMORY_ACCEPT_DECISION,
    PROGRAM_MEMORY_REJECT_DECISION,
    PROGRAM_MEMORY_EDIT_DECISION,
}

PROGRAM_MEMORY_V2_ASSET_FIELDNAMES = [
    "asset_id",
    "molecule",
    "target",
    "target_symbols_json",
    "target_class",
    "mechanism",
    "modality",
]
PROGRAM_MEMORY_V2_EVENT_FIELDNAMES = [
    "event_id",
    "asset_id",
    "sponsor",
    "population",
    "domain",
    "mono_or_adjunct",
    "phase",
    "event_type",
    "event_date",
    "primary_outcome_result",
    "failure_reason_taxonomy",
    "confidence",
    "notes",
    "sort_order",
]
PROGRAM_MEMORY_V2_PROVENANCE_FIELDNAMES = [
    "event_id",
    "source_tier",
    "source_url",
]
PROGRAM_MEMORY_V2_DIRECTIONALITY_FIELDNAMES = [
    "hypothesis_id",
    "entity_id",
    "entity_label",
    "desired_perturbation_direction",
    "modality_hypothesis",
    "preferred_modalities_json",
    "confidence",
    "ambiguity",
    "evidence_basis",
    "supporting_event_ids_json",
    "contradiction_conditions_json",
    "falsification_conditions_json",
    "open_risks_json",
    "sort_order",
]


def _require_text(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    context: str,
) -> str:
    value = clean_text(str(payload.get(field_name) or ""))
    if not value:
        raise ValueError(f"{context} requires {field_name}")
    return value


def _read_mapping(
    payload: Mapping[str, Any],
    field_name: str,
) -> Mapping[str, Any] | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"program memory adjudication payload {field_name} must be an object")
    return value


@dataclass(frozen=True)
class ProgramMemoryAdjudicationDecision:
    suggestion_id: str
    decision: str
    rationale: str
    asset: ProgramMemoryAsset | None = None
    event: ProgramMemoryEvent | None = None
    provenance: ProgramMemoryProvenance | None = None
    directionality_hypothesis: ProgramMemoryDirectionalityHypothesis | None = None

    def __post_init__(self) -> None:
        if not self.suggestion_id:
            raise ValueError("program memory adjudication decisions require suggestion_id")
        if self.decision not in PROGRAM_MEMORY_ADJUDICATION_DECISIONS:
            raise ValueError(
                f"unsupported program memory adjudication decision {self.decision!r}"
            )
        has_event_override = any(
            item is not None for item in (self.asset, self.event, self.provenance)
        )
        has_hypothesis_override = self.directionality_hypothesis is not None
        if self.decision == PROGRAM_MEMORY_ACCEPT_DECISION:
            if has_event_override or has_hypothesis_override:
                raise ValueError(
                    "accept decisions cannot include edited program memory payloads"
                )
            return
        if self.decision == PROGRAM_MEMORY_REJECT_DECISION:
            if has_event_override or has_hypothesis_override:
                raise ValueError(
                    "reject decisions cannot include edited program memory payloads"
                )
            return
        if has_hypothesis_override and has_event_override:
            raise ValueError(
                "edit decisions cannot mix event payloads with directionality payloads"
            )
        if has_event_override:
            if self.asset is None or self.event is None or self.provenance is None:
                raise ValueError(
                    "edited event decisions require asset, event, and provenance payloads"
                )
            if self.asset.asset_id != self.event.asset_id:
                raise ValueError(
                    "edited event decisions require matching asset.asset_id and event.asset_id"
                )
            if self.event.event_id != self.provenance.event_id:
                raise ValueError(
                    "edited event decisions require matching event.event_id and provenance.event_id"
                )
            return
        if self.directionality_hypothesis is None:
            raise ValueError(
                "edit decisions require either event payloads or a directionality hypothesis payload"
            )

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> ProgramMemoryAdjudicationDecision:
        asset_payload = _read_mapping(payload, "asset")
        event_payload = _read_mapping(payload, "event")
        provenance_payload = _read_mapping(payload, "provenance")
        hypothesis_payload = _read_mapping(payload, "directionality_hypothesis")
        return cls(
            suggestion_id=_require_text(
                payload,
                "suggestion_id",
                context="program memory adjudication decision",
            ),
            decision=_require_text(
                payload,
                "decision",
                context="program memory adjudication decision",
            ).lower(),
            rationale=clean_text(str(payload.get("rationale") or "")),
            asset=(
                parse_program_memory_asset(asset_payload)
                if asset_payload is not None
                else None
            ),
            event=(
                parse_program_memory_event(event_payload)
                if event_payload is not None
                else None
            ),
            provenance=(
                parse_program_memory_provenance(provenance_payload)
                if provenance_payload is not None
                else None
            ),
            directionality_hypothesis=(
                parse_program_memory_directionality_hypothesis(hypothesis_payload)
                if hypothesis_payload is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "suggestion_id": self.suggestion_id,
            "decision": self.decision,
            "rationale": self.rationale,
        }
        if self.asset is not None:
            payload["asset"] = {
                "asset_id": self.asset.asset_id,
                "molecule": self.asset.molecule,
                "target": self.asset.target,
                "target_symbols": list(self.asset.target_symbols),
                "target_class": self.asset.target_class,
                "mechanism": self.asset.mechanism,
                "modality": self.asset.modality,
            }
        if self.event is not None:
            payload["event"] = {
                "event_id": self.event.event_id,
                "asset_id": self.event.asset_id,
                "sponsor": self.event.sponsor,
                "population": self.event.population,
                "domain": self.event.domain,
                "mono_or_adjunct": self.event.mono_or_adjunct,
                "phase": self.event.phase,
                "event_type": self.event.event_type,
                "event_date": self.event.event_date,
                "primary_outcome_result": self.event.primary_outcome_result,
                "failure_reason_taxonomy": self.event.failure_reason_taxonomy,
                "confidence": self.event.confidence,
                "notes": self.event.notes,
                "sort_order": self.event.sort_order,
            }
        if self.provenance is not None:
            payload["provenance"] = {
                "event_id": self.provenance.event_id,
                "source_tier": self.provenance.source_tier,
                "source_url": self.provenance.source_url,
            }
        if self.directionality_hypothesis is not None:
            payload["directionality_hypothesis"] = {
                "hypothesis_id": self.directionality_hypothesis.hypothesis_id,
                "entity_id": self.directionality_hypothesis.entity_id,
                "entity_label": self.directionality_hypothesis.entity_label,
                "desired_perturbation_direction": (
                    self.directionality_hypothesis.desired_perturbation_direction
                ),
                "modality_hypothesis": (
                    self.directionality_hypothesis.modality_hypothesis
                ),
                "preferred_modalities": list(
                    self.directionality_hypothesis.preferred_modalities
                ),
                "confidence": self.directionality_hypothesis.confidence,
                "ambiguity": self.directionality_hypothesis.ambiguity,
                "evidence_basis": self.directionality_hypothesis.evidence_basis,
                "supporting_event_ids": list(
                    self.directionality_hypothesis.supporting_event_ids
                ),
                "contradiction_conditions": list(
                    self.directionality_hypothesis.contradiction_conditions
                ),
                "falsification_conditions": list(
                    self.directionality_hypothesis.falsification_conditions
                ),
                "open_risks": list(self.directionality_hypothesis.open_risks),
                "sort_order": self.directionality_hypothesis.sort_order,
            }
        return payload


@dataclass(frozen=True)
class ProgramMemoryAdjudicationRecord:
    schema_version: str
    adjudication_id: str
    harvest_id: str
    reviewer: str
    reviewed_at: str
    notes: str
    decisions: tuple[ProgramMemoryAdjudicationDecision, ...]

    def __post_init__(self) -> None:
        if self.schema_version != PROGRAM_MEMORY_ADJUDICATION_SCHEMA_VERSION:
            raise ValueError(
                "unsupported program memory adjudication schema_version "
                f"{self.schema_version!r}"
            )
        if not self.adjudication_id:
            raise ValueError("program memory adjudication records require adjudication_id")
        if not self.harvest_id:
            raise ValueError("program memory adjudication records require harvest_id")
        if not self.reviewer:
            raise ValueError("program memory adjudication records require reviewer")
        suggestion_ids: set[str] = set()
        for decision in self.decisions:
            if decision.suggestion_id in suggestion_ids:
                raise ValueError(
                    "duplicate adjudication decision for suggestion_id "
                    f"{decision.suggestion_id!r}"
                )
            suggestion_ids.add(decision.suggestion_id)

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> ProgramMemoryAdjudicationRecord:
        raw_decisions = payload.get("decisions")
        if not isinstance(raw_decisions, list):
            raise ValueError("program memory adjudication record requires decisions list")
        if not all(isinstance(item, Mapping) for item in raw_decisions):
            raise ValueError("program memory adjudication decisions must be objects")
        return cls(
            schema_version=_require_text(
                payload,
                "schema_version",
                context="program memory adjudication record",
            ),
            adjudication_id=_require_text(
                payload,
                "adjudication_id",
                context="program memory adjudication record",
            ),
            harvest_id=_require_text(
                payload,
                "harvest_id",
                context="program memory adjudication record",
            ),
            reviewer=_require_text(
                payload,
                "reviewer",
                context="program memory adjudication record",
            ),
            reviewed_at=clean_text(str(payload.get("reviewed_at") or "")),
            notes=clean_text(str(payload.get("notes") or "")),
            decisions=tuple(
                ProgramMemoryAdjudicationDecision.from_dict(item)
                for item in raw_decisions
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "adjudication_id": self.adjudication_id,
            "harvest_id": self.harvest_id,
            "reviewer": self.reviewer,
            "reviewed_at": self.reviewed_at,
            "notes": self.notes,
            "decisions": [decision.to_dict() for decision in self.decisions],
        }


@dataclass(frozen=True)
class ProgramMemoryAdjudicationOutcome:
    adjudication_id: str
    harvest_id: str
    reviewer: str
    reviewed_at: str
    adjudicated_suggestions: tuple[ProgramMemorySuggestion, ...]
    rejected_suggestion_ids: tuple[str, ...]
    pending_suggestion_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "adjudication_id": self.adjudication_id,
            "harvest_id": self.harvest_id,
            "reviewer": self.reviewer,
            "reviewed_at": self.reviewed_at,
            "accepted_suggestion_ids": [
                suggestion.suggestion_id for suggestion in self.adjudicated_suggestions
            ],
            "rejected_suggestion_ids": list(self.rejected_suggestion_ids),
            "pending_suggestion_ids": list(self.pending_suggestion_ids),
            "accepted_event_count": len(
                [
                    suggestion
                    for suggestion in self.adjudicated_suggestions
                    if suggestion.suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION
                ]
            ),
            "accepted_directionality_count": len(
                [
                    suggestion
                    for suggestion in self.adjudicated_suggestions
                    if suggestion.suggestion_kind
                    == PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION
                ]
            ),
        }


def build_program_memory_adjudication_record(
    *,
    adjudication_id: str,
    harvest_id: str,
    reviewer: str,
    reviewed_at: str,
    decision_payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    notes: str = "",
) -> ProgramMemoryAdjudicationRecord:
    return ProgramMemoryAdjudicationRecord(
        schema_version=PROGRAM_MEMORY_ADJUDICATION_SCHEMA_VERSION,
        adjudication_id=clean_text(adjudication_id),
        harvest_id=clean_text(harvest_id),
        reviewer=clean_text(reviewer),
        reviewed_at=clean_text(reviewed_at),
        notes=clean_text(notes),
        decisions=tuple(
            ProgramMemoryAdjudicationDecision.from_dict(payload)
            for payload in decision_payloads
        ),
    )


def load_program_memory_adjudication_record(
    path: Path,
) -> ProgramMemoryAdjudicationRecord:
    payload = read_json(path)
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"program memory adjudication record must be a JSON object: {path}"
        )
    return ProgramMemoryAdjudicationRecord.from_dict(payload)


def write_program_memory_adjudication_record(
    path: Path,
    adjudication: ProgramMemoryAdjudicationRecord,
) -> None:
    write_json(path, adjudication.to_dict())


def apply_program_memory_adjudication(
    harvest: ProgramMemoryHarvestBatch,
    adjudication: ProgramMemoryAdjudicationRecord,
) -> ProgramMemoryAdjudicationOutcome:
    if adjudication.harvest_id != harvest.harvest_id:
        raise ValueError(
            "program memory adjudication harvest_id does not match harvest batch"
        )
    suggestions_by_id = {
        suggestion.suggestion_id: suggestion for suggestion in harvest.suggestions
    }
    unknown_suggestion_ids = sorted(
        decision.suggestion_id
        for decision in adjudication.decisions
        if decision.suggestion_id not in suggestions_by_id
    )
    if unknown_suggestion_ids:
        raise ValueError(
            "program memory adjudication references unknown suggestion_ids "
            f"{unknown_suggestion_ids}"
        )

    decisions_by_id = {
        decision.suggestion_id: decision for decision in adjudication.decisions
    }
    adjudicated_suggestions: list[ProgramMemorySuggestion] = []
    rejected_suggestion_ids: list[str] = []
    pending_suggestion_ids: list[str] = []
    for suggestion in harvest.suggestions:
        decision = decisions_by_id.get(suggestion.suggestion_id)
        if decision is None:
            pending_suggestion_ids.append(suggestion.suggestion_id)
            continue
        if decision.decision == PROGRAM_MEMORY_REJECT_DECISION:
            rejected_suggestion_ids.append(suggestion.suggestion_id)
            continue
        if decision.decision == PROGRAM_MEMORY_ACCEPT_DECISION:
            adjudicated_suggestions.append(suggestion)
            continue
        adjudicated_suggestions.append(_apply_edit_decision(suggestion, decision))
    return ProgramMemoryAdjudicationOutcome(
        adjudication_id=adjudication.adjudication_id,
        harvest_id=adjudication.harvest_id,
        reviewer=adjudication.reviewer,
        reviewed_at=adjudication.reviewed_at,
        adjudicated_suggestions=tuple(adjudicated_suggestions),
        rejected_suggestion_ids=tuple(rejected_suggestion_ids),
        pending_suggestion_ids=tuple(pending_suggestion_ids),
    )


def _apply_edit_decision(
    suggestion: ProgramMemorySuggestion,
    decision: ProgramMemoryAdjudicationDecision,
) -> ProgramMemorySuggestion:
    if suggestion.suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION:
        if decision.asset is None or decision.event is None or decision.provenance is None:
            raise ValueError(
                f"edit decision for event suggestion {suggestion.suggestion_id!r} "
                "must include asset, event, and provenance"
            )
        return replace(
            suggestion,
            asset=decision.asset,
            event=decision.event,
            provenance=decision.provenance,
        )
    if decision.directionality_hypothesis is None:
        raise ValueError(
            "edit decision for directionality suggestion must include "
            "directionality_hypothesis"
        )
    return replace(
        suggestion,
        directionality_hypothesis=decision.directionality_hypothesis,
    )


def materialize_adjudicated_program_memory_dataset(
    outcome: ProgramMemoryAdjudicationOutcome,
) -> ProgramMemoryDataset:
    assets_by_id: dict[str, ProgramMemoryAsset] = {}
    events_by_id: dict[str, ProgramMemoryEvent] = {}
    provenances_by_event_id: dict[str, ProgramMemoryProvenance] = {}
    hypotheses_by_id: dict[str, ProgramMemoryDirectionalityHypothesis] = {}

    for suggestion in outcome.adjudicated_suggestions:
        if suggestion.suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION:
            assert suggestion.asset is not None
            assert suggestion.event is not None
            assert suggestion.provenance is not None
            _store_unique(
                assets_by_id,
                suggestion.asset.asset_id,
                suggestion.asset,
                label="asset_id",
            )
            _store_unique(
                events_by_id,
                suggestion.event.event_id,
                suggestion.event,
                label="event_id",
            )
            _store_unique(
                provenances_by_event_id,
                suggestion.provenance.event_id,
                suggestion.provenance,
                label="event_id",
            )
            continue
        assert suggestion.directionality_hypothesis is not None
        _store_unique(
            hypotheses_by_id,
            suggestion.directionality_hypothesis.hypothesis_id,
            suggestion.directionality_hypothesis,
            label="hypothesis_id",
        )

    _validate_directionality_support(
        hypotheses=tuple(hypotheses_by_id.values()),
        accepted_event_ids=set(events_by_id),
    )

    ordered_assets = tuple(assets_by_id[key] for key in sorted(assets_by_id))
    ordered_events = tuple(
        event
        for event in sorted(
            events_by_id.values(),
            key=lambda item: (item.sort_order, item.event_date, item.event_id),
        )
    )
    ordered_provenances = tuple(
        provenances_by_event_id[event.event_id] for event in ordered_events
    )
    ordered_hypotheses = tuple(
        hypothesis
        for hypothesis in sorted(
            hypotheses_by_id.values(),
            key=lambda item: (item.sort_order, item.entity_label.lower(), item.hypothesis_id),
        )
    )
    return ProgramMemoryDataset(
        assets=ordered_assets,
        events=ordered_events,
        provenances=ordered_provenances,
        directionality_hypotheses=ordered_hypotheses,
    )


def _store_unique(
    existing: dict[str, Any],
    key: str,
    value: Any,
    *,
    label: str,
) -> None:
    prior_value = existing.get(key)
    if prior_value is None:
        existing[key] = value
        return
    if prior_value != value:
        raise ValueError(f"conflicting adjudicated rows for {label} {key!r}")


def _validate_directionality_support(
    *,
    hypotheses: tuple[ProgramMemoryDirectionalityHypothesis, ...],
    accepted_event_ids: set[str],
) -> None:
    invalid_support: list[str] = []
    for hypothesis in hypotheses:
        missing_event_ids = [
            event_id
            for event_id in hypothesis.supporting_event_ids
            if event_id not in accepted_event_ids
        ]
        if not missing_event_ids:
            continue
        invalid_support.append(
            f"{hypothesis.hypothesis_id!r} -> {missing_event_ids!r}"
        )
    if invalid_support:
        raise ValueError(
            "adjudicated directionality hypotheses reference supporting_event_ids "
            f"that were not accepted or edited: {', '.join(invalid_support)}"
        )


def write_adjudicated_program_memory_dataset(
    output_dir: Path,
    dataset: ProgramMemoryDataset,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(
        output_dir / "assets.csv",
        [_asset_row(asset) for asset in dataset.assets],
        PROGRAM_MEMORY_V2_ASSET_FIELDNAMES,
    )
    write_csv(
        output_dir / "events.csv",
        [_event_row(event) for event in dataset.events],
        PROGRAM_MEMORY_V2_EVENT_FIELDNAMES,
    )
    write_csv(
        output_dir / "event_provenance.csv",
        [_provenance_row(provenance) for provenance in dataset.provenances],
        PROGRAM_MEMORY_V2_PROVENANCE_FIELDNAMES,
    )
    write_csv(
        output_dir / "directionality_hypotheses.csv",
        [
            _directionality_hypothesis_row(hypothesis)
            for hypothesis in dataset.directionality_hypotheses
        ],
        PROGRAM_MEMORY_V2_DIRECTIONALITY_FIELDNAMES,
    )


def _asset_row(asset: ProgramMemoryAsset) -> dict[str, object]:
    return {
        "asset_id": asset.asset_id,
        "molecule": asset.molecule,
        "target": asset.target,
        "target_symbols_json": encode_string_list(asset.target_symbols),
        "target_class": asset.target_class,
        "mechanism": asset.mechanism,
        "modality": asset.modality,
    }


def _event_row(event: ProgramMemoryEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "asset_id": event.asset_id,
        "sponsor": event.sponsor,
        "population": event.population,
        "domain": event.domain,
        "mono_or_adjunct": event.mono_or_adjunct,
        "phase": event.phase,
        "event_type": event.event_type,
        "event_date": event.event_date,
        "primary_outcome_result": event.primary_outcome_result,
        "failure_reason_taxonomy": event.failure_reason_taxonomy,
        "confidence": event.confidence,
        "notes": event.notes,
        "sort_order": event.sort_order,
    }


def _provenance_row(provenance: ProgramMemoryProvenance) -> dict[str, str]:
    return {
        "event_id": provenance.event_id,
        "source_tier": provenance.source_tier,
        "source_url": provenance.source_url,
    }


def _directionality_hypothesis_row(
    hypothesis: ProgramMemoryDirectionalityHypothesis,
) -> dict[str, object]:
    return {
        "hypothesis_id": hypothesis.hypothesis_id,
        "entity_id": hypothesis.entity_id,
        "entity_label": hypothesis.entity_label,
        "desired_perturbation_direction": hypothesis.desired_perturbation_direction,
        "modality_hypothesis": hypothesis.modality_hypothesis,
        "preferred_modalities_json": encode_string_list(hypothesis.preferred_modalities),
        "confidence": hypothesis.confidence,
        "ambiguity": hypothesis.ambiguity,
        "evidence_basis": hypothesis.evidence_basis,
        "supporting_event_ids_json": encode_string_list(hypothesis.supporting_event_ids),
        "contradiction_conditions_json": encode_string_list(
            hypothesis.contradiction_conditions
        ),
        "falsification_conditions_json": encode_string_list(
            hypothesis.falsification_conditions
        ),
        "open_risks_json": encode_string_list(hypothesis.open_risks),
        "sort_order": hypothesis.sort_order,
    }


def write_program_memory_adjudication_outputs(
    output_dir: Path,
    adjudication: ProgramMemoryAdjudicationRecord,
    outcome: ProgramMemoryAdjudicationOutcome,
) -> ProgramMemoryDataset:
    dataset = materialize_adjudicated_program_memory_dataset(outcome)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_program_memory_adjudication_record(
        output_dir / "adjudication_record.json",
        adjudication,
    )
    write_json(output_dir / "adjudication_summary.json", outcome.to_dict())
    write_adjudicated_program_memory_dataset(output_dir / "proposed_v2", dataset)
    return dataset
