from __future__ import annotations

from pathlib import Path

from scz_target_engine.program_memory._helpers import encode_string_list
from scz_target_engine.program_memory.loaders import (
    index_directionality_hypotheses,
    load_directionality_hypotheses_legacy_rows,
    load_program_history_legacy_rows,
    load_program_memory_dataset,
    parse_legacy_directionality_hypothesis_rows,
    parse_legacy_program_history_rows,
    resolve_program_memory_v2_dir,
)
from scz_target_engine.program_memory.models import (
    DirectionalityHypothesis,
    ProgramHistoryEvent,
    ProgramMemoryDataset,
)


def materialize_legacy_program_history_rows(
    dataset: ProgramMemoryDataset,
) -> list[dict[str, str]]:
    assets_by_id = {asset.asset_id: asset for asset in dataset.assets}
    provenances_by_event_id = {
        provenance.event_id: provenance
        for provenance in dataset.provenances
    }
    rows: list[dict[str, str]] = []
    ordered_events = sorted(
        dataset.events,
        key=lambda event: (event.sort_order, event.event_date, event.event_id),
    )
    for event in ordered_events:
        asset = assets_by_id[event.asset_id]
        provenance = provenances_by_event_id[event.event_id]
        rows.append(
            {
                "program_id": event.event_id,
                "sponsor": event.sponsor,
                "molecule": asset.molecule,
                "target": asset.target,
                "target_class": asset.target_class,
                "mechanism": asset.mechanism,
                "modality": asset.modality,
                "population": event.population,
                "domain": event.domain,
                "mono_or_adjunct": event.mono_or_adjunct,
                "phase": event.phase,
                "event_type": event.event_type,
                "date": event.event_date,
                "primary_outcome_result": event.primary_outcome_result,
                "failure_reason_taxonomy": event.failure_reason_taxonomy,
                "source_tier": provenance.source_tier,
                "source_url": provenance.source_url,
                "confidence": event.confidence,
                "notes": event.notes,
            }
        )
    return rows


def materialize_legacy_directionality_hypothesis_rows(
    dataset: ProgramMemoryDataset,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    ordered_hypotheses = sorted(
        dataset.directionality_hypotheses,
        key=lambda hypothesis: (
            hypothesis.sort_order,
            hypothesis.entity_label.lower(),
            hypothesis.hypothesis_id,
        ),
    )
    for hypothesis in ordered_hypotheses:
        rows.append(
            {
                "entity_id": hypothesis.entity_id,
                "entity_label": hypothesis.entity_label,
                "desired_perturbation_direction": (
                    hypothesis.desired_perturbation_direction
                ),
                "modality_hypothesis": hypothesis.modality_hypothesis,
                "preferred_modalities_json": encode_string_list(
                    hypothesis.preferred_modalities
                ),
                "confidence": hypothesis.confidence,
                "ambiguity": hypothesis.ambiguity,
                "evidence_basis": hypothesis.evidence_basis,
                "supporting_program_ids_json": encode_string_list(
                    hypothesis.supporting_event_ids
                ),
                "contradiction_conditions_json": encode_string_list(
                    hypothesis.contradiction_conditions
                ),
                "falsification_conditions_json": encode_string_list(
                    hypothesis.falsification_conditions
                ),
                "open_risks_json": encode_string_list(hypothesis.open_risks),
            }
        )
    return rows


def load_program_history_compatibility_view(path: Path) -> list[ProgramHistoryEvent]:
    if resolve_program_memory_v2_dir(path) is not None:
        dataset = load_program_memory_dataset(path)
        return parse_legacy_program_history_rows(
            materialize_legacy_program_history_rows(dataset)
        )
    return parse_legacy_program_history_rows(load_program_history_legacy_rows(path))


def load_directionality_hypotheses_compatibility_view(
    path: Path,
) -> list[DirectionalityHypothesis]:
    if resolve_program_memory_v2_dir(path) is not None:
        dataset = load_program_memory_dataset(path)
        return parse_legacy_directionality_hypothesis_rows(
            materialize_legacy_directionality_hypothesis_rows(dataset)
        )
    return parse_legacy_directionality_hypothesis_rows(
        load_directionality_hypotheses_legacy_rows(path)
    )


def load_directionality_hypotheses_compatibility_index(
    path: Path,
) -> dict[tuple[str, str], DirectionalityHypothesis]:
    return index_directionality_hypotheses(
        load_directionality_hypotheses_compatibility_view(path)
    )
