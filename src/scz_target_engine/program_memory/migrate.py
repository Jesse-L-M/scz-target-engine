from __future__ import annotations

from pathlib import Path

from scz_target_engine.io import read_csv_rows
from scz_target_engine.program_memory._helpers import (
    clean_text,
    default_asset_lineage_id,
    default_target_class_lineage_id,
    parse_string_list,
    slugify,
    split_target_symbols,
)
from scz_target_engine.program_memory.models import (
    ProgramMemoryAsset,
    ProgramMemoryDataset,
    ProgramMemoryDirectionalityHypothesis,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
)


def _ensure_unique_id(base_id: str, used_ids: set[str], *, fallback_prefix: str) -> str:
    candidate = base_id or f"{fallback_prefix}-{len(used_ids) + 1}"
    suffix = 2
    while candidate in used_ids:
        candidate = f"{base_id}-{suffix}" if base_id else f"{fallback_prefix}-{suffix}"
        suffix += 1
    used_ids.add(candidate)
    return candidate


def migrate_legacy_program_memory(
    program_history_rows: list[dict[str, str]],
    directionality_rows: list[dict[str, str]],
) -> ProgramMemoryDataset:
    assets: list[ProgramMemoryAsset] = []
    events: list[ProgramMemoryEvent] = []
    provenances: list[ProgramMemoryProvenance] = []
    hypotheses: list[ProgramMemoryDirectionalityHypothesis] = []

    asset_ids_by_key: dict[tuple[str, str, str, str, str], str] = {}
    used_asset_ids: set[str] = set()
    for index, row in enumerate(program_history_rows, start=1):
        molecule = clean_text(row.get("molecule"))
        target = clean_text(row.get("target"))
        target_class = clean_text(row.get("target_class"))
        mechanism = clean_text(row.get("mechanism"))
        modality = clean_text(row.get("modality"))
        asset_key = (molecule, target, target_class, mechanism, modality)
        asset_id = asset_ids_by_key.get(asset_key)
        if asset_id is None:
            asset_id = _ensure_unique_id(
                slugify(molecule),
                used_asset_ids,
                fallback_prefix="asset",
            )
            asset_ids_by_key[asset_key] = asset_id
            assets.append(
                ProgramMemoryAsset(
                    asset_id=asset_id,
                    molecule=molecule,
                    target=target,
                    target_symbols=split_target_symbols(target),
                    target_class=target_class,
                    mechanism=mechanism,
                    modality=modality,
                    asset_lineage_id=default_asset_lineage_id(asset_id, molecule),
                    target_class_lineage_id=default_target_class_lineage_id(
                        target_class
                    ),
                )
            )

        event_id = clean_text(row.get("program_id")) or f"event-{index}"
        events.append(
            ProgramMemoryEvent(
                event_id=event_id,
                asset_id=asset_id,
                sponsor=clean_text(row.get("sponsor")),
                population=clean_text(row.get("population")),
                domain=clean_text(row.get("domain")),
                mono_or_adjunct=clean_text(row.get("mono_or_adjunct")),
                phase=clean_text(row.get("phase")),
                event_type=clean_text(row.get("event_type")),
                event_date=clean_text(row.get("date")),
                primary_outcome_result=clean_text(row.get("primary_outcome_result")),
                failure_reason_taxonomy=clean_text(row.get("failure_reason_taxonomy")),
                confidence=clean_text(row.get("confidence")).lower(),
                notes=clean_text(row.get("notes")),
                sort_order=index,
            )
        )
        provenances.append(
            ProgramMemoryProvenance(
                event_id=event_id,
                source_tier=clean_text(row.get("source_tier")),
                source_url=clean_text(row.get("source_url")),
            )
        )

    used_hypothesis_ids: set[str] = set()
    for index, row in enumerate(directionality_rows, start=1):
        seed = clean_text(row.get("entity_id")) or clean_text(row.get("entity_label"))
        hypothesis_id = _ensure_unique_id(
            slugify(seed),
            used_hypothesis_ids,
            fallback_prefix="hypothesis",
        )
        hypotheses.append(
            ProgramMemoryDirectionalityHypothesis(
                hypothesis_id=hypothesis_id,
                entity_id=clean_text(row.get("entity_id")),
                entity_label=clean_text(row.get("entity_label")),
                desired_perturbation_direction=clean_text(
                    row.get("desired_perturbation_direction")
                )
                or "undetermined",
                modality_hypothesis=clean_text(row.get("modality_hypothesis"))
                or "undetermined",
                preferred_modalities=parse_string_list(
                    row.get("preferred_modalities_json")
                ),
                confidence=(clean_text(row.get("confidence")) or "low").lower(),
                ambiguity=clean_text(row.get("ambiguity")),
                evidence_basis=clean_text(row.get("evidence_basis")),
                supporting_event_ids=parse_string_list(
                    row.get("supporting_program_ids_json")
                ),
                contradiction_conditions=parse_string_list(
                    row.get("contradiction_conditions_json")
                ),
                falsification_conditions=parse_string_list(
                    row.get("falsification_conditions_json")
                ),
                open_risks=parse_string_list(row.get("open_risks_json")),
                sort_order=index,
            )
        )

    return ProgramMemoryDataset(
        assets=tuple(assets),
        events=tuple(events),
        provenances=tuple(provenances),
        directionality_hypotheses=tuple(hypotheses),
    )


def migrate_legacy_program_memory_files(
    program_history_path: Path,
    directionality_hypotheses_path: Path,
) -> ProgramMemoryDataset:
    return migrate_legacy_program_memory(
        read_csv_rows(program_history_path),
        read_csv_rows(directionality_hypotheses_path),
    )
