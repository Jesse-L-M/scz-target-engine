from __future__ import annotations

from pathlib import Path

from scz_target_engine.io import read_csv_rows
from scz_target_engine.program_memory._helpers import (
    clean_text,
    parse_int,
    parse_string_list,
    split_target_symbols,
)
from scz_target_engine.program_memory.models import (
    DirectionalityHypothesis,
    ProgramHistoryEvent,
    ProgramMemoryAsset,
    ProgramMemoryDataset,
    ProgramMemoryDirectionalityHypothesis,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
)


PROGRAM_MEMORY_V2_FILENAMES = (
    "assets.csv",
    "events.csv",
    "event_provenance.csv",
    "directionality_hypotheses.csv",
)


def _has_program_memory_v2_files(directory: Path) -> bool:
    return all((directory / name).exists() for name in PROGRAM_MEMORY_V2_FILENAMES)


def resolve_program_memory_v2_dir(path: Path) -> Path | None:
    candidates: list[Path] = []
    if path.exists() and path.is_dir():
        candidates.append(path)
    candidates.append(path.parent)
    candidates.append(path.parent / "v2")
    for candidate in candidates:
        if _has_program_memory_v2_files(candidate):
            return candidate
    return None


def load_program_history_legacy_rows(path: Path) -> list[dict[str, str]]:
    return read_csv_rows(path)


def load_directionality_hypotheses_legacy_rows(path: Path) -> list[dict[str, str]]:
    return read_csv_rows(path)


def parse_legacy_program_history_rows(
    rows: list[dict[str, str]],
) -> list[ProgramHistoryEvent]:
    events: list[ProgramHistoryEvent] = []
    for row in rows:
        target = clean_text(row.get("target"))
        events.append(
            ProgramHistoryEvent(
                program_id=clean_text(row.get("program_id")),
                sponsor=clean_text(row.get("sponsor")),
                molecule=clean_text(row.get("molecule")),
                target=target,
                target_symbols=split_target_symbols(target),
                target_class=clean_text(row.get("target_class")),
                mechanism=clean_text(row.get("mechanism")),
                modality=clean_text(row.get("modality")),
                population=clean_text(row.get("population")),
                domain=clean_text(row.get("domain")),
                mono_or_adjunct=clean_text(row.get("mono_or_adjunct")),
                phase=clean_text(row.get("phase")),
                event_type=clean_text(row.get("event_type")),
                date=clean_text(row.get("date")),
                primary_outcome_result=clean_text(row.get("primary_outcome_result")),
                failure_reason_taxonomy=clean_text(row.get("failure_reason_taxonomy")),
                source_tier=clean_text(row.get("source_tier")),
                source_url=clean_text(row.get("source_url")),
                confidence=clean_text(row.get("confidence")).lower(),
                notes=clean_text(row.get("notes")),
            )
        )
    return events


def parse_legacy_directionality_hypothesis_rows(
    rows: list[dict[str, str]],
) -> list[DirectionalityHypothesis]:
    hypotheses: list[DirectionalityHypothesis] = []
    for row in rows:
        entity_label = clean_text(row.get("entity_label"))
        if not entity_label:
            raise ValueError("directionality hypotheses require entity_label")
        hypotheses.append(
            DirectionalityHypothesis(
                entity_id=clean_text(row.get("entity_id")),
                entity_label=entity_label,
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
                supporting_program_ids=parse_string_list(
                    row.get("supporting_program_ids_json")
                ),
                contradiction_conditions=parse_string_list(
                    row.get("contradiction_conditions_json")
                ),
                falsification_conditions=parse_string_list(
                    row.get("falsification_conditions_json")
                ),
                open_risks=parse_string_list(row.get("open_risks_json")),
            )
        )
    return hypotheses


def index_directionality_hypotheses(
    hypotheses: list[DirectionalityHypothesis],
) -> dict[tuple[str, str], DirectionalityHypothesis]:
    indexed: dict[tuple[str, str], DirectionalityHypothesis] = {}
    for hypothesis in hypotheses:
        indexed[(hypothesis.entity_id, hypothesis.entity_label.upper())] = hypothesis
        indexed.setdefault(("", hypothesis.entity_label.upper()), hypothesis)
    return indexed


def load_legacy_program_history(path: Path) -> list[ProgramHistoryEvent]:
    return parse_legacy_program_history_rows(load_program_history_legacy_rows(path))


def load_legacy_directionality_hypotheses(
    path: Path,
) -> dict[tuple[str, str], DirectionalityHypothesis]:
    return index_directionality_hypotheses(
        parse_legacy_directionality_hypothesis_rows(
            load_directionality_hypotheses_legacy_rows(path)
        )
    )


def load_program_memory_dataset(path: Path) -> ProgramMemoryDataset:
    v2_dir = resolve_program_memory_v2_dir(path)
    if v2_dir is None:
        raise FileNotFoundError(f"program memory v2 dataset not found from {path}")

    assets: list[ProgramMemoryAsset] = []
    assets_by_id: dict[str, ProgramMemoryAsset] = {}
    for row in read_csv_rows(v2_dir / "assets.csv"):
        asset_id = clean_text(row.get("asset_id"))
        if not asset_id:
            raise ValueError("program memory assets require asset_id")
        if asset_id in assets_by_id:
            raise ValueError(f"duplicate program memory asset_id {asset_id!r}")
        target = clean_text(row.get("target"))
        asset = ProgramMemoryAsset(
            asset_id=asset_id,
            molecule=clean_text(row.get("molecule")),
            target=target,
            target_symbols=parse_string_list(row.get("target_symbols_json"))
            or split_target_symbols(target),
            target_class=clean_text(row.get("target_class")),
            mechanism=clean_text(row.get("mechanism")),
            modality=clean_text(row.get("modality")),
        )
        assets.append(asset)
        assets_by_id[asset_id] = asset

    events: list[ProgramMemoryEvent] = []
    event_ids: set[str] = set()
    for index, row in enumerate(read_csv_rows(v2_dir / "events.csv"), start=1):
        event_id = clean_text(row.get("event_id"))
        if not event_id:
            raise ValueError("program memory events require event_id")
        if event_id in event_ids:
            raise ValueError(f"duplicate program memory event_id {event_id!r}")
        asset_id = clean_text(row.get("asset_id"))
        if asset_id not in assets_by_id:
            raise ValueError(
                f"program memory event {event_id!r} references unknown asset_id {asset_id!r}"
            )
        event_ids.add(event_id)
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
                event_date=clean_text(row.get("event_date")),
                primary_outcome_result=clean_text(row.get("primary_outcome_result")),
                failure_reason_taxonomy=clean_text(row.get("failure_reason_taxonomy")),
                confidence=clean_text(row.get("confidence")).lower(),
                notes=clean_text(row.get("notes")),
                sort_order=parse_int(row.get("sort_order"), default=index),
            )
        )

    provenances: list[ProgramMemoryProvenance] = []
    provenance_ids: set[str] = set()
    for row in read_csv_rows(v2_dir / "event_provenance.csv"):
        event_id = clean_text(row.get("event_id"))
        if not event_id:
            raise ValueError("program memory provenance requires event_id")
        if event_id not in event_ids:
            raise ValueError(
                f"program memory provenance references unknown event_id {event_id!r}"
            )
        if event_id in provenance_ids:
            raise ValueError(f"duplicate program memory provenance for {event_id!r}")
        provenance_ids.add(event_id)
        provenances.append(
            ProgramMemoryProvenance(
                event_id=event_id,
                source_tier=clean_text(row.get("source_tier")),
                source_url=clean_text(row.get("source_url")),
            )
        )
    if provenance_ids != event_ids:
        missing = sorted(event_ids - provenance_ids)
        raise ValueError(f"program memory provenance missing for event_ids {missing}")

    hypotheses: list[ProgramMemoryDirectionalityHypothesis] = []
    hypothesis_ids: set[str] = set()
    for index, row in enumerate(
        read_csv_rows(v2_dir / "directionality_hypotheses.csv"),
        start=1,
    ):
        hypothesis_id = clean_text(row.get("hypothesis_id"))
        if not hypothesis_id:
            raise ValueError("program memory directionality rows require hypothesis_id")
        if hypothesis_id in hypothesis_ids:
            raise ValueError(
                f"duplicate program memory directionality hypothesis_id {hypothesis_id!r}"
            )
        supporting_event_ids = parse_string_list(row.get("supporting_event_ids_json"))
        unknown_event_ids = [
            event_id
            for event_id in supporting_event_ids
            if event_id not in event_ids
        ]
        if unknown_event_ids:
            raise ValueError(
                f"program memory hypothesis {hypothesis_id!r} references unknown event_ids "
                f"{unknown_event_ids}"
            )
        hypothesis_ids.add(hypothesis_id)
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
                supporting_event_ids=supporting_event_ids,
                contradiction_conditions=parse_string_list(
                    row.get("contradiction_conditions_json")
                ),
                falsification_conditions=parse_string_list(
                    row.get("falsification_conditions_json")
                ),
                open_risks=parse_string_list(row.get("open_risks_json")),
                sort_order=parse_int(row.get("sort_order"), default=index),
            )
        )

    return ProgramMemoryDataset(
        assets=tuple(assets),
        events=tuple(events),
        provenances=tuple(provenances),
        directionality_hypotheses=tuple(hypotheses),
    )
