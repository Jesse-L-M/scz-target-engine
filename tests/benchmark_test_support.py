from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil

from scz_target_engine.benchmark_intervention_objects import (
    build_intervention_object_public_slice_rows,
)
from scz_target_engine.io import write_csv


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)

INTERVENTION_OBJECT_FIXTURE_SNAPSHOT_ID = "synthetic_intervention_object_2024_06_20"
INTERVENTION_OBJECT_FIXTURE_COHORT_ID = (
    "synthetic_intervention_object_2024_06_20_cohort"
)


@dataclass(frozen=True)
class InterventionObjectSliceFixture:
    snapshot_request_file: Path
    source_archives_file: Path
    cohort_members_file: Path
    future_outcomes_file: Path
    program_universe_file: Path
    program_history_events_file: Path
    snapshot_id: str
    cohort_id: str


def _write_local_archive_fixture(fixture_dir: Path) -> Path:
    source_archives_payload = json.loads(
        (FIXTURE_DIR / "source_archives.json").read_text(encoding="utf-8")
    )
    archives_dir = fixture_dir / "archives"
    for archive in source_archives_payload.get("archives", []):
        archive_file = Path(str(archive["archive_file"]))
        source_path = (FIXTURE_DIR / archive_file).resolve()
        destination_path = (fixture_dir / archive_file).resolve()
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
    source_archives_file = fixture_dir / "source_archives.json"
    source_archives_file.write_text(
        json.dumps(source_archives_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return source_archives_file


def write_intervention_object_slice_fixture(
    tmp_path: Path,
) -> InterventionObjectSliceFixture:
    fixture_dir = tmp_path / "synthetic_intervention_object_slice"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    source_archives_file = _write_local_archive_fixture(fixture_dir)
    program_universe_file = fixture_dir / "program_universe.csv"
    program_history_events_file = fixture_dir / "events.csv"
    shutil.copy2(
        (
            Path(__file__).resolve().parents[1]
            / "data"
            / "curated"
            / "program_history"
            / "v2"
            / "program_universe.csv"
        ),
        program_universe_file,
    )
    shutil.copy2(
        (
            Path(__file__).resolve().parents[1]
            / "data"
            / "curated"
            / "program_history"
            / "v2"
            / "events.csv"
        ),
        program_history_events_file,
    )

    snapshot_request_file = fixture_dir / "snapshot_request.json"
    snapshot_request_file.write_text(
        json.dumps(
            {
                "snapshot_id": INTERVENTION_OBJECT_FIXTURE_SNAPSHOT_ID,
                "cohort_id": INTERVENTION_OBJECT_FIXTURE_COHORT_ID,
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "scz_translational_task",
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2025-06-30",
                "entity_types": ["intervention_object"],
                "baseline_ids": ["v0_current", "v1_current", "random_with_coverage"],
                "program_universe_file": program_universe_file.name,
                "program_history_events_file": program_history_events_file.name,
                "notes": "Synthetic intervention-object benchmark slice for tests.",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    cohort_rows, _future_outcome_rows = build_intervention_object_public_slice_rows(
        as_of_date="2024-06-20",
        outcome_observation_closed_at="2025-06-30",
        program_universe_path=program_universe_file,
        events_path=program_history_events_file,
    )
    cohort_members_file = fixture_dir / "cohort_members.csv"
    write_csv(
        cohort_members_file,
        cohort_rows,
        ["entity_type", "entity_id", "entity_label"],
    )
    ulotaront_entity_id = next(
        row["entity_id"]
        for row in cohort_rows
        if str(row["entity_label"]).startswith("ulotaront | ")
    )

    future_outcomes_file = fixture_dir / "future_outcomes.csv"
    write_csv(
        future_outcomes_file,
        [
            {
                "entity_type": "intervention_object",
                "entity_id": ulotaront_entity_id,
                "outcome_label": "future_schizophrenia_positive_signal",
                "outcome_date": "2024-10-01",
                "label_source": "fixture_program_history",
                "label_notes": (
                    "Synthetic positive outcome for intervention-object benchmark tests."
                ),
            }
        ],
        [
            "entity_type",
            "entity_id",
            "outcome_label",
            "outcome_date",
            "label_source",
            "label_notes",
        ],
    )

    return InterventionObjectSliceFixture(
        snapshot_request_file=snapshot_request_file,
        source_archives_file=source_archives_file,
        cohort_members_file=cohort_members_file,
        future_outcomes_file=future_outcomes_file,
        program_universe_file=program_universe_file,
        program_history_events_file=program_history_events_file,
        snapshot_id=INTERVENTION_OBJECT_FIXTURE_SNAPSHOT_ID,
        cohort_id=INTERVENTION_OBJECT_FIXTURE_COHORT_ID,
    )
