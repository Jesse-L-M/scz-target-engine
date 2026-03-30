from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

from scz_target_engine.benchmark_backfill import (
    materialize_public_benchmark_slices,
    plan_public_benchmark_slices,
)
from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.cli import main


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)


def _sha256_for_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _write_sparse_custom_registry_fixture(
    tmp_path: Path,
) -> tuple[Path, Path]:
    fixture_dir = tmp_path / "fixture"
    archives_dir = fixture_dir / "archives" / "pgc"
    archives_dir.mkdir(parents=True)

    archive_contents = (
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.9\n"
    )
    archive_file = archives_dir / "pgc_fixture.csv"
    archive_file.write_text(archive_contents, encoding="utf-8")

    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(
            {
                "snapshot_id": "scz_fixture_2024_06_20",
                "cohort_id": "scz_fixture_small",
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "sparse_fixture_task",
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2029-06-30",
                "entity_types": ["gene"],
                "baseline_ids": ["pgc_only", "random_with_coverage"],
                "notes": "Sparse archive fixture",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        "entity_type,entity_id,entity_label\n"
        "gene,GENE_A,Gene A\n",
        encoding="utf-8",
    )
    (fixture_dir / "future_outcomes.csv").write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
        "gene,GENE_A,future_schizophrenia_program_started,2024-12-01,fixture,Fixture outcome\n",
        encoding="utf-8",
    )
    (fixture_dir / "source_archives.json").write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": "archives/pgc/pgc_fixture.csv",
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": _sha256_for_text(archive_contents),
                        "notes": "Sparse archive fixture.",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    task_registry_path = tmp_path / "task_registry.csv"
    task_registry_path.write_text(
        (
            "suite_id,suite_label,task_id,task_label,protocol_id,benchmark_question_id,"
            "entity_types,supported_baseline_ids,emitted_artifact_names,"
            "fixture_snapshot_request_file,fixture_cohort_members_file,"
            "fixture_future_outcomes_file,fixture_archive_index_file,notes\n"
            "scz_translational_suite,Schizophrenia Translational Benchmark Suite,"
            "sparse_fixture_task,Sparse fixture task,frozen_benchmark_protocol_v1,"
            "scz_translational_ranking_v1,gene,pgc_only|random_with_coverage,"
            "benchmark_snapshot_manifest|benchmark_cohort_labels|benchmark_model_run_manifest|"
            "benchmark_metric_output_payload|benchmark_confidence_interval_payload,"
            f"{fixture_dir / 'snapshot_request.json'},"
            f"{fixture_dir / 'cohort_members.csv'},"
            f"{fixture_dir / 'future_outcomes.csv'},"
            f"{fixture_dir / 'source_archives.json'},"
            "Sparse public-slice coverage fixture.\n"
        ),
        encoding="utf-8",
    )
    return task_registry_path, fixture_dir


def test_plan_public_benchmark_slices_discovers_honest_fixture_cutoffs() -> None:
    plan = plan_public_benchmark_slices(
        benchmark_task_id="scz_translational_task"
    )

    assert plan.benchmark_suite_id == "scz_translational_suite"
    assert plan.benchmark_task_id == "scz_translational_task"
    assert plan.coverage_limitation == ""
    assert [slice_spec.slice_id for slice_spec in plan.slices] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
    ]
    assert [slice_spec.included_sources for slice_spec in plan.slices] == [
        ("PGC",),
        ("PGC", "PsychENCODE"),
        ("PGC", "PsychENCODE", "Open Targets"),
    ]
    earliest_slice_exclusions = {
        source_status.source_name: source_status.exclusion_reason
        for source_status in plan.slices[0].excluded_sources
    }
    assert (
        "no archived release descriptor available on or before 2024-06-15"
        in earliest_slice_exclusions["Open Targets"]
    )
    assert (
        "no archived release descriptor available on or before 2024-06-15"
        in earliest_slice_exclusions["ChEMBL"]
    )


def test_early_public_slice_excludes_post_cutoff_archive_entries_and_files(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "public_slices"
    result = materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="scz_translational_task",
    )

    assert result["public_slice_ids"] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
    ]
    catalog_file = output_dir / "catalog.json"
    assert catalog_file.exists()

    early_slice_dir = output_dir / "scz_translational_2024_06_15"
    early_archive_payload = json.loads(
        (early_slice_dir / "source_archives.json").read_text(encoding="utf-8")
    )
    assert [archive["source_name"] for archive in early_archive_payload["archives"]] == [
        "PGC"
    ]
    assert (early_slice_dir / "archives" / "pgc" / "scz2022_fixture.csv").exists()
    assert not (early_slice_dir / "archives" / "opentargets").exists()
    assert not (early_slice_dir / "archives" / "psychencode").exists()
    assert not (early_slice_dir / "archives" / "chembl").exists()

    slice_dir = output_dir / "scz_translational_2024_06_20"
    assert (slice_dir / "snapshot_request.json").exists()
    assert (slice_dir / "source_archives.json").exists()
    assert (slice_dir / "cohort_members.csv").exists()
    assert (slice_dir / "future_outcomes.csv").exists()
    assert (slice_dir / "archives" / "pgc" / "scz2022_fixture.csv").exists()

    generated_dir = tmp_path / "generated"
    snapshot_manifest_file = generated_dir / "snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=slice_dir / "snapshot_request.json",
        archive_index_file=slice_dir / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-30",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    assert manifest.snapshot_id == "scz_translational_2024_06_20"
    assert manifest.as_of_date == "2024-06-20"

    cohort_labels_file = generated_dir / "cohort_labels.csv"
    materialize_benchmark_cohort_labels(
        manifest=manifest,
        cohort_members_file=slice_dir / "cohort_members.csv",
        future_outcomes_file=slice_dir / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    run_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=slice_dir / "source_archives.json",
        output_dir=generated_dir / "runner_outputs",
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-03-30T00:00:00Z",
    )

    assert run_result["snapshot_id"] == "scz_translational_2024_06_20"
    assert run_result["executed_baselines"] == [
        "pgc_only",
        "opentargets_only",
        "v0_current",
        "v1_current",
        "random_with_coverage",
    ]
    assert run_result["run_manifest_files"]


def test_public_slice_builder_reports_sparse_archive_coverage_limitation(
    tmp_path: Path,
) -> None:
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(tmp_path)

    result = materialize_public_benchmark_slices(
        output_dir=tmp_path / "public_slices",
        benchmark_task_id="sparse_fixture_task",
        task_registry_path=task_registry_path,
    )

    assert result["public_slice_ids"] == ["sparse_fixture_2024_06_15"]
    assert (
        "Archived descriptor coverage is too sparse for multiple public slices"
        in result["coverage_limitation"]
    )


def test_custom_registry_slice_round_trips_through_emitted_slice_artifacts(
    tmp_path: Path,
) -> None:
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(tmp_path)
    output_dir = tmp_path / "public_slices"
    materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="sparse_fixture_task",
        task_registry_path=task_registry_path,
    )

    slice_dir = output_dir / "sparse_fixture_2024_06_15"
    snapshot_request_payload = json.loads(
        (slice_dir / "snapshot_request.json").read_text(encoding="utf-8")
    )
    assert snapshot_request_payload["task_registry_path"] == str(
        task_registry_path.resolve()
    )

    generated_dir = tmp_path / "generated"
    snapshot_manifest_file = generated_dir / "snapshot_manifest.json"
    cohort_labels_file = generated_dir / "cohort_labels.csv"
    runner_output_dir = generated_dir / "runner_outputs"

    assert (
        main(
            [
                "build-benchmark-snapshot",
                "--request-file",
                str(slice_dir / "snapshot_request.json"),
                "--archive-index-file",
                str(slice_dir / "source_archives.json"),
                "--output-file",
                str(snapshot_manifest_file),
                "--materialized-at",
                "2026-03-30",
            ]
        )
        == 0
    )
    snapshot_manifest_payload = json.loads(
        snapshot_manifest_file.read_text(encoding="utf-8")
    )
    assert snapshot_manifest_payload["task_registry_path"] == str(
        task_registry_path.resolve()
    )

    assert (
        main(
            [
                "build-benchmark-cohort",
                "--manifest-file",
                str(snapshot_manifest_file),
                "--cohort-members-file",
                str(slice_dir / "cohort_members.csv"),
                "--future-outcomes-file",
                str(slice_dir / "future_outcomes.csv"),
                "--output-file",
                str(cohort_labels_file),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "run-benchmark",
                "--manifest-file",
                str(snapshot_manifest_file),
                "--cohort-labels-file",
                str(cohort_labels_file),
                "--archive-index-file",
                str(slice_dir / "source_archives.json"),
                "--output-dir",
                str(runner_output_dir),
                "--config",
                str(Path("config/v0.toml").resolve()),
                "--deterministic-test-mode",
            ]
        )
        == 0
    )
    assert list((runner_output_dir / "run_manifests").glob("*.json"))
