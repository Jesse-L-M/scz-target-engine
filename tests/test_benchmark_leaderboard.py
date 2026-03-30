from __future__ import annotations

from pathlib import Path

from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_leaderboard import (
    LEADERBOARD_SCHEMA_NAME,
    REPORT_CARD_SCHEMA_NAME,
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)


def test_materialize_benchmark_reporting_emits_fixture_report_cards_and_leaderboards(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_output_dir = tmp_path / "public_payloads"

    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    benchmark_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-03-28T12:00:00Z",
    )

    assert reporting_result["benchmark_suite_id"] == "scz_translational_suite"
    assert reporting_result["benchmark_task_id"] == "scz_translational_task"
    assert reporting_result["snapshot_id"] == "scz_fixture_2024_06_30"
    assert len(reporting_result["report_card_files"]) == len(
        benchmark_result["run_manifest_files"]
    )
    assert reporting_result["leaderboard_payload_files"]

    v1_report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "v1_current" in path
    )
    assert str(v1_report_card_path.parent).endswith(
        "report_cards/scz_translational_suite/scz_translational_task/"
        "scz_fixture_2024_06_30"
    )

    v1_report_card = read_benchmark_report_card_payload(v1_report_card_path)
    assert v1_report_card.schema_name == REPORT_CARD_SCHEMA_NAME
    assert v1_report_card.benchmark_suite_id == "scz_translational_suite"
    assert v1_report_card.benchmark_task_id == "scz_translational_task"
    assert v1_report_card.baseline_id == "v1_current"
    assert v1_report_card.run_parameterization is not None
    assert v1_report_card.run_parameterization["bootstrap_iterations"] == 25
    assert {
        artifact.artifact_name for artifact in v1_report_card.evaluation_input_artifacts
    } >= {
        "benchmark_snapshot_manifest",
        "benchmark_cohort_labels",
        "engine_config",
    }
    assert {
        artifact.artifact_name for artifact in v1_report_card.derived_from_artifacts
    } >= {
        "benchmark_model_run_manifest",
        "benchmark_metric_output_payload",
        "benchmark_confidence_interval_payload",
    }
    assert len(v1_report_card.source_snapshots) == 5

    gene_three_year_slice = next(
        slice_report
        for slice_report in v1_report_card.slices
        if slice_report.entity_type == "gene" and slice_report.horizon == "3y"
    )
    assert gene_three_year_slice.covered_entity_count == 2
    assert gene_three_year_slice.admissible_entity_count == 2
    assert gene_three_year_slice.positive_entity_count == 1
    assert {
        metric.metric_name for metric in gene_three_year_slice.metrics
    } == {
        "average_precision_any_positive_outcome",
        "mean_reciprocal_rank_any_positive_outcome",
        "precision_at_1_any_positive_outcome",
        "precision_at_3_any_positive_outcome",
        "precision_at_5_any_positive_outcome",
        "recall_at_1_any_positive_outcome",
        "recall_at_3_any_positive_outcome",
        "recall_at_5_any_positive_outcome",
    }
    assert {
        metric.interval_method for metric in gene_three_year_slice.metrics
    } == {"percentile_bootstrap"}

    leaderboard_path = (
        reporting_output_dir
        / "leaderboards"
        / "scz_translational_suite"
        / "scz_translational_task"
        / "scz_fixture_2024_06_30"
        / "gene"
        / "3y"
        / "average_precision_any_positive_outcome.json"
    )
    leaderboard_payload = read_benchmark_leaderboard_payload(leaderboard_path)
    assert leaderboard_payload.schema_name == LEADERBOARD_SCHEMA_NAME
    assert leaderboard_payload.metric_name == "average_precision_any_positive_outcome"
    assert leaderboard_payload.entity_type == "gene"
    assert leaderboard_payload.horizon == "3y"
    assert leaderboard_payload.bootstrap_iterations == 25
    assert leaderboard_payload.interval_method == "percentile_bootstrap"
    assert {entry.baseline_id for entry in leaderboard_payload.entries} == {
        "pgc_only",
        "opentargets_only",
        "v0_current",
        "v1_current",
        "random_with_coverage",
    }
    assert leaderboard_payload.entries[0].metric_value >= leaderboard_payload.entries[-1].metric_value
    assert leaderboard_payload.entries[0].rank == 1
    assert leaderboard_payload.entries[-1].rank == len(leaderboard_payload.entries)
    assert all(
        Path(entry.report_card_path).exists()
        for entry in leaderboard_payload.entries
    )
