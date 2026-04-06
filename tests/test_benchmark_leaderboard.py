from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_leaderboard import (
    LEADERBOARD_SCHEMA_NAME,
    REPORT_CARD_SCHEMA_NAME,
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_metrics import RETRIEVAL_METRIC_NAMES
from scz_target_engine.benchmark_metrics import read_benchmark_metric_output_payload
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from tests.benchmark_test_support import write_intervention_object_slice_fixture


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)
TRACK_B_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_failure_memory_2025_02_01"
)


def _materialize_fixture_runner_outputs(
    tmp_path: Path,
) -> tuple[Path, Path, Path, dict[str, object]]:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
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
    return (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        benchmark_result,
    )


def _materialize_track_b_fixture_runner_outputs(
    tmp_path: Path,
) -> tuple[Path, Path, Path, dict[str, object]]:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

    materialize_benchmark_snapshot_manifest(
        request_file=TRACK_B_FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=TRACK_B_FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=TRACK_B_FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    benchmark_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )
    return (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        benchmark_result,
    )


def test_materialize_benchmark_reporting_emits_fixture_report_cards_and_leaderboards(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"

    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        benchmark_result,
    ) = _materialize_fixture_runner_outputs(tmp_path)

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
        "benchmark_cohort_manifest",
        "benchmark_cohort_members",
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
    assert (
        leaderboard_payload.entries[0].metric_value
        >= leaderboard_payload.entries[-1].metric_value
    )
    assert leaderboard_payload.entries[0].rank == 1
    assert leaderboard_payload.entries[-1].rank == len(leaderboard_payload.entries)
    assert all(
        Path(entry.report_card_path).exists()
        for entry in leaderboard_payload.entries
    )


def test_materialize_benchmark_reporting_keeps_track_b_metrics_scoped_per_run(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        benchmark_result,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-04-05T12:00:00Z",
    )

    runner_replay_status_by_baseline = {}
    for metric_path in benchmark_result["metric_payload_files"]:
        metric_payload = read_benchmark_metric_output_payload(Path(metric_path))
        if metric_payload.metric_name != "replay_status_exact_match":
            continue
        runner_replay_status_by_baseline[metric_payload.baseline_id] = (
            metric_payload.metric_value
        )

    assert (
        runner_replay_status_by_baseline["track_b_nearest_history"]
        < runner_replay_status_by_baseline["track_b_structural_current"]
    )

    report_card_replay_status_by_baseline = {}
    for report_card_path in reporting_result["report_card_files"]:
        report_card = read_benchmark_report_card_payload(Path(report_card_path))
        metric_summary = next(
            metric
            for metric in report_card.slices[0].metrics
            if metric.metric_name == "replay_status_exact_match"
        )
        report_card_replay_status_by_baseline[report_card.baseline_id] = (
            metric_summary.metric_value
        )

    replay_status_leaderboard = read_benchmark_leaderboard_payload(
        next(
            Path(path)
            for path in reporting_result["leaderboard_payload_files"]
            if path.endswith("replay_status_exact_match.json")
        )
    )
    leaderboard_replay_status_by_baseline = {
        entry.baseline_id: entry.metric_value
        for entry in replay_status_leaderboard.entries
    }

    for baseline_id in (
        "track_b_nearest_history",
        "track_b_structural_current",
    ):
        assert report_card_replay_status_by_baseline[baseline_id] == (
            runner_replay_status_by_baseline[baseline_id]
        )
        assert leaderboard_replay_status_by_baseline[baseline_id] == (
            runner_replay_status_by_baseline[baseline_id]
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_analog_recall(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    case_output_path = next(
        path
        for path in (runner_output_dir / "track_b_case_outputs").glob("*.json")
        if "track_b_nearest_history" in path.name
    )
    case_output_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
    tampered_case = next(
        case
        for case in case_output_payload["cases"]
        if case.get("analog_recall_at_3") is not None
        and float(case["analog_recall_at_3"]) > 0.0
    )
    tampered_case["retrieved_analog_event_ids"] = [
        "fake_event_1",
        "fake_event_2",
        "fake_event_3",
    ]
    for analog_payload, event_id in zip(
        tampered_case["retrieved_analogs"],
        tampered_case["retrieved_analog_event_ids"],
        strict=True,
    ):
        analog_payload["event_id"] = event_id
    case_output_path.write_text(
        json.dumps(case_output_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B case output analog_recall_at_3 does not match analog event ids",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_nonevaluable_retrieved_ids(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    case_output_path = next(
        path
        for path in (runner_output_dir / "track_b_case_outputs").glob("*.json")
        if "track_b_nearest_history" in path.name
    )
    case_output_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
    tampered_case = next(
        case
        for case in case_output_payload["cases"]
        if case["case_id"] == "roluperidone_negative_symptoms_phase3"
    )
    assert tampered_case["gold_analog_event_ids"] == []
    tampered_case["retrieved_analog_event_ids"] = ["fake_event_x", "fake_event_y"]
    case_output_path.write_text(
        json.dumps(case_output_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B case output retrieved_analog_event_ids do not match retrieved_analogs",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_excess_retrieved_analogs(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    case_output_path = next(
        path
        for path in (runner_output_dir / "track_b_case_outputs").glob("*.json")
        if "track_b_nearest_history" in path.name
    )
    case_output_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
    tampered_case = next(
        case
        for case in case_output_payload["cases"]
        if case["case_id"] == "roluperidone_negative_symptoms_phase3"
    )
    tampered_case["retrieved_analog_event_ids"].append("fake_event_z")
    tampered_case["retrieved_analogs"].append(
        {
            "asset_id": "fake-asset-z",
            "biological_anchor": False,
            "domain": "negative_symptoms",
            "event_date": "2024-06-01",
            "event_id": "fake_event_z",
            "failure_reason_taxonomy": "unresolved",
            "failure_scope": "unresolved",
            "match_dimensions": ["domain"],
            "match_tier": "nearest_history",
            "molecule": "fake molecule z",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_3",
            "population": "adults",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/fake-event-z",
            "target": "GENE1",
            "target_class": "synthetic class",
        }
    )
    case_output_path.write_text(
        json.dumps(case_output_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B case output retrieved_analogs exceed the Track B retrieval limit",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_track_b_as_of_date_mismatch(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    case_output_path = next((runner_output_dir / "track_b_case_outputs").glob("*.json"))
    case_output_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
    case_output_payload["as_of_date"] = "1999-01-01"
    case_output_path.write_text(
        json.dumps(case_output_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B case output payload as_of_date does not match snapshot manifest",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_emits_intervention_object_error_analysis(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_output_dir = tmp_path / "public_payloads"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )
    materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=public_slice.source_archives_file,
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-02T00:00:00Z",
    )

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-04-02T12:00:00Z",
    )

    assert reporting_result["error_analysis_files"]
    error_analysis_path = next(
        Path(path) for path in reporting_result["error_analysis_files"]
        if "v0_current" in path
    )
    error_analysis_text = error_analysis_path.read_text(encoding="utf-8")
    assert "Track A Error Analysis" in error_analysis_text
    assert "principal horizon: `3y`" in error_analysis_text
    intervention_leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if "/intervention_object/3y/average_precision_any_positive_outcome.json" in path
    )
    leaderboard_payload = read_benchmark_leaderboard_payload(
        intervention_leaderboard_path
    )
    assert leaderboard_payload.entity_type == "intervention_object"


def test_materialize_benchmark_reporting_skips_nonevaluable_public_slice_error_analysis(
    tmp_path: Path,
) -> None:
    public_slice_dir = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "benchmark"
        / "public_slices"
        / "scz_translational_2024_06_20"
    )
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_output_dir = tmp_path / "public_payloads"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice_dir / "snapshot_request.json",
        archive_index_file=public_slice_dir / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice_dir / "cohort_members.csv",
        future_outcomes_file=public_slice_dir / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=public_slice_dir / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-02T00:00:00Z",
    )

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-04-02T12:00:00Z",
    )

    assert reporting_result["leaderboard_payload_files"]
    assert reporting_result["error_analysis_files"] == []


def test_materialize_benchmark_reporting_rejects_stale_intervention_object_bundle_provenance(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_output_dir = tmp_path / "public_payloads"
    pgc_archive_path = (
        public_slice.source_archives_file.parent
        / "archives"
        / "pgc"
        / "scz2022_fixture.csv"
    )

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )
    materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=public_slice.source_archives_file,
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-02T00:00:00Z",
    )

    pgc_archive_lines = pgc_archive_path.read_text(encoding="utf-8").splitlines()
    pgc_archive_lines[1] = "ENSG00000162946,DISC1,0.11"
    pgc_archive_path.write_text("\n".join(pgc_archive_lines) + "\n", encoding="utf-8")
    archive_index_payload = json.loads(
        public_slice.source_archives_file.read_text(encoding="utf-8")
    )
    for archive_payload in archive_index_payload["archives"]:
        if archive_payload["source_name"] != "PGC":
            continue
        archive_payload["sha256"] = sha256(pgc_archive_path.read_bytes()).hexdigest()
        break
    public_slice.source_archives_file.write_text(
        json.dumps(archive_index_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="source snapshot provenance does not match",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-02T12:00:00Z",
        )


def test_materialize_benchmark_reporting_prunes_stale_snapshot_outputs(
    tmp_path: Path,
) -> None:
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_fixture_runner_outputs(tmp_path)
    reporting_output_dir = tmp_path / "public_payloads"

    materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-03-28T12:00:00Z",
    )

    stale_report_card = (
        reporting_output_dir
        / "report_cards"
        / "scz_translational_suite"
        / "scz_translational_task"
        / "scz_fixture_2024_06_30"
        / "stale.json"
    )
    stale_leaderboard = (
        reporting_output_dir
        / "leaderboards"
        / "scz_translational_suite"
        / "scz_translational_task"
        / "scz_fixture_2024_06_30"
        / "gene"
        / "3y"
        / "stale_metric.json"
    )
    stale_error_analysis = (
        reporting_output_dir
        / "error_analysis"
        / "scz_translational_suite"
        / "scz_translational_task"
        / "scz_fixture_2024_06_30"
        / "stale.md"
    )
    stale_report_card.parent.mkdir(parents=True, exist_ok=True)
    stale_leaderboard.parent.mkdir(parents=True, exist_ok=True)
    stale_error_analysis.parent.mkdir(parents=True, exist_ok=True)
    stale_report_card.write_text("{}\n", encoding="utf-8")
    stale_leaderboard.write_text("{}\n", encoding="utf-8")
    stale_error_analysis.write_text("stale\n", encoding="utf-8")

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-03-28T12:05:00Z",
    )

    assert not stale_report_card.exists()
    assert not stale_leaderboard.exists()
    assert not stale_error_analysis.exists()
    assert reporting_result["report_card_files"]
    assert reporting_result["leaderboard_payload_files"]


def test_materialize_benchmark_reporting_fails_for_missing_metric_payload(
    tmp_path: Path,
) -> None:
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_fixture_runner_outputs(tmp_path)
    reporting_output_dir = tmp_path / "public_payloads"
    deleted_metric_path = next(
        path
        for path in (runner_output_dir / "metric_payloads").rglob("*.json")
        if path.name == f"{RETRIEVAL_METRIC_NAMES[0]}.json"
    )
    deleted_metric_path.unlink()

    with pytest.raises(
        ValueError,
        match="incomplete benchmark runner output for reporting",
    ) as exc_info:
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-03-28T12:00:00Z",
        )

    assert RETRIEVAL_METRIC_NAMES[0] in str(exc_info.value)
    assert not reporting_output_dir.exists()
