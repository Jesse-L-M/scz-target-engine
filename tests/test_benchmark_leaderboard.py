from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
import shutil

import pytest

from scz_target_engine.benchmark_labels import (
    benchmark_track_b_auxiliary_source_artifact_path_for_labels_file,
    materialize_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_leaderboard import (
    LEADERBOARD_SCHEMA_NAME,
    REPORT_CARD_SCHEMA_NAME,
    TRACK_B_PUBLIC_COMPLETED_AT,
    TRACK_B_PUBLIC_CODE_VERSION,
    TRACK_B_PUBLIC_RUN_NOTES,
    TRACK_B_PUBLIC_STARTED_AT,
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_metrics import RETRIEVAL_METRIC_NAMES
from scz_target_engine.benchmark_metrics import read_benchmark_metric_output_payload
from scz_target_engine.benchmark_runner import (
    build_track_b_run_id,
    materialize_benchmark_run,
)
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_RUN_PARAMETERIZATION_FIELDS,
    estimate_track_b_metric_intervals,
    read_track_b_case_output_payload,
    track_b_case_output_path,
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


def _track_b_run_id_from_manifest_payload(payload: dict[str, object]) -> str:
    parameterization = payload["parameterization"]
    assert isinstance(parameterization, dict)
    return build_track_b_run_id(
        snapshot_id=str(payload["snapshot_id"]),
        baseline_id=str(payload["baseline_id"]),
        code_version=str(payload["code_version"]),
        parameterization=parameterization,
    )


def _rewrite_track_b_run_id_references(
    runner_output_dir: Path,
    *,
    old_run_id: str,
    new_run_id: str,
) -> None:
    for folder_name in ("track_b_case_outputs", "track_b_confusion_summaries"):
        payload_path = runner_output_dir / folder_name / f"{old_run_id}.json"
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        payload["run_id"] = new_run_id
        payload_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    for folder_name in ("metric_payloads", "confidence_interval_payloads"):
        payload_dir = (
            runner_output_dir
            / folder_name
            / old_run_id
            / "intervention_object"
            / "structural_replay"
        )
        for payload_path in payload_dir.glob("*.json"):
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            payload["run_id"] = new_run_id
            payload_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )


def _rename_track_b_run_bundle_paths(
    runner_output_dir: Path,
    *,
    old_run_id: str,
    new_run_id: str,
) -> None:
    for folder_name in (
        "run_manifests",
        "track_b_case_outputs",
        "track_b_confusion_summaries",
    ):
        source_path = runner_output_dir / folder_name / f"{old_run_id}.json"
        target_path = runner_output_dir / folder_name / f"{new_run_id}.json"
        source_path.rename(target_path)
    for folder_name in ("metric_payloads", "confidence_interval_payloads"):
        source_dir = runner_output_dir / folder_name / old_run_id
        target_dir = runner_output_dir / folder_name / new_run_id
        source_dir.rename(target_dir)


def _track_b_run_id_by_baseline(runner_output_dir: Path) -> dict[str, str]:
    run_ids: dict[str, str] = {}
    for path in (runner_output_dir / "run_manifests").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        run_ids[str(payload["baseline_id"])] = str(payload["run_id"])
    return run_ids


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
    *,
    code_version: str = "fixture-sha",
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
        code_version=code_version,
        execution_timestamp="2026-04-05T00:00:00Z",
    )
    return (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        benchmark_result,
    )


def _materialize_track_b_public_payloads(
    tmp_path: Path,
    *,
    code_version: str = "fixture-sha",
) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(
        tmp_path,
        code_version=code_version,
    )
    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-04-05T12:00:00Z",
    )
    return (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        reporting_output_dir,
        reporting_result,
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
        assert report_card.code_version == TRACK_B_PUBLIC_CODE_VERSION
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
    assert all(
        entry.code_version == TRACK_B_PUBLIC_CODE_VERSION
        for entry in replay_status_leaderboard.entries
    )

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


def test_materialize_benchmark_reporting_rejects_tampered_track_b_fabricated_retrieved_analogs(
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
    tampered_case["retrieved_analog_event_ids"] = [
        "fake_event_a",
        "fake_event_b",
        "fake_event_c",
    ]
    tampered_case["retrieved_analogs"] = [
        {
            "asset_id": "fake-asset-a",
            "biological_anchor": False,
            "domain": "negative_symptoms",
            "event_date": "2024-06-01",
            "event_id": "fake_event_a",
            "failure_reason_taxonomy": "unresolved",
            "failure_scope": "unresolved",
            "match_dimensions": ["domain"],
            "match_tier": "nearest_history",
            "molecule": "fake molecule a",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_3",
            "population": "adults",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/fake-event-a",
            "target": "GENE1",
            "target_class": "synthetic class",
        },
        {
            "asset_id": "fake-asset-b",
            "biological_anchor": False,
            "domain": "negative_symptoms",
            "event_date": "2024-06-01",
            "event_id": "fake_event_b",
            "failure_reason_taxonomy": "unresolved",
            "failure_scope": "unresolved",
            "match_dimensions": ["domain"],
            "match_tier": "nearest_history",
            "molecule": "fake molecule b",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_3",
            "population": "adults",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/fake-event-b",
            "target": "GENE1",
            "target_class": "synthetic class",
        },
        {
            "asset_id": "fake-asset-c",
            "biological_anchor": False,
            "domain": "negative_symptoms",
            "event_date": "2024-06-01",
            "event_id": "fake_event_c",
            "failure_reason_taxonomy": "unresolved",
            "failure_scope": "unresolved",
            "match_dimensions": ["domain"],
            "match_tier": "nearest_history",
            "molecule": "fake molecule c",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_3",
            "population": "adults",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/fake-event-c",
            "target": "GENE1",
            "target_class": "synthetic class",
        },
    ]
    case_output_path.write_text(
        json.dumps(case_output_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B case output does not match the pinned source artifacts",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_track_b_cross_baseline_run_id_swap(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    nearest_run_id = next(
        path.stem
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_nearest_history" in path.name
    )
    structural_run_id = next(
        path.stem
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )

    for folder_name in (
        "run_manifests",
        "track_b_case_outputs",
        "track_b_confusion_summaries",
    ):
        source_path = runner_output_dir / folder_name / f"{nearest_run_id}.json"
        target_path = runner_output_dir / folder_name / f"{structural_run_id}.json"
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        payload["run_id"] = structural_run_id
        target_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    for folder_name in ("metric_payloads", "confidence_interval_payloads"):
        source_dir = (
            runner_output_dir
            / folder_name
            / nearest_run_id
            / "intervention_object"
            / "structural_replay"
        )
        target_dir = (
            runner_output_dir
            / folder_name
            / structural_run_id
            / "intervention_object"
            / "structural_replay"
        )
        for source_path in source_dir.glob("*.json"):
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            payload["run_id"] = structural_run_id
            (target_dir / source_path.name).write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

    with pytest.raises(
        ValueError,
        match="Track B run manifest run_id does not match its baseline/code_version/parameterization contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_missing_track_b_baseline_bundle(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    missing_run_id = _track_b_run_id_by_baseline(runner_output_dir)[
        "track_b_target_class"
    ]
    for folder_name in (
        "run_manifests",
        "track_b_case_outputs",
        "track_b_confusion_summaries",
    ):
        (runner_output_dir / folder_name / f"{missing_run_id}.json").unlink()
    shutil.rmtree(runner_output_dir / "metric_payloads" / missing_run_id)
    shutil.rmtree(runner_output_dir / "confidence_interval_payloads" / missing_run_id)

    with pytest.raises(
        ValueError,
        match="Track B reporting requires the complete expected baseline set",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_input_artifacts(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_id = _track_b_run_id_by_baseline(runner_output_dir)["track_b_structural_current"]
    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    real_casebook = benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
        cohort_labels_file,
        artifact_name="track_b_casebook",
    )
    fake_casebook = tmp_path / "fake_track_b_casebook.csv"
    fake_casebook.write_bytes(real_casebook.read_bytes())
    for artifact in run_manifest_payload["input_artifacts"]:
        if artifact["artifact_name"] == "track_b_casebook":
            artifact["artifact_path"] = str(fake_casebook.resolve())
            artifact["sha256"] = sha256(fake_casebook.read_bytes()).hexdigest()
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B run manifest input_artifact path does not match the pinned cohort/source artifact contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_interval_seed(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_id = _track_b_run_id_by_baseline(runner_output_dir)["track_b_structural_current"]
    case_output_payload = read_track_b_case_output_payload(
        track_b_case_output_path(runner_output_dir, run_id=run_id)
    )
    interval_dir = (
        runner_output_dir
        / "confidence_interval_payloads"
        / run_id
        / "intervention_object"
        / "structural_replay"
    )
    sample_interval_payload = json.loads(
        next(interval_dir.glob("*.json")).read_text(encoding="utf-8")
    )
    tampered_seed = 123456
    tampered_intervals = estimate_track_b_metric_intervals(
        case_output_payload.cases,
        iterations=int(sample_interval_payload["bootstrap_iterations"]),
        confidence_level=float(sample_interval_payload["confidence_level"]),
        random_seed=tampered_seed,
    )
    for interval_path in interval_dir.glob("*.json"):
        interval_payload = json.loads(interval_path.read_text(encoding="utf-8"))
        point_estimate, interval_low, interval_high = tampered_intervals[
            interval_payload["metric_name"]
        ]
        interval_payload["random_seed"] = tampered_seed
        interval_payload["point_estimate"] = point_estimate
        interval_payload["interval_low"] = interval_low
        interval_payload["interval_high"] = interval_high
        interval_path.write_text(
            json.dumps(interval_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    with pytest.raises(
        ValueError,
        match="Track B confidence interval payload random_seed does not match the run manifest contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_manifest_only_provenance(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_id = _track_b_run_id_by_baseline(runner_output_dir)["track_b_structural_current"]
    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_payload["parameterization"]["track_b_casebook_sha256"] = "1" * 64
    new_run_id = _track_b_run_id_from_manifest_payload(run_manifest_payload)
    run_manifest_payload["run_id"] = new_run_id
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _rewrite_track_b_run_id_references(
        runner_output_dir,
        old_run_id=run_id,
        new_run_id=new_run_id,
    )

    with pytest.raises(
        ValueError,
        match="Track B run manifest parameterization track_b_casebook_sha256 does not match the pinned Track B casebook",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


@pytest.mark.parametrize(
    ("artifact_kind", "field_name", "tampered_value"),
    (
        ("run_manifest", "schema_name", "tampered_schema"),
        ("metric_payload", "schema_name", "tampered_schema"),
        ("confidence_interval", "schema_name", "tampered_schema"),
        ("confidence_interval", "schema_version", "v999"),
        ("case_output", "schema_name", "tampered_schema"),
        ("confusion_summary", "schema_name", "tampered_schema"),
    ),
)
def test_materialize_benchmark_reporting_rejects_track_b_schema_identity_tampering(
    tmp_path: Path,
    artifact_kind: str,
    field_name: str,
    tampered_value: str,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    if artifact_kind == "run_manifest":
        artifact_path = next(
            path
            for path in (runner_output_dir / "run_manifests").glob("*.json")
            if "track_b_structural_current" in path.name
        )
    elif artifact_kind == "metric_payload":
        artifact_path = next((runner_output_dir / "metric_payloads").rglob("*.json"))
    elif artifact_kind == "confidence_interval":
        artifact_path = next(
            (runner_output_dir / "confidence_interval_payloads").rglob("*.json")
        )
    elif artifact_kind == "case_output":
        artifact_path = next((runner_output_dir / "track_b_case_outputs").glob("*.json"))
    else:
        artifact_path = next(
            (runner_output_dir / "track_b_confusion_summaries").glob("*.json")
        )

    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload[field_name] = tampered_value
    artifact_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"schema_(name|version) must be"):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_track_b_code_version_same_prefix_tampering(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(
        tmp_path,
        code_version="1234567890ab-original",
    )

    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_payload["code_version"] = "1234567890ab-forged"
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B run manifest run_id does not match its baseline/code_version/parameterization contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_keeps_track_b_public_ids_and_paths_stable_after_bundle_rewrite(
    tmp_path: Path,
) -> None:
    original_public_dir = tmp_path / "original_public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(
        tmp_path,
        code_version="1234567890ab-original",
    )
    original_reporting = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=original_public_dir,
        generated_at="2026-04-05T12:00:00Z",
    )
    original_structural_report_card = next(
        read_benchmark_report_card_payload(Path(path))
        for path in original_reporting["report_card_files"]
        if "track_b_structural_current" in path
    )
    original_replay_status_leaderboard = read_benchmark_leaderboard_payload(
        next(
            Path(path)
            for path in original_reporting["leaderboard_payload_files"]
            if path.endswith("replay_status_exact_match.json")
        )
    )
    original_structural_entry = next(
        entry
        for entry in original_replay_status_leaderboard.entries
        if entry.baseline_id == "track_b_structural_current"
    )
    original_error_analysis_files = sorted(
        path
        for path in original_reporting["error_analysis_files"]
        if "track_b_structural_current" in path
    )

    old_run_id = _track_b_run_id_by_baseline(runner_output_dir)[
        "track_b_structural_current"
    ]
    run_manifest_path = runner_output_dir / "run_manifests" / f"{old_run_id}.json"
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    forged_code_version = "1234567890ab-forged"
    run_manifest_payload["code_version"] = forged_code_version
    new_run_id = _track_b_run_id_from_manifest_payload(run_manifest_payload)
    run_manifest_payload["run_id"] = new_run_id
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _rewrite_track_b_run_id_references(
        runner_output_dir,
        old_run_id=old_run_id,
        new_run_id=new_run_id,
    )
    _rename_track_b_run_bundle_paths(
        runner_output_dir,
        old_run_id=old_run_id,
        new_run_id=new_run_id,
    )

    rewritten_public_dir = tmp_path / "rewritten_public_payloads"
    rewritten_reporting = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=rewritten_public_dir,
        generated_at="2026-04-05T12:00:00Z",
    )

    structural_report_card = next(
        read_benchmark_report_card_payload(Path(path))
        for path in rewritten_reporting["report_card_files"]
        if "track_b_structural_current" in path
    )
    assert structural_report_card.run_id == original_structural_report_card.run_id
    assert structural_report_card.report_card_id == original_structural_report_card.report_card_id
    assert structural_report_card.code_version == TRACK_B_PUBLIC_CODE_VERSION
    assert structural_report_card.code_version != forged_code_version
    assert structural_report_card.run_id != new_run_id

    rewritten_structural_report_card_path = next(
        path
        for path in rewritten_reporting["report_card_files"]
        if "track_b_structural_current" in path
    )
    original_structural_report_card_path = next(
        path
        for path in original_reporting["report_card_files"]
        if "track_b_structural_current" in path
    )
    assert Path(rewritten_structural_report_card_path).name == Path(
        original_structural_report_card_path
    ).name

    replay_status_leaderboard = read_benchmark_leaderboard_payload(
        next(
            Path(path)
            for path in rewritten_reporting["leaderboard_payload_files"]
            if path.endswith("replay_status_exact_match.json")
        )
    )
    structural_entry = next(
        entry
        for entry in replay_status_leaderboard.entries
        if entry.baseline_id == "track_b_structural_current"
    )
    assert structural_entry.run_id == original_structural_entry.run_id
    assert Path(structural_entry.report_card_path).name == Path(
        original_structural_entry.report_card_path
    ).name
    assert structural_entry.code_version == TRACK_B_PUBLIC_CODE_VERSION
    assert structural_entry.code_version != forged_code_version
    assert structural_entry.run_id != new_run_id

    rewritten_error_analysis_files = sorted(
        path
        for path in rewritten_reporting["error_analysis_files"]
        if "track_b_structural_current" in path
    )
    assert [Path(path).name for path in rewritten_error_analysis_files] == [
        Path(path).name for path in original_error_analysis_files
    ]


def test_materialize_benchmark_reporting_redacts_track_b_runner_operational_metadata(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    forged_started_at = "1900-01-01T00:00:00Z"
    forged_completed_at = "1900-01-02T00:00:00Z"
    forged_notes = "forged runner operational notes"
    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_payload["started_at"] = forged_started_at
    run_manifest_payload["completed_at"] = forged_completed_at
    run_manifest_payload["notes"] = forged_notes
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-04-05T12:00:00Z",
    )

    structural_report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    structural_report_card = read_benchmark_report_card_payload(
        structural_report_card_path
    )
    assert structural_report_card.started_at == TRACK_B_PUBLIC_STARTED_AT
    assert structural_report_card.completed_at == TRACK_B_PUBLIC_COMPLETED_AT
    assert structural_report_card.run_notes == TRACK_B_PUBLIC_RUN_NOTES

    report_card_payload = json.loads(
        structural_report_card_path.read_text(encoding="utf-8")
    )
    assert report_card_payload["started_at"] != forged_started_at
    assert report_card_payload["completed_at"] != forged_completed_at
    assert report_card_payload["run_notes"] != forged_notes


def test_read_track_b_report_card_rejects_missing_run_parameterization(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload.pop("run_parameterization")
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B public report card run_parameterization is required",
    ):
        read_benchmark_report_card_payload(report_card_path)


@pytest.mark.parametrize(
    ("field_name", "tampered_value", "error_fragment"),
    (
        (
            "code_version",
            "forged-code-version",
            "Track B public report card code_version must be",
        ),
        (
            "started_at",
            "forged-started-at",
            "Track B public report card started_at must be",
        ),
        (
            "completed_at",
            "forged-completed-at",
            "Track B public report card completed_at must be",
        ),
        (
            "run_notes",
            "forged-run-notes",
            "Track B public report card run_notes must be",
        ),
    ),
)
def test_read_track_b_report_card_rejects_tampered_redacted_fields(
    tmp_path: Path,
    field_name: str,
    tampered_value: str,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload[field_name] = tampered_value
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_report_card_payload(report_card_path)


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload["source_snapshots"][0].pop("included"),
            "included must be an explicit boolean",
        ),
        (
            lambda payload: payload["source_snapshots"][0].__setitem__(
                "included",
                "false",
            ),
            "included must be an explicit boolean",
        ),
    ),
)
def test_read_track_b_report_card_rejects_invalid_source_snapshot_included(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    mutator(payload)
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_report_card_payload(report_card_path)


@pytest.mark.parametrize(
    ("field_name", "tampered_value"),
    (
        ("schema_name", "tampered_schema"),
        ("schema_version", "v999"),
    ),
)
def test_read_track_b_report_card_rejects_tampered_public_schema_identity(
    tmp_path: Path,
    field_name: str,
    tampered_value: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload[field_name] = tampered_value
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"schema_(name|version) must be"):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_leaderboard_rejects_tampered_entry_code_version(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    payload["entries"][0]["code_version"] = "forged-code-version"
    leaderboard_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B public leaderboard entry code_version must be",
    ):
        read_benchmark_leaderboard_payload(leaderboard_path)


@pytest.mark.parametrize(
    ("field_name", "tampered_value"),
    (
        ("schema_name", "tampered_schema"),
        ("schema_version", "v999"),
    ),
)
def test_read_track_b_leaderboard_rejects_tampered_public_schema_identity(
    tmp_path: Path,
    field_name: str,
    tampered_value: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    payload[field_name] = tampered_value
    leaderboard_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"schema_(name|version) must be"):
        read_benchmark_leaderboard_payload(leaderboard_path)


def test_materialize_benchmark_reporting_rejects_unexpected_track_b_parameterization_keys(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_id = _track_b_run_id_by_baseline(runner_output_dir)["track_b_structural_current"]
    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_payload["parameterization"]["unexpected_public_key"] = "poison"
    run_manifest_payload["run_id"] = _track_b_run_id_from_manifest_payload(
        run_manifest_payload
    )
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _rewrite_track_b_run_id_references(
        runner_output_dir,
        old_run_id=run_id,
        new_run_id=str(run_manifest_payload["run_id"]),
    )

    with pytest.raises(
        ValueError,
        match="Track B run manifest parameterization does not match the exact reporting contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


@pytest.mark.parametrize(
    ("field_name", "tampered_value", "error_fragment"),
    (
        ("track_b_case_count", 6.9, "track_b_case_count must be an integer"),
        ("random_seed", "17", "random_seed must be an integer"),
        (
            "bootstrap_confidence_level",
            "0.95",
            "bootstrap_confidence_level must be a float",
        ),
    ),
)
def test_materialize_benchmark_reporting_rejects_typed_track_b_parameterization_tampering(
    tmp_path: Path,
    field_name: str,
    tampered_value: object,
    error_fragment: str,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_payload["parameterization"][field_name] = tampered_value
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_tampered_track_b_metric_unit(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    for metric_path in (runner_output_dir / "metric_payloads").rglob("*.json"):
        payload = json.loads(metric_path.read_text(encoding="utf-8"))
        payload["metric_unit"] = "percent"
        metric_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    with pytest.raises(
        ValueError,
        match="Track B metric payload metric_unit does not match the Track B metric contract",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_benchmark_reporting_rejects_omitted_track_b_metric_unit(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    metric_path = next((runner_output_dir / "metric_payloads").rglob("*.json"))
    payload = json.loads(metric_path.read_text(encoding="utf-8"))
    payload.pop("metric_unit")
    metric_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="benchmark_metric_output_payload metric_unit is required",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=reporting_output_dir,
            generated_at="2026-04-05T12:00:00Z",
        )


def test_materialize_track_b_runner_outputs_emit_explicit_metric_unit(
    tmp_path: Path,
) -> None:
    _, _, _, benchmark_result = _materialize_track_b_fixture_runner_outputs(tmp_path)

    metric_payload = json.loads(
        Path(benchmark_result["metric_payload_files"][0]).read_text(encoding="utf-8")
    )

    assert metric_payload["metric_unit"] == "fraction"


def test_materialize_benchmark_reporting_rejects_duplicate_track_b_input_artifact_names(
    tmp_path: Path,
) -> None:
    reporting_output_dir = tmp_path / "public_payloads"
    (
        snapshot_manifest_file,
        cohort_labels_file,
        runner_output_dir,
        _,
    ) = _materialize_track_b_fixture_runner_outputs(tmp_path)

    run_manifest_path = next(
        path
        for path in (runner_output_dir / "run_manifests").glob("*.json")
        if "track_b_structural_current" in path.name
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    duplicated_artifact = next(
        artifact
        for artifact in run_manifest_payload["input_artifacts"]
        if artifact["artifact_name"] == "track_b_casebook"
    )
    run_manifest_payload["input_artifacts"].append(dict(duplicated_artifact))
    run_manifest_path.write_text(
        json.dumps(run_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B run manifest input_artifacts .* duplicate artifact_name entries",
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
