from __future__ import annotations

import csv
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
    _build_track_b_public_id,
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_metrics import RETRIEVAL_METRIC_NAMES
from scz_target_engine.benchmark_metrics import (
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_runner import (
    build_track_b_run_id,
    materialize_benchmark_run,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_RUN_PARAMETERIZATION_FIELDS,
    estimate_track_b_metric_intervals,
    read_track_b_case_output_payload,
    read_track_b_confusion_summary,
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


def _write_json_payload(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _track_b_public_report_card_path(
    reporting_result: dict[str, object],
    *,
    baseline_id: str,
) -> Path:
    return next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if baseline_id in path
    )


def _rewrite_track_b_public_report_card_identity(
    report_card_path: Path,
    *,
    new_baseline_id: str,
) -> Path:
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    old_public_id = str(payload["report_card_id"])
    parameterization = dict(payload["run_parameterization"])
    parameterization["track_b_baseline_mode"] = new_baseline_id
    new_public_id = _build_track_b_public_id(
        snapshot_id=str(payload["snapshot_id"]),
        baseline_id=new_baseline_id,
        run_parameterization=parameterization,
    )
    payload["baseline_id"] = new_baseline_id
    payload["run_id"] = new_public_id
    payload["report_card_id"] = new_public_id
    payload["run_parameterization"] = parameterization
    for artifact in payload["derived_from_artifacts"]:
        artifact["artifact_path"] = str(artifact["artifact_path"]).replace(
            old_public_id,
            new_public_id,
        )
    new_report_card_path = report_card_path.with_name(f"{new_public_id}.json")
    _write_json_payload(new_report_card_path, payload)
    if new_report_card_path != report_card_path:
        report_card_path.unlink()
    return new_report_card_path


def _refresh_track_b_public_derived_artifact_sha(
    report_card_path: Path,
    *,
    artifact_name: str,
    artifact_path: Path,
) -> None:
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    artifact = next(
        item
        for item in payload["derived_from_artifacts"]
        if item["artifact_name"] == artifact_name
    )
    artifact["sha256"] = sha256(artifact_path.read_bytes()).hexdigest()
    _write_json_payload(report_card_path, payload)


def _track_b_public_input_artifact_path(
    report_card_path: Path,
    *,
    artifact_name: str,
) -> Path:
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    artifact_path = next(
        item["artifact_path"]
        for item in payload["evaluation_input_artifacts"]
        if item["artifact_name"] == artifact_name
    )
    return (report_card_path.parent / artifact_path).resolve()


def _refresh_track_b_public_input_artifact_sha(
    report_card_path: Path,
    reporting_output_dir: Path,
    *,
    artifact_name: str,
    artifact_path: Path,
) -> None:
    artifact_sha = sha256(artifact_path.read_bytes()).hexdigest()
    report_card_payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    report_card_artifact = next(
        item
        for item in report_card_payload["evaluation_input_artifacts"]
        if item["artifact_name"] == artifact_name
    )
    report_card_artifact["sha256"] = artifact_sha
    _write_json_payload(report_card_path, report_card_payload)

    run_manifest_path = next(
        reporting_output_dir / item["artifact_path"]
        for item in report_card_payload["derived_from_artifacts"]
        if item["artifact_name"] == "benchmark_model_run_manifest"
    )
    run_manifest_payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest_artifact = next(
        item
        for item in run_manifest_payload["input_artifacts"]
        if item["artifact_name"] == artifact_name
    )
    run_manifest_artifact["sha256"] = artifact_sha
    _write_json_payload(run_manifest_path, run_manifest_payload)


def _write_track_b_registry_variant(
    tmp_path: Path,
    *,
    supported_baseline_ids: tuple[str, ...],
) -> Path:
    source_registry_path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "curated"
        / "rescue_tasks"
        / "task_registry.csv"
    )
    rows = list(
        csv.DictReader(source_registry_path.read_text(encoding="utf-8").splitlines())
    )
    for row in rows:
        if row["task_id"] == "scz_failure_memory_track_b_task":
            row["supported_baseline_ids"] = "|".join(supported_baseline_ids)
    output_path = tmp_path / "track_b_task_registry.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return output_path


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
            "included must be a boolean",
        ),
        (
            lambda payload: payload["source_snapshots"][0].__setitem__(
                "included",
                "false",
            ),
            "included must be a boolean",
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


def test_read_track_b_report_card_rejects_tampered_source_snapshot_provenance(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload["source_snapshots"][0]["source_version"] = "forged-version"
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="source_snapshots do not match the pinned benchmark_snapshot_manifest",
    ):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_tampered_evaluation_input_artifacts(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload["evaluation_input_artifacts"][0]["sha256"] = "0" * 64
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="evaluation_input_artifacts do not match the pinned cohort/source artifact contract",
    ):
        read_benchmark_report_card_payload(report_card_path)


@pytest.mark.parametrize(
    ("field_name", "tampered_value", "error_fragment"),
    (
        (
            "sha256",
            "f" * 64,
            "derived_from_artifacts do not match the materialized public runner bundle",
        ),
        (
            "notes",
            "forged-public-note",
            "derived_from_artifacts does not match the stable public contract",
        ),
    ),
)
def test_read_track_b_report_card_rejects_tampered_derived_artifact_provenance(
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
    artifact = next(
        item
        for item in payload["derived_from_artifacts"]
        if item["artifact_name"] == "track_b_case_output_payload"
    )
    artifact[field_name] = tampered_value
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_tampered_public_metric_unit(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = next(
        Path(path)
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload["slices"][0]["metrics"][0]["metric_unit"] = "percent"
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B public report card metric_unit does not match the Track B metric contract",
    ):
        read_benchmark_report_card_payload(report_card_path)


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload.__setitem__("baseline_label", False),
            "baseline_label must be a string",
        ),
        (
            lambda payload: payload["slices"][0]["metrics"][0].__setitem__(
                "metric_value",
                "0.75",
            ),
            "metric_value must be a float",
        ),
        (
            lambda payload: payload["slices"][0]["metrics"][0].__setitem__(
                "cohort_size",
                6.5,
            ),
            "cohort_size must be an integer",
        ),
    ),
)
def test_read_track_b_report_card_rejects_malformed_json_types(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    mutator(payload)
    _write_json_payload(report_card_path, payload)

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_forged_public_case_output_bundle(
    tmp_path: Path,
) -> None:
    _, _, _, reporting_output_dir, reporting_result = _materialize_track_b_public_payloads(
        tmp_path
    )

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    report_card_payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    case_output_path = reporting_output_dir / next(
        artifact["artifact_path"]
        for artifact in report_card_payload["derived_from_artifacts"]
        if artifact["artifact_name"] == "track_b_case_output_payload"
    )
    case_output_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
    case_output_payload["cases"][0]["proposal_entity_label"] = "forged public label"
    _write_json_payload(case_output_path, case_output_payload)
    _refresh_track_b_public_derived_artifact_sha(
        report_card_path,
        artifact_name="track_b_case_output_payload",
        artifact_path=case_output_path,
    )

    with pytest.raises(
        ValueError,
        match="Track B case output does not match the pinned source artifacts",
    ):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_forged_track_b_baseline_outside_task_contract(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    forged_report_card_path = _rewrite_track_b_public_report_card_identity(
        report_card_path,
        new_baseline_id="track_b_totally_forged",
    )

    with pytest.raises(
        ValueError,
        match="baseline_id is not part of the Track B task contract",
    ):
        read_benchmark_report_card_payload(forged_report_card_path)


def test_read_track_b_report_card_rejects_absolute_evaluation_input_artifact_paths(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    artifact_path = Path(payload["evaluation_input_artifacts"][0]["artifact_path"])
    payload["evaluation_input_artifacts"][0]["artifact_path"] = str(
        (report_card_path.parent / artifact_path).resolve()
    )
    _write_json_payload(report_card_path, payload)

    with pytest.raises(
        ValueError,
        match=r"evaluation_input_artifacts\[\]\.artifact_path must be a relative path",
    ):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_escaped_public_input_artifact_paths(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    payload = json.loads(report_card_path.read_text(encoding="utf-8"))
    payload["evaluation_input_artifacts"][0]["artifact_path"] = (
        "../../../../../forged_snapshot_manifest.json"
    )
    _write_json_payload(report_card_path, payload)

    with pytest.raises(
        ValueError,
        match="must stay within the Track B public payload root",
    ):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_rejects_off_tree_nested_cohort_manifest_reference(
    tmp_path: Path,
) -> None:
    (
        _,
        _,
        _,
        reporting_output_dir,
        reporting_result,
    ) = _materialize_track_b_public_payloads(tmp_path)

    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    cohort_manifest_path = _track_b_public_input_artifact_path(
        report_card_path,
        artifact_name="benchmark_cohort_manifest",
    )
    cohort_manifest_payload = json.loads(
        cohort_manifest_path.read_text(encoding="utf-8")
    )
    cohort_manifest_payload["source_cohort_members_path"] = "../forged_source.csv"
    _write_json_payload(cohort_manifest_path, cohort_manifest_payload)
    _refresh_track_b_public_input_artifact_sha(
        report_card_path,
        reporting_output_dir,
        artifact_name="benchmark_cohort_manifest",
        artifact_path=cohort_manifest_path,
    )

    with pytest.raises(
        ValueError,
        match=(
            "benchmark cohort manifest must point to the canonical "
            "benchmark_source_cohort_members artifact"
        ),
    ):
        read_benchmark_report_card_payload(report_card_path)


def test_read_track_b_report_card_ignores_external_shadow_source_artifacts(
    tmp_path: Path,
) -> None:
    (
        _,
        _,
        _,
        _,
        reporting_result,
    ) = _materialize_track_b_public_payloads(tmp_path)

    external_events_path = tmp_path / "events.csv"
    external_events_path.write_text(
        "totally,forged,content\n",
        encoding="utf-8",
    )

    report_card = read_benchmark_report_card_payload(
        _track_b_public_report_card_path(
            reporting_result,
            baseline_id="track_b_structural_current",
        )
    )

    assert report_card.baseline_id == "track_b_structural_current"


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda metric: metric.__setitem__(
                "metric_value",
                float(metric["metric_value"]) + 0.123456,
            ),
            "Track B public report card metric_value does not match the materialized public runner bundle",
        ),
        (
            lambda metric: metric.__setitem__(
                "interval_low",
                float(metric["interval_low"]) - 0.000001,
            ),
            "Track B public report card interval_low does not match the materialized public runner bundle",
        ),
        (
            lambda metric: metric.__setitem__(
                "interval_high",
                float(metric["interval_high"]) + 0.000001,
            ),
            "Track B public report card interval_high does not match the materialized public runner bundle",
        ),
    ),
)
def test_read_track_b_report_card_rejects_tampered_headline_metrics(
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
    mutator(payload["slices"][0]["metrics"][0])
    report_card_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_report_card_payload(report_card_path)


def test_materialize_benchmark_reporting_materializes_track_b_public_runner_bundle(
    tmp_path: Path,
) -> None:
    _, _, _, reporting_output_dir, reporting_result = _materialize_track_b_public_payloads(
        tmp_path
    )

    report_card = next(
        read_benchmark_report_card_payload(Path(path))
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )

    for artifact in report_card.derived_from_artifacts:
        materialized_path = reporting_output_dir / artifact.artifact_path
        assert materialized_path.exists()
        assert sha256(materialized_path.read_bytes()).hexdigest() == artifact.sha256


def test_materialize_benchmark_reporting_emits_relative_track_b_public_paths(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    report_card = read_benchmark_report_card_payload(
        _track_b_public_report_card_path(
            reporting_result,
            baseline_id="track_b_structural_current",
        )
    )
    leaderboard = read_benchmark_leaderboard_payload(
        next(
            Path(path)
            for path in reporting_result["leaderboard_payload_files"]
            if path.endswith("replay_status_exact_match.json")
        )
    )

    assert all(
        not Path(artifact.artifact_path).is_absolute()
        for artifact in report_card.evaluation_input_artifacts
    )
    assert all(
        not Path(path_text).is_absolute()
        for path_text in leaderboard.report_card_files
    )
    assert all(
        not Path(entry.report_card_path).is_absolute()
        for entry in leaderboard.entries
    )


def test_materialize_track_b_public_bundle_rewrites_runner_contract(
    tmp_path: Path,
) -> None:
    forged_code_version = "forged-track-b-code-version"
    (
        _,
        _,
        runner_output_dir,
        reporting_output_dir,
        reporting_result,
    ) = _materialize_track_b_public_payloads(
        tmp_path,
        code_version=forged_code_version,
    )

    internal_run_id = _track_b_run_id_by_baseline(runner_output_dir)[
        "track_b_structural_current"
    ]
    report_card = next(
        read_benchmark_report_card_payload(Path(path))
        for path in reporting_result["report_card_files"]
        if "track_b_structural_current" in path
    )
    run_manifest_path = next(
        reporting_output_dir / artifact.artifact_path
        for artifact in report_card.derived_from_artifacts
        if artifact.artifact_name == "benchmark_model_run_manifest"
    )
    public_run_manifest = read_benchmark_model_run_manifest(run_manifest_path)

    assert public_run_manifest.run_id == report_card.report_card_id
    assert public_run_manifest.run_id != internal_run_id
    assert public_run_manifest.code_version == TRACK_B_PUBLIC_CODE_VERSION
    assert public_run_manifest.code_version != forged_code_version
    assert public_run_manifest.started_at == TRACK_B_PUBLIC_STARTED_AT
    assert public_run_manifest.completed_at == TRACK_B_PUBLIC_COMPLETED_AT
    assert public_run_manifest.notes == TRACK_B_PUBLIC_RUN_NOTES
    assert all(
        not Path(artifact.artifact_path).is_absolute()
        for artifact in public_run_manifest.input_artifacts
    )
    assert str(tmp_path) not in run_manifest_path.read_text(encoding="utf-8")

    metric_payload_path = next(
        reporting_output_dir / artifact.artifact_path
        for artifact in report_card.derived_from_artifacts
        if artifact.artifact_name == "benchmark_metric_output_payload"
    )
    public_metric_payload = read_benchmark_metric_output_payload(metric_payload_path)
    assert public_metric_payload.run_id == report_card.report_card_id

    interval_payload_path = next(
        reporting_output_dir / artifact.artifact_path
        for artifact in report_card.derived_from_artifacts
        if artifact.artifact_name == "benchmark_confidence_interval_payload"
    )
    public_interval_payload = read_benchmark_confidence_interval_payload(
        interval_payload_path
    )
    assert public_interval_payload.run_id == report_card.report_card_id

    case_output_path = next(
        reporting_output_dir / artifact.artifact_path
        for artifact in report_card.derived_from_artifacts
        if artifact.artifact_name == "track_b_case_output_payload"
    )
    public_case_output = read_track_b_case_output_payload(case_output_path)
    assert public_case_output.run_id == report_card.report_card_id

    confusion_summary_path = next(
        reporting_output_dir / artifact.artifact_path
        for artifact in report_card.derived_from_artifacts
        if artifact.artifact_name == "track_b_confusion_summary"
    )
    public_confusion_summary = read_track_b_confusion_summary(confusion_summary_path)
    assert public_confusion_summary.run_id == report_card.report_card_id

    for artifact in report_card.derived_from_artifacts:
        payload_text = (reporting_output_dir / artifact.artifact_path).read_text(
            encoding="utf-8"
        )
        assert internal_run_id not in payload_text
        assert forged_code_version not in payload_text


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


def test_read_track_b_leaderboard_rejects_tampered_leaderboard_id(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    payload["leaderboard_id"] = "forged_leaderboard_id"
    leaderboard_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="leaderboard_id does not match the stable public contract",
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


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload.__setitem__("metric_name", False),
            "metric_name must be a string",
        ),
        (
            lambda payload: payload.__setitem__("confidence_level", "0.95"),
            "confidence_level must be a float",
        ),
        (
            lambda payload: payload["entries"][0].__setitem__("rank", 1.5),
            "rank must be an integer",
        ),
    ),
)
def test_read_track_b_leaderboard_rejects_malformed_json_types(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    mutator(payload)
    _write_json_payload(leaderboard_path, payload)

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_leaderboard_payload(leaderboard_path)


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload["entries"][0].__setitem__(
                "metric_value",
                float(payload["entries"][0]["metric_value"]) + 0.25,
            ),
            "Track B public leaderboard entries do not match the referenced public report cards",
        ),
        (
            lambda payload: payload["entries"][0].__setitem__("rank", 99),
            "Track B public leaderboard entries do not match the referenced public report cards",
        ),
        (
            lambda payload: payload["entries"][0].__setitem__(
                "positive_entity_count",
                int(payload["entries"][0]["positive_entity_count"]) + 1,
            ),
            "Track B public leaderboard entries do not match the referenced public report cards",
        ),
    ),
)
def test_read_track_b_leaderboard_rejects_tampered_entry_values_ranks_and_counts(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    mutator(payload)
    leaderboard_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_leaderboard_payload(leaderboard_path)


def test_read_track_b_leaderboard_rejects_nonexistent_report_card_path(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    missing_report_card_id = "track_b_public__missing_report_card"
    missing_report_card_path = str(
        Path(payload["report_card_files"][0]).with_name(f"{missing_report_card_id}.json")
    )
    payload["report_card_files"][0] = missing_report_card_path
    payload["entries"][0]["report_card_id"] = missing_report_card_id
    payload["entries"][0]["run_id"] = missing_report_card_id
    payload["entries"][0]["report_card_path"] = missing_report_card_path
    leaderboard_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B public leaderboard report_card_path does not exist",
    ):
        read_benchmark_leaderboard_payload(leaderboard_path)


def test_read_track_b_leaderboard_rejects_incomplete_expected_baseline_set(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    payload["report_card_files"] = payload["report_card_files"][:-1]
    payload["entries"] = payload["entries"][:-1]
    _write_json_payload(leaderboard_path, payload)

    with pytest.raises(
        ValueError,
        match="Track B public leaderboard report_card_files do not match the complete expected baseline set",
    ):
        read_benchmark_leaderboard_payload(leaderboard_path)


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload, path: payload["report_card_files"].__setitem__(
                0,
                str((path.parent / payload["report_card_files"][0]).resolve()),
            ),
            r"Track B public leaderboard report_card_files\[\] must be a relative path",
        ),
        (
            lambda payload, path: payload["entries"][0].__setitem__(
                "report_card_path",
                str((path.parent / payload["entries"][0]["report_card_path"]).resolve()),
            ),
            "Track B public leaderboard entry report_card_path must be a relative path",
        ),
    ),
)
def test_read_track_b_leaderboard_rejects_absolute_public_report_card_paths(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    mutator(payload, leaderboard_path)
    _write_json_payload(leaderboard_path, payload)

    with pytest.raises(ValueError, match=error_fragment):
        read_benchmark_leaderboard_payload(leaderboard_path)


def test_read_track_b_leaderboard_rejects_escaped_public_report_card_paths(
    tmp_path: Path,
) -> None:
    _, _, _, _, reporting_result = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    escaped_path = "../../../../../" + payload["report_card_files"][0]
    payload["report_card_files"][0] = escaped_path
    payload["entries"][0]["report_card_path"] = escaped_path
    _write_json_payload(leaderboard_path, payload)

    with pytest.raises(
        ValueError,
        match="must stay within the Track B public payload root",
    ):
        read_benchmark_leaderboard_payload(leaderboard_path)


def test_read_track_b_leaderboard_ignores_registry_redirection_when_validating_baseline_set(
    tmp_path: Path,
) -> None:
    (
        _,
        _,
        _,
        reporting_output_dir,
        reporting_result,
    ) = _materialize_track_b_public_payloads(tmp_path)

    leaderboard_path = next(
        Path(path)
        for path in reporting_result["leaderboard_payload_files"]
        if path.endswith("replay_status_exact_match.json")
    )
    report_card_path = _track_b_public_report_card_path(
        reporting_result,
        baseline_id="track_b_structural_current",
    )
    snapshot_manifest_path = _track_b_public_input_artifact_path(
        report_card_path,
        artifact_name="benchmark_snapshot_manifest",
    )
    registry_path = _write_track_b_registry_variant(
        tmp_path,
        supported_baseline_ids=("track_b_structural_current",),
    )
    snapshot_manifest_payload = json.loads(
        snapshot_manifest_path.read_text(encoding="utf-8")
    )
    snapshot_manifest_payload["task_registry_path"] = str(registry_path)
    snapshot_manifest_payload["baseline_ids"] = ["track_b_structural_current"]
    _write_json_payload(snapshot_manifest_path, snapshot_manifest_payload)
    _refresh_track_b_public_input_artifact_sha(
        report_card_path,
        reporting_output_dir,
        artifact_name="benchmark_snapshot_manifest",
        artifact_path=snapshot_manifest_path,
    )
    cohort_manifest_path = _track_b_public_input_artifact_path(
        report_card_path,
        artifact_name="benchmark_cohort_manifest",
    )
    cohort_manifest_payload = json.loads(
        cohort_manifest_path.read_text(encoding="utf-8")
    )
    cohort_manifest_payload["snapshot_manifest_artifact_sha256"] = sha256(
        snapshot_manifest_path.read_bytes()
    ).hexdigest()
    _write_json_payload(cohort_manifest_path, cohort_manifest_payload)
    _refresh_track_b_public_input_artifact_sha(
        report_card_path,
        reporting_output_dir,
        artifact_name="benchmark_cohort_manifest",
        artifact_path=cohort_manifest_path,
    )

    leaderboard_payload = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    leaderboard_payload["report_card_files"] = tuple(
        path_text
        for path_text in leaderboard_payload["report_card_files"]
        if "track_b_structural_current" in path_text
    )
    leaderboard_payload["entries"] = [
        entry
        for entry in leaderboard_payload["entries"]
        if entry["baseline_id"] == "track_b_structural_current"
    ]
    _write_json_payload(leaderboard_path, leaderboard_payload)

    with pytest.raises(
        ValueError,
        match=(
            "Track B public report card snapshot manifest baseline_ids must match "
            "the full frozen Track B available_now baseline set"
        ),
    ):
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
        match="metric_unit must be a string",
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
