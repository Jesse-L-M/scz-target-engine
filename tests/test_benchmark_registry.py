from pathlib import Path

from scz_target_engine.benchmark_protocol import FROZEN_BASELINE_IDS
from scz_target_engine.benchmark_registry import (
    DEFAULT_TASK_REGISTRY_PATH,
    load_benchmark_suite_contracts,
    resolve_benchmark_task_contract,
)


def test_registry_resolves_current_scz_fixture_task_contract() -> None:
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id="scz_translational_task"
    )

    assert task_contract.suite_id == "scz_translational_suite"
    assert task_contract.benchmark_question_id == "scz_translational_ranking_v1"
    assert task_contract.supported_baseline_ids == FROZEN_BASELINE_IDS
    assert task_contract.emitted_artifact_names == (
        "benchmark_snapshot_manifest",
        "benchmark_cohort_members",
        "benchmark_source_cohort_members",
        "benchmark_source_future_outcomes",
        "benchmark_cohort_manifest",
        "benchmark_cohort_labels",
        "benchmark_model_run_manifest",
        "benchmark_metric_output_payload",
        "benchmark_confidence_interval_payload",
    )
    assert task_contract.fixture_paths.snapshot_request_file == (
        Path("data/benchmark/fixtures/scz_small/snapshot_request.json").resolve()
    )
    assert task_contract.fixture_paths.archive_index_file == (
        Path("data/benchmark/fixtures/scz_small/source_archives.json").resolve()
    )


def test_registry_groups_fixture_task_under_single_suite() -> None:
    suites = load_benchmark_suite_contracts()

    assert len(suites) == 1
    assert suites[0].suite_id == "scz_translational_suite"
    assert suites[0].tasks[0].task_id == "scz_translational_task"
    assert DEFAULT_TASK_REGISTRY_PATH.exists()
