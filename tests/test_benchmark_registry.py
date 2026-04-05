from pathlib import Path

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
    assert task_contract.supported_baseline_ids == (
        "pgc_only",
        "schema_only",
        "opentargets_only",
        "v0_current",
        "v1_current",
        "v1_pre_numeric_pr7_heads",
        "v1_post_numeric_pr7_heads",
        "chembl_only",
        "random_with_coverage",
    )
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


def test_registry_resolves_track_b_failure_memory_task_contract() -> None:
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id="scz_failure_memory_track_b_task"
    )

    assert task_contract.suite_id == "scz_translational_suite"
    assert task_contract.benchmark_question_id == "scz_translational_ranking_v1"
    assert task_contract.entity_types == ("intervention_object",)
    assert task_contract.supported_baseline_ids == (
        "track_b_exact_target",
        "track_b_target_class",
        "track_b_nearest_history",
        "track_b_structural_current",
    )
    assert task_contract.fixture_paths.snapshot_request_file == (
        Path(
            "data/benchmark/fixtures/scz_failure_memory_2025_02_01/snapshot_request.json"
        ).resolve()
    )


def test_registry_preserves_legacy_question_lookup_default_task() -> None:
    task_contract = resolve_benchmark_task_contract(
        benchmark_question_id="scz_translational_ranking_v1"
    )

    assert task_contract.task_id == "scz_translational_task"


def test_registry_preserves_legacy_suite_lookup_default_task() -> None:
    task_contract = resolve_benchmark_task_contract(
        benchmark_suite_id="scz_translational_suite"
    )

    assert task_contract.task_id == "scz_translational_task"


def test_registry_resolves_task_from_question_and_baseline_context() -> None:
    task_contract = resolve_benchmark_task_contract(
        benchmark_question_id="scz_translational_ranking_v1",
        entity_types=("intervention_object",),
        baseline_ids=(
            "track_b_exact_target",
            "track_b_target_class",
            "track_b_nearest_history",
            "track_b_structural_current",
        ),
    )

    assert task_contract.task_id == "scz_failure_memory_track_b_task"


def test_registry_groups_fixture_task_under_single_suite() -> None:
    suites = load_benchmark_suite_contracts()

    assert len(suites) == 1
    assert suites[0].suite_id == "scz_translational_suite"
    assert suites[0].tasks[0].task_id == "scz_translational_task"
    assert tuple(task.task_id for task in suites[0].tasks) == (
        "scz_translational_task",
        "scz_failure_memory_track_b_task",
    )
    assert DEFAULT_TASK_REGISTRY_PATH.exists()
