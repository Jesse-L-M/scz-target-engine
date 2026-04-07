from __future__ import annotations

import json
from pathlib import Path

import pytest

from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_leaderboard import materialize_benchmark_reporting
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.observatory.benchmark_nav import (
    BenchmarkSuiteView,
    BenchmarkTaskView,
    LeaderboardSliceKey,
    ReportCardBrowseResult,
    browse_leaderboard,
    browse_report_cards,
    list_available_leaderboard_slices,
    list_benchmark_suites,
    list_benchmark_tasks,
    list_public_slices,
)
from scz_target_engine.observatory.loaders import (
    PublicSliceCatalog,
    PublicSliceSummary,
    discover_generated_payloads,
    load_public_slice_catalog,
)
from scz_target_engine.observatory.shell import (
    ObservatoryIndex,
    build_observatory_index,
    format_leaderboard,
    format_observatory_index,
    format_report_cards,
)


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)


# --- loader tests ---


def test_load_public_slice_catalog_returns_catalog() -> None:
    catalog = load_public_slice_catalog()
    assert catalog is not None
    assert isinstance(catalog, PublicSliceCatalog)
    assert catalog.benchmark_suite_id == "scz_translational_suite"
    assert catalog.benchmark_task_id == "scz_translational_task"
    assert len(catalog.slices) == 3


def test_load_public_slice_catalog_missing_file_returns_none(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "nonexistent.json"
    assert load_public_slice_catalog(catalog_path=missing) is None


@pytest.mark.parametrize(
    "excluded_source_entry",
    (
        {},
        {"source_name": ""},
    ),
)
def test_load_public_slice_catalog_rejects_missing_excluded_source_name(
    tmp_path: Path,
    excluded_source_entry: dict[str, str],
) -> None:
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "scz_translational_task",
                "slices": [
                    {
                        "slice_id": "slice_1",
                        "as_of_date": "2024-06-15",
                        "included_sources": ["PGC"],
                        "excluded_sources": [excluded_source_entry],
                        "slice_dir": "data/benchmark/public_slices/slice_1",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="source_name is required"):
        load_public_slice_catalog(catalog_path=catalog_path)


def test_discover_generated_payloads_empty_dir(tmp_path: Path) -> None:
    index = discover_generated_payloads(generated_dir=tmp_path)
    assert index.report_card_files == ()
    assert index.leaderboard_files == ()
    assert index.snapshot_manifest_files == ()


def test_discover_generated_payloads_nonexistent_dir(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    index = discover_generated_payloads(generated_dir=missing)
    assert index.report_card_files == ()


# --- benchmark_nav tests ---


def test_list_benchmark_suites() -> None:
    suites = list_benchmark_suites()
    assert len(suites) >= 1
    suite = suites[0]
    assert isinstance(suite, BenchmarkSuiteView)
    assert suite.suite_id == "scz_translational_suite"
    assert suite.task_count >= 1


def test_list_benchmark_tasks() -> None:
    tasks = list_benchmark_tasks()
    assert len(tasks) >= 1
    task = tasks[0]
    assert isinstance(task, BenchmarkTaskView)
    assert task.task_id == "scz_translational_task"
    assert "gene" in task.entity_types


def test_list_benchmark_tasks_filter_by_suite() -> None:
    tasks = list_benchmark_tasks(suite_id="scz_translational_suite")
    assert len(tasks) >= 1
    assert all(t.suite_id == "scz_translational_suite" for t in tasks)


def test_list_benchmark_tasks_unknown_suite() -> None:
    tasks = list_benchmark_tasks(suite_id="nonexistent_suite")
    assert tasks == ()


def test_list_public_slices() -> None:
    slices = list_public_slices()
    assert len(slices) == 3
    assert isinstance(slices[0], PublicSliceSummary)
    slice_ids = {s.slice_id for s in slices}
    assert "scz_translational_2024_06_15" in slice_ids
    assert "scz_translational_2024_06_18" in slice_ids
    assert "scz_translational_2024_06_20" in slice_ids


def test_list_available_leaderboard_slices_empty(tmp_path: Path) -> None:
    slices = list_available_leaderboard_slices(generated_dir=tmp_path)
    assert slices == ()


def test_browse_report_cards_empty(tmp_path: Path) -> None:
    cards = browse_report_cards(generated_dir=tmp_path)
    assert cards == ()


def test_browse_leaderboard_empty(tmp_path: Path) -> None:
    result = browse_leaderboard(
        entity_type="gene",
        horizon="1y",
        metric_name="average_precision_any_positive_outcome",
        generated_dir=tmp_path,
    )
    assert result is None


# --- shell tests ---


def test_build_observatory_index() -> None:
    index = build_observatory_index()
    assert isinstance(index, ObservatoryIndex)
    assert len(index.suites) >= 1
    assert len(index.tasks) >= 1
    assert len(index.public_slices) == 3


def test_format_observatory_index() -> None:
    index = build_observatory_index()
    output = format_observatory_index(index)
    assert "Observatory Index" in output
    assert "Benchmark Suites" in output
    assert "scz_translational_suite" in output
    assert "scz_translational_task" in output
    assert "Public Historical Slices" in output
    assert "scz_translational_2024_06_15" in output
    assert "Generated Artifacts" in output


def test_format_observatory_index_empty() -> None:
    index = ObservatoryIndex(
        suites=(),
        tasks=(),
        public_slices=(),
        generated_report_card_count=0,
        generated_leaderboard_count=0,
        generated_snapshot_count=0,
    )
    output = format_observatory_index(index)
    assert "(none)" in output
    assert "report cards: 0" in output


# --- integration test with generated outputs ---


def _materialize_fixture_outputs(
    tmp_path: Path,
) -> Path:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_dir = tmp_path / "public_payloads"

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
    materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )
    materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_dir,
    )
    return reporting_dir


def test_browse_report_cards_with_generated_outputs(tmp_path: Path) -> None:
    reporting_dir = _materialize_fixture_outputs(tmp_path)
    cards = browse_report_cards(generated_dir=reporting_dir.parent)
    assert len(cards) >= 1
    card = cards[0]
    assert isinstance(card, ReportCardBrowseResult)
    assert card.snapshot_id == "scz_fixture_2024_06_30"
    assert card.slice_count > 0


def test_list_leaderboard_slices_with_generated_outputs(tmp_path: Path) -> None:
    reporting_dir = _materialize_fixture_outputs(tmp_path)
    slices = list_available_leaderboard_slices(
        generated_dir=reporting_dir.parent,
    )
    assert len(slices) > 0
    assert all(isinstance(s, LeaderboardSliceKey) for s in slices)
    entity_types = {s.entity_type for s in slices}
    assert "gene" in entity_types


def test_browse_leaderboard_with_generated_outputs(tmp_path: Path) -> None:
    reporting_dir = _materialize_fixture_outputs(tmp_path)
    slices = list_available_leaderboard_slices(
        generated_dir=reporting_dir.parent,
    )
    assert slices
    first = slices[0]
    result = browse_leaderboard(
        entity_type=first.entity_type,
        horizon=first.horizon,
        metric_name=first.metric_name,
        generated_dir=reporting_dir.parent,
    )
    assert result is not None
    assert len(result.entries) >= 1
    assert result.entries[0].rank == 1


def test_format_leaderboard_with_generated_outputs(tmp_path: Path) -> None:
    reporting_dir = _materialize_fixture_outputs(tmp_path)
    slices = list_available_leaderboard_slices(
        generated_dir=reporting_dir.parent,
    )
    first = slices[0]
    result = browse_leaderboard(
        entity_type=first.entity_type,
        horizon=first.horizon,
        metric_name=first.metric_name,
        generated_dir=reporting_dir.parent,
    )
    assert result is not None
    output = format_leaderboard(result)
    assert "Leaderboard:" in output
    assert "Rank" in output
    assert "Baseline" in output


def test_format_report_cards_with_generated_outputs(tmp_path: Path) -> None:
    reporting_dir = _materialize_fixture_outputs(tmp_path)
    cards = browse_report_cards(generated_dir=reporting_dir.parent)
    output = format_report_cards(cards)
    assert "Report Cards" in output
    assert "snapshot:" in output


def test_format_report_cards_empty() -> None:
    output = format_report_cards(())
    assert "Report Cards" in output
    assert "(none generated)" in output


# --- CLI integration tests ---


def test_observatory_browse_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "browse"])
    assert result == 0


def test_observatory_browse_legacy_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory-browse"])
    assert result == 0


def test_observatory_report_cards_cli_empty(tmp_path: Path) -> None:
    from scz_target_engine.cli import main

    result = main([
        "observatory", "report-cards",
        "--generated-dir", str(tmp_path),
    ])
    assert result == 0


def test_observatory_leaderboard_slices_cli_empty(tmp_path: Path) -> None:
    from scz_target_engine.cli import main

    result = main([
        "observatory", "leaderboard-slices",
        "--generated-dir", str(tmp_path),
    ])
    assert result == 0


def test_observatory_browse_cli_with_generated_dir(tmp_path: Path) -> None:
    from scz_target_engine.cli import main

    result = main([
        "observatory", "browse",
        "--generated-dir", str(tmp_path),
    ])
    assert result == 0


# --- regression: no mixed-root data ---


def test_custom_generated_dir_does_not_mix_with_repo_defaults(
    tmp_path: Path,
) -> None:
    """A custom generated-dir must only affect generated artifact counts.

    The previous --data-dir flag was half-wired: generated counts came from the
    custom root but suites/tasks/public-slices still came from repo defaults.
    After the fix, build_observatory_index accepts only --generated-dir, which
    truthfully overrides only the generated artifact tree.
    """
    fake_report_card_dir = (
        tmp_path / "report_cards" / "suite" / "task" / "snap"
    )
    fake_report_card_dir.mkdir(parents=True)
    (fake_report_card_dir / "fake.json").write_text("{}")

    index = build_observatory_index(generated_dir=tmp_path)

    # Generated counts come from the custom dir.
    assert index.generated_report_card_count == 1
    assert index.generated_leaderboard_count == 0
    assert index.generated_snapshot_count == 0

    # Structural metadata always comes from the repo.
    assert len(index.suites) >= 1
    assert index.suites[0].suite_id == "scz_translational_suite"
    assert len(index.tasks) >= 1
    assert index.tasks[0].task_id == "scz_translational_task"
    assert len(index.public_slices) == 3


def test_build_observatory_index_no_data_dir_parameter() -> None:
    """build_observatory_index must not accept a data_dir parameter.

    Ensures the old half-wired --data-dir override cannot be reintroduced.
    """
    import inspect

    sig = inspect.signature(build_observatory_index)
    assert "data_dir" not in sig.parameters, (
        "build_observatory_index must not accept data_dir; "
        "use generated_dir instead"
    )
    assert "generated_dir" in sig.parameters


def test_empty_generated_dir_shows_zero_counts(tmp_path: Path) -> None:
    """An empty custom generated-dir yields zero counts, not repo defaults."""
    index = build_observatory_index(generated_dir=tmp_path)
    assert index.generated_report_card_count == 0
    assert index.generated_leaderboard_count == 0
    assert index.generated_snapshot_count == 0
    # Structural metadata still present from repo.
    assert len(index.suites) >= 1
    assert len(index.public_slices) == 3
