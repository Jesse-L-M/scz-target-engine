"""Benchmark navigation surfaces for the observatory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.benchmark_leaderboard import (
    BenchmarkLeaderboardEntry,
    BenchmarkReportCardSlice,
    read_benchmark_leaderboard_payload,
)
from scz_target_engine.benchmark_registry import load_benchmark_suite_contracts
from scz_target_engine.observatory.loaders import (
    PublicSliceSummary,
    discover_generated_payloads,
    load_public_slice_catalog,
    load_report_cards,
)


@dataclass(frozen=True)
class BenchmarkSuiteView:
    suite_id: str
    suite_label: str
    task_count: int
    task_ids: tuple[str, ...]


@dataclass(frozen=True)
class BenchmarkTaskView:
    suite_id: str
    task_id: str
    task_label: str
    benchmark_question_id: str
    entity_types: tuple[str, ...]
    supported_baseline_ids: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class LeaderboardSliceKey:
    entity_type: str
    horizon: str
    metric_name: str


@dataclass(frozen=True)
class LeaderboardBrowseResult:
    leaderboard_id: str
    entity_type: str
    horizon: str
    metric_name: str
    snapshot_id: str
    as_of_date: str
    entries: tuple[BenchmarkLeaderboardEntry, ...]


@dataclass(frozen=True)
class ReportCardBrowseResult:
    report_card_id: str
    baseline_id: str
    baseline_label: str
    snapshot_id: str
    as_of_date: str
    slice_count: int
    slices: tuple[BenchmarkReportCardSlice, ...]


def list_benchmark_suites(
    task_registry_path: Path | None = None,
) -> tuple[BenchmarkSuiteView, ...]:
    suites = load_benchmark_suite_contracts(task_registry_path=task_registry_path)
    return tuple(
        BenchmarkSuiteView(
            suite_id=suite.suite_id,
            suite_label=suite.suite_label,
            task_count=len(suite.tasks),
            task_ids=tuple(task.task_id for task in suite.tasks),
        )
        for suite in suites
    )


def list_benchmark_tasks(
    suite_id: str | None = None,
    task_registry_path: Path | None = None,
) -> tuple[BenchmarkTaskView, ...]:
    suites = load_benchmark_suite_contracts(task_registry_path=task_registry_path)
    views: list[BenchmarkTaskView] = []
    for suite in suites:
        if suite_id and suite.suite_id != suite_id:
            continue
        for task in suite.tasks:
            views.append(
                BenchmarkTaskView(
                    suite_id=task.suite_id,
                    task_id=task.task_id,
                    task_label=task.task_label,
                    benchmark_question_id=task.benchmark_question_id,
                    entity_types=task.entity_types,
                    supported_baseline_ids=task.supported_baseline_ids,
                    notes=task.notes,
                )
            )
    return tuple(views)


def list_public_slices(
    catalog_path: Path | None = None,
) -> tuple[PublicSliceSummary, ...]:
    catalog = load_public_slice_catalog(catalog_path)
    if catalog is None:
        return ()
    return catalog.slices


def list_available_leaderboard_slices(
    generated_dir: Path | None = None,
) -> tuple[LeaderboardSliceKey, ...]:
    index = discover_generated_payloads(generated_dir)
    if not index.leaderboard_files:
        return ()
    keys: list[LeaderboardSliceKey] = []
    for path in index.leaderboard_files:
        lb = read_benchmark_leaderboard_payload(path)
        keys.append(
            LeaderboardSliceKey(
                entity_type=lb.entity_type,
                horizon=lb.horizon,
                metric_name=lb.metric_name,
            )
        )
    return tuple(keys)


def browse_leaderboard(
    entity_type: str,
    horizon: str,
    metric_name: str,
    generated_dir: Path | None = None,
) -> LeaderboardBrowseResult | None:
    index = discover_generated_payloads(generated_dir)
    for path in index.leaderboard_files:
        lb = read_benchmark_leaderboard_payload(path)
        if (
            lb.entity_type == entity_type
            and lb.horizon == horizon
            and lb.metric_name == metric_name
        ):
            return LeaderboardBrowseResult(
                leaderboard_id=lb.leaderboard_id,
                entity_type=lb.entity_type,
                horizon=lb.horizon,
                metric_name=lb.metric_name,
                snapshot_id=lb.snapshot_id,
                as_of_date=lb.as_of_date,
                entries=lb.entries,
            )
    return None


def browse_report_cards(
    generated_dir: Path | None = None,
) -> tuple[ReportCardBrowseResult, ...]:
    index = discover_generated_payloads(generated_dir)
    if not index.report_card_files:
        return ()
    cards = load_report_cards(index.report_card_files)
    return tuple(
        ReportCardBrowseResult(
            report_card_id=card.report_card_id,
            baseline_id=card.baseline_id,
            baseline_label=card.baseline_label,
            snapshot_id=card.snapshot_id,
            as_of_date=card.as_of_date,
            slice_count=len(card.slices),
            slices=card.slices,
        )
        for card in cards
    )


__all__ = [
    "BenchmarkSuiteView",
    "BenchmarkTaskView",
    "LeaderboardBrowseResult",
    "LeaderboardSliceKey",
    "ReportCardBrowseResult",
    "browse_leaderboard",
    "browse_report_cards",
    "list_available_leaderboard_slices",
    "list_benchmark_suites",
    "list_benchmark_tasks",
    "list_public_slices",
]
