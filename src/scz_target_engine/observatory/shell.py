"""Observatory shell: top-level index and navigation surface."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.observatory.benchmark_nav import (
    BenchmarkSuiteView,
    BenchmarkTaskView,
    LeaderboardBrowseResult,
    ReportCardBrowseResult,
    list_benchmark_suites,
    list_benchmark_tasks,
    list_public_slices,
)
from scz_target_engine.observatory.loaders import (
    PublicSliceSummary,
    discover_generated_payloads,
)


@dataclass(frozen=True)
class ObservatoryIndex:
    suites: tuple[BenchmarkSuiteView, ...]
    tasks: tuple[BenchmarkTaskView, ...]
    public_slices: tuple[PublicSliceSummary, ...]
    generated_report_card_count: int
    generated_leaderboard_count: int
    generated_snapshot_count: int


def build_observatory_index(
    data_dir: Path | None = None,
    task_registry_path: Path | None = None,
) -> ObservatoryIndex:
    suites = list_benchmark_suites(task_registry_path=task_registry_path)
    tasks = list_benchmark_tasks(task_registry_path=task_registry_path)
    slices = list_public_slices()
    gen_dir = (data_dir / "benchmark" / "generated") if data_dir else None
    gen_index = discover_generated_payloads(generated_dir=gen_dir)
    return ObservatoryIndex(
        suites=suites,
        tasks=tasks,
        public_slices=slices,
        generated_report_card_count=len(gen_index.report_card_files),
        generated_leaderboard_count=len(gen_index.leaderboard_files),
        generated_snapshot_count=len(gen_index.snapshot_manifest_files),
    )


def format_observatory_index(index: ObservatoryIndex) -> str:
    lines: list[str] = []
    lines.append("Observatory Index")
    lines.append("=" * 50)
    lines.append("")
    lines.append("Benchmark Suites")
    lines.append("-" * 50)
    if not index.suites:
        lines.append("  (none)")
    for suite in index.suites:
        lines.append(f"  {suite.suite_id}")
        lines.append(f"    label: {suite.suite_label}")
        lines.append(f"    tasks: {', '.join(suite.task_ids)}")
    lines.append("")

    lines.append("Benchmark Tasks")
    lines.append("-" * 50)
    if not index.tasks:
        lines.append("  (none)")
    for task in index.tasks:
        lines.append(f"  {task.task_id}")
        lines.append(f"    suite: {task.suite_id}")
        lines.append(f"    question: {task.benchmark_question_id}")
        lines.append(f"    entity types: {', '.join(task.entity_types)}")
        baselines = ", ".join(task.supported_baseline_ids)
        lines.append(f"    baselines: {baselines}")
    lines.append("")

    lines.append("Public Historical Slices")
    lines.append("-" * 50)
    if not index.public_slices:
        lines.append("  (none)")
    for sl in index.public_slices:
        lines.append(f"  {sl.slice_id}")
        lines.append(f"    as_of_date: {sl.as_of_date}")
        included = ", ".join(sl.included_sources) or "none"
        lines.append(f"    included: {included}")
        excluded = ", ".join(sl.excluded_source_names) or "none"
        lines.append(f"    excluded: {excluded}")
    lines.append("")

    lines.append("Generated Artifacts")
    lines.append("-" * 50)
    lines.append(f"  report cards: {index.generated_report_card_count}")
    lines.append(f"  leaderboards: {index.generated_leaderboard_count}")
    lines.append(f"  snapshot manifests: {index.generated_snapshot_count}")
    lines.append("")

    return "\n".join(lines)


def format_leaderboard(result: LeaderboardBrowseResult) -> str:
    lines: list[str] = []
    header = (
        f"Leaderboard: {result.entity_type} / "
        f"{result.horizon} / {result.metric_name}"
    )
    lines.append(header)
    lines.append(
        f"  snapshot: {result.snapshot_id} (as_of_date: {result.as_of_date})"
    )
    lines.append("=" * 70)
    lines.append("")
    lines.append(
        f"{'Rank':<6} {'Baseline':<24} {'Value':>8} "
        f"{'CI Low':>8} {'CI High':>8} {'Coverage':>10}"
    )
    lines.append("-" * 70)
    for entry in result.entries:
        if entry.covered_entity_count is not None:
            coverage = (
                f"{entry.covered_entity_count}/{entry.admissible_entity_count}"
            )
        else:
            coverage = str(entry.admissible_entity_count)
        lines.append(
            f"{entry.rank:<6} {entry.baseline_label:<24} "
            f"{entry.metric_value:>8.4f} {entry.interval_low:>8.4f} "
            f"{entry.interval_high:>8.4f} {coverage:>10}"
        )
    lines.append("")
    return "\n".join(lines)


def format_report_cards(results: tuple[ReportCardBrowseResult, ...]) -> str:
    lines: list[str] = []
    lines.append("Report Cards")
    lines.append("=" * 50)
    if not results:
        lines.append("  (none generated)")
        return "\n".join(lines)
    for card in results:
        lines.append("")
        lines.append(f"  {card.baseline_label} ({card.baseline_id})")
        lines.append(
            f"    snapshot: {card.snapshot_id} (as_of_date: {card.as_of_date})"
        )
        lines.append(f"    slices: {card.slice_count}")
        for sl in card.slices:
            primary = sl.metrics[0] if sl.metrics else None
            metric_text = (
                f"{primary.metric_name}={primary.metric_value:.4f}"
                if primary
                else "no metrics"
            )
            lines.append(f"      {sl.entity_type}/{sl.horizon}: {metric_text}")
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "ObservatoryIndex",
    "build_observatory_index",
    "format_leaderboard",
    "format_observatory_index",
    "format_report_cards",
]
