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
from scz_target_engine.observatory.packet_nav import (
    FailureAnalogView,
    PacketDetailView,
    PacketSummaryView,
    PolicyComparisonView,
    RescueEvidenceSummaryView,
    RescueTaskRegistryEntry,
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
    generated_dir: Path | None = None,
    task_registry_path: Path | None = None,
) -> ObservatoryIndex:
    suites = list_benchmark_suites(task_registry_path=task_registry_path)
    tasks = list_benchmark_tasks(task_registry_path=task_registry_path)
    slices = list_public_slices()
    gen_index = discover_generated_payloads(generated_dir=generated_dir)
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


def format_packet_list(packets: tuple[PacketSummaryView, ...]) -> str:
    lines: list[str] = []
    lines.append("Hypothesis Packets")
    lines.append("=" * 70)
    if not packets:
        lines.append("  (none)")
        return "\n".join(lines)
    lines.append("")
    lines.append(
        f"{'Packet ID':<36} {'Entity':<12} {'Policy':<30} "
        f"{'Score':>7} {'Status':<10}"
    )
    lines.append("-" * 70)
    for pkt in packets:
        score_str = f"{pkt.policy_score:.3f}" if pkt.policy_score is not None else "N/A"
        lines.append(
            f"{pkt.packet_id:<36} {pkt.entity_label:<12} "
            f"{pkt.policy_label:<30} {score_str:>7} {pkt.policy_status:<10}"
        )
    lines.append("")
    lines.append(f"Total: {len(packets)} packets")
    lines.append("")
    return "\n".join(lines)


def format_packet_detail(detail: PacketDetailView) -> str:
    lines: list[str] = []
    lines.append(f"Packet: {detail.packet_id}")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"  Entity: {detail.entity_label} ({detail.entity_id})")
    lines.append(f"  Policy: {detail.policy_label} ({detail.policy_id})")
    lines.append(f"  Priority domain: {detail.priority_domain}")
    lines.append("")

    # Decision focus
    lines.append("Decision Focus")
    lines.append("-" * 50)
    df = detail.decision_focus
    lines.append(f"  Review question: {df.get('review_question', '')}")
    lines.append(
        f"  Decision options: {', '.join(df.get('decision_options', []))}"
    )
    lines.append(f"  Current readout: {df.get('current_readout', '')}")
    lines.append("")

    # Hypothesis
    lines.append("Hypothesis")
    lines.append("-" * 50)
    hyp = detail.hypothesis
    lines.append(f"  Statement: {hyp.get('statement', '')}")
    lines.append(
        f"  Direction: {hyp.get('desired_perturbation_direction', '')} "
        f"via {hyp.get('modality_hypothesis', '')}"
    )
    lines.append(f"  Confidence: {hyp.get('confidence', '')}")
    lines.append("")

    # Policy signal
    lines.append("Policy Signal")
    lines.append("-" * 50)
    ps = detail.policy_signal_summary
    score = ps.get("score")
    score_str = f"{score:.3f}" if isinstance(score, (int, float)) else "N/A"
    base = ps.get("base_score")
    base_str = f"{base:.3f}" if isinstance(base, (int, float)) else "N/A"
    lines.append(f"  Score: {score_str} (base: {base_str})")
    lines.append(f"  Status: {ps.get('status', '')}")
    lines.append(f"  Description: {ps.get('description', '')}")
    lines.append("")

    # Evidence anchors
    lines.append("Evidence Anchors")
    lines.append("-" * 50)
    lines.append(f"  Gap status: {detail.evidence_anchor_gap_status}")
    lines.append(f"  Program history: {detail.program_history_gap_status}")
    for anchor in detail.evidence_anchors:
        lines.append(
            f"  {anchor.get('role', '')}: {anchor.get('event_id', '')} "
            f"({anchor.get('event_type', '')}, {anchor.get('outcome', '')}) - "
            f"{anchor.get('why_it_matters', '')}"
        )
    lines.append("")

    # Risk digest
    lines.append("Risk Digest")
    lines.append("-" * 50)
    for risk in detail.risk_digest:
        lines.append(f"  {risk}")
    lines.append("")

    # Contradiction handling
    lines.append("Contradiction Handling")
    lines.append("-" * 50)
    ch = detail.contradiction_handling
    lines.append(f"  Status: {ch.get('status', '')}")
    for cond in ch.get("contradiction_conditions", []):
        lines.append(f"  Condition: {cond}")
    lines.append("")

    # Failure memory
    lines.append("Failure Memory")
    lines.append("-" * 50)
    fm = detail.failure_memory_summary
    lines.append(f"  Replay status: {fm.get('replay_status', '')}")
    lines.append(f"  Replay summary: {fm.get('replay_summary', '')}")
    lines.append(
        f"  Supporting: {fm.get('supporting_reason_count', 0)}, "
        f"Offsetting: {fm.get('offsetting_reason_count', 0)}, "
        f"Uncertainty: {fm.get('uncertainty_reason_count', 0)}"
    )
    lines.append("")

    # Escape logic
    lines.append("Failure Escape Logic")
    lines.append("-" * 50)
    fe = detail.failure_escape_logic
    lines.append(f"  Escape status: {fe.get('status', '')}")
    for route in fe.get("escape_routes", []):
        if isinstance(route, dict):
            lines.append(
                f"  Route: {route.get('event_id', '')} - "
                f"{route.get('explanation', '')}"
            )
    lines.append("")

    # Evidence needed next
    lines.append("Evidence Needed Next")
    lines.append("-" * 50)
    for item in detail.evidence_needed_next:
        lines.append(f"  {item}")
    lines.append("")

    # Traceability
    lines.append("Traceability")
    lines.append("-" * 50)
    tr = detail.traceability
    source_artifacts = tr.get("source_artifacts", {})
    if isinstance(source_artifacts, dict):
        for name, ref in source_artifacts.items():
            lines.append(f"  {name}: {ref}")
    lines.append(
        f"  Policy pointer: {tr.get('policy_entity_pointer', '')} -> "
        f"{tr.get('policy_score_pointer', '')}"
    )
    lines.append(f"  Ledger pointer: {tr.get('ledger_target_pointer', '')}")
    lines.append("")

    # Rescue evidence (if present)
    if detail.rescue_evidence is not None:
        lines.append("Rescue Evidence")
        lines.append("-" * 50)
        re_ev = detail.rescue_evidence
        lines.append(f"  Coverage: {re_ev.get('coverage_status', '')}")
        for task in re_ev.get("matched_tasks", []):
            if isinstance(task, dict):
                lines.append(
                    f"  Task: {task.get('task_label', '')} "
                    f"(decision: {task.get('entity_rescue_decision', '')})"
                )
        for sig in re_ev.get("conflict_signals", []):
            lines.append(f"  Conflict: {sig}")
        lines.append("")

    # First assay (if present)
    if detail.first_assay is not None:
        lines.append("First Assay Recommendation")
        lines.append("-" * 50)
        fa = detail.first_assay
        lines.append(f"  Class: {fa.get('recommended_assay_class', '')}")
        lines.append(f"  Rationale: {fa.get('rationale', '')}")
        for step in fa.get("next_steps", []):
            lines.append(f"  Next: {step}")
        lines.append("")

    return "\n".join(lines)


def format_failure_analogs(analogs: tuple[FailureAnalogView, ...]) -> str:
    lines: list[str] = []
    lines.append("Failure Analog / Replay Comparisons")
    lines.append("=" * 70)
    if not analogs:
        lines.append("  (none)")
        return "\n".join(lines)
    for analog in analogs:
        lines.append("")
        lines.append(
            f"  {analog.entity_label} | {analog.policy_label} "
            f"({analog.packet_id})"
        )
        lines.append(f"    Replay status: {analog.replay_status}")
        lines.append(f"    Replay summary: {analog.replay_summary}")
        lines.append(f"    Escape status: {analog.escape_status}")
        lines.append(
            f"    Supporting reasons: {len(analog.supporting_reasons)}"
        )
        lines.append(
            f"    Offsetting reasons: {len(analog.offsetting_reasons)}"
        )
        lines.append(
            f"    Uncertainty reasons: {len(analog.uncertainty_reasons)}"
        )
        if analog.escape_routes:
            for route in analog.escape_routes:
                lines.append(
                    f"    Escape route: {route.get('event_id', '')} - "
                    f"{route.get('explanation', '')}"
                )
        if analog.falsification_conditions:
            lines.append("    Falsification conditions:")
            for cond in analog.falsification_conditions:
                lines.append(f"      {cond}")
    lines.append("")
    return "\n".join(lines)


def format_policy_comparison(view: PolicyComparisonView) -> str:
    lines: list[str] = []
    lines.append("Policy Comparison")
    lines.append("=" * 70)
    lines.append("")
    lines.append(
        f"{'Entity':<12} {'Policy':<30} {'Score':>7} {'Base':>7} "
        f"{'Contra':<12} {'Replay':<18}"
    )
    lines.append("-" * 70)
    for row in view.rows:
        score_str = f"{row.score:.3f}" if row.score is not None else "N/A"
        base_str = f"{row.base_score:.3f}" if row.base_score is not None else "N/A"
        lines.append(
            f"{row.entity_label:<12} {row.policy_label:<30} "
            f"{score_str:>7} {base_str:>7} "
            f"{row.contradiction_status:<12} {row.replay_status:<18}"
        )
    lines.append("")
    lines.append(
        f"Entities: {len(view.entity_ids)}, "
        f"Policies: {len(view.policy_ids)}"
    )
    lines.append("")
    return "\n".join(lines)


def format_rescue_tasks(tasks: tuple[RescueTaskRegistryEntry, ...]) -> str:
    lines: list[str] = []
    lines.append("Rescue Task Registry")
    lines.append("=" * 70)
    if not tasks:
        lines.append("  (none)")
        return "\n".join(lines)
    for task in tasks:
        lines.append("")
        lines.append(f"  {task.task_id}")
        lines.append(f"    label: {task.task_label}")
        lines.append(f"    type: {task.task_type}")
        lines.append(f"    disease: {task.disease}")
        lines.append(f"    entity_type: {task.entity_type}")
        lines.append(f"    scope: {task.contract_scope}")
        lines.append(f"    status: {task.registry_status}")
    lines.append("")
    return "\n".join(lines)


def format_rescue_evidence_list(
    evidence: tuple[RescueEvidenceSummaryView, ...],
) -> str:
    lines: list[str] = []
    lines.append("Rescue Evidence Summary")
    lines.append("=" * 70)
    if not evidence:
        lines.append("  (no rescue evidence in loaded packets)")
        return "\n".join(lines)
    for ev in evidence:
        lines.append("")
        lines.append(
            f"  {ev.entity_label} | {ev.policy_label} ({ev.packet_id})"
        )
        lines.append(f"    Coverage: {ev.coverage_status}")
        lines.append(f"    Matched tasks: {ev.matched_task_count}")
        lines.append(f"    Assay class: {ev.assay_class}")
        if ev.conflict_signals:
            for sig in ev.conflict_signals:
                lines.append(f"    Conflict: {sig}")
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "ObservatoryIndex",
    "build_observatory_index",
    "format_failure_analogs",
    "format_leaderboard",
    "format_observatory_index",
    "format_packet_detail",
    "format_packet_list",
    "format_policy_comparison",
    "format_report_cards",
    "format_rescue_evidence_list",
    "format_rescue_tasks",
]
