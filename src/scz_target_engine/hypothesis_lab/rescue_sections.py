"""Rescue evidence augmentation for hypothesis packets.

Adds rescue evidence sections and first-assay logic to existing
hypothesis packets without modifying the post-review packet contract.
Rescue evidence is grounded in shipped rescue task outputs: baseline
comparison summaries, model admission decisions, and frozen evaluation
labels.
"""
from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

from scz_target_engine.io import read_csv_rows, read_json, write_json


RESCUE_AUGMENTED_PACKETS_SCHEMA_VERSION = "v1"
RESCUE_COVERAGE_MATCH = "rescue_task_match"
RESCUE_COVERAGE_NONE = "no_rescue_coverage"

FIRST_ASSAY_CLASS_RESCUE_ADVANCE = "rescue_informed_advance"
FIRST_ASSAY_CLASS_RESCUE_HOLD = "rescue_informed_hold"
FIRST_ASSAY_CLASS_RESCUE_DEPRIORITIZE = "rescue_informed_deprioritize"
FIRST_ASSAY_CLASS_NO_RESCUE = "policy_only"
FIRST_ASSAY_CLASS_CONFLICT = "rescue_policy_conflict"


def build_rescue_entity_labels(
    evaluation_label_rows: list[dict[str, str]],
    *,
    task_id: str,
    task_label: str,
    entity_id_field: str = "gene_id",
) -> dict[str, dict[str, str]]:
    """Build a lookup from entity_id to rescue task label metadata.

    Returns a dict mapping entity_id to a dict with task_id, task_label,
    decision, evaluation_label, split_name, and label_rationale.
    """
    _require_text(task_id, "task_id")
    _require_text(task_label, "task_label")
    labels: dict[str, dict[str, str]] = {}
    for row in evaluation_label_rows:
        entity_id = row.get(entity_id_field, "").strip()
        if not entity_id:
            continue
        labels[entity_id] = {
            "task_id": task_id,
            "task_label": task_label,
            "entity_id": entity_id,
            "gene_symbol": row.get("gene_symbol", "").strip(),
            "decision": row.get("decision", "").strip(),
            "evaluation_label": row.get("evaluation_label", "").strip(),
            "split_name": row.get("split_name", "").strip(),
            "label_rationale": row.get("label_rationale", "").strip(),
        }
    return labels


def build_rescue_evidence_section(
    packet: dict[str, object],
    *,
    rescue_entity_labels: dict[str, dict[str, str]],
    baseline_comparison_summaries: list[dict[str, object]],
    model_admission_summaries: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build the rescue_evidence section for a single hypothesis packet.

    Matches the packet entity to rescue tasks by entity_id. Returns a
    rescue evidence payload grounded in concrete rescue task outputs.
    """
    entity_id = _require_text(
        packet.get("entity_id"), "packet.entity_id"
    )
    entity_label = _require_text(
        packet.get("entity_label"), "packet.entity_label"
    )

    entity_label_entry = rescue_entity_labels.get(entity_id)
    matched_tasks = _build_matched_tasks(
        entity_id=entity_id,
        entity_label=entity_label,
        entity_label_entry=entity_label_entry,
        baseline_comparison_summaries=baseline_comparison_summaries,
        model_admission_summaries=model_admission_summaries or [],
    )

    coverage_status = RESCUE_COVERAGE_MATCH if matched_tasks else RESCUE_COVERAGE_NONE
    conflict_signals = _build_conflict_signals(
        packet=packet,
        matched_tasks=matched_tasks,
    )

    return {
        "coverage_status": coverage_status,
        "matched_tasks": matched_tasks,
        "conflict_signals": conflict_signals,
    }


def build_first_assay_section(
    packet: dict[str, object],
    *,
    rescue_evidence: dict[str, object],
) -> dict[str, object]:
    """Build the first_assay section for a single hypothesis packet.

    Combines rescue evidence with the packet's policy signal,
    contradiction handling, and replay risk to recommend a first
    assay class and concrete next steps.
    """
    policy_signal = _require_mapping(
        packet.get("policy_signal"), "packet.policy_signal"
    )
    contradiction_handling = _require_mapping(
        packet.get("contradiction_handling"), "packet.contradiction_handling"
    )
    failure_memory = _require_mapping(
        packet.get("failure_memory"), "packet.failure_memory"
    )
    replay_risk = _require_mapping(
        failure_memory.get("replay_risk"), "packet.failure_memory.replay_risk"
    )

    coverage_status = _require_text(
        rescue_evidence.get("coverage_status"),
        "rescue_evidence.coverage_status",
    )
    matched_tasks = _require_list(
        rescue_evidence.get("matched_tasks"),
        "rescue_evidence.matched_tasks",
    )
    conflict_signals = _require_list(
        rescue_evidence.get("conflict_signals"),
        "rescue_evidence.conflict_signals",
    )

    policy_status = str(policy_signal.get("status", ""))
    contradiction_status = str(contradiction_handling.get("status", ""))
    replay_status = str(replay_risk.get("status", ""))

    rescue_label = _primary_rescue_label(matched_tasks)
    assay_class = _determine_assay_class(
        rescue_label=rescue_label,
        has_conflicts=bool(conflict_signals),
        coverage_status=coverage_status,
    )
    rationale = _build_assay_rationale(
        assay_class=assay_class,
        rescue_label=rescue_label,
        policy_status=policy_status,
        contradiction_status=contradiction_status,
        replay_status=replay_status,
        conflict_signals=conflict_signals,
        matched_tasks=matched_tasks,
    )
    next_steps = _build_next_steps(
        packet=packet,
        assay_class=assay_class,
        rescue_label=rescue_label,
        matched_tasks=matched_tasks,
    )

    return {
        "recommended_assay_class": assay_class,
        "rationale": rationale,
        "grounding": {
            "policy_signal_status": policy_status,
            "rescue_coverage": coverage_status == RESCUE_COVERAGE_MATCH,
            "rescue_label": rescue_label,
            "contradiction_status": contradiction_status,
            "replay_status": replay_status,
        },
        "next_steps": next_steps,
    }


def augment_packets_with_rescue(
    hypothesis_payload: dict[str, object],
    *,
    rescue_entity_labels: dict[str, dict[str, str]],
    baseline_comparison_summaries: list[dict[str, object]],
    model_admission_summaries: list[dict[str, object]] | None = None,
    rescue_source_artifacts: dict[str, str] | None = None,
) -> dict[str, object]:
    """Augment all packets in a hypothesis payload with rescue evidence.

    Returns a new payload with rescue_evidence and first_assay added to
    each packet. The original packet contract fields are preserved
    exactly. The top-level payload is extended with rescue metadata.
    """
    packets = _require_list(
        hypothesis_payload.get("packets"), "hypothesis_packets.packets"
    )
    augmented_packets: list[dict[str, object]] = []
    rescue_match_count = 0

    for packet in packets:
        packet_copy = deepcopy(packet)
        rescue_evidence = build_rescue_evidence_section(
            packet_copy,
            rescue_entity_labels=rescue_entity_labels,
            baseline_comparison_summaries=baseline_comparison_summaries,
            model_admission_summaries=model_admission_summaries,
        )
        first_assay = build_first_assay_section(
            packet_copy,
            rescue_evidence=rescue_evidence,
        )
        packet_copy["rescue_evidence"] = rescue_evidence
        packet_copy["first_assay"] = first_assay
        if rescue_evidence["coverage_status"] == RESCUE_COVERAGE_MATCH:
            rescue_match_count += 1
        augmented_packets.append(packet_copy)

    augmented_payload = deepcopy(hypothesis_payload)
    augmented_payload["packets"] = augmented_packets
    augmented_payload["rescue_augmentation"] = {
        "schema_version": RESCUE_AUGMENTED_PACKETS_SCHEMA_VERSION,
        "rescue_match_count": rescue_match_count,
        "rescue_unmatched_count": len(packets) - rescue_match_count,
        "rescue_task_ids": sorted(
            {
                task["task_id"]
                for labels in rescue_entity_labels.values()
                for task in [labels]
                if "task_id" in task
            }
        ),
        "source_artifacts": rescue_source_artifacts or {},
    }
    return augmented_payload


def materialize_rescue_augmented_packets(
    hypothesis_packets_file: Path,
    *,
    evaluation_labels_file: Path,
    task_id: str,
    task_label: str,
    baseline_comparison_summary_file: Path | None = None,
    model_admission_summary_file: Path | None = None,
    output_file: Path | None = None,
    entity_id_field: str = "gene_id",
) -> dict[str, object]:
    """End-to-end materialization path from checked-in artifacts.

    Reads a hypothesis packets artifact and rescue artifacts, augments
    the packets with rescue evidence and first-assay logic, and
    optionally writes the result.
    """
    resolved_packets_path = hypothesis_packets_file.resolve()
    resolved_labels_path = evaluation_labels_file.resolve()

    hypothesis_payload = _load_json(resolved_packets_path)
    evaluation_rows = read_csv_rows(resolved_labels_path)

    rescue_entity_labels = build_rescue_entity_labels(
        evaluation_rows,
        task_id=task_id,
        task_label=task_label,
        entity_id_field=entity_id_field,
    )

    baseline_summaries: list[dict[str, object]] = []
    if baseline_comparison_summary_file is not None:
        resolved_summary_path = baseline_comparison_summary_file.resolve()
        summary = _load_json(resolved_summary_path)
        if isinstance(summary, dict):
            baseline_summaries.append(summary)

    model_summaries: list[dict[str, object]] = []
    if model_admission_summary_file is not None:
        resolved_admission_path = model_admission_summary_file.resolve()
        admission = _load_json(resolved_admission_path)
        if isinstance(admission, dict):
            model_summaries.append(admission)

    output_dir = (
        output_file.resolve().parent
        if output_file is not None
        else resolved_packets_path.parent
    )
    rescue_source_artifacts: dict[str, str] = {
        "hypothesis_packets_v1": os.path.relpath(
            resolved_packets_path, output_dir
        ),
        "evaluation_labels": os.path.relpath(
            resolved_labels_path, output_dir
        ),
    }
    if baseline_comparison_summary_file is not None:
        rescue_source_artifacts["baseline_comparison_summary"] = os.path.relpath(
            baseline_comparison_summary_file.resolve(), output_dir
        )
    if model_admission_summary_file is not None:
        rescue_source_artifacts["model_admission_summary"] = os.path.relpath(
            model_admission_summary_file.resolve(), output_dir
        )

    augmented_payload = augment_packets_with_rescue(
        hypothesis_payload,
        rescue_entity_labels=rescue_entity_labels,
        baseline_comparison_summaries=baseline_summaries,
        model_admission_summaries=model_summaries or None,
        rescue_source_artifacts=rescue_source_artifacts,
    )

    if output_file is not None:
        write_json(output_file.resolve(), augmented_payload)

    return augmented_payload


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_matched_tasks(
    *,
    entity_id: str,
    entity_label: str,
    entity_label_entry: dict[str, str] | None,
    baseline_comparison_summaries: list[dict[str, object]],
    model_admission_summaries: list[dict[str, object]],
) -> list[dict[str, object]]:
    if entity_label_entry is None:
        return []

    task_id = entity_label_entry.get("task_id", "")
    task_label = entity_label_entry.get("task_label", "")

    baseline_summary_for_task = _find_summary_for_task(
        task_id, baseline_comparison_summaries
    )
    model_admission_for_task = _find_admission_for_task(
        task_id, model_admission_summaries
    )

    matched_task: dict[str, object] = {
        "task_id": task_id,
        "task_label": task_label,
        "entity_id": entity_id,
        "entity_label": entity_label,
        "entity_rescue_decision": entity_label_entry.get("decision", ""),
        "entity_evaluation_label": entity_label_entry.get("evaluation_label", ""),
        "entity_split": entity_label_entry.get("split_name", ""),
        "label_rationale": entity_label_entry.get("label_rationale", ""),
    }

    if baseline_summary_for_task is not None:
        matched_task["baseline_performance"] = _extract_baseline_performance(
            baseline_summary_for_task
        )
    else:
        matched_task["baseline_performance"] = {
            "status": "no_baseline_data",
        }

    if model_admission_for_task is not None:
        matched_task["model_admission"] = _extract_model_admission(
            model_admission_for_task
        )
    else:
        matched_task["model_admission"] = {
            "status": "no_models_evaluated",
        }

    return [matched_task]


def _find_summary_for_task(
    task_id: str,
    summaries: list[dict[str, object]],
) -> dict[str, object] | None:
    for summary in summaries:
        if summary.get("task_id") == task_id:
            return summary
    return None


def _find_admission_for_task(
    task_id: str,
    summaries: list[dict[str, object]],
) -> dict[str, object] | None:
    for summary in summaries:
        decisions = summary.get("decisions")
        if not isinstance(decisions, list):
            continue
        for decision in decisions:
            if isinstance(decision, dict) and decision.get("model_id", "").startswith(task_id):
                return summary
        if summary.get("admitted_model_ids") is not None:
            return summary
    return None


def _extract_baseline_performance(
    summary: dict[str, object],
) -> dict[str, object]:
    best_by_split = summary.get("best_by_split")
    principal_split = summary.get("principal_split", "")
    metric_names = summary.get("metric_names", [])

    if not isinstance(best_by_split, dict):
        return {"status": "no_baseline_data"}

    principal_best = best_by_split.get(principal_split, {})
    if not isinstance(principal_best, dict):
        return {"status": "no_baseline_data"}

    return {
        "status": "baselines_available",
        "principal_split": principal_split,
        "metric_names": list(metric_names) if isinstance(metric_names, list) else [],
        "best_by_metric": {
            metric_name: {
                "scorer_id": entry.get("scorer_id", ""),
                "scorer_label": entry.get("scorer_label", ""),
                "value": entry.get("metric_value"),
            }
            for metric_name, entry in principal_best.items()
            if isinstance(entry, dict)
        },
    }


def _extract_model_admission(
    summary: dict[str, object],
) -> dict[str, object]:
    admitted_ids = summary.get("admitted_model_ids", [])
    if not isinstance(admitted_ids, list):
        admitted_ids = []
    decisions = summary.get("decisions", [])
    if not isinstance(decisions, list):
        decisions = []

    if admitted_ids:
        status = "models_admitted"
    elif decisions:
        status = "no_models_admitted"
    else:
        status = "no_models_evaluated"

    return {
        "status": status,
        "admitted_model_ids": admitted_ids,
        "decision_count": len(decisions),
    }


def _build_conflict_signals(
    *,
    packet: dict[str, object],
    matched_tasks: list[dict[str, object]],
) -> list[str]:
    signals: list[str] = []
    if not matched_tasks:
        return signals

    hypothesis = packet.get("hypothesis")
    if not isinstance(hypothesis, dict):
        return signals

    contradiction_handling = packet.get("contradiction_handling")
    if not isinstance(contradiction_handling, dict):
        return signals

    failure_memory = packet.get("failure_memory")
    replay_risk: dict[str, object] = {}
    if isinstance(failure_memory, dict):
        rr = failure_memory.get("replay_risk")
        if isinstance(rr, dict):
            replay_risk = rr

    entity_label = str(packet.get("entity_label", ""))
    contradiction_status = str(contradiction_handling.get("status", ""))
    replay_status = str(replay_risk.get("status", ""))

    for task in matched_tasks:
        rescue_decision = str(task.get("entity_rescue_decision", ""))
        task_label = str(task.get("task_label", ""))

        if rescue_decision == "deprioritize":
            signals.append(
                f"Rescue task '{task_label}' labels {entity_label} as "
                f"'deprioritize' while the hypothesis packet proposes it for "
                f"further evaluation."
            )

        if rescue_decision == "advance" and contradiction_status == "contradicted":
            signals.append(
                f"Rescue task '{task_label}' labels {entity_label} as "
                f"'advance' but the packet contradiction status is "
                f"'contradicted'."
            )

        if rescue_decision == "advance" and replay_status == "replay_supported":
            signals.append(
                f"Rescue task '{task_label}' labels {entity_label} as "
                f"'advance' but the packet replay status is "
                f"'replay_supported'."
            )

        if rescue_decision == "hold" and contradiction_status == "clear" and replay_status in (
            "replay_not_supported",
            "insufficient_history",
        ):
            signals.append(
                f"Rescue task '{task_label}' labels {entity_label} as "
                f"'hold' but policy signals are clear with no replay concern."
            )

    return signals


def _primary_rescue_label(matched_tasks: list[dict[str, object]]) -> str:
    if not matched_tasks:
        return ""
    return str(matched_tasks[0].get("entity_rescue_decision", ""))


def _determine_assay_class(
    *,
    rescue_label: str,
    has_conflicts: bool,
    coverage_status: str,
) -> str:
    if coverage_status != RESCUE_COVERAGE_MATCH:
        return FIRST_ASSAY_CLASS_NO_RESCUE
    if has_conflicts:
        return FIRST_ASSAY_CLASS_CONFLICT
    if rescue_label == "advance":
        return FIRST_ASSAY_CLASS_RESCUE_ADVANCE
    if rescue_label == "hold":
        return FIRST_ASSAY_CLASS_RESCUE_HOLD
    if rescue_label == "deprioritize":
        return FIRST_ASSAY_CLASS_RESCUE_DEPRIORITIZE
    return FIRST_ASSAY_CLASS_NO_RESCUE


def _build_assay_rationale(
    *,
    assay_class: str,
    rescue_label: str,
    policy_status: str,
    contradiction_status: str,
    replay_status: str,
    conflict_signals: list[str],
    matched_tasks: list[dict[str, object]],
) -> str:
    task_context = ""
    if matched_tasks:
        task = matched_tasks[0]
        task_context = (
            f" Rescue task '{task.get('task_label', '')}' labels entity as "
            f"'{rescue_label}'"
            + (f": {task.get('label_rationale', '')}" if task.get("label_rationale") else "")
            + "."
        )

    if assay_class == FIRST_ASSAY_CLASS_CONFLICT:
        conflict_detail = " ".join(conflict_signals[:2])
        return (
            f"Rescue and policy signals conflict.{task_context} "
            f"Policy status: {policy_status}; contradiction: "
            f"{contradiction_status}; replay: {replay_status}. "
            f"{conflict_detail} Resolve conflict before designing assay."
        )

    if assay_class == FIRST_ASSAY_CLASS_RESCUE_ADVANCE:
        return (
            f"Rescue evidence supports advancing.{task_context} "
            f"Policy status: {policy_status}; contradiction: "
            f"{contradiction_status}; replay: {replay_status}. "
            f"Design first assay to test the hypothesis directionally."
        )

    if assay_class == FIRST_ASSAY_CLASS_RESCUE_HOLD:
        return (
            f"Rescue evidence recommends hold.{task_context} "
            f"Policy status: {policy_status}; contradiction: "
            f"{contradiction_status}; replay: {replay_status}. "
            f"Gather additional evidence before committing to an assay."
        )

    if assay_class == FIRST_ASSAY_CLASS_RESCUE_DEPRIORITIZE:
        return (
            f"Rescue evidence recommends deprioritization.{task_context} "
            f"Policy status: {policy_status}; contradiction: "
            f"{contradiction_status}; replay: {replay_status}. "
            f"Redirect assay resources unless new evidence emerges."
        )

    return (
        f"No rescue task covers this entity. "
        f"Policy status: {policy_status}; contradiction: "
        f"{contradiction_status}; replay: {replay_status}. "
        f"Design first assay from policy and failure-memory signals only."
    )


def _build_next_steps(
    *,
    packet: dict[str, object],
    assay_class: str,
    rescue_label: str,
    matched_tasks: list[dict[str, object]],
) -> list[str]:
    steps: list[str] = []
    evidence_needed_next = packet.get("evidence_needed_next")
    if isinstance(evidence_needed_next, list):
        for item in evidence_needed_next[:2]:
            if isinstance(item, str) and item.strip():
                steps.append(item)

    if assay_class == FIRST_ASSAY_CLASS_CONFLICT:
        steps.append(
            "Reconcile rescue task label with policy/failure-memory signals "
            "before committing to an experimental design."
        )
    elif assay_class == FIRST_ASSAY_CLASS_RESCUE_ADVANCE:
        hypothesis = packet.get("hypothesis")
        if isinstance(hypothesis, dict):
            direction = str(hypothesis.get("desired_perturbation_direction", ""))
            modality = str(hypothesis.get("modality_hypothesis", ""))
            if direction and modality:
                steps.append(
                    f"Design directional assay: {direction} via {modality}, "
                    f"informed by rescue task label '{rescue_label}'."
                )
    elif assay_class == FIRST_ASSAY_CLASS_RESCUE_HOLD:
        steps.append(
            "Identify specific evidence gaps flagged by the rescue hold "
            "decision before advancing to assay design."
        )
    elif assay_class == FIRST_ASSAY_CLASS_RESCUE_DEPRIORITIZE:
        steps.append(
            "Deprioritized by rescue task. Revisit only if new evidence "
            "contradicts the deprioritization rationale."
        )
    elif assay_class == FIRST_ASSAY_CLASS_NO_RESCUE:
        steps.append(
            "No rescue task data available. Design assay from policy and "
            "failure-memory evidence."
        )

    return steps


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        import json
        return json.load(handle)


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value
