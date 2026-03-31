"""Packet and rescue evidence navigation surfaces for the observatory.

Provides browsing views over hypothesis packets, rescue-augmented packets,
failure analog replay comparisons, and policy comparison surfaces.  All
views are leakage-safe: rescue evaluation metadata (held-out labels, split
assignments, label rationales) never appears in any browse result.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from scz_target_engine.io import read_json
from scz_target_engine.observatory.loaders import (
    REPO_ROOT,
    load_hypothesis_packets,
    load_rescue_augmented_packets,
    load_rescue_task_registry,
)


# ---------------------------------------------------------------------------
# View models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PacketSummaryView:
    packet_id: str
    entity_type: str
    entity_id: str
    entity_label: str
    policy_id: str
    policy_label: str
    priority_domain: str
    hypothesis_statement: str
    policy_score: float | None
    policy_status: str
    contradiction_status: str
    replay_status: str
    evidence_anchor_count: int
    evidence_anchor_gap_status: str
    program_history_gap_status: str
    escape_status: str
    has_rescue_evidence: bool
    rescue_coverage_status: str
    rescue_assay_class: str


@dataclass(frozen=True)
class PacketDetailView:
    packet_id: str
    entity_type: str
    entity_id: str
    entity_label: str
    policy_id: str
    policy_label: str
    priority_domain: str
    decision_focus: dict[str, object]
    hypothesis: dict[str, object]
    policy_signal_summary: dict[str, object]
    evidence_anchors: list[dict[str, object]]
    evidence_anchor_gap_status: str
    program_history_gap_status: str
    risk_digest: list[str]
    evidence_needed_next: list[str]
    contradiction_handling: dict[str, object]
    failure_memory_summary: dict[str, object]
    failure_escape_logic: dict[str, object]
    traceability: dict[str, object]
    rescue_evidence: dict[str, object] | None
    first_assay: dict[str, object] | None


@dataclass(frozen=True)
class FailureAnalogView:
    packet_id: str
    entity_label: str
    policy_label: str
    replay_status: str
    replay_summary: str
    supporting_reasons: list[dict[str, object]]
    offsetting_reasons: list[dict[str, object]]
    uncertainty_reasons: list[dict[str, object]]
    escape_status: str
    escape_routes: list[dict[str, object]]
    falsification_conditions: list[str]


@dataclass(frozen=True)
class PolicyComparisonRow:
    entity_id: str
    entity_label: str
    policy_id: str
    policy_label: str
    score: float | None
    base_score: float | None
    status: str
    contradiction_status: str
    replay_status: str
    rescue_assay_class: str


@dataclass(frozen=True)
class PolicyComparisonView:
    entity_ids: tuple[str, ...]
    policy_ids: tuple[str, ...]
    rows: tuple[PolicyComparisonRow, ...]


@dataclass(frozen=True)
class RescueTaskRegistryEntry:
    task_id: str
    task_label: str
    task_type: str
    disease: str
    entity_type: str
    contract_scope: str
    registry_status: str
    contract_file: str


@dataclass(frozen=True)
class RescueEvidenceSummaryView:
    packet_id: str
    entity_label: str
    policy_label: str
    coverage_status: str
    matched_task_count: int
    matched_tasks: list[dict[str, object]]
    conflict_signals: list[str]
    assay_class: str
    assay_rationale: str


# ---------------------------------------------------------------------------
# Packet browsing
# ---------------------------------------------------------------------------


def list_packets(
    packets_file: Path | None = None,
) -> tuple[PacketSummaryView, ...]:
    """List all hypothesis packets as summary views."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return ()
    return tuple(
        _build_packet_summary(packet)
        for packet in _require_list(payload.get("packets"), "packets")
    )


def browse_packet(
    packet_id: str,
    *,
    packets_file: Path | None = None,
) -> PacketDetailView | None:
    """Browse a single packet by its packet_id."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return None
    for packet in _require_list(payload.get("packets"), "packets"):
        if not isinstance(packet, dict):
            continue
        if packet.get("packet_id") == packet_id:
            return _build_packet_detail(packet)
    return None


def browse_packet_by_entity(
    entity_label: str,
    policy_id: str,
    *,
    packets_file: Path | None = None,
) -> PacketDetailView | None:
    """Browse a single packet by entity_label + policy_id."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return None
    for packet in _require_list(payload.get("packets"), "packets"):
        if not isinstance(packet, dict):
            continue
        if (
            packet.get("entity_label") == entity_label
            and packet.get("policy_id") == policy_id
        ):
            return _build_packet_detail(packet)
    return None


# ---------------------------------------------------------------------------
# Failure analog / replay comparison browsing
# ---------------------------------------------------------------------------


def list_failure_analogs(
    packets_file: Path | None = None,
) -> tuple[FailureAnalogView, ...]:
    """Browse failure analog replay evidence across all packets."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return ()
    views: list[FailureAnalogView] = []
    for packet in _require_list(payload.get("packets"), "packets"):
        if not isinstance(packet, dict):
            continue
        view = _build_failure_analog_view(packet)
        if view is not None:
            views.append(view)
    return tuple(views)


def browse_failure_analog(
    packet_id: str,
    *,
    packets_file: Path | None = None,
) -> FailureAnalogView | None:
    """Browse failure analog evidence for a specific packet."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return None
    for packet in _require_list(payload.get("packets"), "packets"):
        if not isinstance(packet, dict):
            continue
        if packet.get("packet_id") == packet_id:
            return _build_failure_analog_view(packet)
    return None


# ---------------------------------------------------------------------------
# Policy comparison browsing
# ---------------------------------------------------------------------------


def browse_policy_comparison(
    packets_file: Path | None = None,
) -> PolicyComparisonView | None:
    """Build a policy comparison view across all entities and policies."""
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return None
    packets = _require_list(payload.get("packets"), "packets")
    if not packets:
        return None

    rows: list[PolicyComparisonRow] = []
    entity_ids: list[str] = []
    policy_ids: list[str] = []
    for packet in packets:
        if not isinstance(packet, dict):
            continue
        entity_id = str(packet.get("entity_id", ""))
        policy_id = str(packet.get("policy_id", ""))
        if entity_id not in entity_ids:
            entity_ids.append(entity_id)
        if policy_id not in policy_ids:
            policy_ids.append(policy_id)

        policy_signal = packet.get("policy_signal", {})
        if not isinstance(policy_signal, dict):
            policy_signal = {}
        contradiction_handling = packet.get("contradiction_handling", {})
        if not isinstance(contradiction_handling, dict):
            contradiction_handling = {}
        failure_memory = packet.get("failure_memory", {})
        if not isinstance(failure_memory, dict):
            failure_memory = {}
        replay_risk = failure_memory.get("replay_risk", {})
        if not isinstance(replay_risk, dict):
            replay_risk = {}

        rescue_assay_class = ""
        first_assay = packet.get("first_assay")
        if isinstance(first_assay, dict):
            rescue_assay_class = str(first_assay.get("recommended_assay_class", ""))

        score = policy_signal.get("score")
        base_score = policy_signal.get("base_score")
        rows.append(
            PolicyComparisonRow(
                entity_id=entity_id,
                entity_label=str(packet.get("entity_label", "")),
                policy_id=policy_id,
                policy_label=str(packet.get("policy_label", "")),
                score=score if isinstance(score, (int, float)) else None,
                base_score=base_score if isinstance(base_score, (int, float)) else None,
                status=str(policy_signal.get("status", "")),
                contradiction_status=str(contradiction_handling.get("status", "")),
                replay_status=str(replay_risk.get("status", "")),
                rescue_assay_class=rescue_assay_class,
            )
        )
    return PolicyComparisonView(
        entity_ids=tuple(entity_ids),
        policy_ids=tuple(policy_ids),
        rows=tuple(rows),
    )


# ---------------------------------------------------------------------------
# Rescue evidence browsing (leakage-safe)
# ---------------------------------------------------------------------------


def list_rescue_tasks() -> tuple[RescueTaskRegistryEntry, ...]:
    """List rescue tasks from the checked-in registry."""
    rows = load_rescue_task_registry()
    return tuple(
        RescueTaskRegistryEntry(
            task_id=row.get("task_id", ""),
            task_label=row.get("task_label", ""),
            task_type=row.get("task_type", ""),
            disease=row.get("disease", ""),
            entity_type=row.get("entity_type", ""),
            contract_scope=row.get("contract_scope", ""),
            registry_status=row.get("registry_status", ""),
            contract_file=row.get("contract_file", ""),
        )
        for row in rows
    )


def list_rescue_evidence(
    packets_file: Path | None = None,
) -> tuple[RescueEvidenceSummaryView, ...]:
    """Browse rescue evidence summaries across all augmented packets.

    Only emits leakage-safe fields: task_id, task_label, entity_id,
    entity_label, entity_rescue_decision, baseline_performance, and
    model_admission.  Held-out labels, split assignments, and label
    rationales are never surfaced.
    """
    payload = _load_packets_payload(packets_file)
    if payload is None:
        return ()
    views: list[RescueEvidenceSummaryView] = []
    for packet in _require_list(payload.get("packets"), "packets"):
        if not isinstance(packet, dict):
            continue
        rescue_evidence = packet.get("rescue_evidence")
        if not isinstance(rescue_evidence, dict):
            continue
        first_assay = packet.get("first_assay", {})
        if not isinstance(first_assay, dict):
            first_assay = {}
        matched_tasks = rescue_evidence.get("matched_tasks", [])
        if not isinstance(matched_tasks, list):
            matched_tasks = []
        conflict_signals = rescue_evidence.get("conflict_signals", [])
        if not isinstance(conflict_signals, list):
            conflict_signals = []
        views.append(
            RescueEvidenceSummaryView(
                packet_id=str(packet.get("packet_id", "")),
                entity_label=str(packet.get("entity_label", "")),
                policy_label=str(packet.get("policy_label", "")),
                coverage_status=str(rescue_evidence.get("coverage_status", "")),
                matched_task_count=len(matched_tasks),
                matched_tasks=_sanitize_matched_tasks(matched_tasks),
                conflict_signals=[str(s) for s in conflict_signals],
                assay_class=str(first_assay.get("recommended_assay_class", "")),
                assay_rationale=str(first_assay.get("rationale", "")),
            )
        )
    return tuple(views)


def browse_rescue_evidence(
    packet_id: str,
    *,
    packets_file: Path | None = None,
) -> RescueEvidenceSummaryView | None:
    """Browse rescue evidence for a specific packet by packet_id."""
    for view in list_rescue_evidence(packets_file=packets_file):
        if view.packet_id == packet_id:
            return view
    return None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_SAFE_MATCHED_TASK_FIELDS = frozenset({
    "task_id",
    "task_label",
    "entity_id",
    "entity_label",
    "entity_rescue_decision",
    "baseline_performance",
    "model_admission",
})


def _sanitize_matched_tasks(
    matched_tasks: list[object],
) -> list[dict[str, object]]:
    """Strip any fields outside the safe whitelist from matched tasks."""
    safe_tasks: list[dict[str, object]] = []
    for task in matched_tasks:
        if not isinstance(task, dict):
            continue
        safe_tasks.append(
            {k: v for k, v in task.items() if k in _SAFE_MATCHED_TASK_FIELDS}
        )
    return safe_tasks


def _load_packets_payload(
    packets_file: Path | None = None,
) -> dict[str, object] | None:
    """Load hypothesis packets (plain or rescue-augmented) from file."""
    if packets_file is not None:
        resolved = packets_file.resolve()
        if not resolved.exists():
            return None
        return load_hypothesis_packets(resolved)

    # Try rescue-augmented first, then plain
    augmented = load_rescue_augmented_packets()
    if augmented is not None:
        return augmented
    return load_hypothesis_packets()


def _build_packet_summary(packet: object) -> PacketSummaryView:
    if not isinstance(packet, dict):
        raise ValueError("packet must be a dict")

    policy_signal = packet.get("policy_signal", {})
    if not isinstance(policy_signal, dict):
        policy_signal = {}
    contradiction_handling = packet.get("contradiction_handling", {})
    if not isinstance(contradiction_handling, dict):
        contradiction_handling = {}
    failure_memory = packet.get("failure_memory", {})
    if not isinstance(failure_memory, dict):
        failure_memory = {}
    replay_risk = failure_memory.get("replay_risk", {})
    if not isinstance(replay_risk, dict):
        replay_risk = {}
    failure_escape = packet.get("failure_escape_logic", {})
    if not isinstance(failure_escape, dict):
        failure_escape = {}
    hypothesis = packet.get("hypothesis", {})
    if not isinstance(hypothesis, dict):
        hypothesis = {}
    evidence_anchors = packet.get("evidence_anchors", [])
    if not isinstance(evidence_anchors, list):
        evidence_anchors = []

    has_rescue = "rescue_evidence" in packet
    rescue_evidence = packet.get("rescue_evidence", {})
    if not isinstance(rescue_evidence, dict):
        rescue_evidence = {}
    first_assay = packet.get("first_assay", {})
    if not isinstance(first_assay, dict):
        first_assay = {}

    score = policy_signal.get("score")
    return PacketSummaryView(
        packet_id=str(packet.get("packet_id", "")),
        entity_type=str(packet.get("entity_type", "")),
        entity_id=str(packet.get("entity_id", "")),
        entity_label=str(packet.get("entity_label", "")),
        policy_id=str(packet.get("policy_id", "")),
        policy_label=str(packet.get("policy_label", "")),
        priority_domain=str(packet.get("priority_domain", "")),
        hypothesis_statement=str(hypothesis.get("statement", "")),
        policy_score=score if isinstance(score, (int, float)) else None,
        policy_status=str(policy_signal.get("status", "")),
        contradiction_status=str(contradiction_handling.get("status", "")),
        replay_status=str(replay_risk.get("status", "")),
        evidence_anchor_count=len(evidence_anchors),
        evidence_anchor_gap_status=str(
            packet.get("evidence_anchor_gap_status", "")
        ),
        program_history_gap_status=str(
            packet.get("program_history_gap_status", "")
        ),
        escape_status=str(failure_escape.get("status", "")),
        has_rescue_evidence=has_rescue,
        rescue_coverage_status=str(rescue_evidence.get("coverage_status", "")),
        rescue_assay_class=str(
            first_assay.get("recommended_assay_class", "")
        ),
    )


def _build_packet_detail(packet: dict[str, object]) -> PacketDetailView:
    policy_signal = packet.get("policy_signal", {})
    if not isinstance(policy_signal, dict):
        policy_signal = {}
    failure_memory = packet.get("failure_memory", {})
    if not isinstance(failure_memory, dict):
        failure_memory = {}
    replay_risk = failure_memory.get("replay_risk", {})
    if not isinstance(replay_risk, dict):
        replay_risk = {}

    # Build a summary of the policy signal without leaking raw internals
    policy_signal_summary = {
        "policy_id": policy_signal.get("policy_id", ""),
        "label": policy_signal.get("label", ""),
        "description": policy_signal.get("description", ""),
        "score": policy_signal.get("score"),
        "base_score": policy_signal.get("base_score"),
        "status": policy_signal.get("status", ""),
    }
    failure_memory_summary = {
        "replay_status": str(replay_risk.get("status", "")),
        "replay_summary": str(replay_risk.get("summary", "")),
        "supporting_reason_count": replay_risk.get("supporting_reason_count", 0),
        "offsetting_reason_count": replay_risk.get("offsetting_reason_count", 0),
        "uncertainty_reason_count": replay_risk.get("uncertainty_reason_count", 0),
    }

    rescue_evidence = packet.get("rescue_evidence")
    if isinstance(rescue_evidence, dict):
        # Sanitize matched_tasks to prevent leakage
        matched_tasks = rescue_evidence.get("matched_tasks", [])
        if isinstance(matched_tasks, list):
            rescue_evidence = dict(rescue_evidence)
            rescue_evidence["matched_tasks"] = _sanitize_matched_tasks(
                matched_tasks
            )
    else:
        rescue_evidence = None

    first_assay = packet.get("first_assay")
    if not isinstance(first_assay, dict):
        first_assay = None

    evidence_anchors = packet.get("evidence_anchors", [])
    if not isinstance(evidence_anchors, list):
        evidence_anchors = []
    risk_digest = packet.get("risk_digest", [])
    if not isinstance(risk_digest, list):
        risk_digest = []
    evidence_needed = packet.get("evidence_needed_next", [])
    if not isinstance(evidence_needed, list):
        evidence_needed = []

    return PacketDetailView(
        packet_id=str(packet.get("packet_id", "")),
        entity_type=str(packet.get("entity_type", "")),
        entity_id=str(packet.get("entity_id", "")),
        entity_label=str(packet.get("entity_label", "")),
        policy_id=str(packet.get("policy_id", "")),
        policy_label=str(packet.get("policy_label", "")),
        priority_domain=str(packet.get("priority_domain", "")),
        decision_focus=dict(packet.get("decision_focus", {}))
        if isinstance(packet.get("decision_focus"), dict)
        else {},
        hypothesis=dict(packet.get("hypothesis", {}))
        if isinstance(packet.get("hypothesis"), dict)
        else {},
        policy_signal_summary=policy_signal_summary,
        evidence_anchors=[
            dict(a) for a in evidence_anchors if isinstance(a, dict)
        ],
        evidence_anchor_gap_status=str(
            packet.get("evidence_anchor_gap_status", "")
        ),
        program_history_gap_status=str(
            packet.get("program_history_gap_status", "")
        ),
        risk_digest=[str(r) for r in risk_digest],
        evidence_needed_next=[str(e) for e in evidence_needed],
        contradiction_handling=dict(packet.get("contradiction_handling", {}))
        if isinstance(packet.get("contradiction_handling"), dict)
        else {},
        failure_memory_summary=failure_memory_summary,
        failure_escape_logic=dict(packet.get("failure_escape_logic", {}))
        if isinstance(packet.get("failure_escape_logic"), dict)
        else {},
        traceability=dict(packet.get("traceability", {}))
        if isinstance(packet.get("traceability"), dict)
        else {},
        rescue_evidence=rescue_evidence,
        first_assay=first_assay,
    )


def _build_failure_analog_view(
    packet: dict[str, object],
) -> FailureAnalogView | None:
    failure_memory = packet.get("failure_memory")
    if not isinstance(failure_memory, dict):
        return None
    replay_risk = failure_memory.get("replay_risk")
    if not isinstance(replay_risk, dict):
        return None
    failure_escape = packet.get("failure_escape_logic")
    if not isinstance(failure_escape, dict):
        return None

    supporting = replay_risk.get("supporting_reasons", [])
    if not isinstance(supporting, list):
        supporting = []
    offsetting = replay_risk.get("offsetting_reasons", [])
    if not isinstance(offsetting, list):
        offsetting = []
    uncertainty = replay_risk.get("uncertainty_reasons", [])
    if not isinstance(uncertainty, list):
        uncertainty = []
    escape_routes = failure_escape.get("escape_routes", [])
    if not isinstance(escape_routes, list):
        escape_routes = []
    falsification = failure_escape.get("next_evidence", [])
    if not isinstance(falsification, list):
        falsification = []

    return FailureAnalogView(
        packet_id=str(packet.get("packet_id", "")),
        entity_label=str(packet.get("entity_label", "")),
        policy_label=str(packet.get("policy_label", "")),
        replay_status=str(replay_risk.get("status", "")),
        replay_summary=str(replay_risk.get("summary", "")),
        supporting_reasons=[dict(r) for r in supporting if isinstance(r, dict)],
        offsetting_reasons=[dict(r) for r in offsetting if isinstance(r, dict)],
        uncertainty_reasons=[dict(r) for r in uncertainty if isinstance(r, dict)],
        escape_status=str(failure_escape.get("status", "")),
        escape_routes=[dict(r) for r in escape_routes if isinstance(r, dict)],
        falsification_conditions=[str(f) for f in falsification],
    )


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        return []
    return value


__all__ = [
    "FailureAnalogView",
    "PacketDetailView",
    "PacketSummaryView",
    "PolicyComparisonRow",
    "PolicyComparisonView",
    "RescueEvidenceSummaryView",
    "RescueTaskRegistryEntry",
    "browse_failure_analog",
    "browse_packet",
    "browse_packet_by_entity",
    "browse_policy_comparison",
    "browse_rescue_evidence",
    "list_failure_analogs",
    "list_packets",
    "list_rescue_evidence",
    "list_rescue_tasks",
]
