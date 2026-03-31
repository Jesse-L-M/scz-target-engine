"""Hypothesis co-scientist drafting agent.

Drafts grounded hypothesis packets using the shipped post-review packet
contract, rescue augmentation, and prospective credibility infrastructure.
Every output is a *draft* that requires explicit human adjudication --
no autonomous decision-making ever happens.

Trust boundary
--------------
The agent is a drafting tool, not a decision-maker.  It synthesizes
evidence from checked-in artifacts into structured sections that a human
reviewer can accept, revise, or reject.  Where rescue evidence and
policy signals disagree, the draft surfaces the disagreement explicitly
rather than smoothing it over.

Typical flow
------------
1. Load an augmented hypothesis packet payload (or plain packets).
2. Optionally load prospective forecast registrations for credibility
   grounding.
3. Call ``build_hypothesis_draft`` per packet, or
   ``build_hypothesis_drafts`` for the full payload.
4. Persist with ``write_hypothesis_drafts``.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scz_target_engine.io import write_json


HYPOTHESIS_DRAFT_SCHEMA_VERSION = "hypothesis-co-scientist-draft-v1"

RESCUE_POLICY_ALIGNED = "aligned"
RESCUE_POLICY_CONFLICTED = "conflicted"
RESCUE_POLICY_NO_DATA = "no_rescue_data"

_DRAFT_SECTION_IDS = frozenset({
    "hypothesis_summary",
    "decision_focus",
    "contradiction",
    "replay_and_failure_memory",
    "rescue_evidence",
    "rescue_policy_alignment",
    "credibility_grounding",
    "kill_conditions",
    "assay_recommendation",
    "evidence_gaps",
})


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HypothesisDraftSection:
    """One section of a hypothesis draft.

    Every section carries grounding_refs that trace back to the artifact
    fields it was derived from, so a reviewer can verify provenance.
    """
    section_id: str
    heading: str
    lines: tuple[str, ...]
    grounding_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.section_id:
            raise ValueError("section_id is required")
        if self.section_id not in _DRAFT_SECTION_IDS:
            raise ValueError(
                f"unknown section_id {self.section_id!r}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "section_id": self.section_id,
            "heading": self.heading,
            "lines": list(self.lines),
            "grounding_refs": list(self.grounding_refs),
        }


@dataclass(frozen=True)
class HypothesisDraft:
    """A complete co-scientist draft for one hypothesis packet.

    Human reviewers must treat every section as a draft suggestion.
    No section may be treated as a final decision without explicit
    reviewer sign-off.
    """
    draft_id: str
    packet_id: str
    entity_id: str
    entity_label: str
    policy_id: str
    policy_label: str
    priority_domain: str
    sections: tuple[HypothesisDraftSection, ...]
    contradiction_status: str
    replay_status: str
    rescue_policy_alignment: str
    requires_human_review: bool

    def __post_init__(self) -> None:
        if not self.draft_id:
            raise ValueError("draft_id is required")
        if not self.packet_id:
            raise ValueError("packet_id is required")
        if not self.requires_human_review:
            raise ValueError(
                "hypothesis drafts must always require human review"
            )
        section_ids = [s.section_id for s in self.sections]
        if len(section_ids) != len(set(section_ids)):
            raise ValueError("duplicate section_id in draft")

    def to_dict(self) -> dict[str, object]:
        return {
            "draft_id": self.draft_id,
            "packet_id": self.packet_id,
            "entity_id": self.entity_id,
            "entity_label": self.entity_label,
            "policy_id": self.policy_id,
            "policy_label": self.policy_label,
            "priority_domain": self.priority_domain,
            "sections": [s.to_dict() for s in self.sections],
            "contradiction_status": self.contradiction_status,
            "replay_status": self.replay_status,
            "rescue_policy_alignment": self.rescue_policy_alignment,
            "requires_human_review": self.requires_human_review,
        }


@dataclass(frozen=True)
class HypothesisDraftPayload:
    """Container for a batch of hypothesis drafts plus metadata."""
    schema_version: str
    drafts: tuple[HypothesisDraft, ...]
    source_artifacts: dict[str, str]
    trust_boundary: str

    def __post_init__(self) -> None:
        if self.schema_version != HYPOTHESIS_DRAFT_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported schema_version {self.schema_version!r}"
            )
        draft_ids = [d.draft_id for d in self.drafts]
        if len(draft_ids) != len(set(draft_ids)):
            raise ValueError("duplicate draft_id in payload")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "draft_count": len(self.drafts),
            "drafts": [d.to_dict() for d in self.drafts],
            "source_artifacts": dict(self.source_artifacts),
            "trust_boundary": self.trust_boundary,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_hypothesis_draft(
    packet: dict[str, object],
    *,
    prospective_registrations: list[dict[str, object]] | None = None,
) -> HypothesisDraft:
    """Build a single hypothesis draft from a (possibly augmented) packet.

    The packet may be a plain post-review packet or one that has been
    augmented with rescue_evidence and first_assay sections.  The draft
    adapts to whichever fields are present.
    """
    packet_id = _require_text(packet.get("packet_id"), "packet.packet_id")
    entity_id = _require_text(packet.get("entity_id"), "packet.entity_id")
    entity_label = _require_text(
        packet.get("entity_label"), "packet.entity_label"
    )
    policy_id = _require_text(packet.get("policy_id"), "packet.policy_id")
    policy_label = _require_text(
        packet.get("policy_label"), "packet.policy_label"
    )
    priority_domain = _require_text(
        packet.get("priority_domain"), "packet.priority_domain"
    )

    hypothesis = _require_mapping(
        packet.get("hypothesis"), "packet.hypothesis"
    )
    decision_focus = _require_mapping(
        packet.get("decision_focus"), "packet.decision_focus"
    )
    contradiction_handling = _require_mapping(
        packet.get("contradiction_handling"),
        "packet.contradiction_handling",
    )
    failure_memory = _require_mapping(
        packet.get("failure_memory"), "packet.failure_memory"
    )
    replay_risk = _require_mapping(
        failure_memory.get("replay_risk"),
        "packet.failure_memory.replay_risk",
    )

    contradiction_status = str(contradiction_handling.get("status", ""))
    replay_status = str(replay_risk.get("status", ""))

    # Rescue sections are optional (plain packets don't have them)
    rescue_evidence = packet.get("rescue_evidence")
    first_assay = packet.get("first_assay")
    has_rescue = isinstance(rescue_evidence, dict) and rescue_evidence.get(
        "coverage_status"
    ) == "rescue_task_match"

    registrations_for_packet = _filter_registrations_for_packet(
        packet_id, prospective_registrations or []
    )

    # Build sections
    sections: list[HypothesisDraftSection] = []
    sections.append(_build_hypothesis_summary_section(hypothesis, packet))
    sections.append(_build_decision_focus_section(decision_focus, packet))
    sections.append(
        _build_contradiction_section(contradiction_handling, packet)
    )
    sections.append(
        _build_replay_section(replay_risk, failure_memory, packet)
    )
    sections.append(
        _build_rescue_evidence_section(rescue_evidence, packet)
    )

    rescue_policy_alignment = _determine_rescue_policy_alignment(
        rescue_evidence, contradiction_handling, replay_risk
    )
    sections.append(
        _build_rescue_policy_alignment_section(
            rescue_evidence,
            contradiction_handling,
            replay_risk,
            rescue_policy_alignment,
        )
    )
    sections.append(
        _build_credibility_section(registrations_for_packet, packet_id)
    )
    sections.append(
        _build_kill_conditions_section(
            contradiction_handling, replay_risk, packet
        )
    )
    sections.append(
        _build_assay_section(first_assay, hypothesis, packet)
    )
    sections.append(
        _build_evidence_gaps_section(packet)
    )

    return HypothesisDraft(
        draft_id=f"draft__{packet_id}",
        packet_id=packet_id,
        entity_id=entity_id,
        entity_label=entity_label,
        policy_id=policy_id,
        policy_label=policy_label,
        priority_domain=priority_domain,
        sections=tuple(sections),
        contradiction_status=contradiction_status,
        replay_status=replay_status,
        rescue_policy_alignment=rescue_policy_alignment,
        requires_human_review=True,
    )


def build_hypothesis_drafts(
    payload: dict[str, object],
    *,
    prospective_registrations: list[dict[str, object]] | None = None,
    source_artifacts: dict[str, str] | None = None,
) -> HypothesisDraftPayload:
    """Build drafts for all packets in a hypothesis payload."""
    packets = _require_list(payload.get("packets"), "payload.packets")
    drafts: list[HypothesisDraft] = []
    for packet_item in packets:
        packet = _require_mapping(packet_item, "payload.packets[]")
        draft = build_hypothesis_draft(
            packet,
            prospective_registrations=prospective_registrations,
        )
        drafts.append(draft)

    return HypothesisDraftPayload(
        schema_version=HYPOTHESIS_DRAFT_SCHEMA_VERSION,
        drafts=tuple(drafts),
        source_artifacts=source_artifacts or {},
        trust_boundary=(
            "DRAFT ONLY -- every section requires explicit human "
            "review before any decision is taken"
        ),
    )


def write_hypothesis_drafts(
    path: Path,
    draft_payload: HypothesisDraftPayload,
) -> None:
    """Persist a hypothesis draft payload as JSON for human review."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, draft_payload.to_dict())


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _build_hypothesis_summary_section(
    hypothesis: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    lines: list[str] = [
        _require_text(
            hypothesis.get("statement"), "packet.hypothesis.statement"
        ),
        (
            f"Direction: "
            f"{_safe_text(hypothesis.get('desired_perturbation_direction'))} "
            f"via {_safe_text(hypothesis.get('modality_hypothesis'))}"
        ),
        f"Confidence: {_safe_text(hypothesis.get('confidence'))}",
        f"Ambiguity: {_safe_text(hypothesis.get('ambiguity'))}",
        f"Evidence basis: {_safe_text(hypothesis.get('evidence_basis'))}",
    ]
    preferred = hypothesis.get("preferred_modalities")
    if isinstance(preferred, list) and preferred:
        lines.append(f"Preferred modalities: {', '.join(str(m) for m in preferred)}")

    return HypothesisDraftSection(
        section_id="hypothesis_summary",
        heading="Hypothesis summary",
        lines=tuple(lines),
        grounding_refs=(
            "packet.hypothesis.statement",
            "packet.hypothesis.desired_perturbation_direction",
            "packet.hypothesis.modality_hypothesis",
            "packet.hypothesis.confidence",
            "packet.hypothesis.evidence_basis",
        ),
    )


def _build_decision_focus_section(
    decision_focus: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    lines: list[str] = [
        _require_text(
            decision_focus.get("review_question"),
            "packet.decision_focus.review_question",
        ),
        f"Options: {', '.join(str(o) for o in _safe_list(decision_focus.get('decision_options')))}",
        _require_text(
            decision_focus.get("current_readout"),
            "packet.decision_focus.current_readout",
        ),
    ]
    return HypothesisDraftSection(
        section_id="decision_focus",
        heading="Decision focus",
        lines=tuple(lines),
        grounding_refs=(
            "packet.decision_focus.review_question",
            "packet.decision_focus.decision_options",
            "packet.decision_focus.current_readout",
        ),
    )


def _build_contradiction_section(
    contradiction_handling: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    status = str(contradiction_handling.get("status", ""))
    lines: list[str] = [f"Contradiction status: {status}"]

    conditions = contradiction_handling.get("contradiction_conditions")
    if isinstance(conditions, list):
        for condition in conditions:
            if isinstance(condition, str) and condition.strip():
                lines.append(f"Contradiction condition: {condition}")

    falsification = contradiction_handling.get(
        "directionality_falsification_conditions"
    )
    if isinstance(falsification, list):
        for condition in falsification:
            if isinstance(condition, str) and condition.strip():
                lines.append(f"Falsification condition: {condition}")

    open_risks = contradiction_handling.get("open_risks")
    if isinstance(open_risks, list):
        for risk in open_risks:
            if isinstance(risk, dict):
                severity = str(risk.get("severity", ""))
                text = str(risk.get("text", ""))
                if text:
                    lines.append(f"Open risk ({severity}): {text}")

    return HypothesisDraftSection(
        section_id="contradiction",
        heading="Contradiction handling",
        lines=tuple(lines),
        grounding_refs=(
            "packet.contradiction_handling.status",
            "packet.contradiction_handling.contradiction_conditions",
            "packet.contradiction_handling.directionality_falsification_conditions",
            "packet.contradiction_handling.open_risks",
        ),
    )


def _build_replay_section(
    replay_risk: dict[str, object],
    failure_memory: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    status = str(replay_risk.get("status", ""))
    summary = str(replay_risk.get("summary", ""))
    lines: list[str] = [
        f"Replay status: {status}",
        f"Replay summary: {summary}",
    ]

    structural = failure_memory.get("structural_failure_history")
    if isinstance(structural, dict):
        event_count = structural.get("matched_event_count", 0)
        failure_count = structural.get("failure_event_count", 0)
        nonfailure_count = structural.get("nonfailure_event_count", 0)
        lines.append(
            f"Structural history: {event_count} events "
            f"({failure_count} failure, {nonfailure_count} nonfailure)"
        )

    offsetting = replay_risk.get("offsetting_reasons")
    if isinstance(offsetting, list):
        for reason in offsetting:
            if isinstance(reason, dict):
                event_id = str(reason.get("event_id", ""))
                explanation = str(reason.get("explanation", ""))
                lines.append(
                    f"Offsetting: {event_id} -- {explanation}"
                )

    uncertainty = replay_risk.get("uncertainty_reasons")
    if isinstance(uncertainty, list):
        for reason in uncertainty:
            if isinstance(reason, dict):
                event_id = str(reason.get("event_id", ""))
                explanation = str(reason.get("explanation", ""))
                lines.append(
                    f"Uncertainty: {event_id} -- {explanation}"
                )

    flags = replay_risk.get("uncertainty_flags")
    if isinstance(flags, list):
        for flag in flags:
            if isinstance(flag, dict):
                code = str(flag.get("code", ""))
                explanation = str(flag.get("explanation", ""))
                lines.append(f"Flag ({code}): {explanation}")

    return HypothesisDraftSection(
        section_id="replay_and_failure_memory",
        heading="Replay risk and failure memory",
        lines=tuple(lines),
        grounding_refs=(
            "packet.failure_memory.replay_risk.status",
            "packet.failure_memory.replay_risk.summary",
            "packet.failure_memory.structural_failure_history",
            "packet.failure_memory.replay_risk.offsetting_reasons",
            "packet.failure_memory.replay_risk.uncertainty_reasons",
        ),
    )


def _build_rescue_evidence_section(
    rescue_evidence: object,
    packet: dict[str, object],
) -> HypothesisDraftSection:
    if not isinstance(rescue_evidence, dict):
        return HypothesisDraftSection(
            section_id="rescue_evidence",
            heading="Rescue evidence",
            lines=("No rescue augmentation data available for this packet.",),
            grounding_refs=(),
        )

    coverage = str(rescue_evidence.get("coverage_status", ""))
    lines: list[str] = [f"Rescue coverage: {coverage}"]

    matched_tasks = rescue_evidence.get("matched_tasks")
    if isinstance(matched_tasks, list):
        for task in matched_tasks:
            if isinstance(task, dict):
                task_label = str(task.get("task_label", ""))
                decision = str(task.get("entity_rescue_decision", ""))
                lines.append(
                    f"Rescue task '{task_label}': decision = {decision}"
                )

                baseline = task.get("baseline_performance")
                if isinstance(baseline, dict):
                    bl_status = str(baseline.get("status", ""))
                    lines.append(f"  Baseline performance: {bl_status}")

                admission = task.get("model_admission")
                if isinstance(admission, dict):
                    adm_status = str(admission.get("status", ""))
                    lines.append(f"  Model admission: {adm_status}")

    conflict_signals = rescue_evidence.get("conflict_signals")
    if isinstance(conflict_signals, list) and conflict_signals:
        for signal in conflict_signals:
            if isinstance(signal, str) and signal.strip():
                lines.append(f"CONFLICT: {signal}")

    return HypothesisDraftSection(
        section_id="rescue_evidence",
        heading="Rescue evidence",
        lines=tuple(lines),
        grounding_refs=(
            "packet.rescue_evidence.coverage_status",
            "packet.rescue_evidence.matched_tasks",
            "packet.rescue_evidence.conflict_signals",
        ),
    )


def _build_rescue_policy_alignment_section(
    rescue_evidence: object,
    contradiction_handling: dict[str, object],
    replay_risk: dict[str, object],
    alignment: str,
) -> HypothesisDraftSection:
    lines: list[str] = [f"Rescue-policy alignment: {alignment}"]

    if alignment == RESCUE_POLICY_CONFLICTED:
        lines.append(
            "WARNING: Rescue and policy signals disagree. This draft "
            "surfaces the disagreement for human resolution. The agent "
            "does not resolve conflicts autonomously."
        )
        # Surface what specifically conflicts
        if isinstance(rescue_evidence, dict):
            conflict_signals = rescue_evidence.get("conflict_signals")
            if isinstance(conflict_signals, list):
                for signal in conflict_signals:
                    if isinstance(signal, str) and signal.strip():
                        lines.append(f"  Conflict detail: {signal}")
    elif alignment == RESCUE_POLICY_NO_DATA:
        lines.append(
            "No rescue task data available. Draft is grounded in "
            "policy and failure-memory signals only."
        )
    else:
        lines.append(
            "Rescue evidence and policy signals are broadly consistent."
        )

    return HypothesisDraftSection(
        section_id="rescue_policy_alignment",
        heading="Rescue-policy alignment",
        lines=tuple(lines),
        grounding_refs=(
            "packet.rescue_evidence.conflict_signals",
            "packet.contradiction_handling.status",
            "packet.failure_memory.replay_risk.status",
        ),
    )


def _build_credibility_section(
    registrations: list[dict[str, object]],
    packet_id: str,
) -> HypothesisDraftSection:
    if not registrations:
        return HypothesisDraftSection(
            section_id="credibility_grounding",
            heading="Prospective credibility grounding",
            lines=(
                "No prospective forecast registrations found for this packet.",
            ),
            grounding_refs=(),
        )

    lines: list[str] = [
        f"Found {len(registrations)} prospective forecast(s) "
        f"for packet {packet_id}."
    ]
    for reg in registrations:
        reg_id = str(reg.get("registration_id", ""))
        frozen = reg.get("frozen_forecast_payload")
        if isinstance(frozen, dict):
            predicted = str(frozen.get("predicted_outcome", ""))
            probs = frozen.get("option_probabilities")
            rationale = frozen.get("rationale")
            lines.append(
                f"Forecast '{reg_id}': predicted {predicted}"
            )
            if isinstance(probs, dict):
                prob_parts = [
                    f"{k}={v:.2f}" if isinstance(v, (int, float)) else f"{k}={v}"
                    for k, v in sorted(probs.items())
                ]
                lines.append(f"  Probabilities: {', '.join(prob_parts)}")
            if isinstance(rationale, list):
                for item in rationale[:3]:
                    if isinstance(item, str) and item.strip():
                        lines.append(f"  Rationale: {item}")

        window = None
        if isinstance(frozen, dict):
            window = frozen.get("outcome_window")
        if isinstance(window, dict):
            opens = str(window.get("opens_on", ""))
            closes = str(window.get("closes_on", ""))
            lines.append(f"  Window: {opens} to {closes}")

    return HypothesisDraftSection(
        section_id="credibility_grounding",
        heading="Prospective credibility grounding",
        lines=tuple(lines),
        grounding_refs=(
            "prospective_registry.registrations",
            f"prospective_registry.registrations[packet_id={packet_id}]",
        ),
    )


def _build_kill_conditions_section(
    contradiction_handling: dict[str, object],
    replay_risk: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    lines: list[str] = []

    # From contradiction conditions
    conditions = contradiction_handling.get("contradiction_conditions")
    if isinstance(conditions, list):
        for condition in conditions:
            if isinstance(condition, str) and condition.strip():
                lines.append(f"Kill if: {condition}")

    # From falsification conditions
    falsification = contradiction_handling.get(
        "directionality_falsification_conditions"
    )
    if isinstance(falsification, list):
        for condition in falsification:
            if isinstance(condition, str) and condition.strip():
                lines.append(f"Kill if: {condition}")

    # From replay falsification conditions
    replay_falsification = replay_risk.get("falsification_conditions")
    if isinstance(replay_falsification, list):
        for condition in replay_falsification:
            if isinstance(condition, str) and condition.strip():
                candidate = f"Replay escalates if: {condition}"
                if candidate not in lines:
                    lines.append(candidate)

    if not lines:
        lines.append(
            "No explicit kill conditions found. This is a gap that "
            "should be addressed before the hypothesis advances."
        )

    return HypothesisDraftSection(
        section_id="kill_conditions",
        heading="Kill conditions",
        lines=tuple(lines),
        grounding_refs=(
            "packet.contradiction_handling.contradiction_conditions",
            "packet.contradiction_handling.directionality_falsification_conditions",
            "packet.failure_memory.replay_risk.falsification_conditions",
        ),
    )


def _build_assay_section(
    first_assay: object,
    hypothesis: dict[str, object],
    packet: dict[str, object],
) -> HypothesisDraftSection:
    if not isinstance(first_assay, dict):
        direction = _safe_text(
            hypothesis.get("desired_perturbation_direction")
        )
        modality = _safe_text(hypothesis.get("modality_hypothesis"))
        lines: list[str] = [
            "No rescue-informed assay recommendation available.",
            f"Hypothesis direction: {direction} via {modality}.",
            "Design assay from policy and failure-memory signals.",
        ]
        return HypothesisDraftSection(
            section_id="assay_recommendation",
            heading="Assay recommendation",
            lines=tuple(lines),
            grounding_refs=(
                "packet.hypothesis.desired_perturbation_direction",
                "packet.hypothesis.modality_hypothesis",
            ),
        )

    assay_class = str(first_assay.get("recommended_assay_class", ""))
    rationale = str(first_assay.get("rationale", ""))
    lines = [
        f"Recommended assay class: {assay_class}",
        f"Rationale: {rationale}",
    ]

    next_steps = first_assay.get("next_steps")
    if isinstance(next_steps, list):
        for step in next_steps:
            if isinstance(step, str) and step.strip():
                lines.append(f"Next step: {step}")

    grounding = first_assay.get("grounding")
    if isinstance(grounding, dict):
        lines.append(
            f"Grounding: policy={grounding.get('policy_signal_status')}, "
            f"rescue={grounding.get('rescue_coverage')}, "
            f"contradiction={grounding.get('contradiction_status')}, "
            f"replay={grounding.get('replay_status')}"
        )

    return HypothesisDraftSection(
        section_id="assay_recommendation",
        heading="Assay recommendation",
        lines=tuple(lines),
        grounding_refs=(
            "packet.first_assay.recommended_assay_class",
            "packet.first_assay.rationale",
            "packet.first_assay.next_steps",
            "packet.first_assay.grounding",
        ),
    )


def _build_evidence_gaps_section(
    packet: dict[str, object],
) -> HypothesisDraftSection:
    lines: list[str] = []

    evidence_needed = packet.get("evidence_needed_next")
    if isinstance(evidence_needed, list):
        for item in evidence_needed:
            if isinstance(item, str) and item.strip():
                lines.append(f"Evidence needed: {item}")

    anchor_status = packet.get("evidence_anchor_gap_status")
    if isinstance(anchor_status, str):
        lines.append(f"Evidence anchor status: {anchor_status}")

    history_status = packet.get("program_history_gap_status")
    if isinstance(history_status, str):
        lines.append(f"Program history status: {history_status}")

    risk_digest = packet.get("risk_digest")
    if isinstance(risk_digest, list):
        for item in risk_digest:
            if isinstance(item, str) and item.strip():
                lines.append(f"Risk: {item}")

    if not lines:
        lines.append("No evidence gap signals found in packet.")

    return HypothesisDraftSection(
        section_id="evidence_gaps",
        heading="Evidence gaps and next evidence",
        lines=tuple(lines),
        grounding_refs=(
            "packet.evidence_needed_next",
            "packet.evidence_anchor_gap_status",
            "packet.program_history_gap_status",
            "packet.risk_digest",
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _determine_rescue_policy_alignment(
    rescue_evidence: object,
    contradiction_handling: dict[str, object],
    replay_risk: dict[str, object],
) -> str:
    if not isinstance(rescue_evidence, dict):
        return RESCUE_POLICY_NO_DATA
    if rescue_evidence.get("coverage_status") != "rescue_task_match":
        return RESCUE_POLICY_NO_DATA

    conflict_signals = rescue_evidence.get("conflict_signals")
    if isinstance(conflict_signals, list) and conflict_signals:
        return RESCUE_POLICY_CONFLICTED

    return RESCUE_POLICY_ALIGNED


def _filter_registrations_for_packet(
    packet_id: str,
    registrations: list[dict[str, object]],
) -> list[dict[str, object]]:
    matched: list[dict[str, object]] = []
    for reg in registrations:
        packet_artifact = reg.get("packet_artifact")
        if isinstance(packet_artifact, dict):
            if packet_artifact.get("packet_id") == packet_id:
                matched.append(reg)
        packet_scope = reg.get("packet_scope")
        if isinstance(packet_scope, dict) and not matched:
            # fallback: check if scope matches
            scope_entity = str(packet_scope.get("entity_id", ""))
            scope_policy = str(packet_scope.get("policy_id", ""))
            if f"{scope_entity}__{scope_policy}" == packet_id:
                matched.append(reg)
    return matched


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


def _safe_text(value: object) -> str:
    if isinstance(value, str):
        return value
    return ""


def _safe_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    return []
