from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.io import write_json


BLINDED_EXPERT_REVIEW_SCHEMA_VERSION = "v1"
BLINDED_EXPERT_REVIEW_RUBRIC = {
    "rubric_id": "blinded_expert_review_rubric_v1",
    "review_goal": (
        "Choose the blinded packet variant you would rather send into an expert "
        "packet review, then record the contract changes the comparison exposed."
    ),
    "comparison_prompt": (
        "For each target-policy pair, pick the blinded packet that best supports a "
        "decision-relevant expert review without breaking traceability."
    ),
    "dimensions": [
        {
            "dimension_id": "decision_readiness",
            "label": "Decision readiness",
            "question": (
                "Could an expert decide advance, hold, or kill from this packet "
                "without hunting through upstream artifacts?"
            ),
            "scale_min": 1,
            "scale_max": 5,
            "low_anchor": "Decision path is too vague or incomplete.",
            "high_anchor": "Decision path is explicit and reviewable.",
        },
        {
            "dimension_id": "evidence_traceability",
            "label": "Evidence traceability",
            "question": (
                "Can the packet's claims be traced back to shipped packet artifacts "
                "or concrete program-history anchors?"
            ),
            "scale_min": 1,
            "scale_max": 5,
            "low_anchor": "Claims require guesswork or extra lookup.",
            "high_anchor": "Claims stay attached to specific artifact anchors.",
        },
        {
            "dimension_id": "falsifiability",
            "label": "Falsifiability",
            "question": (
                "Does the packet make contradiction conditions or change-my-mind "
                "evidence explicit enough to challenge the hypothesis?"
            ),
            "scale_min": 1,
            "scale_max": 5,
            "low_anchor": "No clear kill conditions or reversal path.",
            "high_anchor": "Kill conditions and missing evidence are explicit.",
        },
        {
            "dimension_id": "schema_change_signal",
            "label": "Schema-change signal",
            "question": (
                "Does the packet make missing contract fields obvious enough to feed "
                "the next schema revision?"
            ),
            "scale_min": 1,
            "scale_max": 5,
            "low_anchor": "Packet hides what needs to change.",
            "high_anchor": "Packet clearly exposes contract pressure points.",
        },
    ],
    "required_findings": [
        "winner_reason",
        "loser_reason",
        "missing_fields",
        "traceability_gaps",
        "schema_change_requests",
        "generator_revision_requests",
    ],
}

REVIEW_PACKETS_FILENAME = "blinded_expert_review_packets_v1.json"
REVIEW_KEY_FILENAME = "blinded_expert_review_key_v1.json"
RESPONSE_TEMPLATE_FILENAME = "blinded_expert_review_response_template_v1.json"
EXPERT_PACKET_STYLE_ID = "traceable_expert_packet"
BASELINE_PACKET_STYLE_ID = "simplified_baseline_packet"
LEGACY_REQUIRED_FINDING_DEFAULTS = {
    "winner_reason": "",
    "loser_reason": "",
    "missing_fields": [],
    "traceability_gaps": [],
    "schema_change_requests": [],
    "generator_revision_requests": [],
}


def build_blinded_expert_review_payloads(
    hypothesis_payload: dict[str, object],
    *,
    rubric_payload: dict[str, object],
    hypothesis_artifact_ref: str,
    hypothesis_artifact_dir: Path,
    output_dir: Path,
    rubric_artifact_ref: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    packets = _require_list(hypothesis_payload.get("packets"), "hypothesis_packets_v1.packets")
    comparison_prompt = _require_text(
        rubric_payload.get("comparison_prompt"),
        "review_rubric.comparison_prompt",
    )
    dimensions = _require_list(rubric_payload.get("dimensions"), "review_rubric.dimensions")
    required_findings = _require_string_list(
        rubric_payload.get("required_findings"),
        "review_rubric.required_findings",
    )

    comparisons: list[dict[str, object]] = []
    key_comparisons: list[dict[str, object]] = []
    template_comparisons: list[dict[str, object]] = []
    for packet_index, packet_item in enumerate(packets):
        packet = _require_mapping(packet_item, f"hypothesis_packets_v1.packets[{packet_index}]")
        source_packet_id = _require_text(
            packet.get("packet_id"),
            f"hypothesis_packets_v1.packets[{packet_index}].packet_id",
        )
        comparison_id = f"comparison_{packet_index + 1:03d}"
        variants = [
            (
                EXPERT_PACKET_STYLE_ID,
                _build_expert_review_packet(packet),
                _expert_packet_source_fields(packet_index),
            ),
            (
                BASELINE_PACKET_STYLE_ID,
                _build_baseline_review_packet(packet),
                _baseline_packet_source_fields(packet_index),
            ),
        ]
        if _should_swap_variant_order(source_packet_id):
            variants.reverse()

        blinded_variants: list[dict[str, object]] = []
        blinded_key_variants: list[dict[str, object]] = []
        blind_ids: list[str] = []
        for variant_index, (style_id, review_packet, source_field_paths) in enumerate(variants):
            blind_id = f"{comparison_id}_{chr(ord('A') + variant_index)}"
            blind_ids.append(blind_id)
            content_sha256 = _hash_payload(review_packet)
            blinded_variants.append(
                {
                    "blind_id": blind_id,
                    "content_sha256": content_sha256,
                    "review_packet": review_packet,
                }
            )
            blinded_key_variants.append(
                {
                    "blind_id": blind_id,
                    "content_sha256": content_sha256,
                    "style_id": style_id,
                    "source_field_paths": source_field_paths,
                }
            )

        topic = {
            "entity_label": _require_text(
                packet.get("entity_label"),
                f"hypothesis_packets_v1.packets[{packet_index}].entity_label",
            ),
            "policy_label": _require_text(
                packet.get("policy_label"),
                f"hypothesis_packets_v1.packets[{packet_index}].policy_label",
            ),
            "priority_domain": _require_text(
                packet.get("priority_domain"),
                f"hypothesis_packets_v1.packets[{packet_index}].priority_domain",
            ),
        }
        comparisons.append(
            {
                "comparison_id": comparison_id,
                "topic": topic,
                "comparison_prompt": comparison_prompt,
                "variants": blinded_variants,
            }
        )
        key_comparisons.append(
            {
                "comparison_id": comparison_id,
                "source_packet_id": source_packet_id,
                "source_packet_pointer": f"/packets/{packet_index}",
                "source_traceability": deepcopy(
                    _rebase_traceability_paths(
                        _require_mapping(
                            packet.get("traceability"),
                            f"hypothesis_packets_v1.packets[{packet_index}].traceability",
                        ),
                        hypothesis_artifact_dir=hypothesis_artifact_dir,
                        output_dir=output_dir,
                    )
                ),
                "variants": blinded_key_variants,
            }
        )
        template_comparisons.append(
            {
                "comparison_id": comparison_id,
                "topic": topic,
                "available_blind_ids": blind_ids,
                "preferred_blind_id": None,
                "blind_scores": {
                    blind_id: {
                        _require_text(
                            _require_mapping(
                                dimension,
                                "review_rubric.dimensions[]",
                            ).get("dimension_id"),
                            "review_rubric.dimensions[].dimension_id",
                        ): None
                        for dimension in dimensions
                    }
                    for blind_id in blind_ids
                },
                **_build_required_finding_placeholders(required_findings),
            }
        )

    review_packets_payload = {
        "schema_version": BLINDED_EXPERT_REVIEW_SCHEMA_VERSION,
        "artifact_kind": "blinded_expert_review_packets",
        "source_artifacts": {
            "hypothesis_packets_v1": hypothesis_artifact_ref,
            "review_rubric": rubric_artifact_ref,
        },
        "style_family_count": 2,
        "comparison_count": len(comparisons),
        "comparison_prompt": comparison_prompt,
        "comparisons": comparisons,
    }
    review_key_payload = {
        "schema_version": BLINDED_EXPERT_REVIEW_SCHEMA_VERSION,
        "artifact_kind": "blinded_expert_review_key",
        "source_artifacts": {
            "hypothesis_packets_v1": hypothesis_artifact_ref,
            "review_packets_file": REVIEW_PACKETS_FILENAME,
            "response_template_file": RESPONSE_TEMPLATE_FILENAME,
            "review_rubric": rubric_artifact_ref,
        },
        "style_families": [
            {
                "style_id": EXPERT_PACKET_STYLE_ID,
                "description": (
                    "Decision-oriented expert review packet with evidence anchors, "
                    "change-my-mind conditions, and explicit traceability."
                ),
            },
            {
                "style_id": BASELINE_PACKET_STYLE_ID,
                "description": (
                    "Simpler baseline packet with a short summary, rationale, and "
                    "risk snapshot."
                ),
            },
        ],
        "comparison_count": len(key_comparisons),
        "comparisons": key_comparisons,
    }
    response_template_payload = {
        "schema_version": BLINDED_EXPERT_REVIEW_SCHEMA_VERSION,
        "artifact_kind": "blinded_expert_review_response_template",
        "source_artifacts": {
            "review_packets_file": REVIEW_PACKETS_FILENAME,
            "review_key_file": REVIEW_KEY_FILENAME,
            "review_rubric": rubric_artifact_ref,
        },
        "rubric": deepcopy(rubric_payload),
        "reviewer": {
            "reviewer_id": "",
            "reviewer_role": "",
            "completed_at": "",
        },
        "comparison_count": len(template_comparisons),
        "comparisons": template_comparisons,
    }
    return review_packets_payload, review_key_payload, response_template_payload


def materialize_blinded_expert_review_packets(
    hypothesis_artifact_file: Path,
    *,
    output_dir: Path,
    rubric_file: Path | None = None,
) -> dict[str, object]:
    resolved_hypothesis_path = hypothesis_artifact_file.resolve()
    resolved_output_dir = output_dir.resolve()
    resolved_rubric_path = (
        rubric_file.resolve()
        if rubric_file is not None
        else (
            Path(__file__).resolve().parents[3]
            / "docs"
            / "review_rubrics"
            / "blinded_expert_review_rubric.json"
        ).resolve()
    )
    rubric_payload = _load_review_rubric_payload(resolved_rubric_path)
    hypothesis_artifact = load_artifact(
        resolved_hypothesis_path,
        artifact_name="hypothesis_packets_v1",
    )
    review_packets_payload, review_key_payload, response_template_payload = (
        build_blinded_expert_review_payloads(
            dict(hypothesis_artifact.payload),
            rubric_payload=rubric_payload,
            hypothesis_artifact_ref=os.path.relpath(
                resolved_hypothesis_path,
                resolved_output_dir,
            ),
            hypothesis_artifact_dir=resolved_hypothesis_path.parent,
            output_dir=resolved_output_dir,
            rubric_artifact_ref=os.path.relpath(
                resolved_rubric_path,
                resolved_output_dir,
            ),
        )
    )

    review_packets_path = resolved_output_dir / REVIEW_PACKETS_FILENAME
    review_key_path = resolved_output_dir / REVIEW_KEY_FILENAME
    response_template_path = resolved_output_dir / RESPONSE_TEMPLATE_FILENAME
    write_json(review_packets_path, review_packets_payload)
    write_json(review_key_path, review_key_payload)
    write_json(response_template_path, response_template_payload)
    return {
        "comparison_count": review_packets_payload["comparison_count"],
        "output_dir": str(resolved_output_dir),
        "review_packets_file": str(review_packets_path),
        "review_key_file": str(review_key_path),
        "response_template_file": str(response_template_path),
        "source_hypothesis_artifact": str(resolved_hypothesis_path),
    }


def _build_expert_review_packet(packet: dict[str, object]) -> dict[str, object]:
    hypothesis = _require_mapping(packet.get("hypothesis"), "packet.hypothesis")
    policy_signal = _require_mapping(packet.get("policy_signal"), "packet.policy_signal")
    contradiction_handling = _require_mapping(
        packet.get("contradiction_handling"),
        "packet.contradiction_handling",
    )
    failure_memory = _require_mapping(packet.get("failure_memory"), "packet.failure_memory")
    replay_risk = _require_mapping(failure_memory.get("replay_risk"), "packet.failure_memory.replay_risk")
    failure_escape_logic = _require_mapping(
        packet.get("failure_escape_logic"),
        "packet.failure_escape_logic",
    )
    traceability = _require_mapping(packet.get("traceability"), "packet.traceability")
    sections = [
        {
            "heading": "Decision focus",
            "lines": [
                f"Target: {_require_text(packet.get('entity_label'), 'packet.entity_label')}",
                f"Policy: {_require_text(packet.get('policy_label'), 'packet.policy_label')}",
                f"Priority domain: {_require_text(packet.get('priority_domain'), 'packet.priority_domain')}",
                (
                    "Decision ask: Decide whether this hypothesis is reviewable "
                    "enough to advance, hold, or kill."
                ),
            ],
        },
        {
            "heading": "Hypothesis",
            "lines": [
                _require_text(hypothesis.get("statement"), "packet.hypothesis.statement"),
                (
                    "Direction / modality: "
                    f"{_require_text(hypothesis.get('desired_perturbation_direction'), 'packet.hypothesis.desired_perturbation_direction')} "
                    f"via {_require_text(hypothesis.get('modality_hypothesis'), 'packet.hypothesis.modality_hypothesis')}"
                ),
                (
                    "Confidence: "
                    f"{_require_text(hypothesis.get('confidence'), 'packet.hypothesis.confidence')}"
                ),
                (
                    "Evidence basis: "
                    f"{_require_string(hypothesis.get('evidence_basis'), 'packet.hypothesis.evidence_basis')}"
                ),
            ],
        },
        {
            "heading": "Why it made the packet",
            "lines": [
                (
                    "Policy signal: "
                    f"{_require_text(policy_signal.get('label'), 'packet.policy_signal.label')} "
                    f"scored {_format_score(policy_signal.get('score'))} "
                    f"({ _require_text(policy_signal.get('status'), 'packet.policy_signal.status') }) "
                    f"with base {_format_score(policy_signal.get('base_score'))}."
                ),
                _require_text(
                    policy_signal.get("description"),
                    "packet.policy_signal.description",
                ),
                (
                    "Replay summary: "
                    f"{_require_text(replay_risk.get('summary'), 'packet.failure_memory.replay_risk.summary')}"
                ),
            ],
        },
        {
            "heading": "Evidence anchors",
            "lines": _build_evidence_anchor_lines(packet),
        },
        {
            "heading": "Contradictions and risks",
            "lines": _build_contradiction_and_risk_lines(contradiction_handling),
        },
        {
            "heading": "Change-my-mind evidence",
            "lines": _build_change_my_mind_lines(failure_escape_logic),
        },
        {
            "heading": "Traceability",
            "lines": [
                f"Source packet id: {_require_text(packet.get('packet_id'), 'packet.packet_id')}",
                (
                    "Policy pointer: "
                    f"{_require_text(traceability.get('policy_entity_pointer'), 'packet.traceability.policy_entity_pointer')} "
                    f"-> {_require_text(traceability.get('policy_score_pointer'), 'packet.traceability.policy_score_pointer')}"
                ),
                (
                    "Ledger pointer: "
                    f"{_require_text(traceability.get('ledger_target_pointer'), 'packet.traceability.ledger_target_pointer')}"
                ),
                (
                    "Replay event ids: "
                    f"{_format_string_list(_require_string_list(traceability.get('replay_reason_event_ids'), 'packet.traceability.replay_reason_event_ids'))}"
                ),
            ],
        },
    ]
    return {
        "title": (
            f"{_require_text(packet.get('entity_label'), 'packet.entity_label')} | "
            f"{_require_text(packet.get('policy_label'), 'packet.policy_label')}"
        ),
        "sections": sections,
    }


def _rebase_traceability_paths(
    traceability: dict[str, object],
    *,
    hypothesis_artifact_dir: Path,
    output_dir: Path,
) -> dict[str, object]:
    rebased_traceability = deepcopy(traceability)
    source_artifacts = _require_mapping(
        rebased_traceability.get("source_artifacts"),
        "packet.traceability.source_artifacts",
    )
    rebased_traceability["source_artifacts"] = {
        artifact_name: _rebase_artifact_reference(
            _require_text(
                reference,
                f"packet.traceability.source_artifacts.{artifact_name}",
            ),
            hypothesis_artifact_dir=hypothesis_artifact_dir,
            output_dir=output_dir,
        )
        for artifact_name, reference in source_artifacts.items()
    }
    return rebased_traceability


def _rebase_artifact_reference(
    reference: str,
    *,
    hypothesis_artifact_dir: Path,
    output_dir: Path,
) -> str:
    candidate = Path(reference)
    resolved = (
        candidate.resolve()
        if candidate.is_absolute()
        else (hypothesis_artifact_dir / candidate).resolve()
    )
    return os.path.relpath(resolved, output_dir)


def _load_review_rubric_payload(rubric_file: Path) -> dict[str, object]:
    with rubric_file.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    rubric = _require_mapping(payload, "review_rubric")
    _require_text(rubric.get("rubric_id"), "review_rubric.rubric_id")
    _require_text(rubric.get("review_goal"), "review_rubric.review_goal")
    _require_text(rubric.get("comparison_prompt"), "review_rubric.comparison_prompt")
    dimensions = _require_list(rubric.get("dimensions"), "review_rubric.dimensions")
    if not dimensions:
        raise ValueError("review_rubric.dimensions must not be empty")
    for index, item in enumerate(dimensions):
        dimension = _require_mapping(item, f"review_rubric.dimensions[{index}]")
        _require_text(
            dimension.get("dimension_id"),
            f"review_rubric.dimensions[{index}].dimension_id",
        )
        _require_text(dimension.get("label"), f"review_rubric.dimensions[{index}].label")
        _require_text(
            dimension.get("question"),
            f"review_rubric.dimensions[{index}].question",
        )
        _require_int(
            dimension.get("scale_min"),
            f"review_rubric.dimensions[{index}].scale_min",
        )
        _require_int(
            dimension.get("scale_max"),
            f"review_rubric.dimensions[{index}].scale_max",
        )
        _require_text(
            dimension.get("low_anchor"),
            f"review_rubric.dimensions[{index}].low_anchor",
        )
        _require_text(
            dimension.get("high_anchor"),
            f"review_rubric.dimensions[{index}].high_anchor",
        )
    required_findings = _require_string_list(
        rubric.get("required_findings"),
        "review_rubric.required_findings",
    )
    if not required_findings:
        raise ValueError("review_rubric.required_findings must not be empty")
    if len(required_findings) != len(set(required_findings)):
        raise ValueError("review_rubric.required_findings must not repeat fields")
    return rubric


def _build_required_finding_placeholders(required_findings: list[str]) -> dict[str, object]:
    return {
        finding_name: deepcopy(LEGACY_REQUIRED_FINDING_DEFAULTS.get(finding_name))
        for finding_name in required_findings
    }


def _build_baseline_review_packet(packet: dict[str, object]) -> dict[str, object]:
    hypothesis = _require_mapping(packet.get("hypothesis"), "packet.hypothesis")
    policy_signal = _require_mapping(packet.get("policy_signal"), "packet.policy_signal")
    contradiction_handling = _require_mapping(
        packet.get("contradiction_handling"),
        "packet.contradiction_handling",
    )
    replay_risk = _require_mapping(
        _require_mapping(packet.get("failure_memory"), "packet.failure_memory").get("replay_risk"),
        "packet.failure_memory.replay_risk",
    )
    sections = [
        {
            "heading": "Summary",
            "lines": [
                (
                    f"{_require_text(packet.get('entity_label'), 'packet.entity_label')} | "
                    f"{_require_text(packet.get('policy_label'), 'packet.policy_label')} | "
                    f"{_require_text(packet.get('priority_domain'), 'packet.priority_domain')}"
                ),
                _require_text(hypothesis.get("statement"), "packet.hypothesis.statement"),
                (
                    "Policy score: "
                    f"{_format_score(policy_signal.get('score'))} "
                    f"({ _require_text(policy_signal.get('status'), 'packet.policy_signal.status') })"
                ),
            ],
        },
        {
            "heading": "Rationale",
            "lines": [
                _require_string(hypothesis.get("evidence_basis"), "packet.hypothesis.evidence_basis"),
                _require_text(policy_signal.get("description"), "packet.policy_signal.description"),
            ],
        },
        {
            "heading": "Risks",
            "lines": _build_baseline_risk_lines(contradiction_handling, replay_risk),
        },
    ]
    return {
        "title": (
            f"{_require_text(packet.get('entity_label'), 'packet.entity_label')} | "
            f"{_require_text(packet.get('policy_label'), 'packet.policy_label')}"
        ),
        "sections": sections,
    }


def _build_evidence_anchor_lines(packet: dict[str, object]) -> list[str]:
    hypothesis = _require_mapping(packet.get("hypothesis"), "packet.hypothesis")
    failure_memory = _require_mapping(packet.get("failure_memory"), "packet.failure_memory")
    structural_failure_history = _require_mapping(
        failure_memory.get("structural_failure_history"),
        "packet.failure_memory.structural_failure_history",
    )
    replay_risk = _require_mapping(failure_memory.get("replay_risk"), "packet.failure_memory.replay_risk")
    structural_events = {
        _require_text(event.get("program_id"), "structural_failure_history.events[].program_id"): event
        for event in (
            _require_mapping(item, "structural_failure_history.events[]")
            for item in _require_list(
                structural_failure_history.get("events"),
                "structural_failure_history.events",
            )
        )
    }

    seen_event_ids: set[str] = set()
    lines: list[str] = []
    for program_id in _require_string_list(
        hypothesis.get("supporting_program_ids"),
        "packet.hypothesis.supporting_program_ids",
    ):
        lines.append(
            _format_anchor_line(
                role="supporting_program",
                event_id=program_id,
                event=structural_events.get(program_id),
                fallback_reason="Program id is referenced in the packet hypothesis.",
            )
        )
        seen_event_ids.add(program_id)

    for reason_field in (
        ("supporting_reasons", "supporting_reason"),
        ("offsetting_reasons", "offsetting_reason"),
        ("uncertainty_reasons", "uncertainty_reason"),
    ):
        reason_list = _require_list(
            replay_risk.get(reason_field[0]),
            f"packet.failure_memory.replay_risk.{reason_field[0]}",
        )
        for index, item in enumerate(reason_list):
            reason = _require_mapping(
                item,
                f"packet.failure_memory.replay_risk.{reason_field[0]}[{index}]",
            )
            event_id = _require_text(
                reason.get("event_id"),
                f"packet.failure_memory.replay_risk.{reason_field[0]}[{index}].event_id",
            )
            if event_id in seen_event_ids:
                continue
            lines.append(
                _format_anchor_line(
                    role=reason_field[1],
                    event_id=event_id,
                    event=structural_events.get(event_id),
                    fallback_reason=_require_text(
                        reason.get("explanation"),
                        f"packet.failure_memory.replay_risk.{reason_field[0]}[{index}].explanation",
                    ),
                )
            )
            seen_event_ids.add(event_id)

    if lines:
        return lines
    return [
        "No supporting_program_ids or replay_reason_event_ids were emitted for this packet.",
    ]


def _format_anchor_line(
    *,
    role: str,
    event_id: str,
    event: dict[str, object] | None,
    fallback_reason: str,
) -> str:
    if event is None:
        return f"{role}: {event_id} - {fallback_reason}"
    event_type = _require_text(event.get("event_type"), "structural_failure_history.events[].event_type")
    primary_outcome = _require_text(
        event.get("primary_outcome_result"),
        "structural_failure_history.events[].primary_outcome_result",
    )
    notes = _require_text(event.get("notes"), "structural_failure_history.events[].notes")
    return (
        f"{role}: {event_id} ({event_type}, {primary_outcome}) - {notes}"
    )


def _build_contradiction_and_risk_lines(
    contradiction_handling: dict[str, object],
) -> list[str]:
    lines = [
        (
            "Contradiction status: "
            f"{_require_text(contradiction_handling.get('status'), 'packet.contradiction_handling.status')}"
        )
    ]
    contradiction_conditions = _require_string_list(
        contradiction_handling.get("contradiction_conditions"),
        "packet.contradiction_handling.contradiction_conditions",
    )
    if contradiction_conditions:
        lines.extend(
            f"Contradiction condition: {condition}" for condition in contradiction_conditions
        )
    falsification_conditions = _require_string_list(
        contradiction_handling.get("directionality_falsification_conditions"),
        "packet.contradiction_handling.directionality_falsification_conditions",
    )
    lines.extend(
        f"Falsification condition: {condition}" for condition in falsification_conditions
    )
    for risk in _sort_open_risks(
        _require_list(
            contradiction_handling.get("open_risks"),
            "packet.contradiction_handling.open_risks",
        )
    ):
        risk_mapping = _require_mapping(risk, "packet.contradiction_handling.open_risks[]")
        lines.append(
            "Open risk: "
            f"{_require_text(risk_mapping.get('severity'), 'packet.contradiction_handling.open_risks[].severity')} | "
            f"{_require_text(risk_mapping.get('text'), 'packet.contradiction_handling.open_risks[].text')}"
        )
    return lines


def _build_change_my_mind_lines(failure_escape_logic: dict[str, object]) -> list[str]:
    lines = [
        (
            "Escape status: "
            f"{_require_text(failure_escape_logic.get('status'), 'packet.failure_escape_logic.status')}"
        )
    ]
    escape_routes = _require_list(
        failure_escape_logic.get("escape_routes"),
        "packet.failure_escape_logic.escape_routes",
    )
    for route in escape_routes:
        route_mapping = _require_mapping(route, "packet.failure_escape_logic.escape_routes[]")
        lines.append(
            "Escape route: "
            f"{_require_text(route_mapping.get('event_id'), 'packet.failure_escape_logic.escape_routes[].event_id')} - "
            f"{_require_text(route_mapping.get('explanation'), 'packet.failure_escape_logic.escape_routes[].explanation')}"
        )
    next_evidence = _require_string_list(
        failure_escape_logic.get("next_evidence"),
        "packet.failure_escape_logic.next_evidence",
    )
    lines.extend(f"Needed evidence: {item}" for item in next_evidence)
    return lines


def _build_baseline_risk_lines(
    contradiction_handling: dict[str, object],
    replay_risk: dict[str, object],
) -> list[str]:
    lines = [
        (
            "Contradiction status: "
            f"{_require_text(contradiction_handling.get('status'), 'packet.contradiction_handling.status')}"
        ),
        (
            "Replay status: "
            f"{_require_text(replay_risk.get('status'), 'packet.failure_memory.replay_risk.status')}"
        ),
    ]
    contradiction_conditions = _require_string_list(
        contradiction_handling.get("contradiction_conditions"),
        "packet.contradiction_handling.contradiction_conditions",
    )
    if contradiction_conditions:
        lines.append(f"Main contradiction: {contradiction_conditions[0]}")
    open_risks = _sort_open_risks(
        _require_list(
            contradiction_handling.get("open_risks"),
            "packet.contradiction_handling.open_risks",
        )
    )
    for risk in open_risks[:2]:
        risk_mapping = _require_mapping(risk, "packet.contradiction_handling.open_risks[]")
        lines.append(
            "Risk: "
            f"{_require_text(risk_mapping.get('severity'), 'packet.contradiction_handling.open_risks[].severity')} | "
            f"{_require_text(risk_mapping.get('text'), 'packet.contradiction_handling.open_risks[].text')}"
        )
    return lines


def _sort_open_risks(open_risks: list[object]) -> list[object]:
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    return sorted(
        open_risks,
        key=lambda risk: severity_rank.get(
            _require_text(
                _require_mapping(risk, "packet.contradiction_handling.open_risks[]").get("severity"),
                "packet.contradiction_handling.open_risks[].severity",
            ),
            99,
        ),
    )


def _expert_packet_source_fields(packet_index: int) -> list[str]:
    base = f"/packets/{packet_index}"
    return [
        f"{base}/packet_id",
        f"{base}/entity_label",
        f"{base}/policy_label",
        f"{base}/priority_domain",
        f"{base}/hypothesis/statement",
        f"{base}/hypothesis/desired_perturbation_direction",
        f"{base}/hypothesis/modality_hypothesis",
        f"{base}/hypothesis/confidence",
        f"{base}/hypothesis/evidence_basis",
        f"{base}/hypothesis/supporting_program_ids",
        f"{base}/policy_signal/score",
        f"{base}/policy_signal/base_score",
        f"{base}/policy_signal/status",
        f"{base}/policy_signal/description",
        f"{base}/contradiction_handling/status",
        f"{base}/contradiction_handling/contradiction_conditions",
        f"{base}/contradiction_handling/directionality_falsification_conditions",
        f"{base}/contradiction_handling/open_risks",
        f"{base}/failure_memory/structural_failure_history/events",
        f"{base}/failure_memory/replay_risk",
        f"{base}/failure_escape_logic/status",
        f"{base}/failure_escape_logic/escape_routes",
        f"{base}/failure_escape_logic/next_evidence",
        f"{base}/traceability",
    ]


def _baseline_packet_source_fields(packet_index: int) -> list[str]:
    base = f"/packets/{packet_index}"
    return [
        f"{base}/entity_label",
        f"{base}/policy_label",
        f"{base}/priority_domain",
        f"{base}/hypothesis/statement",
        f"{base}/hypothesis/evidence_basis",
        f"{base}/policy_signal/score",
        f"{base}/policy_signal/status",
        f"{base}/policy_signal/description",
        f"{base}/contradiction_handling/status",
        f"{base}/contradiction_handling/contradiction_conditions",
        f"{base}/contradiction_handling/open_risks",
        f"{base}/failure_memory/replay_risk/status",
    ]


def _should_swap_variant_order(source_packet_id: str) -> bool:
    digest = hashlib.sha256(source_packet_id.encode("utf-8")).hexdigest()
    return int(digest, 16) % 2 == 1


def _hash_payload(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _format_score(value: object) -> str:
    if not isinstance(value, (int, float)):
        raise ValueError("packet policy score fields must be numeric")
    return f"{value:.3f}"


def _format_string_list(values: list[str]) -> str:
    if not values:
        return "none"
    return ", ".join(values)


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def _require_int(value: object, field_name: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_string_list(value: object, field_name: str) -> list[str]:
    values = _require_list(value, field_name)
    result = []
    for index, item in enumerate(values):
        result.append(_require_string(item, f"{field_name}[{index}]"))
    return result
