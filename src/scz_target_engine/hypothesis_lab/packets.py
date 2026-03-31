from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.artifacts.validators import (
    validate_hypothesis_packets_payload,
    validate_required_scored_policy_signal,
)
from scz_target_engine.io import write_json


HYPOTHESIS_PACKETS_SCHEMA_VERSION = "v1"
REQUIRE_SCORED_POLICY_SIGNAL = True


def build_hypothesis_packets_payload(
    policy_payload: dict[str, object],
    ledger_payload: dict[str, object],
    *,
    policy_artifact_ref: str,
    ledger_artifact_ref: str,
) -> dict[str, object]:
    policy_entities = _require_gene_policy_entities(policy_payload)
    policy_ids = _require_policy_ids(policy_payload)
    ledger_targets = _require_ledger_targets(ledger_payload)
    ledger_index = {
        _require_text(target.get("entity_id"), "gene_target_ledgers.targets[].entity_id"): (
            index,
            target,
        )
        for index, target in enumerate(ledger_targets)
    }

    packets: list[dict[str, object]] = []
    for entity_index, entity in enumerate(policy_entities):
        entity_id = _require_text(
            entity.get("entity_id"),
            f"policy_decision_vectors_v2.entities.gene[{entity_index}].entity_id",
        )
        entity_label = _require_text(
            entity.get("entity_label"),
            f"policy_decision_vectors_v2.entities.gene[{entity_index}].entity_label",
        )
        ledger_entry = ledger_index.get(entity_id)
        if ledger_entry is None:
            raise ValueError(
                "hypothesis packets require a matching gene_target_ledgers target for "
                f"{entity_label} ({entity_id})"
            )
        ledger_index_value, ledger_target = ledger_entry
        directionality = _require_mapping(
            ledger_target.get("directionality_hypothesis"),
            (
                "gene_target_ledgers.targets"
                f"[{ledger_index_value}].directionality_hypothesis"
            ),
        )
        if directionality.get("status") != "curated":
            continue

        desired_direction = _require_specific_text(
            directionality.get("desired_perturbation_direction"),
            (
                "gene_target_ledgers.targets"
                f"[{ledger_index_value}].directionality_hypothesis"
                ".desired_perturbation_direction"
            ),
        )
        modality_hypothesis = _require_specific_text(
            directionality.get("modality_hypothesis"),
            (
                "gene_target_ledgers.targets"
                f"[{ledger_index_value}].directionality_hypothesis.modality_hypothesis"
            ),
        )
        preferred_modalities = _require_specific_string_list(
            directionality.get("preferred_modalities"),
            (
                "gene_target_ledgers.targets"
                f"[{ledger_index_value}].directionality_hypothesis.preferred_modalities"
            ),
        )
        policy_scores = _require_list(
            entity.get("policy_scores"),
            f"policy_decision_vectors_v2.entities.gene[{entity_index}].policy_scores",
        )
        for score_index, score_item in enumerate(policy_scores):
            score = _require_mapping(
                score_item,
                (
                    "policy_decision_vectors_v2.entities.gene"
                    f"[{entity_index}].policy_scores[{score_index}]"
                ),
            )
            policy_id = _require_text(
                score.get("policy_id"),
                (
                    "policy_decision_vectors_v2.entities.gene"
                    f"[{entity_index}].policy_scores[{score_index}].policy_id"
                ),
            )
            if policy_id not in policy_ids:
                raise ValueError(
                    "hypothesis packets encountered unknown policy_id "
                    f"{policy_id!r} for {entity_label}"
                )
            if not _is_packet_eligible_policy_signal(
                score,
                field_name=(
                    "policy_decision_vectors_v2.entities.gene"
                    f"[{entity_index}].policy_scores[{score_index}]"
                ),
            ):
                continue
            replay_risk = _require_mapping(
                _require_mapping(
                    score.get("uncertainty_context"),
                    (
                        "policy_decision_vectors_v2.entities.gene"
                        f"[{entity_index}].policy_scores[{score_index}].uncertainty_context"
                    ),
                ).get("replay_risk"),
                (
                    "policy_decision_vectors_v2.entities.gene"
                    f"[{entity_index}].policy_scores[{score_index}].uncertainty_context"
                    ".replay_risk"
                ),
            )
            structural_failure_history = _require_mapping(
                ledger_target.get("structural_failure_history"),
                (
                    "gene_target_ledgers.targets"
                    f"[{ledger_index_value}].structural_failure_history"
                ),
            )
            escape_routes = _build_escape_routes(replay_risk)
            packet = {
                "packet_id": f"{entity_id}__{policy_id}",
                "entity_type": "gene",
                "entity_id": entity_id,
                "entity_label": entity_label,
                "policy_id": policy_id,
                "policy_label": _require_text(
                    score.get("label"),
                    (
                        "policy_decision_vectors_v2.entities.gene"
                        f"[{entity_index}].policy_scores[{score_index}].label"
                    ),
                ),
                "priority_domain": _require_text(
                    score.get("primary_domain_slug"),
                    (
                        "policy_decision_vectors_v2.entities.gene"
                        f"[{entity_index}].policy_scores[{score_index}].primary_domain_slug"
                    ),
                ),
                "hypothesis": {
                    "statement": (
                        f"Test {desired_direction} {entity_label} via "
                        f"{modality_hypothesis} ({', '.join(preferred_modalities)}) "
                        f"in {_require_text(score.get('primary_domain_slug'), 'policy score primary_domain_slug')}."
                    ),
                    "desired_perturbation_direction": desired_direction,
                    "modality_hypothesis": modality_hypothesis,
                    "preferred_modalities": preferred_modalities,
                    "confidence": _require_text(
                        directionality.get("confidence"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis.confidence"
                        ),
                    ),
                    "ambiguity": _require_string(
                        directionality.get("ambiguity"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis.ambiguity"
                        ),
                    ),
                    "evidence_basis": _require_string(
                        directionality.get("evidence_basis"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis.evidence_basis"
                        ),
                    ),
                    "supporting_program_ids": _require_string_list(
                        directionality.get("supporting_program_ids"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis.supporting_program_ids"
                        ),
                    ),
                },
                "policy_signal": dict(score),
                "contradiction_handling": {
                    "status": (
                        "contradicted"
                        if _require_string_list(
                            directionality.get("contradiction_conditions"),
                            (
                                "gene_target_ledgers.targets"
                                f"[{ledger_index_value}].directionality_hypothesis"
                                ".contradiction_conditions"
                            ),
                        )
                        else "clear"
                    ),
                    "contradiction_conditions": _require_string_list(
                        directionality.get("contradiction_conditions"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis"
                            ".contradiction_conditions"
                        ),
                    ),
                    "directionality_falsification_conditions": _require_string_list(
                        ledger_target.get("falsification_conditions"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].falsification_conditions"
                        ),
                    ),
                    "open_risks": list(
                        _require_list(
                            ledger_target.get("open_risks"),
                            f"gene_target_ledgers.targets[{ledger_index_value}].open_risks",
                        )
                    ),
                },
                "failure_memory": {
                    "structural_failure_history": dict(structural_failure_history),
                    "replay_risk": dict(replay_risk),
                },
                "failure_escape_logic": {
                    "status": _determine_escape_status(
                        replay_status=_require_text(
                            replay_risk.get("status"),
                            (
                                "policy_decision_vectors_v2.entities.gene"
                                f"[{entity_index}].policy_scores[{score_index}]."
                                "uncertainty_context.replay_risk.status"
                            ),
                        ),
                        escape_routes=escape_routes,
                    ),
                    "escape_routes": escape_routes,
                    "next_evidence": _require_string_list(
                        replay_risk.get("falsification_conditions"),
                        (
                            "policy_decision_vectors_v2.entities.gene"
                            f"[{entity_index}].policy_scores[{score_index}]."
                            "uncertainty_context.replay_risk.falsification_conditions"
                        ),
                    ),
                },
                "traceability": {
                    "source_artifacts": {
                        "policy_decision_vectors_v2": policy_artifact_ref,
                        "gene_target_ledgers": ledger_artifact_ref,
                    },
                    "policy_entity_pointer": f"/entities/gene/{entity_index}",
                    "policy_score_pointer": (
                        f"/entities/gene/{entity_index}/policy_scores/{score_index}"
                    ),
                    "ledger_target_pointer": f"/targets/{ledger_index_value}",
                    "directionality_supporting_program_ids": _require_string_list(
                        directionality.get("supporting_program_ids"),
                        (
                            "gene_target_ledgers.targets"
                            f"[{ledger_index_value}].directionality_hypothesis.supporting_program_ids"
                        ),
                    ),
                    "structural_failure_program_ids": _collect_program_ids(
                        structural_failure_history
                    ),
                    "replay_reason_event_ids": _collect_replay_event_ids(replay_risk),
                },
            }
            packets.append(packet)

    packets.sort(key=lambda packet: (packet["entity_label"].lower(), packet["policy_id"]))
    return {
        "schema_version": HYPOTHESIS_PACKETS_SCHEMA_VERSION,
        "source_artifacts": {
            "policy_decision_vectors_v2": policy_artifact_ref,
            "gene_target_ledgers": ledger_artifact_ref,
        },
        "packet_generation_criteria": {
            "entity_types": ["gene"],
            "require_curated_directionality": True,
            "require_non_stub_hypothesis": True,
            "require_scored_policy_signal": REQUIRE_SCORED_POLICY_SIGNAL,
        },
        "packet_count": len(packets),
        "packets": packets,
    }


def materialize_hypothesis_packets(
    policy_artifact_file: Path,
    ledger_artifact_file: Path,
    *,
    output_file: Path | None = None,
) -> dict[str, object]:
    resolved_policy_path = policy_artifact_file.resolve()
    resolved_ledger_path = ledger_artifact_file.resolve()
    policy_artifact = load_artifact(
        resolved_policy_path,
        artifact_name="policy_decision_vectors_v2",
    )
    ledger_artifact = load_artifact(
        resolved_ledger_path,
        artifact_name="gene_target_ledgers",
    )
    output_dir = (
        output_file.resolve().parent
        if output_file is not None
        else resolved_policy_path.parent
    )
    artifact_output_path = (
        output_file.resolve()
        if output_file is not None
        else (output_dir / "hypothesis_packets_v1.json").resolve()
    )
    payload = build_hypothesis_packets_payload(
        dict(policy_artifact.payload),
        dict(ledger_artifact.payload),
        policy_artifact_ref=os.path.relpath(resolved_policy_path, output_dir),
        ledger_artifact_ref=os.path.relpath(resolved_ledger_path, output_dir),
    )
    validate_hypothesis_packets_payload(
        payload,
        artifact_path=artifact_output_path,
    )
    if output_file is not None:
        write_json(artifact_output_path, payload)
    return payload


def _require_gene_policy_entities(payload: dict[str, object]) -> list[dict[str, object]]:
    entities = _require_mapping(payload.get("entities"), "policy_decision_vectors_v2.entities")
    gene_entities = _require_list(
        entities.get("gene"),
        "policy_decision_vectors_v2.entities.gene",
    )
    return [
        _require_mapping(
            item,
            f"policy_decision_vectors_v2.entities.gene[{index}]",
        )
        for index, item in enumerate(gene_entities)
    ]


def _require_policy_ids(payload: dict[str, object]) -> set[str]:
    definitions = _require_list(
        payload.get("policy_definitions"),
        "policy_decision_vectors_v2.policy_definitions",
    )
    policy_ids: set[str] = set()
    for index, item in enumerate(definitions):
        definition = _require_mapping(
            item,
            f"policy_decision_vectors_v2.policy_definitions[{index}]",
        )
        policy_ids.add(
            _require_text(
                definition.get("policy_id"),
                f"policy_decision_vectors_v2.policy_definitions[{index}].policy_id",
            )
        )
    return policy_ids


def _require_ledger_targets(payload: dict[str, object]) -> list[dict[str, object]]:
    targets = _require_list(payload.get("targets"), "gene_target_ledgers.targets")
    return [
        _require_mapping(item, f"gene_target_ledgers.targets[{index}]")
        for index, item in enumerate(targets)
    ]


def _build_escape_routes(replay_risk: dict[str, object]) -> list[dict[str, object]]:
    routes: list[dict[str, object]] = []
    for index, item in enumerate(
        _require_list(
            replay_risk.get("offsetting_reasons"),
            "replay_risk.offsetting_reasons",
        )
    ):
        reason = _require_mapping(
            item,
            f"replay_risk.offsetting_reasons[{index}]",
        )
        routes.append(
            {
                "route_kind": "offsetting_reason",
                "event_id": _require_text(
                    reason.get("event_id"),
                    f"replay_risk.offsetting_reasons[{index}].event_id",
                ),
                "failure_scope": _require_text(
                    reason.get("failure_scope"),
                    f"replay_risk.offsetting_reasons[{index}].failure_scope",
                ),
                "explanation": _require_text(
                    reason.get("explanation"),
                    f"replay_risk.offsetting_reasons[{index}].explanation",
                ),
            }
        )
    return routes


def _determine_escape_status(
    *,
    replay_status: str,
    escape_routes: list[dict[str, object]],
) -> str:
    if escape_routes:
        return "escape_evidence_present"
    if replay_status == "insufficient_history":
        return "history_insufficient"
    if replay_status == "replay_supported":
        return "escape_blocked"
    return "escape_unresolved"


def _collect_program_ids(structural_failure_history: dict[str, object]) -> list[str]:
    program_ids: list[str] = []
    for index, item in enumerate(
        _require_list(
            structural_failure_history.get("events"),
            "structural_failure_history.events",
        )
    ):
        event = _require_mapping(
            item,
            f"structural_failure_history.events[{index}]",
        )
        program_ids.append(
            _require_text(
                event.get("program_id"),
                f"structural_failure_history.events[{index}].program_id",
            )
        )
    return program_ids


def _collect_replay_event_ids(replay_risk: dict[str, object]) -> list[str]:
    event_ids: list[str] = []
    for list_field in (
        "supporting_reasons",
        "offsetting_reasons",
        "uncertainty_reasons",
    ):
        for index, item in enumerate(
            _require_list(
                replay_risk.get(list_field),
                f"replay_risk.{list_field}",
            )
        ):
            reason = _require_mapping(
                item,
                f"replay_risk.{list_field}[{index}]",
            )
            event_id = _require_text(
                reason.get("event_id"),
                f"replay_risk.{list_field}[{index}].event_id",
            )
            if event_id not in event_ids:
                event_ids.append(event_id)
    return event_ids


def _is_packet_eligible_policy_signal(
    score: dict[str, object],
    *,
    field_name: str,
) -> bool:
    if not REQUIRE_SCORED_POLICY_SIGNAL:
        return score.get("score") is not None
    try:
        validate_required_scored_policy_signal(
            score,
            field_name=field_name,
        )
    except ValueError:
        return False
    return True


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


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_specific_text(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    if text == "undetermined":
        raise ValueError(f"{field_name} must not be undetermined")
    return text


def _require_string_list(value: object, field_name: str) -> list[str]:
    return [_require_text(item, f"{field_name}[]") for item in _require_list(value, field_name)]


def _require_specific_string_list(value: object, field_name: str) -> list[str]:
    values = _require_string_list(value, field_name)
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    for index, item in enumerate(values):
        if item == "undetermined":
            raise ValueError(f"{field_name}[{index}] must not be undetermined")
    return values
