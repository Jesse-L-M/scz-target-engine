from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from scz_target_engine.decision_vector import (
    AVAILABLE_STATUS,
    MISSING_INPUTS_STATUS,
    NOT_APPLICABLE_STATUS,
    PARTIAL_STATUS,
    DecisionVectorV1,
    clamp_score,
    compute_weighted_average,
)
from scz_target_engine.ledger import TargetLedger
from scz_target_engine.policy.config import (
    PolicyDefinition,
    load_policy_definitions,
    serialize_policy_definition,
)
from scz_target_engine.program_memory import (
    InterventionProposal,
    assess_counterfactual_replay_risk,
    load_program_memory_dataset,
)


def build_policy_artifacts(
    gene_vectors: list[DecisionVectorV1],
    module_vectors: list[DecisionVectorV1],
    *,
    ledger_index: Mapping[str, TargetLedger] | None,
    repo_root: Path,
    policy_dir: Path | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    resolved_policy_dir = (
        policy_dir.resolve()
        if policy_dir is not None
        else (repo_root / "config" / "policies").resolve()
    )
    policies = load_policy_definitions(resolved_policy_dir)
    dataset = load_program_memory_dataset(
        (repo_root / "data" / "curated" / "program_history" / "v2").resolve()
    )
    replay_cache: dict[tuple[str, str], dict[str, object]] = {}

    gene_entities = [
        _build_policy_entity_payload(
            vector,
            policies,
            target_ledger=None if ledger_index is None else ledger_index.get(vector.entity_id),
            replay_cache=replay_cache,
            program_memory_dataset=dataset,
        )
        for vector in gene_vectors
    ]
    module_entities = [
        _build_policy_entity_payload(
            vector,
            policies,
            target_ledger=None,
            replay_cache=replay_cache,
            program_memory_dataset=dataset,
        )
        for vector in module_vectors
    ]
    return (
        {
            "schema_version": "v2",
            "policy_config_sources": [policy.source_file for policy in policies],
            "policy_definitions": [
                serialize_policy_definition(policy) for policy in policies
            ],
            "entities": {
                "gene": gene_entities,
                "module": module_entities,
            },
        },
        build_policy_pareto_front_payload(
            gene_entities,
            module_entities,
            policy_ids=[policy.policy_id for policy in policies],
        ),
    )


def build_policy_pareto_front_payload(
    gene_entities: list[dict[str, object]],
    module_entities: list[dict[str, object]],
    *,
    policy_ids: list[str],
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "policy_ids": list(policy_ids),
        "entity_types": {
            "gene": _build_pareto_rows(gene_entities, policy_ids=policy_ids),
            "module": _build_pareto_rows(module_entities, policy_ids=policy_ids),
        },
    }


def _build_policy_entity_payload(
    vector: DecisionVectorV1,
    policies: tuple[PolicyDefinition, ...],
    *,
    target_ledger: TargetLedger | None,
    replay_cache: dict[tuple[str, str], dict[str, object]],
    program_memory_dataset: object,
) -> dict[str, object]:
    policy_scores = [
        _build_policy_score_payload(
            vector,
            policy,
            target_ledger=target_ledger,
            replay_cache=replay_cache,
            program_memory_dataset=program_memory_dataset,
        )
        for policy in policies
    ]
    return {
        "entity_type": vector.entity_type,
        "entity_id": vector.entity_id,
        "entity_label": vector.entity_label,
        "eligible_v0": vector.eligible_v0,
        "heuristic_score_v0": vector.heuristic_score_v0,
        "heuristic_rank_v0": vector.heuristic_rank_v0,
        "heuristic_stable_v0": vector.heuristic_stable_v0,
        "warning_count": vector.warning_count,
        "warning_severity": vector.warning_severity,
        "policy_vector": {
            score["policy_id"]: score
            for score in policy_scores
        },
        "policy_scores": policy_scores,
    }


def _build_policy_score_payload(
    vector: DecisionVectorV1,
    policy: PolicyDefinition,
    *,
    target_ledger: TargetLedger | None,
    replay_cache: dict[tuple[str, str], dict[str, object]],
    program_memory_dataset: object,
) -> dict[str, object]:
    domain_score_index = {
        score.domain_slug: score
        for score in vector.domain_head_scores
    }
    domain_contributions: list[dict[str, object]] = []
    weighted_values: list[tuple[float, float]] = []
    present_weight = 0.0
    for domain_slug, weight in policy.domain_weights:
        domain_score = domain_score_index[domain_slug]
        score_value = domain_score.score
        if score_value is not None:
            weighted_values.append((score_value, weight))
            present_weight += weight
        domain_contributions.append(
            {
                "domain_slug": domain_slug,
                "label": domain_score.domain_label,
                "status": domain_score.status,
                "weight": round(weight, 6),
                "score": score_value,
                "domain_coverage_weight_fraction": domain_score.coverage_weight_fraction,
                "contribution": (
                    round(score_value * weight, 6)
                    if score_value is not None
                    else None
                ),
            }
        )

    base_score = compute_weighted_average(weighted_values)
    coverage = round(
        present_weight / policy.total_domain_weight if policy.total_domain_weight else 0.0,
        6,
    )
    status = MISSING_INPUTS_STATUS
    if base_score is not None:
        status = AVAILABLE_STATUS if coverage == 1.0 else PARTIAL_STATUS

    missing_head_count = sum(
        1 for score in vector.head_scores if score.status == MISSING_INPUTS_STATUS
    )
    partial_head_count = sum(
        1 for score in vector.head_scores if score.status == PARTIAL_STATUS
    )
    not_applicable_head_count = sum(
        1 for score in vector.head_scores if score.status == NOT_APPLICABLE_STATUS
    )
    directionality = (
        target_ledger.directionality_hypothesis
        if target_ledger is not None
        else {
            "open_risks": [],
            "contradiction_conditions": [],
            "falsification_conditions": [],
        }
    )
    replay_risk = _load_replay_risk_snapshot(
        vector,
        policy,
        target_ledger=target_ledger,
        replay_cache=replay_cache,
        program_memory_dataset=program_memory_dataset,
    )

    adjustments: list[dict[str, object]] = []
    _append_adjustment(
        adjustments,
        adjustment_id="low_coverage_penalty",
        label="Low policy-domain coverage penalty",
        delta=-(1.0 - coverage) * policy.adjustment_weights.low_coverage_penalty,
        evidence={
            "coverage_weight_fraction": coverage,
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="missing_head_penalty",
        label="Missing decision-head penalty",
        delta=-missing_head_count * policy.adjustment_weights.missing_head_penalty,
        evidence={"missing_head_count": missing_head_count},
    )
    _append_adjustment(
        adjustments,
        adjustment_id="partial_head_penalty",
        label="Partial decision-head penalty",
        delta=-partial_head_count * policy.adjustment_weights.partial_head_penalty,
        evidence={"partial_head_count": partial_head_count},
    )
    _append_adjustment(
        adjustments,
        adjustment_id="warning_penalty",
        label="Warning-count penalty",
        delta=-vector.warning_count * policy.adjustment_weights.warning_penalty_per_warning,
        evidence={
            "warning_count": vector.warning_count,
            "warning_severity": vector.warning_severity,
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="directionality_open_risk_penalty",
        label="Directionality open-risk penalty",
        delta=-len(directionality["open_risks"])
        * policy.adjustment_weights.directionality_open_risk_penalty,
        evidence={
            "directionality_open_risk_count": len(directionality["open_risks"]),
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="directionality_contradiction_penalty",
        label="Directionality contradiction penalty",
        delta=-len(directionality["contradiction_conditions"])
        * policy.adjustment_weights.directionality_contradiction_penalty,
        evidence={
            "directionality_contradiction_count": len(
                directionality["contradiction_conditions"]
            ),
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="directionality_falsification_penalty",
        label="Directionality falsification-condition penalty",
        delta=-len(directionality["falsification_conditions"])
        * policy.adjustment_weights.directionality_falsification_penalty,
        evidence={
            "directionality_falsification_count": len(
                directionality["falsification_conditions"]
            ),
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="replay_status_adjustment",
        label="Replay-risk status adjustment",
        delta=_replay_status_delta(policy, replay_risk["status"]),
        evidence={"replay_status": replay_risk["status"]},
    )
    _append_adjustment(
        adjustments,
        adjustment_id="replay_supporting_reason_penalty",
        label="Replay supporting-reason penalty",
        delta=-replay_risk["supporting_reason_count"]
        * policy.adjustment_weights.replay_supporting_reason_penalty,
        evidence={
            "supporting_reason_count": replay_risk["supporting_reason_count"],
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="replay_offsetting_reason_bonus",
        label="Replay offsetting-anchor bonus",
        delta=replay_risk["offsetting_reason_count"]
        * policy.adjustment_weights.replay_offsetting_reason_bonus,
        evidence={
            "offsetting_reason_count": replay_risk["offsetting_reason_count"],
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="replay_uncertainty_reason_penalty",
        label="Replay uncertainty-reason penalty",
        delta=-replay_risk["uncertainty_reason_count"]
        * policy.adjustment_weights.replay_uncertainty_reason_penalty,
        evidence={
            "uncertainty_reason_count": replay_risk["uncertainty_reason_count"],
        },
    )
    _append_adjustment(
        adjustments,
        adjustment_id="replay_uncertainty_flag_penalty",
        label="Replay uncertainty-flag penalty",
        delta=-replay_risk["uncertainty_flag_count"]
        * policy.adjustment_weights.replay_uncertainty_flag_penalty,
        evidence={
            "uncertainty_flag_count": replay_risk["uncertainty_flag_count"],
        },
    )

    adjustment_total = round(
        sum(float(adjustment["delta"]) for adjustment in adjustments),
        6,
    )
    score_before_clamp = (
        round(base_score + adjustment_total, 6)
        if base_score is not None
        else None
    )
    return {
        "policy_id": policy.policy_id,
        "label": policy.label,
        "description": policy.description,
        "primary_domain_slug": policy.primary_domain_slug,
        "score": (
            clamp_score(score_before_clamp)
            if score_before_clamp is not None
            else None
        ),
        "base_score": base_score,
        "score_before_clamp": score_before_clamp,
        "status": status,
        "coverage_weight_fraction": coverage,
        "uncertainty_adjustment_total": adjustment_total,
        "domain_contributions": domain_contributions,
        "adjustments": adjustments,
        "uncertainty_context": {
            "warning_count": vector.warning_count,
            "warning_severity": vector.warning_severity,
            "missing_head_count": missing_head_count,
            "partial_head_count": partial_head_count,
            "not_applicable_head_count": not_applicable_head_count,
            "directionality_open_risk_count": len(directionality["open_risks"]),
            "directionality_contradiction_count": len(
                directionality["contradiction_conditions"]
            ),
            "directionality_falsification_count": len(
                directionality["falsification_conditions"]
            ),
            "replay_risk": replay_risk,
        },
    }


def _build_pareto_rows(
    entities: list[dict[str, object]],
    *,
    policy_ids: list[str],
) -> list[dict[str, object]]:
    rows = [
        {
            "entity_type": str(entity["entity_type"]),
            "entity_id": str(entity["entity_id"]),
            "entity_label": str(entity["entity_label"]),
            "policy_scores": {
                policy_id: entity["policy_vector"][policy_id]["score"]
                for policy_id in policy_ids
            },
            "heuristic_score_v0": entity["heuristic_score_v0"],
            "heuristic_rank_v0": entity["heuristic_rank_v0"],
            "heuristic_stable_v0": entity["heuristic_stable_v0"],
            "warning_count": entity["warning_count"],
            "warning_severity": entity["warning_severity"],
        }
        for entity in entities
    ]
    for row in rows:
        missing_policy_score_count = sum(
            1
            for score in row["policy_scores"].values()
            if score is None
        )
        row["missing_policy_score_count"] = missing_policy_score_count
        row["complete_policy_vector"] = missing_policy_score_count == 0
    dominance_counts: list[tuple[int, int]] = []
    for row in rows:
        dominated_by_count = 0
        dominates_count = 0
        for other in rows:
            if not row["complete_policy_vector"] or not other["complete_policy_vector"]:
                continue
            if row is other:
                continue
            if _dominates(other, row, policy_ids=policy_ids):
                dominated_by_count += 1
            if _dominates(row, other, policy_ids=policy_ids):
                dominates_count += 1
        dominance_counts.append((dominated_by_count, dominates_count))
    for row, (dominated_by_count, dominates_count) in zip(
        rows,
        dominance_counts,
        strict=True,
        ):
        row["dominated_by_count"] = dominated_by_count
        row["dominates_count"] = dominates_count

    complete_rows = [row for row in rows if row["complete_policy_vector"]]
    partial_rows = [row for row in rows if not row["complete_policy_vector"]]
    remaining = list(complete_rows)
    current_front = 1
    while remaining:
        front_rows = [
            row
            for row in remaining
            if not any(
                _dominates(other, row, policy_ids=policy_ids)
                for other in remaining
                if other is not row
            )
        ]
        for row in front_rows:
            row["pareto_front"] = current_front
        remaining = [row for row in remaining if row not in front_rows]
        current_front += 1
    if partial_rows:
        # Rows with missing policy scores are not Pareto-eligible against complete vectors.
        fallback_front = current_front if complete_rows else 1
        for row in partial_rows:
            row["pareto_front"] = fallback_front

    rows.sort(
        key=lambda row: (
            int(row["pareto_front"]),
            not bool(row["complete_policy_vector"]),
            int(row["missing_policy_score_count"]),
            -sum(
                float(score)
                for score in row["policy_scores"].values()
                if score is not None
            ),
            str(row["entity_label"]).lower(),
        )
    )
    return rows


def _dominates(
    lhs: dict[str, object],
    rhs: dict[str, object],
    *,
    policy_ids: list[str],
) -> bool:
    lhs_scores = lhs["policy_scores"]
    rhs_scores = rhs["policy_scores"]
    any_strict = False
    for policy_id in policy_ids:
        lhs_score = lhs_scores[policy_id]
        rhs_score = rhs_scores[policy_id]
        if lhs_score is None or rhs_score is None:
            return False
        if float(lhs_score) < float(rhs_score):
            return False
        if float(lhs_score) > float(rhs_score):
            any_strict = True
    return any_strict


def _load_replay_risk_snapshot(
    vector: DecisionVectorV1,
    policy: PolicyDefinition,
    *,
    target_ledger: TargetLedger | None,
    replay_cache: dict[tuple[str, str], dict[str, object]],
    program_memory_dataset: object,
) -> dict[str, object]:
    if vector.entity_type != "gene":
        return {
            "status": NOT_APPLICABLE_STATUS,
            "summary": "Target-level replay risk is not applicable to module entities.",
            "proposal": {
                "entity_id": vector.entity_id,
                "target_symbol": vector.entity_label,
                "domain": policy.primary_domain_slug,
                "population": "",
                "mono_or_adjunct": "",
            },
            "supporting_reason_count": 0,
            "offsetting_reason_count": 0,
            "uncertainty_reason_count": 0,
            "uncertainty_flag_count": 0,
            "supporting_reasons": [],
            "offsetting_reasons": [],
            "uncertainty_reasons": [],
            "uncertainty_flags": [],
            "falsification_conditions": [],
        }

    cache_key = (vector.entity_id, policy.policy_id)
    cached = replay_cache.get(cache_key)
    if cached is not None:
        return cached

    population = ""
    mono_or_adjunct = ""
    if target_ledger is not None:
        if len(target_ledger.subgroup_domain_relevance["clinical_populations"]) == 1:
            population = str(
                target_ledger.subgroup_domain_relevance["clinical_populations"][0]
            )
        if len(target_ledger.subgroup_domain_relevance["mono_or_adjunct_contexts"]) == 1:
            mono_or_adjunct = str(
                target_ledger.subgroup_domain_relevance["mono_or_adjunct_contexts"][0]
            )

    proposal = InterventionProposal(
        entity_id=vector.entity_id,
        target_symbol=vector.entity_label,
        domain=policy.primary_domain_slug,
        population=population,
        mono_or_adjunct=mono_or_adjunct,
    )
    assessment = assess_counterfactual_replay_risk(
        program_memory_dataset,
        proposal,
    )
    snapshot = {
        "status": assessment.status,
        "summary": assessment.summary,
        "proposal": {
            "entity_id": proposal.entity_id,
            "target_symbol": proposal.target_symbol,
            "domain": proposal.domain,
            "population": proposal.population,
            "mono_or_adjunct": proposal.mono_or_adjunct,
        },
        "supporting_reason_count": len(assessment.supporting_reasons),
        "offsetting_reason_count": len(assessment.offsetting_reasons),
        "uncertainty_reason_count": len(assessment.uncertainty_reasons),
        "uncertainty_flag_count": len(assessment.uncertainty_flags),
        "supporting_reasons": [
            _serialize_replay_reason(
                relation="supports_replay",
                reason=reason,
            )
            for reason in assessment.supporting_reasons
        ],
        "offsetting_reasons": [
            _serialize_replay_reason(
                relation="argues_against_replay",
                reason=reason,
            )
            for reason in assessment.offsetting_reasons
        ],
        "uncertainty_reasons": [
            _serialize_replay_reason(
                relation="raises_uncertainty",
                reason=reason,
            )
            for reason in assessment.uncertainty_reasons
        ],
        "uncertainty_flags": [
            {
                "code": flag.code,
                "explanation": flag.explanation,
            }
            for flag in assessment.uncertainty_flags
        ],
        "falsification_conditions": list(assessment.falsification_conditions),
    }
    replay_cache[cache_key] = snapshot
    return snapshot


def _serialize_replay_reason(
    *,
    relation: str,
    reason: object,
) -> dict[str, object]:
    return {
        "relation": relation,
        "event_id": reason.event_id,
        "failure_scope": reason.failure_scope,
        "explanation": reason.explanation,
    }


def _replay_status_delta(policy: PolicyDefinition, replay_status: str) -> float:
    if replay_status == "replay_supported":
        return -policy.adjustment_weights.replay_supported_penalty
    if replay_status == "replay_inconclusive":
        return -policy.adjustment_weights.replay_inconclusive_penalty
    if replay_status == "replay_not_supported":
        return policy.adjustment_weights.replay_not_supported_bonus
    return 0.0


def _append_adjustment(
    adjustments: list[dict[str, object]],
    *,
    adjustment_id: str,
    label: str,
    delta: float,
    evidence: dict[str, object],
) -> None:
    rounded_delta = round(delta, 6)
    if rounded_delta == 0:
        return
    adjustments.append(
        {
            "adjustment_id": adjustment_id,
            "label": label,
            "delta": rounded_delta,
            "evidence": evidence,
        }
    )
