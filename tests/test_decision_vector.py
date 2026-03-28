import json

from scz_target_engine.decision_vector import (
    DOMAIN_HEAD_DEFINITIONS,
    MISSING_INPUTS_STATUS,
    build_decision_vector,
    build_decision_vector_payload,
    build_decision_vectors,
    rank_domain_head_rows,
    serialize_decision_vector,
)
from scz_target_engine.ledger import TargetLedger
from scz_target_engine.scoring import RankedEntity


def make_ranked_entity(
    *,
    entity_id: str,
    entity_label: str,
    layer_values: dict[str, float | None],
    metadata: dict[str, str],
    composite_score: float,
    rank: int,
) -> RankedEntity:
    return RankedEntity(
        entity_type="gene",
        entity_id=entity_id,
        entity_label=entity_label,
        composite_score=composite_score,
        eligible=True,
        rank=rank,
        decision_grade=True,
        sensitivity_survival_rate=1.0,
        layer_values=layer_values,
        warning_records=[],
        warnings=[],
        warning_count=0,
        warning_severity="none",
        metadata=metadata,
    )


def make_failure_event(
    *,
    failure_scope: str,
    evidence_strength: str,
    failure_reason_taxonomy: str,
) -> dict[str, object]:
    return {
        "failure_scope": failure_scope,
        "evidence_strength": evidence_strength,
        "failure_reason_taxonomy": failure_reason_taxonomy,
    }


def make_target_ledger(
    *,
    entity_id: str,
    entity_label: str,
    failure_events: list[dict[str, object]] | None = None,
    clinical_domains: list[str] | None = None,
    clinical_populations: list[str] | None = None,
    mono_or_adjunct_contexts: list[str] | None = None,
    psychencode_deg_top_cell_types: list[dict[str, object]] | None = None,
    psychencode_grn_top_cell_types: list[dict[str, object]] | None = None,
    directionality_status: str = "undetermined",
    directionality_confidence: str = "low",
    desired_perturbation_direction: str = "undetermined",
    modality_hypothesis: str = "undetermined",
    contradiction_conditions: list[str] | None = None,
    falsification_conditions: list[str] | None = None,
    directionality_open_risks: list[str] | None = None,
) -> TargetLedger:
    events = list(failure_events or [])
    failure_scopes = sorted(
        {
            str(event["failure_scope"])
            for event in events
            if event["failure_scope"] != "nonfailure"
        }
    )
    failure_taxonomies = sorted(
        {
            str(event["failure_reason_taxonomy"])
            for event in events
            if event["failure_reason_taxonomy"] != "not_applicable_nonfailure"
        }
    )
    return TargetLedger(
        entity_id=entity_id,
        entity_label=entity_label,
        v0_snapshot={},
        source_primitives={},
        subgroup_domain_relevance={
            "clinical_domains": list(clinical_domains or []),
            "clinical_populations": list(clinical_populations or []),
            "mono_or_adjunct_contexts": list(mono_or_adjunct_contexts or []),
            "psychencode_deg_top_cell_types": list(
                psychencode_deg_top_cell_types or []
            ),
            "psychencode_grn_top_cell_types": list(
                psychencode_grn_top_cell_types or []
            ),
        },
        structural_failure_history={
            "matched_event_count": len(events),
            "failure_event_count": sum(
                1 for event in events if event["failure_scope"] != "nonfailure"
            ),
            "nonfailure_event_count": sum(
                1 for event in events if event["failure_scope"] == "nonfailure"
            ),
            "event_count_by_scope": {},
            "failure_taxonomy_counts": {},
            "failure_scopes": failure_scopes,
            "failure_taxonomies": failure_taxonomies,
            "events": events,
        },
        directionality_hypothesis={
            "status": directionality_status,
            "desired_perturbation_direction": desired_perturbation_direction,
            "modality_hypothesis": modality_hypothesis,
            "preferred_modalities": [],
            "confidence": directionality_confidence,
            "ambiguity": "",
            "evidence_basis": "",
            "supporting_program_ids": [],
            "contradiction_conditions": list(contradiction_conditions or []),
            "falsification_conditions": list(falsification_conditions or []),
            "open_risks": list(directionality_open_risks or []),
        },
        falsification_conditions=list(falsification_conditions or []),
        open_risks=[],
    )


def test_build_decision_vector_marks_pr7_heads_missing_without_ledger() -> None:
    entity = make_ranked_entity(
        entity_id="ENSGHEAD0",
        entity_label="HEAD0",
        layer_values={
            "common_variant_support": 0.80,
            "rare_variant_support": 0.60,
            "cell_state_support": 0.90,
            "developmental_regulatory_support": 0.70,
            "tractability_compoundability": 0.50,
        },
        metadata={"generic_platform_baseline": "0.75"},
        composite_score=0.7,
        rank=2,
    )

    vector = build_decision_vector(entity)
    head_index = {score.head_name: score for score in vector.head_scores}

    assert head_index["failure_burden_score"].status == MISSING_INPUTS_STATUS
    assert head_index["directionality_confidence"].status == MISSING_INPUTS_STATUS
    assert head_index["subgroup_resolution_score"].status == MISSING_INPUTS_STATUS
    assert head_index["failure_burden_score"].missing_inputs == ("target_ledger",)


def test_build_decision_vector_computes_numeric_pr7_heads_from_ledger() -> None:
    entity = make_ranked_entity(
        entity_id="ENSGHEAD1",
        entity_label="HEAD1",
        layer_values={
            "common_variant_support": 0.80,
            "rare_variant_support": 0.60,
            "cell_state_support": 0.90,
            "developmental_regulatory_support": 0.70,
            "tractability_compoundability": 0.50,
        },
        metadata={"generic_platform_baseline": "0.75"},
        composite_score=0.7,
        rank=2,
    )
    ledger = make_target_ledger(
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
        psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
    )

    vector = build_decision_vector(entity, target_ledger=ledger)
    head_index = {score.head_name: score for score in vector.head_scores}

    assert vector.heuristic_score_v0 == 0.7
    assert head_index["human_support_score"].score == 0.7
    assert head_index["biology_context_score"].score == 0.8
    assert head_index["intervention_readiness_score"].score == 0.6
    assert head_index["failure_burden_score"].score == 1.0
    assert head_index["directionality_confidence"].score == 0.25
    assert head_index["subgroup_resolution_score"].score == 0.3
    assert head_index["failure_burden_score"].status == "available"
    assert head_index["directionality_confidence"].status == "available"
    assert head_index["subgroup_resolution_score"].status == "available"
    assert head_index["directionality_confidence"].used_inputs == (
        "directionality_hypothesis.status",
        "directionality_hypothesis.confidence",
        "directionality_hypothesis.desired_perturbation_direction",
        "directionality_hypothesis.modality_hypothesis",
        "directionality_hypothesis.contradiction_conditions",
        "directionality_hypothesis.falsification_conditions",
        "directionality_hypothesis.open_risks",
    )


def test_build_decision_vector_penalizes_failure_directionality_and_heterogeneity() -> None:
    entity = make_ranked_entity(
        entity_id="ENSGHEAD2",
        entity_label="HEAD2",
        layer_values={
            "common_variant_support": 0.50,
            "rare_variant_support": 0.50,
            "cell_state_support": 0.50,
            "developmental_regulatory_support": 0.50,
            "tractability_compoundability": 0.50,
        },
        metadata={"generic_platform_baseline": "0.50"},
        composite_score=0.5,
        rank=3,
    )
    ledger = make_target_ledger(
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        failure_events=[
            make_failure_event(
                failure_scope="target_class",
                evidence_strength="strong",
                failure_reason_taxonomy="target_class_failure",
            ),
            make_failure_event(
                failure_scope="molecule",
                evidence_strength="moderate",
                failure_reason_taxonomy="molecule_failure",
            ),
            make_failure_event(
                failure_scope="endpoint",
                evidence_strength="moderate",
                failure_reason_taxonomy="endpoint_mismatch",
            ),
            make_failure_event(
                failure_scope="population",
                evidence_strength="provisional",
                failure_reason_taxonomy="heterogeneity_or_subgroup_dilution",
            ),
            make_failure_event(
                failure_scope="unresolved",
                evidence_strength="moderate",
                failure_reason_taxonomy="unresolved",
            ),
        ],
        clinical_domains=["acute_positive_symptoms"],
        clinical_populations=[
            "adults with schizophrenia",
            "treatment-resistant adults",
        ],
        mono_or_adjunct_contexts=["monotherapy"],
        directionality_status="curated",
        directionality_confidence="medium",
        desired_perturbation_direction="increase_activity",
        modality_hypothesis="agonism",
        contradiction_conditions=["Repeated well-engaged failures."],
        falsification_conditions=["Aligned programs repeatedly fail."],
        directionality_open_risks=["Selectivity may not hold.", "Exposure may confound."],
    )

    vector = build_decision_vector(entity, target_ledger=ledger)
    head_index = {score.head_name: score for score in vector.head_scores}

    assert head_index["failure_burden_score"].score == 0.15
    assert head_index["directionality_confidence"].score == 0.5
    assert head_index["subgroup_resolution_score"].score == 0.2


def test_domain_head_rankings_consume_numeric_pr7_heads() -> None:
    supportive = make_ranked_entity(
        entity_id="ENSGGOOD",
        entity_label="GOOD",
        layer_values={
            "common_variant_support": 0.60,
            "rare_variant_support": 0.60,
            "cell_state_support": 0.60,
            "developmental_regulatory_support": 0.60,
            "tractability_compoundability": 0.60,
        },
        metadata={"generic_platform_baseline": "0.60"},
        composite_score=0.6,
        rank=1,
    )
    burdened = make_ranked_entity(
        entity_id="ENSGBAD",
        entity_label="BAD",
        layer_values=dict(supportive.layer_values),
        metadata=dict(supportive.metadata),
        composite_score=0.6,
        rank=2,
    )

    supportive_ledger = make_target_ledger(
        entity_id=supportive.entity_id,
        entity_label=supportive.entity_label,
        clinical_domains=["treatment_resistant_schizophrenia"],
        clinical_populations=["treatment-resistant adults"],
        mono_or_adjunct_contexts=["monotherapy"],
        psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
        psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
        directionality_status="curated",
        directionality_confidence="high",
        desired_perturbation_direction="decrease_activity",
        modality_hypothesis="antagonism",
        contradiction_conditions=["Class-level off-target risk remains."],
        falsification_conditions=["Aligned antagonists repeatedly fail."],
        directionality_open_risks=["Safety window may be narrow."],
    )
    burdened_ledger = make_target_ledger(
        entity_id=burdened.entity_id,
        entity_label=burdened.entity_label,
        failure_events=[
            make_failure_event(
                failure_scope="target_class",
                evidence_strength="strong",
                failure_reason_taxonomy="target_class_failure",
            ),
            make_failure_event(
                failure_scope="unresolved",
                evidence_strength="moderate",
                failure_reason_taxonomy="unresolved",
            ),
        ],
        directionality_status="undetermined",
        directionality_confidence="low",
    )

    rows = rank_domain_head_rows(
        build_decision_vectors(
            [supportive, burdened],
            ledger_index={
                supportive.entity_id: supportive_ledger,
                burdened.entity_id: burdened_ledger,
            },
        )
    )
    trs_rows = [
        row
        for row in rows
        if row["domain_slug"] == "treatment_resistant_schizophrenia"
    ]

    assert trs_rows[0]["entity_label"] == "GOOD"
    assert trs_rows[0]["domain_rank_v1"] == 1
    assert trs_rows[1]["entity_label"] == "BAD"
    assert trs_rows[1]["domain_rank_v1"] == 2
    assert trs_rows[0]["domain_head_score_v1"] > trs_rows[1]["domain_head_score_v1"]


def test_domain_head_rankings_separate_use_cases() -> None:
    acute_favored = make_ranked_entity(
        entity_id="ENSGACUTE",
        entity_label="ACUTE",
        layer_values={
            "common_variant_support": 0.95,
            "rare_variant_support": 0.90,
            "cell_state_support": 0.35,
            "developmental_regulatory_support": 0.25,
            "tractability_compoundability": 0.95,
        },
        metadata={"generic_platform_baseline": "0.90"},
        composite_score=0.68,
        rank=1,
    )
    biology_favored = make_ranked_entity(
        entity_id="ENSGBIO",
        entity_label="BIO",
        layer_values={
            "common_variant_support": 0.60,
            "rare_variant_support": 0.55,
            "cell_state_support": 0.95,
            "developmental_regulatory_support": 0.90,
            "tractability_compoundability": 0.20,
        },
        metadata={"generic_platform_baseline": "0.30"},
        composite_score=0.62,
        rank=2,
    )
    neutral_ledger = make_target_ledger(
        entity_id=acute_favored.entity_id,
        entity_label=acute_favored.entity_label,
        psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
        psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
    )

    rows = rank_domain_head_rows(
        build_decision_vectors(
            [acute_favored, biology_favored],
            ledger_index={
                acute_favored.entity_id: neutral_ledger,
                biology_favored.entity_id: make_target_ledger(
                    entity_id=biology_favored.entity_id,
                    entity_label=biology_favored.entity_label,
                    psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
                    psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
                ),
            },
        )
    )
    acute_rows = [row for row in rows if row["domain_slug"] == "acute_positive_symptoms"]
    negative_rows = [row for row in rows if row["domain_slug"] == "negative_symptoms"]

    assert acute_rows[0]["entity_label"] == "ACUTE"
    assert acute_rows[0]["domain_rank_v1"] == 1
    assert negative_rows[0]["entity_label"] == "BIO"
    assert negative_rows[0]["domain_rank_v1"] == 1


def test_decision_vector_payload_keeps_head_schema_and_domains() -> None:
    entity = make_ranked_entity(
        entity_id="ENSGPAYLOAD",
        entity_label="PAYLOAD",
        layer_values={
            "common_variant_support": 0.70,
            "rare_variant_support": 0.50,
            "cell_state_support": 0.80,
            "developmental_regulatory_support": 0.60,
            "tractability_compoundability": 0.40,
        },
        metadata={"generic_platform_baseline": "0.20"},
        composite_score=0.6,
        rank=3,
    )
    ledger = make_target_ledger(
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
        psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
    )

    payload = build_decision_vector_payload([build_decision_vector(entity, ledger)], [])

    assert payload["schema_version"] == "v1"
    assert len(payload["decision_head_definitions"]) == 6
    assert len(payload["domain_head_definitions"]) == len(DOMAIN_HEAD_DEFINITIONS)
    serialized = json.loads(json.dumps(payload))
    gene_entity = serialized["entities"]["gene"][0]
    assert gene_entity["human_support_score"] == 0.6
    assert gene_entity["human_support_score_status"] == "available"
    assert gene_entity["decision_vector"]["human_support_score"]["score"] == 0.6
    assert gene_entity["decision_vector"]["failure_burden_score"]["score"] == 1.0
    assert (
        gene_entity["domain_profiles"]["acute_positive_symptoms"]["label"]
        == "Acute positive symptoms"
    )
    assert gene_entity["head_scores"][0]["head_name"] == "human_support_score"
    assert gene_entity["domain_head_scores"][0]["domain_slug"] == "acute_positive_symptoms"


def test_serialize_decision_vector_exposes_named_head_fields() -> None:
    entity = make_ranked_entity(
        entity_id="ENSGSERIAL",
        entity_label="SERIAL",
        layer_values={
            "common_variant_support": 0.90,
            "rare_variant_support": 0.50,
            "cell_state_support": 0.80,
            "developmental_regulatory_support": 0.60,
            "tractability_compoundability": 0.30,
        },
        metadata={"generic_platform_baseline": "0.20"},
        composite_score=0.62,
        rank=5,
    )
    ledger = make_target_ledger(
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        psychencode_deg_top_cell_types=[{"cell_type": "Vip", "row_score": 0.2}],
        psychencode_grn_top_cell_types=[{"cell_type": "L2.3.IT", "score": 0.8}],
    )

    serialized = serialize_decision_vector(build_decision_vector(entity, ledger))

    assert serialized["human_support_score"] == 0.7
    assert serialized["biology_context_score"] == 0.7
    assert serialized["intervention_readiness_score"] == 0.26
    assert serialized["failure_burden_score"] == 1.0
    assert serialized["directionality_confidence"] == 0.25
    assert serialized["subgroup_resolution_score"] == 0.3
    assert serialized["decision_vector"]["failure_burden_score"]["status"] == "available"
    assert "negative_symptoms" in serialized["domain_profiles"]
