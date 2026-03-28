import json

from scz_target_engine.decision_vector import (
    DOMAIN_HEAD_DEFINITIONS,
    PR7_SUBSTRATE_STATUS,
    build_decision_vector,
    build_decision_vector_payload,
    build_decision_vectors,
    rank_domain_head_rows,
    serialize_decision_vector,
)
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


def test_build_decision_vector_computes_available_and_pending_heads() -> None:
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

    vector = build_decision_vector(entity)
    head_index = {score.head_name: score for score in vector.head_scores}

    assert vector.heuristic_score_v0 == 0.7
    assert head_index["human_support_score"].score == 0.7
    assert head_index["biology_context_score"].score == 0.8
    assert head_index["intervention_readiness_score"].score == 0.6
    assert head_index["failure_burden_score"].status == PR7_SUBSTRATE_STATUS
    assert head_index["directionality_confidence"].status == PR7_SUBSTRATE_STATUS
    assert head_index["subgroup_resolution_score"].status == PR7_SUBSTRATE_STATUS


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

    rows = rank_domain_head_rows(build_decision_vectors([acute_favored, biology_favored]))
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

    payload = build_decision_vector_payload([build_decision_vector(entity)], [])

    assert payload["schema_version"] == "v1"
    assert len(payload["decision_head_definitions"]) == 6
    assert len(payload["domain_head_definitions"]) == len(DOMAIN_HEAD_DEFINITIONS)
    serialized = json.loads(json.dumps(payload))
    gene_entity = serialized["entities"]["gene"][0]
    assert gene_entity["human_support_score"] == 0.6
    assert gene_entity["human_support_score_status"] == "available"
    assert gene_entity["decision_vector"]["human_support_score"]["score"] == 0.6
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

    serialized = serialize_decision_vector(build_decision_vector(entity))

    assert serialized["human_support_score"] == 0.7
    assert serialized["biology_context_score"] == 0.7
    assert serialized["intervention_readiness_score"] == 0.26
    assert serialized["decision_vector"]["failure_burden_score"]["status"] == PR7_SUBSTRATE_STATUS
    assert "negative_symptoms" in serialized["domain_profiles"]
