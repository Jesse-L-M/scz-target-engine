from scz_target_engine.scoring import (
    GENE_REQUIRED_GROUPS,
    EntityRecord,
    StabilityResult,
    annotate_ranked_entities,
    compute_weighted_score,
    run_stability_analysis,
)


def test_gene_requires_genetic_and_biological_support() -> None:
    record = EntityRecord(
        entity_type="gene",
        entity_id="ENSGTEST1",
        entity_label="TEST1",
        layer_values={
            "common_variant_support": 0.9,
            "rare_variant_support": None,
            "cell_state_support": None,
            "developmental_regulatory_support": None,
            "tractability_compoundability": 0.5,
        },
        metadata={},
    )
    score, eligible = compute_weighted_score(
        record.layer_values,
        {
            "common_variant_support": 0.2,
            "rare_variant_support": 0.2,
            "cell_state_support": 0.2,
            "developmental_regulatory_support": 0.2,
            "tractability_compoundability": 0.2,
        },
        GENE_REQUIRED_GROUPS,
    )
    assert score is None
    assert eligible is False


def test_stability_analysis_returns_survival_rates() -> None:
    records = [
        EntityRecord(
            entity_type="gene",
            entity_id=f"ENSGTEST{i}",
            entity_label=f"TEST{i}",
            layer_values={
                "common_variant_support": 0.9 - (i * 0.05),
                "rare_variant_support": 0.8 - (i * 0.03),
                "cell_state_support": 0.7 - (i * 0.02),
                "developmental_regulatory_support": 0.75 - (i * 0.01),
                "tractability_compoundability": 0.6,
            },
            metadata={},
        )
        for i in range(6)
    ]
    result = run_stability_analysis(
        records=records,
        layer_weights={
            "common_variant_support": 0.2,
            "rare_variant_support": 0.2,
            "cell_state_support": 0.2,
            "developmental_regulatory_support": 0.2,
            "tractability_compoundability": 0.2,
        },
        required_groups=GENE_REQUIRED_GROUPS,
        top_n=3,
        perturbation_fraction=0.2,
        decision_grade_threshold=0.7,
        top10_ejection_limit=0.5,
    )
    assert result.perturbation_overlaps
    assert result.survival_rates


def test_annotate_ranked_entities_adds_required_group_warning() -> None:
    entities = annotate_ranked_entities(
        ranked_rows=[
            {
                "entity_type": "gene",
                "entity_id": "ENSGTEST1",
                "entity_label": "TEST1",
                "layer_values": {
                    "common_variant_support": 0.9,
                    "rare_variant_support": None,
                    "cell_state_support": None,
                    "developmental_regulatory_support": None,
                    "tractability_compoundability": 0.5,
                },
                "metadata": {},
                "composite_score": None,
                "eligible": False,
                "rank": None,
            }
        ],
        warnings_index={},
        stability=StabilityResult(
            perturbation_overlaps=[],
            leave_one_out_overlaps={},
            leave_one_out_top10_ejections={},
            survival_rates={},
            pass_condition=False,
        ),
        decision_grade_threshold=0.7,
    )

    entity = entities[0]
    assert entity.warning_count == 1
    assert entity.warning_severity == "high"
    assert entity.warning_records[0].warning_kind == "required_layer_group_missing"
    assert "Missing required layer groups" in entity.warnings[0]
    assert "biological support" in entity.warnings[0]


def test_annotate_ranked_entities_adds_source_coverage_warning() -> None:
    entities = annotate_ranked_entities(
        ranked_rows=[
            {
                "entity_type": "gene",
                "entity_id": "ENSGTEST2",
                "entity_label": "TEST2",
                "layer_values": {
                    "common_variant_support": 0.71,
                    "rare_variant_support": 0.66,
                    "cell_state_support": 0.62,
                    "developmental_regulatory_support": 0.59,
                    "tractability_compoundability": 0.44,
                },
                "metadata": {
                    "generic_platform_baseline": "0.73",
                    "source_present_pgc": "False",
                    "source_present_schema": "True",
                    "source_present_psychencode": "False",
                    "source_present_chembl": "False",
                    "source_present_opentargets": "True",
                },
                "composite_score": 0.604,
                "eligible": True,
                "rank": 7,
            }
        ],
        warnings_index={},
        stability=StabilityResult(
            perturbation_overlaps=[],
            leave_one_out_overlaps={},
            leave_one_out_top10_ejections={},
            survival_rates={"ENSGTEST2": 1.0},
            pass_condition=False,
        ),
        decision_grade_threshold=0.7,
    )

    entity = entities[0]
    assert entity.warning_count == 1
    assert entity.warning_severity == "medium"
    assert entity.warning_records[0].warning_kind == "source_coverage_gap"
    assert "Missing source-backed biological support" in entity.warnings[0]
    assert "PGC common-variant support provenance" in entity.warnings[0]
