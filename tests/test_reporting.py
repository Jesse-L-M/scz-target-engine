from scz_target_engine.reporting import build_cards_markdown, ranked_entities_to_rows
from scz_target_engine.scoring import RankedEntity, WarningRecord


def test_build_cards_markdown_surfaces_decision_basis_and_source_coverage() -> None:
    entity = RankedEntity(
        entity_type="gene",
        entity_id="ENSGTEST1",
        entity_label="TEST1",
        composite_score=None,
        eligible=False,
        rank=None,
        decision_grade=False,
        sensitivity_survival_rate=0.0,
        layer_values={
            "common_variant_support": 0.88,
            "rare_variant_support": None,
            "cell_state_support": None,
            "developmental_regulatory_support": None,
            "tractability_compoundability": 0.42,
        },
        warning_records=[
            WarningRecord(
                severity="high",
                warning_kind="required_layer_group_missing",
                warning_text=(
                    "Missing required layer groups: biological support "
                    "(cell_state_support or developmental_regulatory_support)."
                ),
                source="auto",
            ),
            WarningRecord(
                severity="medium",
                warning_kind="source_coverage_gap",
                warning_text=(
                    "Missing source-backed biological support; matched SCHEMA rare-variant "
                    "support; missing PGC common-variant support provenance, "
                    "PsychENCODE biological support, and ChEMBL tractability support provenance."
                ),
                source="auto",
            ),
        ],
        warnings=[
            "[high] required_layer_group_missing: Missing required layer groups: biological "
            "support (cell_state_support or developmental_regulatory_support).",
            "[medium] source_coverage_gap: Missing source-backed biological support; matched "
            "SCHEMA rare-variant support; missing PGC common-variant support provenance, "
            "PsychENCODE biological support, and ChEMBL tractability support provenance.",
        ],
        warning_count=2,
        warning_severity="high",
        metadata={
            "source_present_pgc": "False",
            "source_present_schema": "True",
            "source_present_psychencode": "False",
            "source_present_chembl": "False",
        },
    )

    markdown = build_cards_markdown(
        "Kill Cards",
        [entity],
        limit=5,
        include_decision_grade=False,
        decision_grade_threshold=0.7,
    )

    assert "- Decision basis:" in markdown
    assert "Ineligible because required biological support is missing." in markdown
    assert "- Evidence coverage:" in markdown
    assert "- Required group status:" in markdown
    assert "- Source coverage:" in markdown
    assert "Matched 1/4 known source checks: SCHEMA rare-variant support" in markdown
    assert "- Warnings (2, highest high):" in markdown


def test_ranked_entities_to_rows_includes_warning_and_coverage_fields() -> None:
    entity = RankedEntity(
        entity_type="gene",
        entity_id="ENSGTEST2",
        entity_label="TEST2",
        composite_score=0.61,
        eligible=True,
        rank=4,
        decision_grade=True,
        sensitivity_survival_rate=1.0,
        layer_values={
            "common_variant_support": 0.7,
            "rare_variant_support": 0.6,
            "cell_state_support": 0.8,
            "developmental_regulatory_support": 0.75,
            "tractability_compoundability": None,
        },
        warning_records=[
            WarningRecord(
                severity="low",
                warning_kind="source_coverage_gap",
                warning_text="Source coverage is partial; matched PGC common-variant support.",
                source="auto",
            )
        ],
        warnings=[
            "[low] source_coverage_gap: Source coverage is partial; matched PGC common-variant support."
        ],
        warning_count=1,
        warning_severity="low",
        metadata={
            "source_present_pgc": "True",
            "source_present_schema": "False",
            "source_present_psychencode": "True",
            "source_present_chembl": "False",
            "source_present_opentargets": "False",
        },
    )

    rows = ranked_entities_to_rows([entity])

    assert rows[0]["present_layer_count"] == 4
    assert rows[0]["warning_count"] == 1
    assert rows[0]["warning_kinds"] == "source_coverage_gap"
    assert rows[0]["source_coverage_summary"].startswith("matched 2/5")
