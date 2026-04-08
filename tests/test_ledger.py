import csv
from pathlib import Path

from scz_target_engine.io import read_csv_rows
from scz_target_engine.ledger import (
    build_target_ledgers,
    target_ledgers_to_payload,
)
from scz_target_engine.program_memory import (
    load_program_history_compatibility_view,
    load_program_memory_dataset,
    materialize_legacy_directionality_hypothesis_rows,
    materialize_legacy_program_history_rows,
    migrate_legacy_program_memory,
)
from scz_target_engine.scoring import RankedEntity, WarningRecord


PROGRAM_HISTORY_FIELDNAMES = [
    "program_id",
    "sponsor",
    "molecule",
    "target",
    "target_class",
    "mechanism",
    "modality",
    "population",
    "domain",
    "mono_or_adjunct",
    "phase",
    "event_type",
    "date",
    "primary_outcome_result",
    "failure_reason_taxonomy",
    "source_tier",
    "source_url",
    "confidence",
    "notes",
]

HYPOTHESIS_FIELDNAMES = [
    "entity_id",
    "entity_label",
    "desired_perturbation_direction",
    "modality_hypothesis",
    "preferred_modalities_json",
    "confidence",
    "ambiguity",
    "evidence_basis",
    "supporting_program_ids_json",
    "contradiction_conditions_json",
    "falsification_conditions_json",
    "open_risks_json",
]


def write_csv_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_ranked_entity(
    *,
    entity_id: str = "ENSGTEST1",
    entity_label: str = "TEST1",
    warning_records: list[WarningRecord] | None = None,
) -> RankedEntity:
    warnings = warning_records or []
    return RankedEntity(
        entity_type="gene",
        entity_id=entity_id,
        entity_label=entity_label,
        composite_score=0.63,
        eligible=True,
        rank=3,
        decision_grade=True,
        sensitivity_survival_rate=0.9,
        layer_values={
            "common_variant_support": 0.71,
            "rare_variant_support": 0.52,
            "cell_state_support": 0.66,
            "developmental_regulatory_support": 0.61,
            "tractability_compoundability": 0.65,
        },
        warning_records=warnings,
        warnings=[],
        warning_count=len(warnings),
        warning_severity=(warnings[0].severity if warnings else "none"),
        metadata={
            "primary_gene_id": entity_id,
            "source_present_pgc": "True",
            "source_present_schema": "False",
            "source_present_psychencode": "True",
            "source_present_opentargets": "True",
            "source_present_chembl": "False",
            "psychencode_deg_top_cell_types_json": (
                '[{"cell_type": "Vip", "row_score": 0.2}]'
            ),
            "psychencode_grn_top_cell_types_json": (
                '[{"cell_type": "L2.3.IT", "score": 0.8}]'
            ),
            "pgc_scz2022_prioritised": "1",
            "schema_match_status": "matched",
        },
    )


def test_build_target_ledgers_maps_failure_taxonomy_distinctions(
    tmp_path: Path,
) -> None:
    programs_path = tmp_path / "programs.csv"
    hypotheses_path = tmp_path / "directionality_hypotheses.csv"
    write_csv_rows(
        programs_path,
        PROGRAM_HISTORY_FIELDNAMES,
        [
            {
                "program_id": "class-failure",
                "sponsor": "Sponsor A",
                "molecule": "asset-a",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism a",
                "modality": "small_molecule",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "date": "2024-01-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "target_class_failure",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/class",
                "confidence": "high",
                "notes": "class baggage",
            },
            {
                "program_id": "molecule-failure",
                "sponsor": "Sponsor B",
                "molecule": "asset-b",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism b",
                "modality": "small_molecule",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "adjunct",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "date": "2024-02-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "molecule_failure",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/molecule",
                "confidence": "medium",
                "notes": "asset issue",
            },
            {
                "program_id": "endpoint-failure",
                "sponsor": "Sponsor C",
                "molecule": "asset-c",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism c",
                "modality": "small_molecule",
                "population": "adults with schizophrenia",
                "domain": "cognition",
                "mono_or_adjunct": "adjunct",
                "phase": "phase_3",
                "event_type": "topline_readout",
                "date": "2024-03-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "endpoint_mismatch",
                "source_tier": "peer_reviewed_primary_results",
                "source_url": "https://example.com/endpoint",
                "confidence": "medium",
                "notes": "endpoint mismatch",
            },
            {
                "program_id": "population-failure",
                "sponsor": "Sponsor D",
                "molecule": "asset-d",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism d",
                "modality": "small_molecule",
                "population": "treatment-resistant adults",
                "domain": "treatment_resistant_schizophrenia",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "date": "2024-04-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "population_mismatch",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/population",
                "confidence": "low",
                "notes": "population mismatch",
            },
            {
                "program_id": "unresolved-failure",
                "sponsor": "Sponsor E",
                "molecule": "asset-e",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism e",
                "modality": "small_molecule",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "date": "2024-05-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "unresolved",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/unresolved",
                "confidence": "medium",
                "notes": "reason unresolved",
            },
        ],
    )
    write_csv_rows(hypotheses_path, HYPOTHESIS_FIELDNAMES, [])

    ledgers = build_target_ledgers(
        [make_ranked_entity()],
        program_history_path=programs_path,
        directionality_hypotheses_path=hypotheses_path,
    )

    ledger = ledgers[0]
    assert ledger.structural_failure_history["matched_event_count"] == 5
    assert ledger.structural_failure_history["failure_event_count"] == 5
    assert ledger.structural_failure_history["event_count_by_scope"] == {
        "endpoint": 1,
        "molecule": 1,
        "population": 1,
        "target_class": 1,
        "unresolved": 1,
    }
    events_by_scope = {
        event["failure_scope"]: event
        for event in ledger.structural_failure_history["events"]
    }
    assert events_by_scope["target_class"]["what_failed"] == "test class"
    assert events_by_scope["molecule"]["what_failed"] == "asset-b"
    assert events_by_scope["endpoint"]["what_failed"] == "did_not_meet_primary_endpoint"
    assert events_by_scope["population"]["what_failed"] == "treatment-resistant adults"
    assert events_by_scope["population"]["where"]["population"] == "treatment-resistant adults"
    assert events_by_scope["unresolved"]["what_failed"] == "undetermined"


def test_build_target_ledgers_includes_directionality_modality_and_open_risks(
    tmp_path: Path,
) -> None:
    programs_path = tmp_path / "programs.csv"
    hypotheses_path = tmp_path / "directionality_hypotheses.csv"
    write_csv_rows(
        programs_path,
        PROGRAM_HISTORY_FIELDNAMES,
        [
            {
                "program_id": "positive-anchor",
                "sponsor": "Sponsor",
                "molecule": "asset",
                "target": "TEST1",
                "target_class": "test class",
                "mechanism": "mechanism",
                "modality": "small_molecule",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "approved",
                "event_type": "regulatory_approval",
                "date": "2024-05-01",
                "primary_outcome_result": "approved",
                "failure_reason_taxonomy": "not_applicable_nonfailure",
                "source_tier": "regulatory",
                "source_url": "https://example.com/anchor",
                "confidence": "high",
                "notes": "positive anchor",
            }
        ],
    )
    write_csv_rows(
        hypotheses_path,
        HYPOTHESIS_FIELDNAMES,
        [
            {
                "entity_id": "ENSGTEST1",
                "entity_label": "TEST1",
                "desired_perturbation_direction": "increase_activity",
                "modality_hypothesis": "agonism_or_positive_allosteric_modulation",
                "preferred_modalities_json": '["small_molecule", "positive_allosteric_modulator"]',
                "confidence": "medium",
                "ambiguity": "Selective execution remains uncertain.",
                "evidence_basis": "Positive anchor plus mixed follow-up evidence.",
                "supporting_program_ids_json": '["positive-anchor"]',
                "contradiction_conditions_json": '["Repeated selective failures."]',
                "falsification_conditions_json": '["Adequately engaged programs repeatedly fail."]',
                "open_risks_json": '["Selectivity may lose the broader signal."]',
            }
        ],
    )

    ledgers = build_target_ledgers(
        [
            make_ranked_entity(
                warning_records=[
                    WarningRecord(
                        severity="medium",
                        warning_kind="direction_of_effect",
                        warning_text="Direction remains partially ambiguous.",
                        source="input",
                    )
                ]
            )
        ],
        program_history_path=programs_path,
        directionality_hypotheses_path=hypotheses_path,
    )

    ledger = ledgers[0]
    assert ledger.structural_failure_history["matched_event_count"] == 1
    assert ledger.structural_failure_history["failure_event_count"] == 0
    assert ledger.structural_failure_history["nonfailure_event_count"] == 1
    assert ledger.structural_failure_history["event_count_by_scope"] == {"nonfailure": 1}
    assert ledger.structural_failure_history["events"][0]["what_failed"] == (
        "not_applicable_nonfailure"
    )
    assert ledger.directionality_hypothesis["status"] == "curated"
    assert ledger.directionality_hypothesis["desired_perturbation_direction"] == (
        "increase_activity"
    )
    assert ledger.directionality_hypothesis["modality_hypothesis"] == (
        "agonism_or_positive_allosteric_modulation"
    )
    assert ledger.directionality_hypothesis["preferred_modalities"] == [
        "small_molecule",
        "positive_allosteric_modulator",
    ]
    assert ledger.directionality_hypothesis["contradiction_conditions"] == [
        "Repeated selective failures."
    ]
    assert ledger.falsification_conditions == [
        "Adequately engaged programs repeatedly fail."
    ]
    assert ledger.subgroup_domain_relevance["clinical_domains"] == [
        "acute_positive_symptoms"
    ]
    assert ledger.subgroup_domain_relevance["psychencode_deg_top_cell_types"] == [
        {"cell_type": "Vip", "row_score": 0.2}
    ]
    risk_sources = [risk["source"] for risk in ledger.open_risks]
    assert "warning_overlay" in risk_sources
    assert "directionality_hypothesis" in risk_sources


def test_target_ledgers_payload_reports_schema_and_counts(tmp_path: Path) -> None:
    programs_path = tmp_path / "programs.csv"
    hypotheses_path = tmp_path / "directionality_hypotheses.csv"
    write_csv_rows(programs_path, PROGRAM_HISTORY_FIELDNAMES, [])
    write_csv_rows(
        hypotheses_path,
        HYPOTHESIS_FIELDNAMES,
        [
            {
                "entity_id": "ENSGTEST1",
                "entity_label": "TEST1",
                "desired_perturbation_direction": "decrease_activity",
                "modality_hypothesis": "antagonism",
                "preferred_modalities_json": '["small_molecule"]',
                "confidence": "low",
                "ambiguity": "",
                "evidence_basis": "",
                "supporting_program_ids_json": "[]",
                "contradiction_conditions_json": "[]",
                "falsification_conditions_json": "[]",
                "open_risks_json": "[]",
            }
        ],
    )

    ledgers = build_target_ledgers(
        [make_ranked_entity()],
        program_history_path=programs_path,
        directionality_hypotheses_path=hypotheses_path,
    )
    payload = target_ledgers_to_payload(
        ledgers,
        program_history_path=programs_path,
        directionality_hypotheses_path=hypotheses_path,
        repo_root=tmp_path,
    )

    assert payload["schema_version"] == "pr7-target-ledger-v1"
    assert payload["scoring_neutral"] is True
    assert payload["data_sources"] == {
        "program_history": "programs.csv",
        "directionality_hypotheses": "directionality_hypotheses.csv",
    }
    assert payload["target_count"] == 1
    assert payload["targets_with_program_history"] == 0
    assert payload["targets_with_curated_directionality"] == 1
    assert payload["targets"][0]["entity_id"] == "ENSGTEST1"


def test_checked_in_v2_program_memory_projects_current_compatibility_rows() -> None:
    dataset = load_program_memory_dataset(Path("data/curated/program_history/v2"))

    assert materialize_legacy_program_history_rows(dataset) == read_csv_rows(
        Path("data/curated/program_history/programs.csv")
    )
    assert materialize_legacy_directionality_hypothesis_rows(dataset) == read_csv_rows(
        Path("data/curated/program_history/directionality_hypotheses.csv")
    )


def test_migrate_legacy_program_memory_round_trips_compatibility_rows() -> None:
    program_rows = [
        {
            "program_id": "legacy-positive",
            "sponsor": "Sponsor A",
            "molecule": "asset-a",
            "target": "TEST1 / TEST2",
            "target_class": "test class",
            "mechanism": "mechanism a",
            "modality": "small_molecule",
            "population": "adults with schizophrenia",
            "domain": "acute_positive_symptoms",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_2",
            "event_type": "topline_readout",
            "date": "2024-01-01",
            "primary_outcome_result": "met_primary_endpoint",
            "failure_reason_taxonomy": "not_applicable_nonfailure",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/positive",
            "confidence": "high",
            "notes": "positive anchor",
        },
        {
            "program_id": "legacy-negative",
            "sponsor": "Sponsor B",
            "molecule": "asset-a",
            "target": "TEST1 / TEST2",
            "target_class": "test class",
            "mechanism": "mechanism a",
            "modality": "small_molecule",
            "population": "treatment-resistant adults",
            "domain": "treatment_resistant_schizophrenia",
            "mono_or_adjunct": "adjunct",
            "phase": "phase_3",
            "event_type": "topline_readout",
            "date": "2024-02-01",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "failure_reason_taxonomy": "population_mismatch",
            "source_tier": "peer_reviewed_primary_results",
            "source_url": "https://example.com/negative",
            "confidence": "medium",
            "notes": "subgroup miss",
        },
    ]
    directionality_rows = [
        {
            "entity_id": "ENSGTEST1",
            "entity_label": "TEST1",
            "desired_perturbation_direction": "increase_activity",
            "modality_hypothesis": "agonism",
            "preferred_modalities_json": '["small_molecule"]',
            "confidence": "medium",
            "ambiguity": "Execution still uncertain.",
            "evidence_basis": "Legacy row basis.",
            "supporting_program_ids_json": '["legacy-positive", "legacy-negative"]',
            "contradiction_conditions_json": '["Repeated aligned failures."]',
            "falsification_conditions_json": '["Programs keep failing."]',
            "open_risks_json": '["Signal may be subgroup-limited."]',
        }
    ]

    dataset = migrate_legacy_program_memory(program_rows, directionality_rows)

    assert materialize_legacy_program_history_rows(dataset) == program_rows
    assert materialize_legacy_directionality_hypothesis_rows(dataset) == directionality_rows


def test_checked_in_ledger_loader_resolves_v2_compatibility_view() -> None:
    program_history = load_program_history_compatibility_view(
        Path("data/curated/program_history/programs.csv")
    )

    assert len(program_history) == 32
    assert any(
        event.program_id == "clozapine-clozaril-trs-approval-us-1989"
        for event in program_history
    )
    assert any(event.molecule == "xanomeline + trospium" for event in program_history)
