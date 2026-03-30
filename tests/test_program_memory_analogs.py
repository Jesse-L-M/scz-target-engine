import csv
from pathlib import Path

from scz_target_engine.program_memory import (
    InterventionProposal,
    assess_counterfactual_replay_risk,
    retrieve_program_memory_analogs,
)


ASSET_FIELDNAMES = [
    "asset_id",
    "molecule",
    "target",
    "target_symbols_json",
    "target_class",
    "mechanism",
    "modality",
]

EVENT_FIELDNAMES = [
    "event_id",
    "asset_id",
    "sponsor",
    "population",
    "domain",
    "mono_or_adjunct",
    "phase",
    "event_type",
    "event_date",
    "primary_outcome_result",
    "failure_reason_taxonomy",
    "confidence",
    "notes",
    "sort_order",
]

PROVENANCE_FIELDNAMES = [
    "event_id",
    "source_tier",
    "source_url",
]

HYPOTHESIS_FIELDNAMES = [
    "hypothesis_id",
    "entity_id",
    "entity_label",
    "desired_perturbation_direction",
    "modality_hypothesis",
    "preferred_modalities_json",
    "confidence",
    "ambiguity",
    "evidence_basis",
    "supporting_event_ids_json",
    "contradiction_conditions_json",
    "falsification_conditions_json",
    "open_risks_json",
    "sort_order",
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


def make_v2_dataset(
    tmp_path: Path,
    *,
    assets: list[dict[str, str]],
    events: list[dict[str, str]],
    provenance: list[dict[str, str]],
    hypotheses: list[dict[str, str]] | None = None,
) -> Path:
    dataset_dir = tmp_path / "v2"
    dataset_dir.mkdir()
    write_csv_rows(dataset_dir / "assets.csv", ASSET_FIELDNAMES, assets)
    write_csv_rows(dataset_dir / "events.csv", EVENT_FIELDNAMES, events)
    write_csv_rows(
        dataset_dir / "event_provenance.csv",
        PROVENANCE_FIELDNAMES,
        provenance,
    )
    write_csv_rows(
        dataset_dir / "directionality_hypotheses.csv",
        HYPOTHESIS_FIELDNAMES,
        hypotheses or [],
    )
    return dataset_dir


def test_retrieve_program_memory_analogs_exposes_provenance_and_context() -> None:
    result = retrieve_program_memory_analogs(
        Path("data/curated/program_history/v2"),
        InterventionProposal(
            target_symbol="CHRM4",
            domain="acute_positive_symptoms",
            mono_or_adjunct="monotherapy",
        ),
    )

    assert result.inferred_target_classes == ("muscarinic cholinergic modulation",)
    assert result.summary.matched_event_count == 2
    assert result.summary.failure_event_count == 1
    assert result.summary.nonfailure_event_count == 1
    assert [analog.event_id for analog in result.matched_analogs] == [
        "emraclidine-empower-acute-scz-topline-2024",
        "cobenfy-xanomeline-trospium-approval-us-2024",
    ]

    miss = result.matched_analogs[0]
    assert miss.record_ref.asset_id == "emraclidine"
    assert miss.record_ref.compatibility_program_id == miss.event_id
    assert miss.record_ref.source_tier == "company_press_release"
    assert miss.has_match("target_symbol", "exact_match")
    assert miss.has_match("target_class")
    assert miss.has_match("domain", "exact_match")
    assert miss.has_match("mono_or_adjunct", "exact_match")
    assert any(flag.code == "unresolved_failure_scope" for flag in miss.uncertainty_flags)

    anchor = result.matched_analogs[1]
    assert anchor.record_ref.asset_id == "xanomeline-trospium"
    assert any(flag.code == "composite_mechanism_analog" for flag in anchor.uncertainty_flags)
    assert any(flag.code == "mixed_history" for flag in result.uncertainty_flags)


def test_assess_counterfactual_replay_risk_explains_non_replay_case() -> None:
    assessment = assess_counterfactual_replay_risk(
        Path("data/curated/program_history/v2"),
        InterventionProposal(
            target_symbol="CHRM4",
            domain="acute_positive_symptoms",
            mono_or_adjunct="monotherapy",
        ),
    )

    assert assessment.status == "replay_not_supported"
    assert [reason.event_id for reason in assessment.offsetting_reasons] == [
        "cobenfy-xanomeline-trospium-approval-us-2024"
    ]
    assert [reason.event_id for reason in assessment.uncertainty_reasons] == [
        "emraclidine-empower-acute-scz-topline-2024"
    ]
    assert assessment.uncertainty_reasons[0].failure_scope == "unresolved"
    assert any(
        "selective CHRM4 program repeatedly fails" in condition
        for condition in assessment.falsification_conditions
    )


def test_limit_is_presentation_only_for_checked_in_chrm4_example() -> None:
    full = assess_counterfactual_replay_risk(
        Path("data/curated/program_history/v2"),
        InterventionProposal(
            target_symbol="CHRM4",
            domain="acute_positive_symptoms",
            mono_or_adjunct="monotherapy",
        ),
    )
    limited = assess_counterfactual_replay_risk(
        Path("data/curated/program_history/v2"),
        InterventionProposal(
            target_symbol="CHRM4",
            domain="acute_positive_symptoms",
            mono_or_adjunct="monotherapy",
        ),
        limit=1,
    )

    assert full.status == "replay_not_supported"
    assert limited.status == full.status
    assert limited.analog_search.summary.matched_event_count == 2
    assert len(limited.analog_search.matched_analogs) == 1
    assert len(limited.analog_search.all_matched_analogs) == 2
    assert [reason.event_id for reason in limited.offsetting_reasons] == [
        "cobenfy-xanomeline-trospium-approval-us-2024"
    ]


def test_assess_counterfactual_replay_risk_detects_molecule_only_class_replay(
    tmp_path: Path,
) -> None:
    dataset_dir = make_v2_dataset(
        tmp_path,
        assets=[
            {
                "asset_id": "asset-a",
                "molecule": "asset-a",
                "target": "TEST1",
                "target_symbols_json": '["TEST1"]',
                "target_class": "test class",
                "mechanism": "mechanism a",
                "modality": "small_molecule",
            }
        ],
        events=[
            {
                "event_id": "class-failure",
                "asset_id": "asset-a",
                "sponsor": "Sponsor A",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "event_date": "2024-01-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "target_class_failure",
                "confidence": "high",
                "notes": "class baggage",
                "sort_order": "1",
            }
        ],
        provenance=[
            {
                "event_id": "class-failure",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/class",
            }
        ],
    )

    assessment = assess_counterfactual_replay_risk(
        dataset_dir,
        InterventionProposal(
            molecule="asset-a",
            domain="acute_positive_symptoms",
            population="adults with schizophrenia",
            mono_or_adjunct="monotherapy",
        ),
    )

    assert assessment.status == "replay_supported"
    assert [reason.event_id for reason in assessment.supporting_reasons] == [
        "class-failure"
    ]
    assert assessment.supporting_reasons[0].failure_scope == "target_class"
    assert not assessment.offsetting_reasons


def test_assess_counterfactual_replay_risk_detects_molecule_only_target_replay(
    tmp_path: Path,
) -> None:
    dataset_dir = make_v2_dataset(
        tmp_path,
        assets=[
            {
                "asset_id": "asset-a",
                "molecule": "asset-a",
                "target": "TEST1",
                "target_symbols_json": '["TEST1"]',
                "target_class": "test class",
                "mechanism": "mechanism a",
                "modality": "small_molecule",
            }
        ],
        events=[
            {
                "event_id": "target-failure",
                "asset_id": "asset-a",
                "sponsor": "Sponsor A",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "event_date": "2024-01-01",
                "primary_outcome_result": "did_not_meet_primary_endpoint",
                "failure_reason_taxonomy": "probable_target_invalidity",
                "confidence": "high",
                "notes": "target baggage",
                "sort_order": "1",
            }
        ],
        provenance=[
            {
                "event_id": "target-failure",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/target",
            }
        ],
    )

    assessment = assess_counterfactual_replay_risk(
        dataset_dir,
        InterventionProposal(
            molecule="asset-a",
            domain="acute_positive_symptoms",
            population="adults with schizophrenia",
            mono_or_adjunct="monotherapy",
        ),
    )

    assert assessment.status == "replay_supported"
    assert [reason.event_id for reason in assessment.supporting_reasons] == [
        "target-failure"
    ]
    assert assessment.supporting_reasons[0].failure_scope == "target"
    assert not assessment.offsetting_reasons


def test_assess_counterfactual_replay_risk_flags_sparse_history() -> None:
    assessment = assess_counterfactual_replay_risk(
        Path("data/curated/program_history/v2"),
        InterventionProposal(
            target_symbol="SLC6A1",
            domain="acute_positive_symptoms",
        ),
    )

    assert assessment.status == "insufficient_history"
    assert assessment.analog_search.summary.matched_event_count == 0
    assert any(
        flag.code == "no_checked_in_analogs"
        for flag in assessment.uncertainty_flags
    )
