import json
from pathlib import Path

import pytest

from scz_target_engine.cli import main
from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.program_memory import (
    PROGRAM_MEMORY_ACCEPT_DECISION,
    PROGRAM_MEMORY_EDIT_DECISION,
    PROGRAM_MEMORY_REJECT_DECISION,
    ProgramMemoryDataset,
    apply_program_memory_adjudication,
    build_program_memory_adjudication_record,
    build_program_memory_coverage_audit,
    build_program_memory_harvest_batch,
    load_program_memory_dataset,
    materialize_adjudicated_program_memory_dataset,
    write_adjudicated_program_memory_dataset,
    write_program_memory_adjudication_outputs,
)
from tests.program_memory_fixtures import (
    make_directionality_suggestion,
    make_event_suggestion,
    make_source_document,
)


def test_machine_suggestions_do_not_bypass_adjudication() -> None:
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-emraclidine",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            make_event_suggestion(),
            make_directionality_suggestion(),
        ],
    )
    adjudication = build_program_memory_adjudication_record(
        adjudication_id="review-empty",
        harvest_id=harvest.harvest_id,
        reviewer="curator@example.com",
        reviewed_at="2026-03-30",
        decision_payloads=[],
    )

    outcome = apply_program_memory_adjudication(harvest, adjudication)
    dataset = materialize_adjudicated_program_memory_dataset(outcome)

    assert outcome.adjudicated_suggestions == ()
    assert outcome.rejected_suggestion_ids == ()
    assert outcome.pending_suggestion_ids == (
        "emraclidine-event-suggestion",
        "chrm4-directionality-suggestion",
    )
    assert dataset.assets == ()
    assert dataset.events == ()
    assert dataset.provenances == ()
    assert dataset.directionality_hypotheses == ()


def test_program_memory_adjudication_canonicalizes_alias_identity() -> None:
    adjudication = build_program_memory_adjudication_record(
        adjudication_id="review-karxt-alias",
        harvest_id="harvest-karxt",
        reviewer="curator@example.com",
        reviewed_at="2026-03-30",
        decision_payloads=[
            {
                "suggestion_id": "karxt-event-edit",
                "decision": PROGRAM_MEMORY_EDIT_DECISION,
                "rationale": "Normalize the alias to the checked-in canonical asset identity.",
                "asset": {
                    "asset_id": "karxt",
                    "molecule": "KarXT",
                    "target": "CHRM1 / CHRM4",
                    "target_symbols": ["CHRM1", "CHRM4"],
                    "target_class": "muscarinic receptor modulation",
                    "mechanism": (
                        "preferential M1 and M4 muscarinic agonism paired with "
                        "peripheral antimuscarinic blockade to improve tolerability"
                    ),
                    "modality": "small_molecule_combination",
                },
                "event": {
                    "event_id": "karxt-acute-scz-topline-2026-candidate",
                    "asset_id": "karxt",
                    "sponsor": "Bristol Myers Squibb",
                    "population": "adults with schizophrenia",
                    "domain": "acute_positive_symptoms",
                    "mono_or_adjunct": "monotherapy",
                    "phase": "phase_3",
                    "event_type": "topline_readout",
                    "event_date": "2026-03-30",
                    "primary_outcome_result": "did_not_meet_primary_endpoint",
                    "failure_reason_taxonomy": "unresolved",
                    "confidence": "medium",
                    "notes": "Alias-coded draft row.",
                    "sort_order": 1,
                },
                "provenance": {
                    "event_id": "karxt-acute-scz-topline-2026-candidate",
                    "source_tier": "company_press_release",
                    "source_url": "https://example.com/karxt",
                },
            }
        ],
    )

    decision = adjudication.decisions[0]
    assert decision.asset is not None
    assert decision.event is not None
    assert decision.asset.asset_id == "xanomeline-trospium"
    assert decision.asset.asset_lineage_id == "asset:xanomeline-trospium"
    assert decision.asset.target_class == "muscarinic cholinergic modulation"
    assert decision.event.asset_id == "xanomeline-trospium"


def test_write_adjudicated_program_memory_dataset_preserves_program_universe(
    tmp_path,
) -> None:
    dataset = load_program_memory_dataset(Path("data/curated/program_history/v2"))

    output_dir = tmp_path / "v2-copy"
    write_adjudicated_program_memory_dataset(output_dir, dataset)

    assert (output_dir / "program_universe.csv").exists()

    reloaded = load_program_memory_dataset(output_dir)
    assert reloaded.program_universe_rows == dataset.program_universe_rows

    audit = build_program_memory_coverage_audit(output_dir)
    assert audit.coverage_manifest["program_universe_row_count"] == len(
        dataset.program_universe_rows
    )


def test_write_adjudicated_program_memory_dataset_removes_stale_program_universe(
    tmp_path,
) -> None:
    source_dataset = load_program_memory_dataset(Path("data/curated/program_history/v2"))
    output_dir = tmp_path / "v2-copy"
    write_adjudicated_program_memory_dataset(output_dir, source_dataset)
    assert (output_dir / "program_universe.csv").exists()

    write_adjudicated_program_memory_dataset(
        output_dir,
        ProgramMemoryDataset(
            assets=(),
            events=(),
            provenances=(),
            directionality_hypotheses=(),
        ),
    )

    assert not (output_dir / "program_universe.csv").exists()


def test_write_adjudicated_program_memory_dataset_preserves_optional_contract(
    tmp_path,
) -> None:
    dataset = ProgramMemoryDataset(
        assets=(),
        events=(),
        provenances=(),
        directionality_hypotheses=(),
        requires_program_universe=False,
    )

    output_dir = tmp_path / "proposal"
    write_adjudicated_program_memory_dataset(output_dir, dataset)

    reloaded = load_program_memory_dataset(output_dir)
    assert reloaded.requires_program_universe is False


def test_write_adjudicated_program_memory_dataset_rejects_missing_required_denominator(
    tmp_path,
) -> None:
    output_dir = tmp_path / "checked-in-like"

    with pytest.raises(
        ValueError,
        match="program_universe.csv is required for denominator coverage-audit",
    ):
        write_adjudicated_program_memory_dataset(
            output_dir,
            ProgramMemoryDataset(
                assets=(),
                events=(),
                provenances=(),
                directionality_hypotheses=(),
                requires_program_universe=True,
            ),
        )

    assert not output_dir.exists()


def test_program_memory_adjudication_requires_explicit_accept_edit_or_reject(
    tmp_path,
) -> None:
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-assisted-curation",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[
            make_source_document(),
            {
                **make_source_document(
                    source_document_id="fda-cobenfy-2024-09-26",
                ),
                "title": "FDA approves xanomeline and trospium combination",
                "source_tier": "regulatory",
                "source_url": "https://example.com/cobenfy",
                "publisher": "FDA",
                "published_at": "2024-09-26",
            },
        ],
        suggestion_payloads=[
            {
                **make_event_suggestion(),
                "suggestion_id": "emraclidine-event-edit",
            },
            {
                **make_event_suggestion(
                    source_document_id="fda-cobenfy-2024-09-26",
                    suggestion_id="cobenfy-event-accept",
                ),
                "asset": {
                    "asset_id": "xanomeline-trospium",
                    "molecule": "xanomeline + trospium",
                    "target": "CHRM1 / CHRM4",
                    "target_symbols": ["CHRM1", "CHRM4"],
                    "target_class": "muscarinic cholinergic modulation",
                    "mechanism": (
                        "preferential M1 and M4 muscarinic agonism paired with "
                        "peripheral antimuscarinic blockade to improve tolerability"
                    ),
                    "modality": "small_molecule_combination",
                },
                "event": {
                    "event_id": "cobenfy-xanomeline-trospium-approval-us-2024-candidate",
                    "asset_id": "xanomeline-trospium",
                    "sponsor": "Bristol Myers Squibb",
                    "population": "adults with schizophrenia",
                    "domain": "acute_positive_symptoms",
                    "mono_or_adjunct": "monotherapy",
                    "phase": "approved",
                    "event_type": "regulatory_approval",
                    "event_date": "2024-09-26",
                    "primary_outcome_result": "approved_for_adults_with_schizophrenia",
                    "failure_reason_taxonomy": "not_applicable_nonfailure",
                    "confidence": "high",
                    "notes": "Machine suggestion pending curator review.",
                    "sort_order": 2,
                },
                "provenance": {
                    "event_id": "cobenfy-xanomeline-trospium-approval-us-2024-candidate",
                    "source_tier": "regulatory",
                    "source_url": "https://example.com/cobenfy",
                },
            },
            make_directionality_suggestion(
                suggestion_id="chrm4-directionality-reject",
            ),
        ],
    )
    adjudication = build_program_memory_adjudication_record(
        adjudication_id="review-assisted-curation",
        harvest_id=harvest.harvest_id,
        reviewer="curator@example.com",
        reviewed_at="2026-03-30",
        notes="Example assisted-curation review",
        decision_payloads=[
            {
                "suggestion_id": "emraclidine-event-edit",
                "decision": PROGRAM_MEMORY_EDIT_DECISION,
                "rationale": "Keep the miss but lower curator confidence until follow-up.",
                "asset": make_event_suggestion()["asset"],
                "event": {
                    **make_event_suggestion()["event"],
                    "event_id": "emraclidine-empower-acute-scz-topline-2024-candidate",
                    "confidence": "low",
                    "notes": (
                        "Human review kept the miss but downgraded confidence pending "
                        "broader historical curation."
                    ),
                },
                "provenance": make_event_suggestion()["provenance"],
            },
            {
                "suggestion_id": "cobenfy-event-accept",
                "decision": PROGRAM_MEMORY_ACCEPT_DECISION,
                "rationale": "Direct regulatory approval record is clean enough to accept.",
            },
            {
                "suggestion_id": "chrm4-directionality-reject",
                "decision": PROGRAM_MEMORY_REJECT_DECISION,
                "rationale": "Directionality still needs a broader curated evidence basis.",
            },
        ],
    )

    outcome = apply_program_memory_adjudication(harvest, adjudication)
    dataset = materialize_adjudicated_program_memory_dataset(outcome)
    written_dataset = write_program_memory_adjudication_outputs(
        tmp_path / "adjudicated",
        adjudication,
        outcome,
    )

    assert [item.suggestion_id for item in outcome.adjudicated_suggestions] == [
        "emraclidine-event-edit",
        "cobenfy-event-accept",
    ]
    assert outcome.rejected_suggestion_ids == ("chrm4-directionality-reject",)
    assert outcome.pending_suggestion_ids == ()
    assert len(dataset.assets) == 2
    assert len(dataset.events) == 2
    assert len(dataset.directionality_hypotheses) == 0
    assert dataset.events[0].confidence == "low"
    assert "downgraded confidence" in dataset.events[0].notes
    assert written_dataset == dataset

    summary = read_json(tmp_path / "adjudicated" / "adjudication_summary.json")
    assert summary["accepted_event_count"] == 2
    assert summary["accepted_directionality_count"] == 0
    assert summary["rejected_suggestion_ids"] == ["chrm4-directionality-reject"]
    assert summary["pending_suggestion_ids"] == []

    event_rows = read_csv_rows(tmp_path / "adjudicated" / "proposed_v2" / "events.csv")
    assert [row["event_id"] for row in event_rows] == [
        "emraclidine-empower-acute-scz-topline-2024-candidate",
        "cobenfy-xanomeline-trospium-approval-us-2024-candidate",
    ]
    assert event_rows[0]["confidence"] == "low"
    assert event_rows[1]["phase"] == "approved"
    hypothesis_rows = read_csv_rows(
        tmp_path / "adjudicated" / "proposed_v2" / "directionality_hypotheses.csv"
    )
    assert hypothesis_rows == []
    assert load_program_memory_dataset(
        tmp_path / "adjudicated" / "proposed_v2"
    ) == dataset


def test_program_memory_adjudication_rejects_hypotheses_with_missing_supporting_events(
    tmp_path,
) -> None:
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-missing-support",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            make_event_suggestion(),
            make_directionality_suggestion(),
        ],
    )
    adjudication = build_program_memory_adjudication_record(
        adjudication_id="review-missing-support",
        harvest_id=harvest.harvest_id,
        reviewer="curator@example.com",
        reviewed_at="2026-03-30",
        decision_payloads=[
            {
                "suggestion_id": "emraclidine-event-suggestion",
                "decision": PROGRAM_MEMORY_REJECT_DECISION,
                "rationale": "Event was not accepted into the adjudicated dataset.",
            },
            {
                "suggestion_id": "chrm4-directionality-suggestion",
                "decision": PROGRAM_MEMORY_ACCEPT_DECISION,
                "rationale": "Accepted by mistake, should fail before writeout.",
            },
        ],
    )

    outcome = apply_program_memory_adjudication(harvest, adjudication)

    with pytest.raises(
        ValueError,
        match="supporting_event_ids that were not accepted or edited",
    ):
        write_program_memory_adjudication_outputs(
            tmp_path / "invalid_adjudication",
            adjudication,
            outcome,
        )

    assert not (tmp_path / "invalid_adjudication").exists()


def test_program_memory_cli_example_curation_path(tmp_path) -> None:
    raw_input_path = tmp_path / "raw_harvest.json"
    raw_input_path.write_text(
        json.dumps(
            {
                "source_documents": [make_source_document()],
                "suggestions": [make_event_suggestion()],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    harvest_path = tmp_path / "harvest.json"
    review_path = tmp_path / "review_queue.csv"
    assert (
        main(
            [
                "program-memory",
                "harvest",
                "--input-file",
                str(raw_input_path),
                "--output-file",
                str(harvest_path),
                "--harvest-id",
                "example-curation",
                "--harvester",
                "llm-assist",
                "--created-at",
                "2026-03-30",
                "--review-file",
                str(review_path),
            ]
        )
        == 0
    )

    decisions_path = tmp_path / "decisions.json"
    decisions_path.write_text(
        json.dumps(
            {
                "notes": "Example curation path from raw suggestion to adjudicated proposal.",
                "decisions": [
                    {
                        "suggestion_id": "emraclidine-event-suggestion",
                        "decision": PROGRAM_MEMORY_EDIT_DECISION,
                        "rationale": "Curator accepted the event but lowered confidence.",
                        "asset": make_event_suggestion()["asset"],
                        "event": {
                            **make_event_suggestion()["event"],
                            "confidence": "low",
                            "notes": (
                                "Curator reviewed the press release and kept the event "
                                "provisional until broader context is added."
                            ),
                        },
                        "provenance": make_event_suggestion()["provenance"],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "cli_adjudicated"
    assert (
        main(
            [
                "program-memory",
                "adjudicate",
                "--harvest-file",
                str(harvest_path),
                "--decisions-file",
                str(decisions_path),
                "--output-dir",
                str(output_dir),
                "--adjudication-id",
                "example-curation-review",
                "--reviewer",
                "curator@example.com",
                "--reviewed-at",
                "2026-03-30",
            ]
        )
        == 0
    )

    review_rows = read_csv_rows(review_path)
    assert review_rows[0]["needs_human_adjudication"] == "true"

    event_rows = read_csv_rows(output_dir / "proposed_v2" / "events.csv")
    assert event_rows == [
        {
            "event_id": "emraclidine-empower-acute-scz-topline-2024-candidate",
            "asset_id": "emraclidine",
            "sponsor": "AbbVie",
            "population": "adults with schizophrenia during acute exacerbation of psychotic symptoms",
            "domain": "acute_positive_symptoms",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_2",
            "event_type": "topline_readout",
            "event_date": "2024-11-11",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "failure_reason_taxonomy": "unresolved",
            "confidence": "low",
            "notes": (
                "Curator reviewed the press release and kept the event provisional "
                "until broader context is added."
            ),
            "sort_order": "1",
        }
    ]

    summary = read_json(output_dir / "adjudication_summary.json")
    assert summary["accepted_event_count"] == 1
    assert summary["accepted_directionality_count"] == 0
    assert summary["pending_suggestion_ids"] == []
