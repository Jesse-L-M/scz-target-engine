import pytest

from scz_target_engine.program_memory import (
    build_program_memory_harvest_batch,
    build_program_memory_harvest_review_rows,
    load_program_memory_harvest_batch,
    write_program_memory_harvest_batch,
)
from tests.program_memory_fixtures import (
    make_directionality_suggestion,
    make_event_suggestion,
    make_source_document,
)


def test_build_program_memory_harvest_batch_round_trips(tmp_path) -> None:
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

    assert harvest.harvest_id == "harvest-emraclidine"
    assert len(harvest.source_documents) == 1
    assert len(harvest.suggestions) == 2
    assert harvest.suggestions[0].asset is not None
    assert harvest.suggestions[0].asset.target_symbols == ("CHRM4",)
    assert harvest.suggestions[1].directionality_hypothesis is not None
    assert harvest.suggestions[1].candidate_identifier == "chrm4-candidate"

    harvest_path = tmp_path / "harvest.json"
    write_program_memory_harvest_batch(harvest_path, harvest)
    assert load_program_memory_harvest_batch(harvest_path) == harvest

    review_rows = build_program_memory_harvest_review_rows(harvest)
    assert review_rows == [
        {
            "suggestion_id": "emraclidine-event-suggestion",
            "suggestion_kind": "event",
            "candidate_identifier": "emraclidine-empower-acute-scz-topline-2024-candidate",
            "machine_confidence": "medium",
            "extractor_name": "llm-assisted-extractor",
            "source_document_id": "abbvie-emraclidine-2024-11-11",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/emraclidine",
            "needs_human_adjudication": "true",
            "proposed_record_type": "event",
        },
        {
            "suggestion_id": "chrm4-directionality-suggestion",
            "suggestion_kind": "directionality_hypothesis",
            "candidate_identifier": "chrm4-candidate",
            "machine_confidence": "low",
            "extractor_name": "llm-assisted-extractor",
            "source_document_id": "abbvie-emraclidine-2024-11-11",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/emraclidine",
            "needs_human_adjudication": "true",
            "proposed_record_type": "directionality_hypothesis",
        },
    ]


def test_build_program_memory_harvest_batch_rejects_unknown_source_document() -> None:
    with pytest.raises(ValueError, match="unknown source_document_id"):
        build_program_memory_harvest_batch(
            harvest_id="harvest-emraclidine",
            harvester="llm-assist",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[
                make_event_suggestion(source_document_id="missing-source"),
            ],
        )
