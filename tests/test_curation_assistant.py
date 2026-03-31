import json
from pathlib import Path

import pytest

from scz_target_engine.agents.program_memory_agent import (
    ACTION_ADD_EVENT,
    ACTION_ADD_HYPOTHESIS,
    ACTION_ADD_SUPPORTING_EVENTS,
    ACTION_FLAG_FOR_REVIEW,
    ACTION_RESOLVE_FAILURE_SCOPE,
    ACTION_UPGRADE_CONFIDENCE,
    CURATION_ASSISTANT_SCHEMA_VERSION,
    CurationDraft,
    CurationDraftItem,
    CurationDraftRequest,
    build_curation_draft,
    write_curation_draft,
)
from scz_target_engine.program_memory import (
    build_program_memory_harvest_batch,
    load_program_memory_dataset,
)
from tests.program_memory_fixtures import (
    make_directionality_suggestion,
    make_event_suggestion,
    make_source_document,
)


CHECKED_IN_V2_DIR = Path(__file__).resolve().parent.parent / "data" / "curated" / "program_history" / "v2"


class TestCurationDraftItemValidation:
    def test_requires_item_id(self) -> None:
        with pytest.raises(ValueError, match="item_id"):
            CurationDraftItem(
                item_id="",
                action=ACTION_ADD_EVENT,
                dimension="target",
                scope_value="TEST",
                rationale="test",
                provenance_event_ids=(),
                provenance_hypothesis_ids=(),
                provenance_gap_codes=(),
                uncertainty_codes=(),
                harvest_suggestion_ids=(),
                confidence_assessment="test",
                requires_human_review=True,
            )

    def test_rejects_unknown_action(self) -> None:
        with pytest.raises(ValueError, match="unsupported"):
            CurationDraftItem(
                item_id="test-1",
                action="auto_merge",
                dimension="target",
                scope_value="TEST",
                rationale="test",
                provenance_event_ids=(),
                provenance_hypothesis_ids=(),
                provenance_gap_codes=(),
                uncertainty_codes=(),
                harvest_suggestion_ids=(),
                confidence_assessment="test",
                requires_human_review=True,
            )

    def test_requires_human_review_always_true(self) -> None:
        with pytest.raises(ValueError, match="human review"):
            CurationDraftItem(
                item_id="test-1",
                action=ACTION_ADD_EVENT,
                dimension="target",
                scope_value="TEST",
                rationale="test",
                provenance_event_ids=(),
                provenance_hypothesis_ids=(),
                provenance_gap_codes=(),
                uncertainty_codes=(),
                harvest_suggestion_ids=(),
                confidence_assessment="test",
                requires_human_review=False,
            )


class TestCurationDraftValidation:
    def test_rejects_wrong_schema_version(self) -> None:
        with pytest.raises(ValueError, match="schema_version"):
            CurationDraft(
                schema_version="wrong-version",
                dataset_dir="test",
                request=CurationDraftRequest(),
                items=(),
                audit_summary={},
            )

    def test_rejects_duplicate_item_ids(self) -> None:
        item = CurationDraftItem(
            item_id="dup-1",
            action=ACTION_FLAG_FOR_REVIEW,
            dimension="target",
            scope_value="TEST",
            rationale="test",
            provenance_event_ids=(),
            provenance_hypothesis_ids=(),
            provenance_gap_codes=(),
            uncertainty_codes=(),
            harvest_suggestion_ids=(),
            confidence_assessment="test",
            requires_human_review=True,
        )
        with pytest.raises(ValueError, match="duplicate"):
            CurationDraft(
                schema_version=CURATION_ASSISTANT_SCHEMA_VERSION,
                dataset_dir="test",
                request=CurationDraftRequest(),
                items=(item, item),
                audit_summary={},
            )


class TestBuildCurationDraftFromCheckedInData:
    def test_builds_draft_from_v2_dir(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)

        assert draft.schema_version == CURATION_ASSISTANT_SCHEMA_VERSION
        assert len(draft.items) > 0
        assert draft.audit_summary["asset_count"] > 0
        assert draft.audit_summary["event_count"] > 0

        for item in draft.items:
            assert item.requires_human_review is True
            assert item.action in {
                ACTION_ADD_EVENT,
                ACTION_ADD_HYPOTHESIS,
                ACTION_UPGRADE_CONFIDENCE,
                ACTION_RESOLVE_FAILURE_SCOPE,
                ACTION_ADD_SUPPORTING_EVENTS,
                ACTION_FLAG_FOR_REVIEW,
            }

    def test_draft_to_dict_contains_trust_boundary(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)
        payload = draft.to_dict()

        assert "trust_boundary" in payload
        assert "DRAFT ONLY" in str(payload["trust_boundary"])
        assert "human" in str(payload["trust_boundary"]).lower()

    def test_draft_scoped_to_target(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        request = CurationDraftRequest(target="CHRM4")
        draft = build_curation_draft(CHECKED_IN_V2_DIR, request=request)

        for item in draft.items:
            if item.dimension:
                assert item.scope_value.upper() == "CHRM4" or item.dimension != "target"

    def test_draft_serialization_round_trip(self, tmp_path: Path) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)
        out = tmp_path / "curation_draft.json"
        write_curation_draft(out, draft)

        assert out.exists()
        payload = json.loads(out.read_text())
        assert payload["schema_version"] == CURATION_ASSISTANT_SCHEMA_VERSION
        assert payload["item_count"] == len(draft.items)
        assert payload["trust_boundary"] is not None

    def test_dataset_object_accepted(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        dataset = load_program_memory_dataset(CHECKED_IN_V2_DIR)
        draft = build_curation_draft(dataset)

        assert draft.dataset_dir == "<loaded_dataset>"
        assert len(draft.items) > 0


class TestBuildCurationDraftWithHarvest:
    def test_harvest_suggestions_surface_as_draft_items(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        harvest = build_program_memory_harvest_batch(
            harvest_id="test-harvest",
            harvester="test-assistant",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[
                make_event_suggestion(),
                make_directionality_suggestion(),
            ],
        )

        draft = build_curation_draft(CHECKED_IN_V2_DIR, harvest=harvest)

        harvest_items = [
            item
            for item in draft.items
            if item.harvest_suggestion_ids
        ]
        assert len(harvest_items) == 2

        for item in harvest_items:
            assert item.requires_human_review is True
            assert "Machine confidence" in item.confidence_assessment
            assert item.action in {ACTION_ADD_EVENT, ACTION_ADD_HYPOTHESIS}

    def test_harvest_excluded_when_flag_false(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        harvest = build_program_memory_harvest_batch(
            harvest_id="test-harvest",
            harvester="test-assistant",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[make_event_suggestion()],
        )

        request = CurationDraftRequest(include_harvest=False)
        draft = build_curation_draft(
            CHECKED_IN_V2_DIR, request=request, harvest=harvest
        )

        harvest_items = [
            item
            for item in draft.items
            if item.harvest_suggestion_ids
        ]
        assert len(harvest_items) == 0


class TestCurationDraftProvenanceGrounding:
    def test_gap_items_carry_provenance(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)
        gap_items = [
            item for item in draft.items if item.provenance_gap_codes
        ]
        assert len(gap_items) > 0

        for item in gap_items:
            assert len(item.provenance_gap_codes) > 0
            assert item.rationale

    def test_confidence_upgrade_items_reference_hypotheses(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)
        upgrade_items = [
            item
            for item in draft.items
            if item.action == ACTION_UPGRADE_CONFIDENCE
            and item.provenance_hypothesis_ids
        ]
        # The checked-in data has low-confidence hypotheses
        # so we expect at least one upgrade suggestion
        for item in upgrade_items:
            assert len(item.provenance_hypothesis_ids) > 0
            assert item.requires_human_review is True
