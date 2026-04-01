import json
from pathlib import Path
from shutil import copytree

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
    PROGRAM_MEMORY_ACCEPT_DECISION,
    apply_program_memory_adjudication,
    build_program_memory_adjudication_record,
    build_program_memory_harvest_batch,
    load_program_memory_dataset,
    migrate_legacy_program_memory_files,
    write_program_memory_adjudication_outputs,
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

    def test_proposed_v2_without_program_universe_still_builds_draft(
        self,
        tmp_path: Path,
    ) -> None:
        harvest = build_program_memory_harvest_batch(
            harvest_id="harvest-proposed-v2",
            harvester="llm-assist",
            created_at="2026-03-30",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[make_event_suggestion()],
        )
        adjudication = build_program_memory_adjudication_record(
            adjudication_id="review-proposed-v2",
            harvest_id=harvest.harvest_id,
            reviewer="curator@example.com",
            reviewed_at="2026-03-30",
            decision_payloads=[
                {
                    "suggestion_id": "emraclidine-event-suggestion",
                    "decision": PROGRAM_MEMORY_ACCEPT_DECISION,
                    "rationale": "Accept the event into the proposed v2 slice.",
                }
            ],
        )
        outcome = apply_program_memory_adjudication(harvest, adjudication)
        write_program_memory_adjudication_outputs(tmp_path, adjudication, outcome)

        draft = build_curation_draft(tmp_path / "proposed_v2")

        assert draft.dataset_dir.endswith("proposed_v2")
        assert draft.audit_summary["event_count"] == 1

    def test_checked_in_like_dataset_without_program_universe_fails(
        self,
        tmp_path: Path,
    ) -> None:
        dataset_dir = tmp_path / "v2"
        copytree(CHECKED_IN_V2_DIR, dataset_dir)
        (dataset_dir / "program_universe.csv").unlink()

        with pytest.raises(
            ValueError,
            match="program_universe.csv is required for denominator coverage-audit",
        ):
            build_curation_draft(dataset_dir)

    def test_loaded_checked_in_like_dataset_without_program_universe_also_fails(
        self,
        tmp_path: Path,
    ) -> None:
        dataset_dir = tmp_path / "v2"
        copytree(CHECKED_IN_V2_DIR, dataset_dir)
        (dataset_dir / "program_universe.csv").unlink()

        with pytest.raises(
            ValueError,
            match="program_universe.csv is required for denominator coverage-audit",
        ):
            load_program_memory_dataset(dataset_dir)

    def test_renamed_proposal_dataset_keeps_optional_denominator_contract(
        self,
        tmp_path: Path,
    ) -> None:
        harvest = build_program_memory_harvest_batch(
            harvest_id="harvest-renamed-proposal",
            harvester="llm-assist",
            created_at="2026-03-30",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[make_event_suggestion()],
        )
        adjudication = build_program_memory_adjudication_record(
            adjudication_id="review-renamed-proposal",
            harvest_id=harvest.harvest_id,
            reviewer="curator@example.com",
            reviewed_at="2026-03-30",
            decision_payloads=[
                {
                    "suggestion_id": "emraclidine-event-suggestion",
                    "decision": PROGRAM_MEMORY_ACCEPT_DECISION,
                    "rationale": "Accept the event into the proposal slice.",
                }
            ],
        )
        outcome = apply_program_memory_adjudication(harvest, adjudication)
        write_program_memory_adjudication_outputs(tmp_path, adjudication, outcome)

        renamed_dir = tmp_path / "review_copy"
        (tmp_path / "proposed_v2").rename(renamed_dir)

        draft_from_path = build_curation_draft(renamed_dir)
        draft_from_dataset = build_curation_draft(load_program_memory_dataset(renamed_dir))

        assert draft_from_path.audit_summary["event_count"] == 1
        assert draft_from_dataset.audit_summary["event_count"] == 1

    def test_migrated_legacy_dataset_still_builds_draft(self) -> None:
        dataset = migrate_legacy_program_memory_files(
            Path("data/curated/program_history/programs.csv"),
            Path("data/curated/program_history/directionality_hypotheses.csv"),
        )

        draft = build_curation_draft(dataset)

        assert dataset.requires_program_universe is False
        assert draft.audit_summary["event_count"] > 0

    def test_legacy_scope_only_v2_without_contract_still_builds_draft(
        self,
        tmp_path: Path,
    ) -> None:
        dataset_dir = tmp_path / "proposed_v2"
        dataset_dir.mkdir()
        for name, header in {
            "assets.csv": (
                "asset_id,molecule,target,target_symbols_json,target_class,"
                "mechanism,modality\n"
            ),
            "events.csv": (
                "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,"
                "event_type,event_date,primary_outcome_result,failure_reason_taxonomy,"
                "confidence,notes,sort_order\n"
            ),
            "event_provenance.csv": "event_id,source_tier,source_url\n",
            "directionality_hypotheses.csv": (
                "hypothesis_id,entity_id,entity_label,desired_perturbation_direction,"
                "modality_hypothesis,preferred_modalities_json,confidence,ambiguity,"
                "evidence_basis,supporting_event_ids_json,"
                "contradiction_conditions_json,falsification_conditions_json,"
                "open_risks_json,sort_order\n"
            ),
        }.items():
            (dataset_dir / name).write_text(header, encoding="utf-8")

        dataset = load_program_memory_dataset(dataset_dir)
        draft = build_curation_draft(dataset_dir)

        assert dataset.requires_program_universe is False
        assert draft.audit_summary["event_count"] == 0


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


class TestScopedDraftWithHarvest:
    """Verify that scoped drafts filter harvest items to the requested scope."""

    def test_scoped_draft_excludes_out_of_scope_harvest(self) -> None:
        """A CHRM4-scoped draft must not include a DRD2 harvest suggestion."""
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        drd2_source_doc = {
            "source_document_id": "drd2-source",
            "title": "DRD2 update",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/drd2",
            "publisher": "TestCo",
            "published_at": "2026-03-01",
            "evidence_excerpt": "DRD2 related evidence.",
            "notes": "",
        }
        drd2_event_suggestion = {
            "suggestion_id": "drd2-event-suggestion",
            "suggestion_kind": "event",
            "source_document_id": "drd2-source",
            "extractor_name": "test-extractor",
            "extractor_version": "1",
            "machine_confidence": "medium",
            "rationale": "DRD2 event.",
            "evidence_excerpt": "DRD2 evidence.",
            "asset": {
                "asset_id": "test-drd2-asset",
                "molecule": "test-drd2-molecule",
                "target": "DRD2",
                "target_class": "dopaminergic",
                "mechanism": "D2 antagonism",
                "modality": "small_molecule",
            },
            "event": {
                "event_id": "test-drd2-event",
                "asset_id": "test-drd2-asset",
                "sponsor": "TestCo",
                "population": "adults with schizophrenia",
                "domain": "acute_positive_symptoms",
                "mono_or_adjunct": "monotherapy",
                "phase": "phase_2",
                "event_type": "topline_readout",
                "event_date": "2026-03-01",
                "primary_outcome_result": "met_primary_endpoint",
                "failure_reason_taxonomy": "not_applicable_nonfailure",
                "confidence": "medium",
                "notes": "",
                "sort_order": 1,
            },
            "provenance": {
                "event_id": "test-drd2-event",
                "source_tier": "company_press_release",
                "source_url": "https://example.com/drd2",
            },
        }

        harvest = build_program_memory_harvest_batch(
            harvest_id="test-scoped-harvest",
            harvester="test",
            source_document_payloads=[
                make_source_document(),
                drd2_source_doc,
            ],
            suggestion_payloads=[
                make_event_suggestion(),       # targets CHRM4
                make_directionality_suggestion(),  # targets CHRM4
                drd2_event_suggestion,         # targets DRD2
            ],
        )

        request = CurationDraftRequest(target="CHRM4")
        draft = build_curation_draft(
            CHECKED_IN_V2_DIR, request=request, harvest=harvest
        )

        harvest_items = [
            item for item in draft.items if item.harvest_suggestion_ids
        ]
        # CHRM4 event + CHRM4 directionality should pass; DRD2 should not
        assert len(harvest_items) == 2
        for item in harvest_items:
            assert "drd2" not in item.harvest_suggestion_ids[0].lower()

    def test_scoped_audit_summary_is_scoped(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        request = CurationDraftRequest(target="CHRM4")
        draft = build_curation_draft(CHECKED_IN_V2_DIR, request=request)

        assert draft.audit_summary.get("scoped") is True
        # Scoped gap count should be less than full audit
        full_draft = build_curation_draft(CHECKED_IN_V2_DIR)
        assert draft.audit_summary["total_gap_count"] < full_draft.audit_summary["total_gap_count"]

    def test_unscoped_audit_summary_is_not_marked_scoped(self) -> None:
        if not CHECKED_IN_V2_DIR.exists():
            pytest.skip("checked-in v2 data not available")

        draft = build_curation_draft(CHECKED_IN_V2_DIR)
        assert "scoped" not in draft.audit_summary


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
