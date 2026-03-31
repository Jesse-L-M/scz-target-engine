"""Tests for hypothesis co-scientist drafting agent.

Verifies that the agent produces grounded drafts tied to the shipped
packet contract, rescue augmentation, and prospective credibility
infrastructure.  Confirms trust boundary enforcement, explicit
contradiction/replay handling, and rescue-policy disagreement surfacing.
"""
import json
from pathlib import Path

import pytest

from scz_target_engine.agents.hypothesis_agent import (
    HYPOTHESIS_DRAFT_SCHEMA_VERSION,
    RESCUE_POLICY_ALIGNED,
    RESCUE_POLICY_CONFLICTED,
    RESCUE_POLICY_NO_DATA,
    HypothesisDraft,
    HypothesisDraftPayload,
    HypothesisDraftSection,
    build_hypothesis_draft,
    build_hypothesis_drafts,
    write_hypothesis_drafts,
)
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs
from scz_target_engine.hypothesis_lab.rescue_sections import (
    augment_packets_with_rescue,
)


def _build_hypothesis_packets(tmp_path: Path) -> dict[str, object]:
    config = load_config(Path("config/v0.toml"))
    build_outputs(config, Path("examples/v0/input").resolve(), tmp_path)
    return json.loads((tmp_path / "hypothesis_packets_v1.json").read_text())


def _find_packet(
    payload: dict[str, object], *, entity_label: str, policy_id: str
) -> dict[str, object]:
    return next(
        p
        for p in payload["packets"]
        if p["entity_label"] == entity_label and p["policy_id"] == policy_id
    )


# ---------------------------------------------------------------------------
# Single draft from plain packet
# ---------------------------------------------------------------------------


def test_draft_from_plain_packet(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    packet = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(packet)

    assert isinstance(draft, HypothesisDraft)
    assert draft.packet_id == "ENSG00000180720__acute_translation_guardrails_v1"
    assert draft.entity_label == "CHRM4"
    assert draft.policy_id == "acute_translation_guardrails_v1"
    assert draft.requires_human_review is True

    section_ids = {s.section_id for s in draft.sections}
    assert "hypothesis_summary" in section_ids
    assert "decision_focus" in section_ids
    assert "contradiction" in section_ids
    assert "replay_and_failure_memory" in section_ids
    assert "rescue_evidence" in section_ids
    assert "rescue_policy_alignment" in section_ids
    assert "credibility_grounding" in section_ids
    assert "kill_conditions" in section_ids
    assert "assay_recommendation" in section_ids
    assert "evidence_gaps" in section_ids


def test_draft_has_no_duplicate_section_ids(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    packet = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(packet)
    section_ids = [s.section_id for s in draft.sections]
    assert len(section_ids) == len(set(section_ids))


def test_draft_always_requires_human_review(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    for packet in payload["packets"]:
        draft = build_hypothesis_draft(packet)
        assert draft.requires_human_review is True


# ---------------------------------------------------------------------------
# Contradiction handling
# ---------------------------------------------------------------------------


def test_draft_surfaces_contradiction_status(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    drd2_acute = _find_packet(
        payload, entity_label="DRD2", policy_id="acute_translation_guardrails_v1"
    )
    assert drd2_acute["contradiction_handling"]["status"] == "contradicted"

    draft = build_hypothesis_draft(drd2_acute)
    assert draft.contradiction_status == "contradicted"

    contradiction_section = next(
        s for s in draft.sections if s.section_id == "contradiction"
    )
    assert any("contradicted" in line for line in contradiction_section.lines)


def test_draft_includes_contradiction_conditions(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_acute)
    contradiction_section = next(
        s for s in draft.sections if s.section_id == "contradiction"
    )
    assert any(
        "Contradiction condition:" in line
        for line in contradiction_section.lines
    )


# ---------------------------------------------------------------------------
# Replay / failure memory
# ---------------------------------------------------------------------------


def test_draft_surfaces_replay_status(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_acute)
    assert draft.replay_status == "replay_not_supported"

    replay_section = next(
        s for s in draft.sections
        if s.section_id == "replay_and_failure_memory"
    )
    assert any("replay_not_supported" in line for line in replay_section.lines)


def test_draft_includes_failure_history(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_acute)
    replay_section = next(
        s for s in draft.sections
        if s.section_id == "replay_and_failure_memory"
    )
    assert any("Structural history:" in line for line in replay_section.lines)
    assert any("Offsetting:" in line for line in replay_section.lines)


# ---------------------------------------------------------------------------
# Kill conditions
# ---------------------------------------------------------------------------


def test_draft_includes_kill_conditions(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_acute)
    kill_section = next(
        s for s in draft.sections if s.section_id == "kill_conditions"
    )
    assert any("Kill if:" in line for line in kill_section.lines)


# ---------------------------------------------------------------------------
# Rescue evidence and policy alignment
# ---------------------------------------------------------------------------


def test_draft_no_rescue_shows_no_data(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    packet = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(packet)
    assert draft.rescue_policy_alignment == RESCUE_POLICY_NO_DATA

    rescue_section = next(
        s for s in draft.sections if s.section_id == "rescue_evidence"
    )
    assert any("No rescue" in line for line in rescue_section.lines)


def test_draft_with_rescue_match_shows_aligned(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "CHRM4"
    )
    rescue_decisions = {
        chrm4_id: {
            "task_id": "test_task",
            "task_label": "Test rescue task",
            "entity_id": chrm4_id,
            "decision": "advance",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )
    # CHRM4 has contradiction status "contradicted", and rescue says "advance"
    # -> this creates a conflict signal
    chrm4_packet = _find_packet(
        augmented,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_packet)
    # Because CHRM4 has contradicted status + advance rescue -> conflict
    assert draft.rescue_policy_alignment == RESCUE_POLICY_CONFLICTED


def test_draft_with_rescue_conflict_surfaces_disagreement(
    tmp_path: Path,
) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "CHRM4"
    )
    rescue_decisions = {
        chrm4_id: {
            "task_id": "t",
            "task_label": "Rescue Task",
            "entity_id": chrm4_id,
            "decision": "deprioritize",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )
    chrm4_packet = _find_packet(
        augmented,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(chrm4_packet)
    assert draft.rescue_policy_alignment == RESCUE_POLICY_CONFLICTED

    alignment_section = next(
        s for s in draft.sections
        if s.section_id == "rescue_policy_alignment"
    )
    assert any("WARNING" in line for line in alignment_section.lines)
    assert any("disagree" in line for line in alignment_section.lines)


def test_draft_rescue_aligned_when_no_conflict(tmp_path: Path) -> None:
    """All example packets have contradicted status; a rescue 'hold'
    decision with contradicted status does NOT trigger a conflict signal
    per the rescue_sections contract, so the alignment should be 'aligned'."""
    payload = _build_hypothesis_packets(tmp_path)
    slc39a8_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "SLC39A8"
    )
    rescue_decisions = {
        slc39a8_id: {
            "task_id": "t",
            "task_label": "Rescue Task",
            "entity_id": slc39a8_id,
            "decision": "hold",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )
    augmented_packet = _find_packet(
        augmented,
        entity_label="SLC39A8",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(augmented_packet)
    assert draft.rescue_policy_alignment == RESCUE_POLICY_ALIGNED


# ---------------------------------------------------------------------------
# Prospective credibility grounding
# ---------------------------------------------------------------------------


def test_draft_with_prospective_registration(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )

    registration = json.loads(
        Path(
            "data/prospective_registry/registrations/"
            "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
        ).read_text(encoding="utf-8")
    )

    draft = build_hypothesis_draft(
        chrm4_acute,
        prospective_registrations=[registration],
    )

    credibility_section = next(
        s for s in draft.sections
        if s.section_id == "credibility_grounding"
    )
    assert any("forecast" in line.lower() for line in credibility_section.lines)
    assert any("advance" in line for line in credibility_section.lines)


def test_draft_without_prospective_shows_none(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    packet = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(packet)
    credibility_section = next(
        s for s in draft.sections
        if s.section_id == "credibility_grounding"
    )
    assert any(
        "No prospective forecast" in line
        for line in credibility_section.lines
    )


# ---------------------------------------------------------------------------
# Batch drafting
# ---------------------------------------------------------------------------


def test_build_hypothesis_drafts_all_packets(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    draft_payload = build_hypothesis_drafts(payload)

    assert isinstance(draft_payload, HypothesisDraftPayload)
    assert draft_payload.schema_version == HYPOTHESIS_DRAFT_SCHEMA_VERSION
    assert len(draft_payload.drafts) == payload["packet_count"]
    assert draft_payload.trust_boundary.startswith("DRAFT ONLY")

    # All drafts require human review
    for draft in draft_payload.drafts:
        assert draft.requires_human_review is True


def test_build_hypothesis_drafts_unique_draft_ids(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    draft_payload = build_hypothesis_drafts(payload)
    draft_ids = [d.draft_id for d in draft_payload.drafts]
    assert len(draft_ids) == len(set(draft_ids))


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def test_write_and_read_drafts(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    draft_payload = build_hypothesis_drafts(
        payload,
        source_artifacts={"hypothesis_packets_v1": "hypothesis_packets_v1.json"},
    )

    output_path = tmp_path / "hypothesis_drafts.json"
    write_hypothesis_drafts(output_path, draft_payload)

    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == HYPOTHESIS_DRAFT_SCHEMA_VERSION
    assert written["draft_count"] == payload["packet_count"]
    assert written["trust_boundary"].startswith("DRAFT ONLY")

    for draft in written["drafts"]:
        assert draft["requires_human_review"] is True
        assert len(draft["sections"]) == 10


def test_draft_to_dict_roundtrip(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    packet = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(packet)
    d = draft.to_dict()
    assert d["packet_id"] == draft.packet_id
    assert d["entity_label"] == draft.entity_label
    assert len(d["sections"]) == len(draft.sections)
    for section_dict in d["sections"]:
        assert "section_id" in section_dict
        assert "heading" in section_dict
        assert "lines" in section_dict
        assert "grounding_refs" in section_dict


# ---------------------------------------------------------------------------
# Trust boundary enforcement
# ---------------------------------------------------------------------------


def test_draft_cannot_skip_human_review() -> None:
    with pytest.raises(ValueError, match="human review"):
        HypothesisDraft(
            draft_id="test",
            packet_id="test",
            entity_id="ENSG00000000001",
            entity_label="TEST",
            policy_id="test_policy",
            policy_label="Test policy",
            priority_domain="test",
            sections=(),
            contradiction_status="clear",
            replay_status="insufficient_history",
            rescue_policy_alignment=RESCUE_POLICY_NO_DATA,
            requires_human_review=False,
        )


def test_payload_rejects_wrong_schema_version() -> None:
    with pytest.raises(ValueError, match="schema_version"):
        HypothesisDraftPayload(
            schema_version="wrong",
            drafts=(),
            source_artifacts={},
            trust_boundary="test",
        )


# ---------------------------------------------------------------------------
# Assay section
# ---------------------------------------------------------------------------


def test_draft_assay_from_rescue_augmented_packet(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    slc39a8_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "SLC39A8"
    )
    rescue_decisions = {
        slc39a8_id: {
            "task_id": "t",
            "task_label": "Rescue Task",
            "entity_id": slc39a8_id,
            "decision": "advance",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )
    augmented_packet = _find_packet(
        augmented,
        entity_label="SLC39A8",
        policy_id="acute_translation_guardrails_v1",
    )
    draft = build_hypothesis_draft(augmented_packet)
    assay_section = next(
        s for s in draft.sections if s.section_id == "assay_recommendation"
    )
    assert any("assay class" in line.lower() for line in assay_section.lines)


# ---------------------------------------------------------------------------
# Concrete example path from checked-in artifacts
# ---------------------------------------------------------------------------


def test_concrete_draft_path_from_checked_in_artifacts(tmp_path: Path) -> None:
    """End-to-end: build packets -> augment with rescue -> load prospective
    registrations -> draft -> verify structure."""
    # Step 1: Build hypothesis packets from shipped artifacts
    payload = _build_hypothesis_packets(tmp_path)
    assert payload["packet_count"] == 8

    # Step 2: Augment with synthetic rescue decisions for CHRM4
    chrm4_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "CHRM4"
    )
    rescue_decisions = {
        chrm4_id: {
            "task_id": "glut_rescue",
            "task_label": "Glutamatergic convergence rescue",
            "entity_id": chrm4_id,
            "decision": "advance",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )

    # Step 3: Load checked-in prospective registration
    registration_path = Path(
        "data/prospective_registry/registrations/"
        "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    registration = json.loads(
        registration_path.read_text(encoding="utf-8")
    )

    # Step 4: Build drafts
    draft_payload = build_hypothesis_drafts(
        augmented,
        prospective_registrations=[registration],
        source_artifacts={
            "hypothesis_packets_v1": "hypothesis_packets_v1.json",
            "prospective_registration": str(registration_path),
        },
    )

    assert len(draft_payload.drafts) == 8
    assert draft_payload.schema_version == HYPOTHESIS_DRAFT_SCHEMA_VERSION

    # Step 5: Verify CHRM4 acute draft has all grounding
    chrm4_draft = next(
        d for d in draft_payload.drafts
        if d.entity_label == "CHRM4"
        and d.policy_id == "acute_translation_guardrails_v1"
    )
    assert chrm4_draft.contradiction_status == "contradicted"
    assert chrm4_draft.replay_status == "replay_not_supported"
    # CHRM4 with contradicted + rescue advance = conflict
    assert chrm4_draft.rescue_policy_alignment == RESCUE_POLICY_CONFLICTED

    # Credibility grounding should reference the forecast
    credibility = next(
        s for s in chrm4_draft.sections
        if s.section_id == "credibility_grounding"
    )
    assert any("forecast" in line.lower() for line in credibility.lines)

    # Kill conditions should be populated
    kill = next(
        s for s in chrm4_draft.sections
        if s.section_id == "kill_conditions"
    )
    assert any("Kill if:" in line for line in kill.lines)

    # Rescue-policy alignment should surface the conflict
    alignment = next(
        s for s in chrm4_draft.sections
        if s.section_id == "rescue_policy_alignment"
    )
    assert any("WARNING" in line for line in alignment.lines)

    # Step 6: Verify non-CHRM4 packets have no rescue match
    slc6a1_draft = next(
        d for d in draft_payload.drafts
        if d.entity_label == "SLC6A1"
    )
    assert slc6a1_draft.rescue_policy_alignment == RESCUE_POLICY_NO_DATA

    # Step 7: Write and verify file
    output_path = tmp_path / "hypothesis_co_scientist_drafts.json"
    write_hypothesis_drafts(output_path, draft_payload)
    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["draft_count"] == 8
    assert written["trust_boundary"].startswith("DRAFT ONLY")

    # Every draft must require human review
    for draft_dict in written["drafts"]:
        assert draft_dict["requires_human_review"] is True
        # Every draft must have all 10 sections
        section_ids = {s["section_id"] for s in draft_dict["sections"]}
        assert "contradiction" in section_ids
        assert "replay_and_failure_memory" in section_ids
        assert "kill_conditions" in section_ids
        assert "rescue_evidence" in section_ids
        assert "assay_recommendation" in section_ids
        assert "credibility_grounding" in section_ids
        assert "rescue_policy_alignment" in section_ids
