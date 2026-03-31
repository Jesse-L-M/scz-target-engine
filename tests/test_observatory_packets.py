"""Tests for the packet and rescue observatory explorer surfaces.

Verifies that browsing views work against checked-in hypothesis packet
artifacts, that rescue evidence views stay leakage-safe, and that
formatting functions produce correct output.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scz_target_engine.observatory.loaders import (
    DEFAULT_HYPOTHESIS_PACKETS_FILE,
    load_hypothesis_packets,
    load_rescue_augmented_packets,
    load_rescue_task_registry,
)
from scz_target_engine.observatory.packet_nav import (
    FailureAnalogView,
    PacketDetailView,
    PacketSummaryView,
    PolicyComparisonView,
    RescueEvidenceSummaryView,
    RescueTaskRegistryEntry,
    browse_failure_analog,
    browse_packet,
    browse_packet_by_entity,
    browse_policy_comparison,
    browse_rescue_evidence,
    list_failure_analogs,
    list_packets,
    list_rescue_evidence,
    list_rescue_tasks,
)
from scz_target_engine.observatory.shell import (
    format_failure_analogs,
    format_packet_detail,
    format_packet_list,
    format_policy_comparison,
    format_rescue_evidence_list,
    format_rescue_tasks,
)


# Fields that must NEVER appear in any rescue-related browse output.
_LEAKED_FIELDS = frozenset({
    "entity_evaluation_label",
    "entity_split",
    "label_rationale",
    "evaluation_label",
    "split_name",
    "gene_symbol",
})


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


def test_load_hypothesis_packets_default() -> None:
    payload = load_hypothesis_packets()
    assert payload is not None
    assert "packets" in payload
    assert payload["packet_count"] == 8


def test_load_hypothesis_packets_from_file() -> None:
    payload = load_hypothesis_packets(DEFAULT_HYPOTHESIS_PACKETS_FILE)
    assert payload is not None
    assert payload["packet_count"] == 8


def test_load_hypothesis_packets_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "nonexistent.json"
    assert load_hypothesis_packets(missing) is None


def test_load_rescue_augmented_packets_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "nonexistent.json"
    assert load_rescue_augmented_packets(missing) is None


def test_load_rescue_augmented_packets_plain_file_returns_none() -> None:
    # A plain hypothesis packets file should return None since it
    # doesn't have rescue_augmentation.
    result = load_rescue_augmented_packets(DEFAULT_HYPOTHESIS_PACKETS_FILE)
    assert result is None


def test_load_rescue_augmented_packets_no_default() -> None:
    assert load_rescue_augmented_packets() is None


def test_load_rescue_task_registry() -> None:
    rows = load_rescue_task_registry()
    assert len(rows) >= 1
    assert any(
        row.get("task_id") == "glutamatergic_convergence_rescue_task"
        for row in rows
    )


# ---------------------------------------------------------------------------
# Packet listing and browsing
# ---------------------------------------------------------------------------


def test_list_packets() -> None:
    packets = list_packets()
    assert len(packets) == 8
    assert all(isinstance(p, PacketSummaryView) for p in packets)
    chrm4_acute = next(
        p for p in packets
        if p.entity_label == "CHRM4"
        and p.policy_id == "acute_translation_guardrails_v1"
    )
    assert chrm4_acute.entity_type == "gene"
    assert chrm4_acute.policy_score is not None
    assert chrm4_acute.policy_status in ("available", "partial")


def test_list_packets_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "none.json"
    assert list_packets(packets_file=missing) == ()


def test_browse_packet() -> None:
    detail = browse_packet("ENSG00000180720__acute_translation_guardrails_v1")
    assert detail is not None
    assert isinstance(detail, PacketDetailView)
    assert detail.entity_label == "CHRM4"
    assert detail.policy_label != ""
    assert detail.traceability != {}
    assert detail.hypothesis.get("statement") is not None


def test_browse_packet_not_found() -> None:
    assert browse_packet("nonexistent_packet_id") is None


def test_browse_packet_by_entity() -> None:
    detail = browse_packet_by_entity(
        "CHRM4", "acute_translation_guardrails_v1"
    )
    assert detail is not None
    assert detail.entity_label == "CHRM4"
    assert detail.policy_id == "acute_translation_guardrails_v1"


def test_browse_packet_by_entity_not_found() -> None:
    assert browse_packet_by_entity("FAKE_GENE", "fake_policy") is None


# ---------------------------------------------------------------------------
# Failure analog browsing
# ---------------------------------------------------------------------------


def test_list_failure_analogs() -> None:
    analogs = list_failure_analogs()
    assert len(analogs) == 8
    assert all(isinstance(a, FailureAnalogView) for a in analogs)
    for analog in analogs:
        assert analog.replay_status != ""
        assert analog.escape_status != ""


def test_browse_failure_analog() -> None:
    analog = browse_failure_analog(
        "ENSG00000180720__acute_translation_guardrails_v1"
    )
    assert analog is not None
    assert analog.entity_label == "CHRM4"
    assert analog.replay_status != ""


def test_browse_failure_analog_not_found() -> None:
    assert browse_failure_analog("nonexistent_id") is None


# ---------------------------------------------------------------------------
# Policy comparison browsing
# ---------------------------------------------------------------------------


def test_browse_policy_comparison() -> None:
    comparison = browse_policy_comparison()
    assert comparison is not None
    assert isinstance(comparison, PolicyComparisonView)
    assert len(comparison.entity_ids) >= 2
    assert len(comparison.policy_ids) >= 2
    assert len(comparison.rows) == 8


def test_browse_policy_comparison_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "none.json"
    assert browse_policy_comparison(packets_file=missing) is None


# ---------------------------------------------------------------------------
# Rescue task browsing
# ---------------------------------------------------------------------------


def test_list_rescue_tasks() -> None:
    tasks = list_rescue_tasks()
    assert len(tasks) >= 1
    assert all(isinstance(t, RescueTaskRegistryEntry) for t in tasks)
    task_ids = {t.task_id for t in tasks}
    assert "glutamatergic_convergence_rescue_task" in task_ids


# ---------------------------------------------------------------------------
# Rescue evidence browsing (leakage-safe)
# ---------------------------------------------------------------------------


def test_list_rescue_evidence_no_augmented_file() -> None:
    # Without a rescue-augmented file, we get the plain packets
    # which have no rescue_evidence.
    evidence = list_rescue_evidence()
    assert evidence == ()


def test_list_rescue_evidence_with_augmented_packets(tmp_path: Path) -> None:
    """Synthetic rescue-augmented packets must produce leakage-safe evidence."""
    payload = load_hypothesis_packets()
    assert payload is not None

    # Add synthetic rescue_evidence to each packet
    for packet in payload["packets"]:
        packet["rescue_evidence"] = {
            "coverage_status": "rescue_task_match",
            "matched_tasks": [
                {
                    "task_id": "test_task",
                    "task_label": "Test task",
                    "entity_id": packet["entity_id"],
                    "entity_label": packet["entity_label"],
                    "entity_rescue_decision": "advance",
                    "baseline_performance": {"status": "no_baseline_data"},
                    "model_admission": {"status": "no_models_evaluated"},
                }
            ],
            "conflict_signals": [],
        }
        packet["first_assay"] = {
            "recommended_assay_class": "rescue_informed_advance",
            "rationale": "Test rationale.",
            "grounding": {},
            "next_steps": ["Test step."],
        }
    payload["rescue_augmentation"] = {
        "schema_version": "v1",
        "rescue_match_count": len(payload["packets"]),
        "rescue_unmatched_count": 0,
        "rescue_task_ids": ["test_task"],
        "source_artifacts": {},
    }
    augmented_path = tmp_path / "augmented.json"
    augmented_path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = list_rescue_evidence(packets_file=augmented_path)
    assert len(evidence) == 8
    for ev in evidence:
        assert isinstance(ev, RescueEvidenceSummaryView)
        assert ev.coverage_status == "rescue_task_match"
        assert ev.matched_task_count == 1
        assert ev.assay_class == "rescue_informed_advance"
        # Verify leakage safety
        for task in ev.matched_tasks:
            for leaked in _LEAKED_FIELDS:
                assert leaked not in task


def test_rescue_evidence_sanitizes_leaked_fields(tmp_path: Path) -> None:
    """Matched tasks with leaked fields must be stripped by the explorer."""
    payload = load_hypothesis_packets()
    assert payload is not None
    packet = payload["packets"][0]
    packet["rescue_evidence"] = {
        "coverage_status": "rescue_task_match",
        "matched_tasks": [
            {
                "task_id": "t",
                "task_label": "T",
                "entity_id": packet["entity_id"],
                "entity_label": packet["entity_label"],
                "entity_rescue_decision": "advance",
                "baseline_performance": {"status": "no_baseline_data"},
                "model_admission": {"status": "no_models_evaluated"},
                # These must be stripped:
                "evaluation_label": "1",
                "split_name": "test",
                "label_rationale": "Leaked!",
                "gene_symbol": "CHRM4",
            }
        ],
        "conflict_signals": [],
    }
    packet["first_assay"] = {
        "recommended_assay_class": "rescue_informed_advance",
        "rationale": "Test.",
        "grounding": {},
        "next_steps": [],
    }
    payload["rescue_augmentation"] = {
        "schema_version": "v1",
        "rescue_match_count": 1,
        "rescue_unmatched_count": 7,
        "rescue_task_ids": ["t"],
        "source_artifacts": {},
    }
    augmented_path = tmp_path / "augmented.json"
    augmented_path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = list_rescue_evidence(packets_file=augmented_path)
    assert len(evidence) >= 1
    for ev in evidence:
        for task in ev.matched_tasks:
            for leaked in _LEAKED_FIELDS:
                assert leaked not in task


# ---------------------------------------------------------------------------
# Formatting tests
# ---------------------------------------------------------------------------


def test_format_packet_list() -> None:
    packets = list_packets()
    output = format_packet_list(packets)
    assert "Hypothesis Packets" in output
    assert "CHRM4" in output
    assert "Total: 8 packets" in output


def test_format_packet_list_empty() -> None:
    output = format_packet_list(())
    assert "(none)" in output


def test_format_packet_detail() -> None:
    detail = browse_packet("ENSG00000180720__acute_translation_guardrails_v1")
    assert detail is not None
    output = format_packet_detail(detail)
    assert "CHRM4" in output
    assert "Decision Focus" in output
    assert "Hypothesis" in output
    assert "Policy Signal" in output
    assert "Evidence Anchors" in output
    assert "Risk Digest" in output
    assert "Contradiction Handling" in output
    assert "Failure Memory" in output
    assert "Failure Escape Logic" in output
    assert "Evidence Needed Next" in output
    assert "Traceability" in output


def test_format_failure_analogs() -> None:
    analogs = list_failure_analogs()
    output = format_failure_analogs(analogs)
    assert "Failure Analog" in output
    assert "CHRM4" in output
    assert "Replay status" in output


def test_format_failure_analogs_empty() -> None:
    output = format_failure_analogs(())
    assert "(none)" in output


def test_format_policy_comparison() -> None:
    comparison = browse_policy_comparison()
    assert comparison is not None
    output = format_policy_comparison(comparison)
    assert "Policy Comparison" in output
    assert "CHRM4" in output
    assert "Entities:" in output


def test_format_rescue_tasks() -> None:
    tasks = list_rescue_tasks()
    output = format_rescue_tasks(tasks)
    assert "Rescue Task Registry" in output
    assert "glutamatergic_convergence_rescue_task" in output


def test_format_rescue_tasks_empty() -> None:
    output = format_rescue_tasks(())
    assert "(none)" in output


def test_format_rescue_evidence_empty() -> None:
    output = format_rescue_evidence_list(())
    assert "no rescue evidence" in output


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


def test_observatory_packets_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "packets"])
    assert result == 0


def test_observatory_packet_detail_cli() -> None:
    from scz_target_engine.cli import main

    result = main([
        "observatory", "packet-detail",
        "ENSG00000180720__acute_translation_guardrails_v1",
    ])
    assert result == 0


def test_observatory_packet_detail_cli_not_found() -> None:
    from scz_target_engine.cli import main

    result = main([
        "observatory", "packet-detail",
        "nonexistent_packet_id",
    ])
    assert result == 1


def test_observatory_failure_analogs_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "failure-analogs"])
    assert result == 0


def test_observatory_policy_comparison_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "policy-comparison"])
    assert result == 0


def test_observatory_rescue_tasks_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "rescue-tasks"])
    assert result == 0


def test_observatory_rescue_evidence_cli() -> None:
    from scz_target_engine.cli import main

    result = main(["observatory", "rescue-evidence"])
    assert result == 0


# ---------------------------------------------------------------------------
# End-to-end browsing path against checked-in artifacts
# ---------------------------------------------------------------------------


def test_end_to_end_browsing_path() -> None:
    """Exercise a concrete browsing path through checked-in artifacts.

    This test simulates a user inspecting packets, reviewing failure
    analogs, comparing policies, and checking rescue task coverage.

    Steps:
    1. List all packets and verify structure
    2. Browse into CHRM4 acute packet for full detail
    3. Check failure analog for DRD2 (known contradicted entity)
    4. Browse policy comparison across all entities
    5. List rescue tasks
    6. Verify provenance/traceability is preserved throughout
    """
    # Step 1: List packets
    packets = list_packets()
    assert len(packets) == 8
    entity_labels = {p.entity_label for p in packets}
    assert "CHRM4" in entity_labels
    assert "DRD2" in entity_labels
    policy_ids = {p.policy_id for p in packets}
    assert len(policy_ids) == 2

    # Step 2: Browse CHRM4 acute detail
    chrm4_detail = browse_packet_by_entity(
        "CHRM4", "acute_translation_guardrails_v1"
    )
    assert chrm4_detail is not None
    assert chrm4_detail.entity_label == "CHRM4"
    assert chrm4_detail.hypothesis.get("statement") is not None
    assert chrm4_detail.policy_signal_summary["score"] is not None
    assert chrm4_detail.traceability.get("source_artifacts") is not None
    assert chrm4_detail.traceability.get("policy_entity_pointer") is not None
    assert chrm4_detail.traceability.get("ledger_target_pointer") is not None

    # Step 3: Failure analog for DRD2
    drd2_packets = [p for p in packets if p.entity_label == "DRD2"]
    assert len(drd2_packets) >= 1
    drd2_analog = browse_failure_analog(drd2_packets[0].packet_id)
    assert drd2_analog is not None
    assert drd2_analog.replay_status != ""
    assert drd2_analog.escape_status != ""

    # Step 4: Policy comparison
    comparison = browse_policy_comparison()
    assert comparison is not None
    assert len(comparison.rows) == 8
    drd2_rows = [r for r in comparison.rows if r.entity_label == "DRD2"]
    assert len(drd2_rows) == 2
    # DRD2 is known to be contradicted
    assert any(r.contradiction_status == "contradicted" for r in drd2_rows)

    # Step 5: Rescue tasks
    rescue_tasks = list_rescue_tasks()
    assert len(rescue_tasks) >= 1
    active_tasks = [t for t in rescue_tasks if t.registry_status == "active"]
    assert len(active_tasks) >= 1

    # Step 6: Verify provenance throughout
    all_analogs = list_failure_analogs()
    for analog in all_analogs:
        assert analog.packet_id != ""
        assert analog.entity_label != ""
        assert analog.policy_label != ""
