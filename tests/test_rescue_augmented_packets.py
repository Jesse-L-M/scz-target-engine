"""Tests for rescue-augmented hypothesis packets.

Verifies that rescue evidence sections and first-assay logic are
correctly integrated into hypothesis packets without regressing the
post-review packet contract.
"""
import json
from pathlib import Path

import pytest

from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs
from scz_target_engine.hypothesis_lab.rescue_sections import (
    FIRST_ASSAY_CLASS_CONFLICT,
    FIRST_ASSAY_CLASS_NO_RESCUE,
    FIRST_ASSAY_CLASS_RESCUE_ADVANCE,
    FIRST_ASSAY_CLASS_RESCUE_DEPRIORITIZE,
    FIRST_ASSAY_CLASS_RESCUE_HOLD,
    RESCUE_AUGMENTED_PACKETS_SCHEMA_VERSION,
    RESCUE_COVERAGE_MATCH,
    RESCUE_COVERAGE_NONE,
    augment_packets_with_rescue,
    build_first_assay_section,
    build_rescue_entity_labels,
    build_rescue_evidence_section,
    materialize_rescue_augmented_packets,
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
# Rescue entity labels
# ---------------------------------------------------------------------------


def test_build_rescue_entity_labels_from_evaluation_rows() -> None:
    rows = [
        {
            "gene_id": "ENSG00000183454",
            "gene_symbol": "GRIN2A",
            "evaluation_label": "1",
            "decision": "advance",
            "split_name": "train",
            "label_rationale": "Cross-source convergence intact.",
        },
        {
            "gene_id": "ENSG00000168959",
            "gene_symbol": "GRM5",
            "evaluation_label": "0",
            "decision": "deprioritize",
            "split_name": "train",
            "label_rationale": "Single-source, mixed missingness.",
        },
    ]
    labels = build_rescue_entity_labels(
        rows,
        task_id="glutamatergic_convergence_rescue_task",
        task_label="Glutamatergic convergence rescue task",
    )
    assert "ENSG00000183454" in labels
    assert labels["ENSG00000183454"]["decision"] == "advance"
    assert labels["ENSG00000183454"]["gene_symbol"] == "GRIN2A"
    assert "ENSG00000168959" in labels
    assert labels["ENSG00000168959"]["decision"] == "deprioritize"


def test_build_rescue_entity_labels_skips_empty_ids() -> None:
    rows = [{"gene_id": "", "gene_symbol": "", "decision": "hold"}]
    labels = build_rescue_entity_labels(
        rows,
        task_id="test_task",
        task_label="Test task",
    )
    assert labels == {}


# ---------------------------------------------------------------------------
# Rescue evidence section
# ---------------------------------------------------------------------------


def test_rescue_evidence_section_no_match(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    evidence = build_rescue_evidence_section(
        chrm4_acute,
        rescue_entity_labels={},
        baseline_comparison_summaries=[],
    )
    assert evidence["coverage_status"] == RESCUE_COVERAGE_NONE
    assert evidence["matched_tasks"] == []
    assert evidence["conflict_signals"] == []


def test_rescue_evidence_section_with_match(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_labels = {
        chrm4_acute["entity_id"]: {
            "task_id": "test_rescue_task",
            "task_label": "Test rescue task",
            "entity_id": chrm4_acute["entity_id"],
            "gene_symbol": "CHRM4",
            "decision": "advance",
            "evaluation_label": "1",
            "split_name": "test",
            "label_rationale": "Convergence support intact.",
        }
    }
    evidence = build_rescue_evidence_section(
        chrm4_acute,
        rescue_entity_labels=rescue_labels,
        baseline_comparison_summaries=[],
    )
    assert evidence["coverage_status"] == RESCUE_COVERAGE_MATCH
    assert len(evidence["matched_tasks"]) == 1
    matched = evidence["matched_tasks"][0]
    assert matched["task_id"] == "test_rescue_task"
    assert matched["entity_rescue_decision"] == "advance"
    assert matched["entity_split"] == "test"
    assert matched["label_rationale"] == "Convergence support intact."
    assert matched["baseline_performance"]["status"] == "no_baseline_data"
    assert matched["model_admission"]["status"] == "no_models_evaluated"


def test_rescue_evidence_section_with_baseline_summary(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    entity_id = chrm4_acute["entity_id"]
    rescue_labels = {
        entity_id: {
            "task_id": "glut_task",
            "task_label": "Glut task",
            "entity_id": entity_id,
            "gene_symbol": "CHRM4",
            "decision": "advance",
            "evaluation_label": "1",
            "split_name": "test",
            "label_rationale": "Good convergence.",
        }
    }
    baseline_summary = {
        "task_id": "glut_task",
        "task_label": "Glut task",
        "principal_split": "test",
        "metric_names": ["average_precision", "mean_reciprocal_rank"],
        "best_by_split": {
            "test": {
                "average_precision": {
                    "scorer_id": "convergence_state",
                    "scorer_label": "Convergence state",
                    "scorer_role": "baseline",
                    "metric_value": 0.75,
                },
                "mean_reciprocal_rank": {
                    "scorer_id": "axis_support",
                    "scorer_label": "Axis support",
                    "scorer_role": "baseline",
                    "metric_value": 1.0,
                },
            }
        },
    }
    evidence = build_rescue_evidence_section(
        chrm4_acute,
        rescue_entity_labels=rescue_labels,
        baseline_comparison_summaries=[baseline_summary],
    )
    matched = evidence["matched_tasks"][0]
    assert matched["baseline_performance"]["status"] == "baselines_available"
    assert matched["baseline_performance"]["principal_split"] == "test"
    best = matched["baseline_performance"]["best_by_metric"]
    assert best["average_precision"]["scorer_id"] == "convergence_state"
    assert best["average_precision"]["value"] == 0.75


# ---------------------------------------------------------------------------
# Conflict signals
# ---------------------------------------------------------------------------


def test_conflict_signal_deprioritize_vs_hypothesis(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_labels = {
        chrm4_acute["entity_id"]: {
            "task_id": "t",
            "task_label": "Rescue Task",
            "entity_id": chrm4_acute["entity_id"],
            "gene_symbol": "CHRM4",
            "decision": "deprioritize",
            "evaluation_label": "0",
            "split_name": "test",
            "label_rationale": "Single-source.",
        }
    }
    evidence = build_rescue_evidence_section(
        chrm4_acute,
        rescue_entity_labels=rescue_labels,
        baseline_comparison_summaries=[],
    )
    assert len(evidence["conflict_signals"]) >= 1
    assert "deprioritize" in evidence["conflict_signals"][0]


def test_conflict_signal_advance_vs_contradicted(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    drd2_acute = _find_packet(
        payload, entity_label="DRD2", policy_id="acute_translation_guardrails_v1"
    )
    assert drd2_acute["contradiction_handling"]["status"] == "contradicted"
    rescue_labels = {
        drd2_acute["entity_id"]: {
            "task_id": "t",
            "task_label": "Rescue Task",
            "entity_id": drd2_acute["entity_id"],
            "gene_symbol": "DRD2",
            "decision": "advance",
            "evaluation_label": "1",
            "split_name": "test",
            "label_rationale": "Converged.",
        }
    }
    evidence = build_rescue_evidence_section(
        drd2_acute,
        rescue_entity_labels=rescue_labels,
        baseline_comparison_summaries=[],
    )
    conflict_texts = " ".join(evidence["conflict_signals"])
    assert "contradicted" in conflict_texts


# ---------------------------------------------------------------------------
# First-assay logic
# ---------------------------------------------------------------------------


def test_first_assay_no_rescue(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_evidence = {
        "coverage_status": RESCUE_COVERAGE_NONE,
        "matched_tasks": [],
        "conflict_signals": [],
    }
    first_assay = build_first_assay_section(
        chrm4_acute, rescue_evidence=rescue_evidence
    )
    assert first_assay["recommended_assay_class"] == FIRST_ASSAY_CLASS_NO_RESCUE
    assert first_assay["grounding"]["rescue_coverage"] is False
    assert first_assay["grounding"]["rescue_label"] == ""
    assert len(first_assay["next_steps"]) >= 1


def test_first_assay_rescue_advance(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_evidence = {
        "coverage_status": RESCUE_COVERAGE_MATCH,
        "matched_tasks": [
            {
                "task_id": "t",
                "task_label": "Test task",
                "entity_rescue_decision": "advance",
                "label_rationale": "Good.",
            }
        ],
        "conflict_signals": [],
    }
    first_assay = build_first_assay_section(
        chrm4_acute, rescue_evidence=rescue_evidence
    )
    assert first_assay["recommended_assay_class"] == FIRST_ASSAY_CLASS_RESCUE_ADVANCE
    assert first_assay["grounding"]["rescue_label"] == "advance"
    assert "Design directional assay" in first_assay["next_steps"][-1]


def test_first_assay_rescue_hold(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_evidence = {
        "coverage_status": RESCUE_COVERAGE_MATCH,
        "matched_tasks": [
            {
                "task_id": "t",
                "task_label": "Test task",
                "entity_rescue_decision": "hold",
                "label_rationale": "Needs more data.",
            }
        ],
        "conflict_signals": [],
    }
    first_assay = build_first_assay_section(
        chrm4_acute, rescue_evidence=rescue_evidence
    )
    assert first_assay["recommended_assay_class"] == FIRST_ASSAY_CLASS_RESCUE_HOLD


def test_first_assay_conflict(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_acute = _find_packet(
        payload, entity_label="CHRM4", policy_id="acute_translation_guardrails_v1"
    )
    rescue_evidence = {
        "coverage_status": RESCUE_COVERAGE_MATCH,
        "matched_tasks": [
            {
                "task_id": "t",
                "task_label": "Test task",
                "entity_rescue_decision": "deprioritize",
                "label_rationale": "Single-source.",
            }
        ],
        "conflict_signals": ["Rescue deprioritizes but hypothesis proposes."],
    }
    first_assay = build_first_assay_section(
        chrm4_acute, rescue_evidence=rescue_evidence
    )
    assert first_assay["recommended_assay_class"] == FIRST_ASSAY_CLASS_CONFLICT
    assert "Reconcile" in first_assay["next_steps"][-1]


# ---------------------------------------------------------------------------
# Full augmentation pipeline
# ---------------------------------------------------------------------------


def test_augment_packets_preserves_post_review_contract(tmp_path: Path) -> None:
    """Rescue augmentation must not remove or alter any post-review fields."""
    payload = _build_hypothesis_packets(tmp_path)
    original_packets = json.loads(json.dumps(payload["packets"]))

    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_labels={},
        baseline_comparison_summaries=[],
    )

    for original, augmented_packet in zip(
        original_packets, augmented["packets"], strict=True
    ):
        for key in original:
            assert key in augmented_packet, f"Missing post-review field: {key}"
            assert augmented_packet[key] == original[key], (
                f"Post-review field {key} was altered"
            )
        assert "rescue_evidence" in augmented_packet
        assert "first_assay" in augmented_packet


def test_augment_packets_adds_rescue_metadata(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_labels={},
        baseline_comparison_summaries=[],
    )
    assert "rescue_augmentation" in augmented
    meta = augmented["rescue_augmentation"]
    assert meta["schema_version"] == RESCUE_AUGMENTED_PACKETS_SCHEMA_VERSION
    assert meta["rescue_match_count"] == 0
    assert meta["rescue_unmatched_count"] == payload["packet_count"]


def test_augment_packets_with_partial_rescue_coverage(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    chrm4_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "CHRM4"
    )
    rescue_labels = {
        chrm4_id: {
            "task_id": "test_task",
            "task_label": "Test task",
            "entity_id": chrm4_id,
            "gene_symbol": "CHRM4",
            "decision": "advance",
            "evaluation_label": "1",
            "split_name": "test",
            "label_rationale": "Good.",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_labels=rescue_labels,
        baseline_comparison_summaries=[],
    )
    meta = augmented["rescue_augmentation"]
    assert meta["rescue_match_count"] == 2  # CHRM4 has 2 policy packets
    assert meta["rescue_unmatched_count"] == 6  # remaining 6 packets


# ---------------------------------------------------------------------------
# Materialization path from checked-in artifacts
# ---------------------------------------------------------------------------


def test_materialize_rescue_augmented_packets_from_checked_in_artifacts(
    tmp_path: Path,
) -> None:
    """Concrete generation path using checked-in hypothesis packets and
    glutamatergic convergence evaluation labels."""
    packets_path = Path(
        "examples/v0/output/hypothesis_packets_v1.json"
    ).resolve()
    labels_path = Path(
        "data/curated/rescue_tasks/glutamatergic_convergence/frozen/"
        "glutamatergic_convergence_evaluation_labels_2025_06_30.csv"
    ).resolve()
    output_path = tmp_path / "rescue_augmented_packets_v1.json"

    result = materialize_rescue_augmented_packets(
        packets_path,
        evaluation_labels_file=labels_path,
        task_id="glutamatergic_convergence_rescue_task",
        task_label="Glutamatergic convergence rescue task",
        output_file=output_path,
    )

    assert output_path.exists()
    written = json.loads(output_path.read_text())
    assert written["rescue_augmentation"]["schema_version"] == RESCUE_AUGMENTED_PACKETS_SCHEMA_VERSION
    assert "rescue_augmentation" in result
    assert result["rescue_augmentation"]["rescue_task_ids"] == [
        "glutamatergic_convergence_rescue_task"
    ]
    assert result["rescue_augmentation"]["source_artifacts"]["hypothesis_packets_v1"]
    assert result["rescue_augmentation"]["source_artifacts"]["evaluation_labels"]

    # No gene overlap between example packets (CHRM4/DRD2/SLC39A8/SLC6A1)
    # and glutamatergic rescue genes (GRIA1/GRIN2A/GRM3/GRM5), so all
    # packets should be unmatched.
    assert result["rescue_augmentation"]["rescue_match_count"] == 0
    assert result["rescue_augmentation"]["rescue_unmatched_count"] == 8

    # Every packet should have rescue_evidence and first_assay
    for packet in result["packets"]:
        assert packet["rescue_evidence"]["coverage_status"] == RESCUE_COVERAGE_NONE
        assert packet["first_assay"]["recommended_assay_class"] == FIRST_ASSAY_CLASS_NO_RESCUE
        # Post-review contract fields preserved
        assert "decision_focus" in packet
        assert "evidence_anchors" in packet
        assert "evidence_anchor_gap_status" in packet
        assert "program_history_gap_status" in packet
        assert "risk_digest" in packet
        assert "evidence_needed_next" in packet
        assert "contradiction_handling" in packet
        assert "failure_memory" in packet
        assert "failure_escape_logic" in packet
        assert "traceability" in packet


def test_materialize_with_synthetic_overlapping_entity(tmp_path: Path) -> None:
    """Build a synthetic rescue label set that overlaps with example
    hypothesis packet entities to exercise the match path."""
    packets_path = Path(
        "examples/v0/output/hypothesis_packets_v1.json"
    ).resolve()

    # Create synthetic evaluation labels that match CHRM4 entity_id
    synthetic_labels_path = tmp_path / "synthetic_labels.csv"
    synthetic_labels_path.write_text(
        "gene_id,gene_symbol,evaluation_label,evaluation_label_name,"
        "decision,adjudicated_at,decision_owner,label_rationale,split_name\n"
        "ENSG00000180720,CHRM4,1,follow_up_priority,advance,2025-06-30,"
        "rescue-review,Muscarinic convergence retained.,test\n"
        "ENSG00000149295,DRD2,0,follow_up_priority,hold,2025-06-30,"
        "rescue-review,Insufficient single-source data.,validation\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "rescue_augmented_packets_v1.json"

    result = materialize_rescue_augmented_packets(
        packets_path,
        evaluation_labels_file=synthetic_labels_path,
        task_id="synthetic_rescue_task",
        task_label="Synthetic rescue task",
        output_file=output_path,
    )

    # CHRM4 has 2 packets, DRD2 has 2 packets => 4 matches
    assert result["rescue_augmentation"]["rescue_match_count"] == 4
    assert result["rescue_augmentation"]["rescue_unmatched_count"] == 4

    chrm4_packets = [
        p for p in result["packets"]
        if p["entity_label"] == "CHRM4"
    ]
    for pkt in chrm4_packets:
        assert pkt["rescue_evidence"]["coverage_status"] == RESCUE_COVERAGE_MATCH
        matched = pkt["rescue_evidence"]["matched_tasks"][0]
        assert matched["entity_rescue_decision"] == "advance"
        assert matched["entity_split"] == "test"
        assert matched["label_rationale"] == "Muscarinic convergence retained."
        # CHRM4 has contradiction_handling.status == "contradicted" and
        # rescue says "advance" -> conflict signal
        assert len(pkt["rescue_evidence"]["conflict_signals"]) >= 1
        assert pkt["first_assay"]["recommended_assay_class"] == FIRST_ASSAY_CLASS_CONFLICT

    drd2_packets = [
        p for p in result["packets"]
        if p["entity_label"] == "DRD2"
    ]
    for pkt in drd2_packets:
        assert pkt["rescue_evidence"]["coverage_status"] == RESCUE_COVERAGE_MATCH
        matched = pkt["rescue_evidence"]["matched_tasks"][0]
        assert matched["entity_rescue_decision"] == "hold"

    slc39a8_packets = [
        p for p in result["packets"]
        if p["entity_label"] == "SLC39A8"
    ]
    for pkt in slc39a8_packets:
        assert pkt["rescue_evidence"]["coverage_status"] == RESCUE_COVERAGE_NONE
        assert pkt["first_assay"]["recommended_assay_class"] == FIRST_ASSAY_CLASS_NO_RESCUE


def test_materialize_with_model_admission_summary(tmp_path: Path) -> None:
    """Exercise model admission data flowing into rescue evidence."""
    packets_path = Path(
        "examples/v0/output/hypothesis_packets_v1.json"
    ).resolve()
    synthetic_labels_path = tmp_path / "labels.csv"
    synthetic_labels_path.write_text(
        "gene_id,gene_symbol,evaluation_label,decision,split_name,label_rationale\n"
        "ENSG00000180720,CHRM4,1,advance,test,Converged.\n",
        encoding="utf-8",
    )
    admission_path = tmp_path / "admission.json"
    admission_payload = {
        "principal_split": "test",
        "baseline_scorer_ids": ["convergence_state"],
        "candidate_model_ids": ["test_model_v1"],
        "admitted_model_ids": ["test_model_v1"],
        "decisions": [
            {
                "model_id": "test_model_v1",
                "admitted": True,
                "blocking_metric_names": [],
            }
        ],
    }
    admission_path.write_text(
        json.dumps(admission_payload), encoding="utf-8"
    )
    output_path = tmp_path / "augmented.json"

    result = materialize_rescue_augmented_packets(
        packets_path,
        evaluation_labels_file=synthetic_labels_path,
        task_id="test_task",
        task_label="Test task",
        model_admission_summary_file=admission_path,
        output_file=output_path,
    )

    chrm4_pkt = next(
        p for p in result["packets"]
        if p["entity_label"] == "CHRM4"
        and p["policy_id"] == "acute_translation_guardrails_v1"
    )
    admission = chrm4_pkt["rescue_evidence"]["matched_tasks"][0]["model_admission"]
    assert admission["status"] == "models_admitted"
    assert "test_model_v1" in admission["admitted_model_ids"]
