import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs
from scz_target_engine.hypothesis_lab import materialize_hypothesis_packets


def _build_hypothesis_packets(tmp_path: Path) -> dict[str, object]:
    config = load_config(Path("config/v0.toml"))
    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )
    return json.loads((tmp_path / "hypothesis_packets_v1.json").read_text())


def _find_packet(
    payload: dict[str, object],
    *,
    entity_label: str,
    policy_id: str,
) -> dict[str, object]:
    return next(
        packet
        for packet in payload["packets"]
        if packet["entity_label"] == entity_label and packet["policy_id"] == policy_id
    )


def test_hypothesis_packets_materialize_from_shipped_artifacts(tmp_path: Path) -> None:
    generated_payload = _build_hypothesis_packets(tmp_path)
    manual_output_path = tmp_path / "manual_hypothesis_packets_v1.json"

    manual_payload = materialize_hypothesis_packets(
        tmp_path / "policy_decision_vectors_v2.json",
        tmp_path / "gene_target_ledgers.json",
        output_file=manual_output_path,
    )

    assert manual_payload == generated_payload
    assert manual_payload == json.loads(
        Path("examples/v0/output/hypothesis_packets_v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert manual_payload["packet_count"] == 8
    assert {packet["entity_label"] for packet in manual_payload["packets"]} == {
        "CHRM4",
        "DRD2",
        "SLC39A8",
        "SLC6A1",
    }
    assert load_artifact(manual_output_path).artifact_name == "hypothesis_packets_v1"


def test_hypothesis_packets_preserve_contradictions_and_failure_escape_logic(
    tmp_path: Path,
) -> None:
    payload = _build_hypothesis_packets(tmp_path)

    drd2_acute = _find_packet(
        payload,
        entity_label="DRD2",
        policy_id="acute_translation_guardrails_v1",
    )
    assert drd2_acute["contradiction_handling"]["status"] == "contradicted"
    assert drd2_acute["contradiction_handling"]["contradiction_conditions"] == [
        "A clinically credible schizophrenia program demonstrates benefit from sustained net DRD2 activation in the same use case."
    ]
    assert drd2_acute["failure_memory"]["replay_risk"]["status"] == "replay_inconclusive"
    assert drd2_acute["failure_escape_logic"] == {
        "status": "escape_unresolved",
        "escape_routes": [],
        "next_evidence": [
            "Robust efficacy without net DRD2 dampening becomes the dominant replicated path for acute antipsychotic benefit.",
            "Repeated adequately engaged failures in the same target and domain context would strengthen the replay claim.",
        ],
    }
    assert drd2_acute["traceability"]["source_artifacts"] == {
        "policy_decision_vectors_v2": "policy_decision_vectors_v2.json",
        "gene_target_ledgers": "gene_target_ledgers.json",
    }
    assert drd2_acute["traceability"]["policy_entity_pointer"].startswith("/entities/gene/")
    assert drd2_acute["traceability"]["policy_score_pointer"].startswith(
        drd2_acute["traceability"]["policy_entity_pointer"]
    )
    assert drd2_acute["traceability"]["ledger_target_pointer"].startswith("/targets/")
    assert drd2_acute["traceability"]["structural_failure_program_ids"]
    assert drd2_acute["traceability"]["replay_reason_event_ids"] == []

    chrm4_acute = _find_packet(
        payload,
        entity_label="CHRM4",
        policy_id="acute_translation_guardrails_v1",
    )
    assert chrm4_acute["hypothesis"]["statement"] == (
        "Test increase_activity CHRM4 via "
        "muscarinic_agonism_or_positive_allosteric_modulation "
        "(small_molecule, positive_allosteric_modulator) in acute_positive_symptoms."
    )
    assert chrm4_acute["contradiction_handling"]["status"] == "contradicted"
    assert chrm4_acute["contradiction_handling"]["contradiction_conditions"] == [
        "Repeated adequately engaged selective CHRM4 failures in aligned acute-schizophrenia populations."
    ]
    assert chrm4_acute["failure_memory"]["replay_risk"]["status"] == "replay_not_supported"
    assert chrm4_acute["failure_escape_logic"]["status"] == "escape_evidence_present"
    assert chrm4_acute["failure_escape_logic"]["escape_routes"] == [
        {
            "route_kind": "offsetting_reason",
            "event_id": "cobenfy-xanomeline-trospium-approval-us-2024",
            "failure_scope": "nonfailure",
            "explanation": (
                "cobenfy-xanomeline-trospium-approval-us-2024 is a checked-in "
                "nonfailure anchor in the same biological neighborhood "
                "(muscarinic cholinergic modulation). It is not a perfectly clean "
                "single-target counterexample."
            ),
        }
    ]
    assert chrm4_acute["traceability"]["replay_reason_event_ids"] == [
        "cobenfy-xanomeline-trospium-approval-us-2024",
        "emraclidine-empower-acute-scz-topline-2024",
    ]


def test_hypothesis_packets_reject_vague_stub_packets(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    payload["packets"][0]["hypothesis"]["modality_hypothesis"] = "undetermined"
    invalid_path = tmp_path / "invalid_hypothesis_packets_v1.json"
    invalid_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="must not be undetermined"):
        load_artifact(
            invalid_path,
            artifact_name="hypothesis_packets_v1",
        )
