from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs
from scz_target_engine.hypothesis_lab import materialize_hypothesis_packets
import scz_target_engine.engine as engine_module


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
    assert manual_payload["packet_generation_criteria"]["require_scored_policy_signal"] is True
    assert {packet["entity_label"] for packet in manual_payload["packets"]} == {
        "CHRM4",
        "DRD2",
        "SLC39A8",
        "SLC6A1",
    }
    assert load_artifact(manual_output_path).artifact_name == "hypothesis_packets_v1"
    assert (
        load_artifact(Path("examples/v0/output/hypothesis_packets_v1.json")).artifact_name
        == "hypothesis_packets_v1"
    )


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


def test_build_outputs_allows_valid_zero_packet_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(Path("config/v0.toml"))
    original_build_target_ledgers = engine_module.build_target_ledgers

    def build_target_ledgers_without_curated_packets(*args: object, **kwargs: object):
        ledgers = original_build_target_ledgers(*args, **kwargs)
        return [
            replace(
                ledger,
                directionality_hypothesis={
                    **ledger.directionality_hypothesis,
                    "status": "undetermined",
                },
            )
            for ledger in ledgers
        ]

    monkeypatch.setattr(
        engine_module,
        "build_target_ledgers",
        build_target_ledgers_without_curated_packets,
    )

    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )

    payload = json.loads((tmp_path / "hypothesis_packets_v1.json").read_text())
    assert payload["packet_count"] == 0
    assert payload["packets"] == []
    assert payload["packet_generation_criteria"]["require_scored_policy_signal"] is True
    assert load_artifact(tmp_path / "hypothesis_packets_v1.json").artifact_name == (
        "hypothesis_packets_v1"
    )


def test_build_outputs_allows_scoreless_curated_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(Path("config/v0.toml"))
    original_build_policy_artifacts = engine_module.build_policy_artifacts

    def build_policy_artifacts_with_scoreless_drd2(*args: object, **kwargs: object):
        policy_payload, pareto_payload = original_build_policy_artifacts(*args, **kwargs)
        mutated_policy_payload = deepcopy(policy_payload)
        for entity in mutated_policy_payload["entities"]["gene"]:
            if entity["entity_label"] != "DRD2":
                continue
            for score in entity["policy_scores"]:
                score["score"] = None
                score["base_score"] = None
                score["score_before_clamp"] = None
                score["status"] = "missing_inputs"
            entity["policy_vector"] = {
                score["policy_id"]: score
                for score in entity["policy_scores"]
            }
        return mutated_policy_payload, pareto_payload

    monkeypatch.setattr(
        engine_module,
        "build_policy_artifacts",
        build_policy_artifacts_with_scoreless_drd2,
    )

    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )

    payload = json.loads((tmp_path / "hypothesis_packets_v1.json").read_text())
    assert payload["packet_count"] == 6
    assert {packet["entity_label"] for packet in payload["packets"]} == {
        "CHRM4",
        "SLC39A8",
        "SLC6A1",
    }
    assert load_artifact(tmp_path / "hypothesis_packets_v1.json").artifact_name == (
        "hypothesis_packets_v1"
    )


def test_hypothesis_packets_reject_stale_traceability_pointers(tmp_path: Path) -> None:
    payload = _build_hypothesis_packets(tmp_path)
    payload["packets"][0]["traceability"]["policy_score_pointer"] = (
        "/entities/gene/999/policy_scores/0"
    )
    invalid_path = tmp_path / "invalid_pointer_hypothesis_packets_v1.json"
    invalid_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="policy_score_pointer"):
        load_artifact(
            invalid_path,
            artifact_name="hypothesis_packets_v1",
        )
