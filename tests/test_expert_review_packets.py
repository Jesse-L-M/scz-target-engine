import json
from pathlib import Path
import shutil

import pytest

from scz_target_engine.hypothesis_lab import (
    BLINDED_EXPERT_REVIEW_RUBRIC,
    build_blinded_expert_review_payloads,
    materialize_blinded_expert_review_packets,
)
from scz_target_engine.hypothesis_lab.expert_packets import (
    BASELINE_PACKET_STYLE_ID,
    EXPERT_PACKET_STYLE_ID,
    RESPONSE_TEMPLATE_FILENAME,
    REVIEW_KEY_FILENAME,
    REVIEW_PACKETS_FILENAME,
)


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_example_hypothesis_payload() -> dict[str, object]:
    return _read_json(Path("examples/v0/output/hypothesis_packets_v1.json"))


def test_blinded_expert_review_examples_are_reproducible_from_shipped_packets(
    tmp_path: Path,
) -> None:
    temp_examples_output_dir = tmp_path / "examples" / "v0" / "output"
    temp_examples_output_dir.mkdir(parents=True)
    temp_docs_dir = tmp_path / "docs" / "review_rubrics"
    temp_docs_dir.mkdir(parents=True)
    temp_review_dir = tmp_path / "examples" / "expert_review"

    shutil.copyfile(
        Path("examples/v0/output/hypothesis_packets_v1.json"),
        temp_examples_output_dir / "hypothesis_packets_v1.json",
    )
    shutil.copyfile(
        Path("examples/v0/output/policy_decision_vectors_v2.json"),
        temp_examples_output_dir / "policy_decision_vectors_v2.json",
    )
    shutil.copyfile(
        Path("examples/v0/output/gene_target_ledgers.json"),
        temp_examples_output_dir / "gene_target_ledgers.json",
    )
    shutil.copyfile(
        Path("docs/review_rubrics/blinded_expert_review_rubric.json"),
        temp_docs_dir / "blinded_expert_review_rubric.json",
    )

    result = materialize_blinded_expert_review_packets(
        temp_examples_output_dir / "hypothesis_packets_v1.json",
        output_dir=temp_review_dir,
        rubric_file=temp_docs_dir / "blinded_expert_review_rubric.json",
    )

    assert result["comparison_count"] == 8
    for filename in (
        REVIEW_PACKETS_FILENAME,
        REVIEW_KEY_FILENAME,
        RESPONSE_TEMPLATE_FILENAME,
    ):
        assert _read_json(temp_review_dir / filename) == _read_json(
            Path("examples/expert_review") / filename
        )


def test_build_payloads_direct_api_accepts_valid_unique_dimension_ids() -> None:
    custom_rubric = {
        "rubric_id": "direct_api_valid_rubric_v1",
        "review_goal": "Verify the direct API respects unique scoring dimensions.",
        "comparison_prompt": "Direct API prompt.",
        "dimensions": [
            {
                "dimension_id": "clarity",
                "label": "Clarity",
                "question": "Is the packet easy to read?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
            {
                "dimension_id": "challengeability",
                "label": "Challengeability",
                "question": "Can the reviewer challenge the packet?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
        ],
        "required_findings": [
            "winner_reason",
            "novel_packet_gap",
        ],
    }

    review_packets_payload, _, response_template_payload = build_blinded_expert_review_payloads(
        _read_example_hypothesis_payload(),
        rubric_payload=custom_rubric,
        hypothesis_artifact_ref="../v0/output/hypothesis_packets_v1.json",
        hypothesis_artifact_dir=Path("examples/v0/output").resolve(),
        output_dir=Path("examples/expert_review").resolve(),
        rubric_artifact_ref="../../docs/review_rubrics/blinded_expert_review_rubric.json",
    )

    assert review_packets_payload["comparison_prompt"] == custom_rubric["comparison_prompt"]
    assert response_template_payload["rubric"] == custom_rubric
    comparison_template = response_template_payload["comparisons"][0]
    assert set(
        comparison_template["blind_scores"][comparison_template["available_blind_ids"][0]]
    ) == {"clarity", "challengeability"}
    assert comparison_template["winner_reason"] == ""
    assert comparison_template["novel_packet_gap"] is None


def test_custom_rubric_file_changes_emitted_template_content(tmp_path: Path) -> None:
    custom_rubric = {
        "rubric_id": "custom_expert_review_rubric_v1",
        "review_goal": "Use a custom rubric for the blinded packet review.",
        "comparison_prompt": "Custom prompt: pick the packet that is easiest to challenge.",
        "dimensions": [
            {
                "dimension_id": "clarity",
                "label": "Clarity",
                "question": "Is the packet easy to understand?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "Hard to understand.",
                "high_anchor": "Very easy to understand.",
            },
            {
                "dimension_id": "challengeability",
                "label": "Challengeability",
                "question": "Does the packet make it easy to disagree with the thesis?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No clear way to challenge it.",
                "high_anchor": "Challenge paths are explicit.",
            },
        ],
        "required_findings": [
            "winner_reason",
            "novel_packet_gap",
            "schema_change_requests",
        ],
    }
    rubric_path = tmp_path / "custom_rubric.json"
    _write_json(rubric_path, custom_rubric)

    materialize_blinded_expert_review_packets(
        Path("examples/v0/output/hypothesis_packets_v1.json"),
        output_dir=tmp_path / "expert_review",
        rubric_file=rubric_path,
    )

    review_packets_payload = _read_json(tmp_path / "expert_review" / REVIEW_PACKETS_FILENAME)
    response_template_payload = _read_json(
        tmp_path / "expert_review" / RESPONSE_TEMPLATE_FILENAME
    )

    assert response_template_payload["rubric"] == custom_rubric
    assert review_packets_payload["comparison_prompt"] == custom_rubric["comparison_prompt"]
    assert review_packets_payload["comparisons"][0]["comparison_prompt"] == custom_rubric[
        "comparison_prompt"
    ]
    assert set(
        response_template_payload["comparisons"][0]["blind_scores"][
            response_template_payload["comparisons"][0]["available_blind_ids"][0]
        ]
    ) == {"clarity", "challengeability"}
    comparison_template = response_template_payload["comparisons"][0]
    assert comparison_template["winner_reason"] == ""
    assert comparison_template["schema_change_requests"] == []
    assert "novel_packet_gap" in comparison_template
    assert comparison_template["novel_packet_gap"] is None
    assert "loser_reason" not in comparison_template
    assert "missing_fields" not in comparison_template


def test_custom_rubric_can_omit_legacy_required_findings(tmp_path: Path) -> None:
    custom_rubric = {
        "rubric_id": "trimmed_expert_review_rubric_v1",
        "review_goal": "Use a smaller template contract for the blinded packet review.",
        "comparison_prompt": "Custom prompt: keep only the findings this rubric asks for.",
        "dimensions": [
            {
                "dimension_id": "decision_readiness",
                "label": "Decision readiness",
                "question": "Does the packet support a clean decision?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            }
        ],
        "required_findings": [
            "winner_reason",
        ],
    }
    rubric_path = tmp_path / "trimmed_rubric.json"
    _write_json(rubric_path, custom_rubric)

    materialize_blinded_expert_review_packets(
        Path("examples/v0/output/hypothesis_packets_v1.json"),
        output_dir=tmp_path / "expert_review",
        rubric_file=rubric_path,
    )

    comparison_template = _read_json(tmp_path / "expert_review" / RESPONSE_TEMPLATE_FILENAME)[
        "comparisons"
    ][0]

    assert comparison_template["winner_reason"] == ""
    assert "loser_reason" not in comparison_template
    assert "missing_fields" not in comparison_template
    assert "traceability_gaps" not in comparison_template
    assert "schema_change_requests" not in comparison_template
    assert "generator_revision_requests" not in comparison_template


def test_custom_rubric_rejects_duplicate_dimension_ids(tmp_path: Path) -> None:
    custom_rubric = {
        "rubric_id": "duplicate_dimension_rubric_v1",
        "review_goal": "Fail when two dimensions claim the same scoring slot.",
        "comparison_prompt": "This rubric should fail validation.",
        "dimensions": [
            {
                "dimension_id": "clarity",
                "label": "Clarity",
                "question": "Is the packet easy to read?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
            {
                "dimension_id": "clarity",
                "label": "Clarity again",
                "question": "Is the packet still easy to read?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
        ],
        "required_findings": [
            "winner_reason",
        ],
    }
    rubric_path = tmp_path / "duplicate_dimension_rubric.json"
    _write_json(rubric_path, custom_rubric)

    with pytest.raises(ValueError, match=r"dimension_id must be unique; duplicates: clarity"):
        materialize_blinded_expert_review_packets(
            Path("examples/v0/output/hypothesis_packets_v1.json"),
            output_dir=tmp_path / "expert_review",
            rubric_file=rubric_path,
        )


def test_build_payloads_direct_api_rejects_duplicate_dimension_ids() -> None:
    custom_rubric = {
        "rubric_id": "direct_api_duplicate_dimension_rubric_v1",
        "review_goal": "Fail direct API payload building when dimensions repeat.",
        "comparison_prompt": "This direct API rubric should fail validation.",
        "dimensions": [
            {
                "dimension_id": "clarity",
                "label": "Clarity",
                "question": "Is the packet easy to read?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
            {
                "dimension_id": "clarity",
                "label": "Clarity duplicate",
                "question": "Would this overwrite the first slot?",
                "scale_min": 1,
                "scale_max": 5,
                "low_anchor": "No.",
                "high_anchor": "Yes.",
            },
        ],
        "required_findings": [
            "winner_reason",
        ],
    }

    with pytest.raises(ValueError, match=r"dimension_id must be unique; duplicates: clarity"):
        build_blinded_expert_review_payloads(
            _read_example_hypothesis_payload(),
            rubric_payload=custom_rubric,
            hypothesis_artifact_ref="../v0/output/hypothesis_packets_v1.json",
            hypothesis_artifact_dir=Path("examples/v0/output").resolve(),
            output_dir=Path("examples/expert_review").resolve(),
            rubric_artifact_ref="../../docs/review_rubrics/blinded_expert_review_rubric.json",
        )


def test_missing_custom_rubric_file_fails(tmp_path: Path) -> None:
    missing_rubric_path = tmp_path / "missing_rubric.json"

    with pytest.raises(FileNotFoundError):
        materialize_blinded_expert_review_packets(
            Path("examples/v0/output/hypothesis_packets_v1.json"),
            output_dir=tmp_path / "expert_review",
            rubric_file=missing_rubric_path,
        )


def test_blinded_expert_review_key_preserves_traceable_and_baseline_styles() -> None:
    payload = _read_json(Path("examples/expert_review") / REVIEW_KEY_FILENAME)

    assert payload["comparison_count"] == 8
    for comparison in payload["comparisons"]:
        assert comparison["source_packet_pointer"].startswith("/packets/")
        assert comparison["source_traceability"]["policy_score_pointer"].startswith(
            "/entities/gene/"
        )
        assert comparison["source_traceability"]["ledger_target_pointer"].startswith(
            "/targets/"
        )
        assert {variant["style_id"] for variant in comparison["variants"]} == {
            EXPERT_PACKET_STYLE_ID,
            BASELINE_PACKET_STYLE_ID,
        }
        for variant in comparison["variants"]:
            assert variant["blind_id"].startswith(comparison["comparison_id"])
            assert variant["content_sha256"]
            assert variant["source_field_paths"]


def test_example_key_provenance_references_resolve_from_output_location() -> None:
    output_dir = Path("examples/expert_review").resolve()
    key_payload = _read_json(output_dir / REVIEW_KEY_FILENAME)

    top_level_sources = key_payload["source_artifacts"]
    assert (output_dir / top_level_sources["review_packets_file"]).resolve().exists()
    assert (output_dir / top_level_sources["response_template_file"]).resolve().exists()
    assert (output_dir / top_level_sources["review_rubric"]).resolve().exists()
    assert (output_dir / top_level_sources["hypothesis_packets_v1"]).resolve().exists()

    for comparison in key_payload["comparisons"]:
        for artifact_ref in comparison["source_traceability"]["source_artifacts"].values():
            assert (output_dir / artifact_ref).resolve().exists()


def test_blinded_expert_review_rubric_docs_match_runtime_contract() -> None:
    assert _read_json(Path("docs/review_rubrics/blinded_expert_review_rubric.json")) == (
        BLINDED_EXPERT_REVIEW_RUBRIC
    )


def test_expert_review_packets_render_revised_packet_contract() -> None:
    review_packets_payload, review_key_payload, _ = build_blinded_expert_review_payloads(
        _read_example_hypothesis_payload(),
        rubric_payload=BLINDED_EXPERT_REVIEW_RUBRIC,
        hypothesis_artifact_ref="../v0/output/hypothesis_packets_v1.json",
        hypothesis_artifact_dir=Path("examples/v0/output").resolve(),
        output_dir=Path("examples/expert_review").resolve(),
        rubric_artifact_ref="../../docs/review_rubrics/blinded_expert_review_rubric.json",
    )
    comparison = review_packets_payload["comparisons"][0]
    key_comparison = review_key_payload["comparisons"][0]
    expert_blind_id = next(
        variant["blind_id"]
        for variant in key_comparison["variants"]
        if variant["style_id"] == EXPERT_PACKET_STYLE_ID
    )
    expert_review_packet = next(
        variant["review_packet"]
        for variant in comparison["variants"]
        if variant["blind_id"] == expert_blind_id
    )

    assert [section["heading"] for section in expert_review_packet["sections"]] == [
        "Decision focus",
        "Hypothesis",
        "Why it made the packet",
        "Evidence anchors",
        "Risk digest",
        "Change-my-mind evidence",
        "Contradictions and risks",
        "Traceability",
    ]
    decision_focus_lines = expert_review_packet["sections"][0]["lines"]
    assert any(line.startswith("Review question: Should ") for line in decision_focus_lines)
    assert "Decision options: advance, hold, kill" in decision_focus_lines
    assert any(line.startswith("Current readout: ") for line in decision_focus_lines)

    evidence_anchor_lines = expert_review_packet["sections"][3]["lines"]
    assert evidence_anchor_lines[0].startswith("Anchor coverage: ")
    assert evidence_anchor_lines[1].startswith("Program history coverage: ")
    assert any("supporting_program:" in line for line in evidence_anchor_lines[2:])

    assert expert_review_packet["sections"][4]["lines"][0].startswith("Replay: ")
    assert expert_review_packet["sections"][5]["lines"][0].startswith("Needed next: ")


def test_pilot_results_decode_to_traceable_expert_packet_wins() -> None:
    pilot_results = _read_json(Path("examples/expert_review/pilot_results_v1.json"))
    key_payload = _read_json(
        Path("examples/expert_review") / pilot_results["materials"]["review_key_file"]
    )
    style_index = {
        comparison["comparison_id"]: {
            variant["blind_id"]: variant["style_id"] for variant in comparison["variants"]
        }
        for comparison in key_payload["comparisons"]
    }

    assert pilot_results["summary"]["preferred_style_id"] == EXPERT_PACKET_STYLE_ID
    assert pilot_results["summary"]["pr53_status"] == "unblocked"
    assert pilot_results["summary"]["preferred_packet_count"] == len(
        pilot_results["comparisons"]
    )
    for comparison in pilot_results["comparisons"]:
        assert style_index[comparison["comparison_id"]][comparison["preferred_blind_id"]] == (
            EXPERT_PACKET_STYLE_ID
        )
        assert style_index[comparison["comparison_id"]][comparison["rejected_blind_id"]] == (
            BASELINE_PACKET_STYLE_ID
        )
