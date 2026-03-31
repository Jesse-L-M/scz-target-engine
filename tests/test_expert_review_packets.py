import json
from pathlib import Path
import shutil

from scz_target_engine.hypothesis_lab import (
    BLINDED_EXPERT_REVIEW_RUBRIC,
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


def test_blinded_expert_review_rubric_docs_match_runtime_contract() -> None:
    assert _read_json(Path("docs/review_rubrics/blinded_expert_review_rubric.json")) == (
        BLINDED_EXPERT_REVIEW_RUBRIC
    )


def test_pilot_results_decode_to_traceable_expert_packet_wins() -> None:
    key_payload = _read_json(Path("examples/expert_review") / REVIEW_KEY_FILENAME)
    style_index = {
        comparison["comparison_id"]: {
            variant["blind_id"]: variant["style_id"] for variant in comparison["variants"]
        }
        for comparison in key_payload["comparisons"]
    }
    pilot_results = _read_json(Path("examples/expert_review/pilot_results_v1.json"))

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
