import json
from pathlib import Path

import pytest

from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.program_memory import (
    materialize_program_memory_v3_adjudication_bundle,
    materialize_program_memory_v3_harvest_bundle,
    materialize_program_memory_v3_insight_packet,
)


def test_program_memory_v3_resolves_karxt_alias_to_canonical_program_id(
    tmp_path: Path,
) -> None:
    harvest_dir = tmp_path / "harvest"

    result = materialize_program_memory_v3_harvest_bundle(
        output_dir=harvest_dir,
        program_id="karxt",
        program_label="KarXT",
        materialized_at="2026-04-12",
    )

    source_manifest = read_json(harvest_dir / "source_manifest.json")
    study_rows = read_csv_rows(harvest_dir / "study_index.csv")
    contradiction_rows = read_csv_rows(harvest_dir / "contradictions.csv")

    assert result["requested_program_id"] == "karxt"
    assert result["program_id"] == "xanomeline-trospium-schizophrenia"
    assert source_manifest["program_id"] == "xanomeline-trospium-schizophrenia"
    assert source_manifest["source_document_count"] == len(
        source_manifest["source_documents"]
    )
    assert len(study_rows) >= 4
    assert contradiction_rows
    assert "first-class source-history diff artifact" in source_manifest[
        "unresolved_questions"
    ][0]


def test_program_memory_v3_fails_closed_on_unresolved_karxt_identity(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="unresolved KarXT/xanomeline-trospium identity"):
        materialize_program_memory_v3_harvest_bundle(
            output_dir=tmp_path / "harvest",
            program_id="xanomeline-muscarinic-program",
            materialized_at="2026-04-12",
        )


def test_program_memory_v3_karxt_adjudication_and_packet_are_populated(
    tmp_path: Path,
) -> None:
    harvest_dir = tmp_path / "harvest"
    adjudicated_dir = tmp_path / "adjudicated"
    packet_path = tmp_path / "packet" / "insight_packet.json"

    materialize_program_memory_v3_harvest_bundle(
        output_dir=harvest_dir,
        program_id="cobenfy",
        materialized_at="2026-04-12",
    )
    adjudication_result = materialize_program_memory_v3_adjudication_bundle(
        harvest_dir=harvest_dir,
        output_dir=adjudicated_dir,
        adjudication_id="karxt_review_v1",
        reviewer="reviewer@example.com",
        reviewed_at="2026-04-12",
    )
    packet_result = materialize_program_memory_v3_insight_packet(
        program_dir=adjudicated_dir,
        output_file=packet_path,
        packet_id="karxt-acute-efficacy-tolerability",
        packet_question=(
            "What should the public KarXT schizophrenia evidence update about acute "
            "efficacy, tolerability burden, and what is molecule-specific vs "
            "mechanism-general?"
        ),
        generated_at="2026-04-12",
    )

    claims = read_csv_rows(adjudicated_dir / "claims.csv")
    caveats = read_csv_rows(adjudicated_dir / "caveats.csv")
    belief_updates = read_csv_rows(adjudicated_dir / "belief_updates.csv")
    program_card = read_json(adjudicated_dir / "program_card.json")
    packet_payload = json.loads(packet_path.read_text(encoding="utf-8"))

    assert claims
    assert caveats
    assert belief_updates
    assert adjudication_result["claim_count"] == len(claims)
    assert adjudication_result["caveat_count"] == len(caveats)
    assert adjudication_result["belief_update_count"] == len(belief_updates)
    assert program_card["overall_verdict"] == (
        "acute_efficacy_supported_with_cholinergic_tolerability_tradeoff"
    )
    assert packet_result["candidate_insight_count"] == len(
        packet_payload["candidate_insights"]
    )
    assert packet_payload["candidate_insights"][2]["contradiction_ids"] == [
        "molecule-vs-mechanism-generalization"
    ]


def test_program_memory_v3_non_karxt_programs_remain_scaffold_only(
    tmp_path: Path,
) -> None:
    harvest_dir = tmp_path / "harvest"
    adjudicated_dir = tmp_path / "adjudicated"

    harvest_result = materialize_program_memory_v3_harvest_bundle(
        output_dir=harvest_dir,
        program_id="example-program",
        program_label="Example Program",
        materialized_at="2026-04-12",
        source_urls=("https://example.com/source",),
    )
    adjudication_result = materialize_program_memory_v3_adjudication_bundle(
        harvest_dir=harvest_dir,
        output_dir=adjudicated_dir,
        adjudication_id="example_review_v1",
        reviewer="reviewer@example.com",
        reviewed_at="2026-04-12",
    )

    source_manifest = read_json(harvest_dir / "source_manifest.json")
    study_rows = read_csv_rows(harvest_dir / "study_index.csv")
    result_rows = read_csv_rows(harvest_dir / "result_observations.csv")
    harm_rows = read_csv_rows(harvest_dir / "harm_observations.csv")
    claim_rows = read_csv_rows(adjudicated_dir / "claims.csv")
    belief_update_rows = read_csv_rows(adjudicated_dir / "belief_updates.csv")
    program_card = read_json(adjudicated_dir / "program_card.json")

    assert harvest_result["identity_resolution"] is None
    assert source_manifest["program_id"] == "example-program"
    assert source_manifest["source_document_count"] == 1
    assert study_rows == []
    assert result_rows == []
    assert harm_rows == []
    assert claim_rows == []
    assert belief_update_rows == []
    assert adjudication_result["claim_count"] == 0
    assert program_card["overall_verdict"] == "unresolved"
