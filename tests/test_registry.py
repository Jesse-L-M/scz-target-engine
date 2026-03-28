import csv
import json
from pathlib import Path

from scz_target_engine.registry import build_candidate_registry


def test_build_candidate_registry_preserves_identity_contract_for_non_seed_ingest(
    tmp_path: Path,
) -> None:
    opentargets_file = tmp_path / "opentargets.csv"
    pgc_file = tmp_path / "pgc.csv"
    output_file = tmp_path / "candidate_gene_registry.csv"

    opentargets_file.write_text(
        (
            "entity_id,entity_label,approved_name,generic_platform_baseline,"
            "opentargets_disease_id\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
            "ENSG00000099381,SETD1A,SET domain containing 1A,0.6123,MONDO_0005090\n"
        ),
        encoding="utf-8",
    )
    pgc_file.write_text(
        (
            "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
            "ENSG00000149295,DRD2,0.1875,1\n"
            "ENSG00000183454,GRIN2A,0.2500,1\n"
        ),
        encoding="utf-8",
    )

    metadata = build_candidate_registry(
        opentargets_file=opentargets_file,
        output_file=output_file,
        pgc_file=pgc_file,
    )

    assert metadata["artifact_name"] == "candidate_gene_registry.csv"
    assert metadata["row_count"] == 3
    assert metadata["pgc_matches"] == 1

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    drd2_row = next(row for row in rows if row["entity_label"] == "DRD2")
    setd1a_row = next(row for row in rows if row["entity_label"] == "SETD1A")
    grin2a_row = next(row for row in rows if row["entity_label"] == "GRIN2A")

    assert drd2_row["entity_id"] == "ENSG00000149295"
    assert drd2_row["primary_gene_id"] == "ENSG00000149295"
    assert drd2_row["canonical_entity_id"] == "ENSG00000149295"
    assert drd2_row["seed_entity_id"] == ""
    assert drd2_row["generic_platform_baseline"] == "0.7446"
    assert drd2_row["common_variant_support"] == "0.1875"
    assert drd2_row["source_present_opentargets"] == "True"
    assert drd2_row["source_present_pgc"] == "True"
    assert drd2_row["opentargets_match_key"] == "entity_id"
    assert drd2_row["pgc_match_key"] == "entity_id"
    assert drd2_row["match_confidence"] == "id_confirmed"
    assert json.loads(drd2_row["source_entity_ids_json"]) == {
        "seed": None,
        "pgc": "ENSG00000149295",
        "schema": None,
        "psychencode": None,
        "opentargets": "ENSG00000149295",
        "chembl": None,
    }
    assert json.loads(drd2_row["provenance_sources_json"]) == [
        "pgc",
        "opentargets",
    ]
    drd2_provenance = json.loads(drd2_row["match_provenance_json"])
    assert [entry["source"] for entry in drd2_provenance] == [
        "seed",
        "pgc",
        "schema",
        "psychencode",
        "opentargets",
        "chembl",
    ]
    assert [entry["matched"] for entry in drd2_provenance] == [
        False,
        True,
        False,
        False,
        True,
        False,
    ]

    assert setd1a_row["entity_id"] == "ENSG00000099381"
    assert setd1a_row["primary_gene_id"] == "ENSG00000099381"
    assert setd1a_row["source_present_opentargets"] == "True"
    assert setd1a_row["source_present_pgc"] == "False"
    assert setd1a_row["match_confidence"] == "source_confirmed"
    assert json.loads(setd1a_row["source_entity_ids_json"]) == {
        "seed": None,
        "pgc": None,
        "schema": None,
        "psychencode": None,
        "opentargets": "ENSG00000099381",
        "chembl": None,
    }

    assert grin2a_row["entity_id"] == "ENSG00000183454"
    assert grin2a_row["primary_gene_id"] == "ENSG00000183454"
    assert grin2a_row["source_present_opentargets"] == "False"
    assert grin2a_row["source_present_pgc"] == "True"
    assert grin2a_row["common_variant_support"] == "0.2500"
    assert grin2a_row["match_confidence"] == "source_confirmed"
    assert json.loads(grin2a_row["source_entity_ids_json"]) == {
        "seed": None,
        "pgc": "ENSG00000183454",
        "schema": None,
        "psychencode": None,
        "opentargets": None,
        "chembl": None,
    }


def test_build_candidate_registry_keeps_same_label_different_ids_as_distinct_candidates(
    tmp_path: Path,
) -> None:
    opentargets_file = tmp_path / "opentargets.csv"
    pgc_file = tmp_path / "pgc.csv"
    output_file = tmp_path / "candidate_gene_registry.csv"

    opentargets_file.write_text(
        (
            "entity_id,entity_label,approved_name,generic_platform_baseline,opentargets_disease_id\n"
            "ENSGOT0001,DISC1,disrupted in schizophrenia 1,0.61,MONDO_0005090\n"
        ),
        encoding="utf-8",
    )
    pgc_file.write_text(
        (
            "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
            "ENSGPGC0001,DISC1,0.44,1\n"
        ),
        encoding="utf-8",
    )

    metadata = build_candidate_registry(
        opentargets_file=opentargets_file,
        output_file=output_file,
        pgc_file=pgc_file,
    )

    assert metadata["row_count"] == 2
    assert metadata["pgc_matches"] == 0
    assert metadata["source_conflict_row_count"] == 0

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    disc1_rows = [row for row in rows if row["entity_label"] == "DISC1"]
    assert len(disc1_rows) == 2
    assert {row["entity_id"] for row in disc1_rows} == {
        "ENSGOT0001",
        "ENSGPGC0001",
    }

    pgc_row = next(row for row in disc1_rows if row["entity_id"] == "ENSGPGC0001")
    opentargets_row = next(
        row for row in disc1_rows if row["entity_id"] == "ENSGOT0001"
    )

    assert pgc_row["source_present_pgc"] == "True"
    assert pgc_row["source_present_opentargets"] == "False"
    assert pgc_row["match_confidence"] == "source_confirmed"
    assert json.loads(pgc_row["provenance_sources_json"]) == ["pgc"]

    assert opentargets_row["source_present_pgc"] == "False"
    assert opentargets_row["source_present_opentargets"] == "True"
    assert opentargets_row["match_confidence"] == "source_confirmed"
    assert json.loads(opentargets_row["provenance_sources_json"]) == ["opentargets"]
