import csv
import json
from pathlib import Path

from scz_target_engine.prepare import prepare_gene_table


def test_prepare_gene_table_merges_sources_and_sets_canonical_id(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed.csv"
    pgc_file = tmp_path / "pgc.csv"
    ot_file = tmp_path / "ot.csv"
    chembl_file = tmp_path / "chembl.csv"
    output_file = tmp_path / "prepared.csv"

    seed_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0001,DRD2\n"
            "ENSGEX0002,SETD1A\n"
        ),
        encoding="utf-8",
    )
    pgc_file.write_text(
        (
            "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
            "ENSGPGC49295,DRD2,0.1875,1\n"
        ),
        encoding="utf-8",
    )
    ot_file.write_text(
        (
            "entity_id,entity_label,approved_name,generic_platform_baseline,opentargets_disease_id\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
        ),
        encoding="utf-8",
    )
    chembl_file.write_text(
        (
            "entity_id,entity_label,approved_name,tractability_compoundability,chembl_target_chembl_id\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.98,CHEMBL217\n"
        ),
        encoding="utf-8",
    )

    metadata = prepare_gene_table(
        seed_file=seed_file,
        output_file=output_file,
        pgc_file=pgc_file,
        opentargets_file=ot_file,
        chembl_file=chembl_file,
    )

    assert metadata["row_count"] == 2
    assert metadata["pgc_matches"] == 1
    assert metadata["opentargets_matches"] == 1
    assert metadata["chembl_matches"] == 1

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    drd2_row = next(row for row in rows if row["entity_label"] == "DRD2")
    setd1a_row = next(row for row in rows if row["entity_label"] == "SETD1A")

    assert drd2_row["common_variant_support"] == "0.1875"
    assert drd2_row["tractability_compoundability"] == "0.98"
    assert drd2_row["generic_platform_baseline"] == "0.7446"
    assert drd2_row["canonical_entity_id"] == "ENSG00000149295"
    assert drd2_row["source_present_pgc"] == "True"
    assert drd2_row["source_present_opentargets"] == "True"
    assert drd2_row["source_present_chembl"] == "True"
    assert drd2_row["pgc_match_key"] == "entity_label"
    assert drd2_row["opentargets_match_key"] == "entity_label"
    assert drd2_row["chembl_match_key"] == "entity_id"
    assert drd2_row["approved_name"] == "dopamine receptor D2"
    assert drd2_row["opentargets_disease_id"] == "MONDO_0005090"
    assert drd2_row["chembl_target_chembl_id"] == "CHEMBL217"
    assert json.loads(drd2_row["provenance_sources_json"]) == [
        "seed",
        "pgc",
        "opentargets",
        "chembl",
    ]

    assert setd1a_row["source_present_pgc"] == "False"
    assert setd1a_row["source_present_opentargets"] == "False"
    assert setd1a_row["source_present_chembl"] == "False"
    assert json.loads(setd1a_row["provenance_sources_json"]) == ["seed"]
