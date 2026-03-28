import csv
import json
from pathlib import Path

from scz_target_engine.prepare import ENGINE_LAYER_COLUMNS, prepare_gene_table, refresh_example_gene_table


def test_prepare_gene_table_preserves_explicit_identity_contract(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed.csv"
    pgc_file = tmp_path / "pgc.csv"
    schema_file = tmp_path / "schema.csv"
    psychencode_file = tmp_path / "psychencode.csv"
    ot_file = tmp_path / "ot.csv"
    chembl_file = tmp_path / "chembl.csv"
    output_file = tmp_path / "prepared.csv"

    seed_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0001,DRD2\n"
            "ENSGEX0002,SETD1A\n"
            "ENSG00000183454,GRIN2A\n"
        ),
        encoding="utf-8",
    )
    pgc_file.write_text(
        (
            "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
            "ENSGPGC49295,DRD2,0.1875,1\n"
            "ENSG00000183454,GRIN2A,0.2500,1\n"
        ),
        encoding="utf-8",
    )
    schema_file.write_text(
        (
            "entity_id,entity_label,approved_name,rare_variant_support,schema_match_status,schema_p_meta\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.0,matched,0.19\n"
            "ENSG00000099381,SETD1A,\"SET domain containing 1A, histone lysine methyltransferase\",0.867278,matched,2e-12\n"
            "ENSG00000183454,GRIN2A,glutamate ionotropic receptor NMDA type subunit 2A,0.36737,matched,0.00167\n"
        ),
        encoding="utf-8",
    )
    psychencode_file.write_text(
        (
            "entity_id,entity_label,approved_name,cell_state_support,developmental_regulatory_support,psychencode_match_status\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.284,0.612,matched_deg_and_grn\n"
            "ENSG00000183454,GRIN2A,glutamate ionotropic receptor NMDA type subunit 2A,0.085828,0.935565,matched_deg_and_grn\n"
        ),
        encoding="utf-8",
    )
    ot_file.write_text(
        (
            "entity_id,entity_label,approved_name,generic_platform_baseline,opentargets_disease_id\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
            "ENSG00000183454,GRIN2A,glutamate ionotropic receptor NMDA type subunit 2A,0.614054,MONDO_0005090\n"
        ),
        encoding="utf-8",
    )
    chembl_file.write_text(
        (
            "entity_id,entity_label,approved_name,tractability_compoundability,chembl_target_chembl_id\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.98,CHEMBL217\n"
            "ENSG00000183454,GRIN2A,glutamate ionotropic receptor NMDA type subunit 2A,0.73,CHEMBL1972\n"
        ),
        encoding="utf-8",
    )

    metadata = prepare_gene_table(
        seed_file=seed_file,
        output_file=output_file,
        pgc_file=pgc_file,
        schema_file=schema_file,
        psychencode_file=psychencode_file,
        opentargets_file=ot_file,
        chembl_file=chembl_file,
    )

    assert metadata["row_count"] == 3
    assert metadata["pgc_matches"] == 2
    assert metadata["schema_matches"] == 3
    assert metadata["psychencode_matches"] == 2
    assert metadata["opentargets_matches"] == 2
    assert metadata["chembl_matches"] == 2

    with output_file.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    assert fieldnames[:5] == [
        "entity_id",
        "primary_gene_id",
        "canonical_entity_id",
        "entity_label",
        "approved_name",
    ]
    assert fieldnames[5:11] == ENGINE_LAYER_COLUMNS
    assert fieldnames[11:26] == [
        "seed_entity_id",
        "source_entity_ids_json",
        "match_confidence",
        "match_provenance_json",
        "provenance_sources_json",
        "source_present_pgc",
        "pgc_match_key",
        "source_present_schema",
        "schema_match_key",
        "source_present_psychencode",
        "psychencode_match_key",
        "source_present_opentargets",
        "opentargets_match_key",
        "source_present_chembl",
        "chembl_match_key",
    ]
    assert fieldnames.index("pgc_scz2022_prioritised") < fieldnames.index("schema_match_status")
    assert fieldnames.index("schema_match_status") < fieldnames.index("psychencode_match_status")
    assert fieldnames.index("psychencode_match_status") < fieldnames.index("opentargets_disease_id")
    assert fieldnames.index("opentargets_disease_id") < fieldnames.index("chembl_target_chembl_id")

    drd2_row = next(row for row in rows if row["entity_label"] == "DRD2")
    setd1a_row = next(row for row in rows if row["entity_label"] == "SETD1A")
    grin2a_row = next(row for row in rows if row["entity_label"] == "GRIN2A")

    assert drd2_row["common_variant_support"] == "0.1875"
    assert drd2_row["cell_state_support"] == "0.284"
    assert drd2_row["developmental_regulatory_support"] == "0.612"
    assert drd2_row["tractability_compoundability"] == "0.98"
    assert drd2_row["generic_platform_baseline"] == "0.7446"
    assert drd2_row["entity_id"] == "ENSGEX0001"
    assert drd2_row["primary_gene_id"] == "ENSGEX0001"
    assert drd2_row["seed_entity_id"] == "ENSGEX0001"
    assert drd2_row["canonical_entity_id"] == "ENSGEX0001"
    assert drd2_row["source_present_pgc"] == "True"
    assert drd2_row["source_present_schema"] == "True"
    assert drd2_row["source_present_psychencode"] == "True"
    assert drd2_row["source_present_opentargets"] == "True"
    assert drd2_row["source_present_chembl"] == "True"
    assert drd2_row["pgc_match_key"] == "entity_label"
    assert drd2_row["schema_match_key"] == "entity_label"
    assert drd2_row["psychencode_match_key"] == "entity_id"
    assert drd2_row["opentargets_match_key"] == "entity_label"
    assert drd2_row["chembl_match_key"] == "entity_id"
    assert drd2_row["approved_name"] == "dopamine receptor D2"
    assert drd2_row["psychencode_match_status"] == "matched_deg_and_grn"
    assert drd2_row["schema_p_meta"] == "0.19"
    assert drd2_row["opentargets_disease_id"] == "MONDO_0005090"
    assert drd2_row["chembl_target_chembl_id"] == "CHEMBL217"
    assert drd2_row["match_confidence"] == "source_conflict"
    assert json.loads(drd2_row["source_entity_ids_json"]) == {
        "seed": "ENSGEX0001",
        "pgc": "ENSGPGC49295",
        "schema": "ENSG00000149295",
        "psychencode": "ENSGEX0001",
        "opentargets": "ENSG00000149295",
        "chembl": "ENSGEX0001",
    }
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
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    assert drd2_provenance[1]["entity_id"] == "ENSGPGC49295"
    assert drd2_provenance[1]["match_key"] == "entity_label"
    assert drd2_provenance[2]["entity_id"] == "ENSG00000149295"
    assert drd2_provenance[2]["match_status"] == "matched"
    assert drd2_provenance[5]["entity_id"] == "ENSGEX0001"
    assert json.loads(drd2_row["provenance_sources_json"]) == [
        "seed",
        "pgc",
        "schema",
        "psychencode",
        "opentargets",
        "chembl",
    ]

    assert setd1a_row["rare_variant_support"] == "0.867278"
    assert setd1a_row["entity_id"] == "ENSGEX0002"
    assert setd1a_row["primary_gene_id"] == "ENSGEX0002"
    assert setd1a_row["seed_entity_id"] == "ENSGEX0002"
    assert setd1a_row["canonical_entity_id"] == "ENSGEX0002"
    assert setd1a_row["source_present_pgc"] == "False"
    assert setd1a_row["source_present_schema"] == "True"
    assert setd1a_row["source_present_psychencode"] == "False"
    assert setd1a_row["source_present_opentargets"] == "False"
    assert setd1a_row["source_present_chembl"] == "False"
    assert setd1a_row["match_confidence"] == "source_conflict"
    assert json.loads(setd1a_row["source_entity_ids_json"]) == {
        "seed": "ENSGEX0002",
        "pgc": None,
        "schema": "ENSG00000099381",
        "psychencode": None,
        "opentargets": None,
        "chembl": None,
    }
    assert json.loads(setd1a_row["provenance_sources_json"]) == ["seed", "schema"]
    setd1a_provenance = json.loads(setd1a_row["match_provenance_json"])
    assert [entry["matched"] for entry in setd1a_provenance] == [
        True,
        False,
        True,
        False,
        False,
        False,
    ]
    assert setd1a_provenance[2]["entity_id"] == "ENSG00000099381"
    assert setd1a_provenance[2]["match_key"] == "entity_label"

    assert grin2a_row["entity_id"] == "ENSG00000183454"
    assert grin2a_row["primary_gene_id"] == "ENSG00000183454"
    assert grin2a_row["seed_entity_id"] == "ENSG00000183454"
    assert grin2a_row["canonical_entity_id"] == "ENSG00000183454"
    assert grin2a_row["common_variant_support"] == "0.2500"
    assert grin2a_row["rare_variant_support"] == "0.36737"
    assert grin2a_row["cell_state_support"] == "0.085828"
    assert grin2a_row["developmental_regulatory_support"] == "0.935565"
    assert grin2a_row["tractability_compoundability"] == "0.73"
    assert grin2a_row["generic_platform_baseline"] == "0.614054"
    assert grin2a_row["match_confidence"] == "id_confirmed"
    assert json.loads(grin2a_row["source_entity_ids_json"]) == {
        "seed": "ENSG00000183454",
        "pgc": "ENSG00000183454",
        "schema": "ENSG00000183454",
        "psychencode": "ENSG00000183454",
        "opentargets": "ENSG00000183454",
        "chembl": "ENSG00000183454",
    }
    assert json.loads(grin2a_row["provenance_sources_json"]) == [
        "seed",
        "pgc",
        "schema",
        "psychencode",
        "opentargets",
        "chembl",
    ]


def test_prepare_gene_table_orders_source_primitive_groups_independently_of_first_match(
    tmp_path: Path,
) -> None:
    seed_file = tmp_path / "seed.csv"
    pgc_file = tmp_path / "pgc.csv"
    schema_file = tmp_path / "schema.csv"
    psychencode_file = tmp_path / "psychencode.csv"
    ot_file = tmp_path / "ot.csv"
    chembl_file = tmp_path / "chembl.csv"
    output_file = tmp_path / "prepared.csv"

    seed_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0002,SETD1A\n"
            "ENSGEX0001,DRD2\n"
        ),
        encoding="utf-8",
    )
    pgc_file.write_text(
        (
            "entity_id,entity_label,common_variant_support,gene_biotype,pgc_scz2022_prioritised\n"
            "ENSGPGC49295,DRD2,0.1875,protein_coding,1\n"
        ),
        encoding="utf-8",
    )
    schema_file.write_text(
        (
            "entity_id,entity_label,approved_name,rare_variant_support,schema_match_status,schema_p_meta\n"
            "ENSG00000099381,SETD1A,SET domain containing 1A,0.867278,matched,2e-12\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.0,matched,0.19\n"
        ),
        encoding="utf-8",
    )
    psychencode_file.write_text(
        (
            "entity_id,entity_label,approved_name,cell_state_support,psychencode_match_status\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.284,matched_deg_only\n"
        ),
        encoding="utf-8",
    )
    ot_file.write_text(
        (
            "entity_id,entity_label,approved_name,generic_platform_baseline,"
            "opentargets_disease_id,opentargets_datatype_scores_json,opentargets_datatype_genetic_association\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090,\"{}\",0.7\n"
        ),
        encoding="utf-8",
    )
    chembl_file.write_text(
        (
            "entity_id,entity_label,approved_name,tractability_compoundability,chembl_match_status,"
            "chembl_activity_count,chembl_mechanism_count,chembl_max_phase,chembl_action_types_json\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.98,matched_exact_human_gene_symbol,32479,68,4,"
            "\"[\"\"ANTAGONIST\"\"]\"\n"
        ),
        encoding="utf-8",
    )

    prepare_gene_table(
        seed_file=seed_file,
        output_file=output_file,
        pgc_file=pgc_file,
        schema_file=schema_file,
        psychencode_file=psychencode_file,
        opentargets_file=ot_file,
        chembl_file=chembl_file,
    )

    with output_file.open(newline="", encoding="utf-8") as handle:
        fieldnames = csv.DictReader(handle).fieldnames or []

    assert fieldnames.index("chembl_match_key") < fieldnames.index("gene_biotype")
    assert fieldnames.index("gene_biotype") < fieldnames.index("schema_match_status")
    assert fieldnames.index("schema_match_status") < fieldnames.index("psychencode_match_status")
    assert fieldnames.index("psychencode_match_status") < fieldnames.index("opentargets_disease_id")
    assert fieldnames.index("opentargets_disease_id") < fieldnames.index("chembl_match_status")
    assert fieldnames.index("opentargets_datatype_scores_json") < fieldnames.index(
        "opentargets_datatype_genetic_association"
    )


def test_refresh_example_gene_table_fetches_sources_and_publishes_curated_csv(
    tmp_path: Path,
    monkeypatch,
) -> None:
    seed_file = tmp_path / "gene_seed.csv"
    output_file = tmp_path / "published" / "gene_evidence.csv"
    work_dir = tmp_path / "work"
    seed_file.write_text(
        "entity_id,entity_label,approved_name\nENSG00000149295,DRD2,dopamine receptor D2\n",
        encoding="utf-8",
    )

    calls: dict[str, Path] = {}

    def fake_fetch_pgc_scz2022_prioritized_genes(*, output_file: Path) -> dict[str, object]:
        calls["pgc"] = output_file
        output_file.write_text("entity_id,entity_label,common_variant_support\n", encoding="utf-8")
        return {"output_file": str(output_file), "row_count": 0}

    def fake_fetch_schema_rare_variant_support(
        *,
        input_file: Path,
        output_file: Path,
        overrides_file: Path | None,
    ) -> dict[str, object]:
        calls["schema_input"] = input_file
        calls["schema"] = output_file
        assert overrides_file is None
        output_file.write_text("entity_id,entity_label,rare_variant_support\n", encoding="utf-8")
        return {"output_file": str(output_file), "row_count": 0}

    def fake_fetch_psychencode_support(
        *,
        input_file: Path,
        output_file: Path,
    ) -> dict[str, object]:
        calls["psychencode_input"] = input_file
        calls["psychencode"] = output_file
        output_file.write_text("entity_id,entity_label,cell_state_support\n", encoding="utf-8")
        return {"output_file": str(output_file), "row_count": 0}

    def fake_fetch_opentargets_baseline(
        *,
        output_file: Path,
        disease_id: str | None,
        disease_query: str | None,
    ) -> dict[str, object]:
        calls["opentargets"] = output_file
        assert disease_id is None
        assert disease_query == "schizophrenia"
        output_file.write_text(
            "entity_id,entity_label,generic_platform_baseline\n",
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 0}

    def fake_fetch_chembl_tractability(
        *,
        input_file: Path,
        output_file: Path,
    ) -> dict[str, object]:
        calls["chembl_input"] = input_file
        calls["chembl"] = output_file
        output_file.write_text(
            "entity_id,entity_label,tractability_compoundability\n",
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 0}

    def fake_prepare_gene_table(
        *,
        seed_file: Path,
        output_file: Path,
        pgc_file: Path | None,
        schema_file: Path | None,
        psychencode_file: Path | None,
        opentargets_file: Path | None,
        chembl_file: Path | None,
    ) -> dict[str, object]:
        calls["prepared_seed"] = seed_file
        calls["prepared"] = output_file
        assert pgc_file == calls["pgc"]
        assert schema_file == calls["schema"]
        assert psychencode_file == calls["psychencode"]
        assert opentargets_file == calls["opentargets"]
        assert chembl_file == calls["chembl"]
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(
            (
                "entity_id,primary_gene_id,canonical_entity_id,entity_label,"
                "common_variant_support,rare_variant_support,"
                "cell_state_support,developmental_regulatory_support,"
                "tractability_compoundability,generic_platform_baseline,"
                "seed_entity_id,source_entity_ids_json,match_confidence,"
                "match_provenance_json\n"
                "ENSG00000149295,ENSG00000149295,ENSG00000149295,DRD2,"
                "0.2,0.3,0.4,0.5,0.6,0.7,ENSG00000149295,"
                "\"{\"\"seed\"\": \"\"ENSG00000149295\"\"}\",id_confirmed,"
                "\"[{\"\"source\"\": \"\"seed\"\", \"\"matched\"\": true}]\"\n"
            ),
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 1}

    monkeypatch.setattr(
        "scz_target_engine.prepare.fetch_pgc_scz2022_prioritized_genes",
        fake_fetch_pgc_scz2022_prioritized_genes,
    )
    monkeypatch.setattr(
        "scz_target_engine.prepare.fetch_schema_rare_variant_support",
        fake_fetch_schema_rare_variant_support,
    )
    monkeypatch.setattr(
        "scz_target_engine.prepare.fetch_psychencode_support",
        fake_fetch_psychencode_support,
    )
    monkeypatch.setattr(
        "scz_target_engine.prepare.fetch_opentargets_baseline",
        fake_fetch_opentargets_baseline,
    )
    monkeypatch.setattr(
        "scz_target_engine.prepare.fetch_chembl_tractability",
        fake_fetch_chembl_tractability,
    )
    monkeypatch.setattr(
        "scz_target_engine.prepare.prepare_gene_table",
        fake_prepare_gene_table,
    )

    result = refresh_example_gene_table(
        seed_file=seed_file,
        output_file=output_file,
        work_dir=work_dir,
    )

    assert calls["schema_input"] == seed_file.resolve()
    assert calls["psychencode_input"] == seed_file.resolve()
    assert calls["chembl_input"] == seed_file.resolve()
    assert calls["prepared_seed"] == seed_file.resolve()
    assert calls["pgc"] == (work_dir.resolve() / "pgc" / "scz2022_prioritized_genes.csv")
    assert calls["schema"] == (
        work_dir.resolve() / "schema" / "example_rare_variant_support.csv"
    )
    assert calls["psychencode"] == (
        work_dir.resolve() / "psychencode" / "example_support.csv"
    )
    assert calls["opentargets"] == (
        work_dir.resolve() / "opentargets" / "schizophrenia_baseline.csv"
    )
    assert calls["chembl"] == (
        work_dir.resolve() / "chembl" / "example_tractability.csv"
    )
    assert calls["prepared"] == (
        work_dir.resolve() / "curated" / "example_gene_evidence.csv"
    )
    assert output_file.read_text(encoding="utf-8") == calls["prepared"].read_text(
        encoding="utf-8"
    )
    assert result["published_output_file"] == str(output_file.resolve())
    assert result["curated_output_file"] == str(calls["prepared"])
