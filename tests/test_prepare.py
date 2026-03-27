import csv
import json
from pathlib import Path

from scz_target_engine.prepare import prepare_gene_table, refresh_example_gene_table


def test_prepare_gene_table_merges_sources_and_sets_canonical_id(tmp_path: Path) -> None:
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
    schema_file.write_text(
        (
            "entity_id,entity_label,approved_name,rare_variant_support,schema_match_status,schema_p_meta\n"
            "ENSG00000149295,DRD2,dopamine receptor D2,0.0,matched,0.19\n"
            "ENSG00000099381,SETD1A,\"SET domain containing 1A, histone lysine methyltransferase\",0.867278,matched,2e-12\n"
        ),
        encoding="utf-8",
    )
    psychencode_file.write_text(
        (
            "entity_id,entity_label,approved_name,cell_state_support,developmental_regulatory_support,psychencode_match_status\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.284,0.612,matched_deg_and_grn\n"
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
        schema_file=schema_file,
        psychencode_file=psychencode_file,
        opentargets_file=ot_file,
        chembl_file=chembl_file,
    )

    assert metadata["row_count"] == 2
    assert metadata["pgc_matches"] == 1
    assert metadata["schema_matches"] == 2
    assert metadata["psychencode_matches"] == 1
    assert metadata["opentargets_matches"] == 1
    assert metadata["chembl_matches"] == 1

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    drd2_row = next(row for row in rows if row["entity_label"] == "DRD2")
    setd1a_row = next(row for row in rows if row["entity_label"] == "SETD1A")

    assert drd2_row["common_variant_support"] == "0.1875"
    assert drd2_row["cell_state_support"] == "0.284"
    assert drd2_row["developmental_regulatory_support"] == "0.612"
    assert drd2_row["tractability_compoundability"] == "0.98"
    assert drd2_row["generic_platform_baseline"] == "0.7446"
    assert drd2_row["canonical_entity_id"] == "ENSG00000149295"
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
    assert json.loads(drd2_row["provenance_sources_json"]) == [
        "seed",
        "pgc",
        "schema",
        "psychencode",
        "opentargets",
        "chembl",
    ]

    assert setd1a_row["rare_variant_support"] == "0.867278"
    assert setd1a_row["canonical_entity_id"] == "ENSG00000099381"
    assert setd1a_row["source_present_pgc"] == "False"
    assert setd1a_row["source_present_schema"] == "True"
    assert setd1a_row["source_present_psychencode"] == "False"
    assert setd1a_row["source_present_opentargets"] == "False"
    assert setd1a_row["source_present_chembl"] == "False"
    assert json.loads(setd1a_row["provenance_sources_json"]) == ["seed", "schema"]


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
                "entity_id,entity_label,common_variant_support,rare_variant_support,"
                "cell_state_support,developmental_regulatory_support,"
                "tractability_compoundability,generic_platform_baseline\n"
                "ENSG00000149295,DRD2,0.2,0.3,0.4,0.5,0.6,0.7\n"
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
