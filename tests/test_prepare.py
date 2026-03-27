from pathlib import Path

from scz_target_engine.prepare import prepare_gene_table


def test_prepare_gene_table_merges_sources_and_sets_canonical_id(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed.csv"
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
        opentargets_file=ot_file,
        chembl_file=chembl_file,
    )

    assert metadata["row_count"] == 2
    prepared = output_file.read_text(encoding="utf-8")
    assert "canonical_entity_id" in prepared
    assert "ENSG00000149295" in prepared
    assert "CHEMBL217" in prepared
    assert "dopamine receptor D2" in prepared
