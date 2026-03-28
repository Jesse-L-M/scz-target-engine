from pathlib import Path

from scz_target_engine.ingest import refresh_candidate_registry


def test_refresh_candidate_registry_fetches_sources_and_publishes_registry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_file = tmp_path / "published" / "candidate_gene_registry.csv"
    work_dir = tmp_path / "work"
    calls: dict[str, Path] = {}

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
            (
                "entity_id,entity_label,approved_name,generic_platform_baseline,"
                "opentargets_disease_id\n"
                "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
            ),
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 1}

    def fake_fetch_pgc_scz2022_prioritized_genes(*, output_file: Path) -> dict[str, object]:
        calls["pgc"] = output_file
        output_file.write_text(
            (
                "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
                "ENSG00000149295,DRD2,0.1875,1\n"
            ),
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 1}

    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_opentargets_baseline",
        fake_fetch_opentargets_baseline,
    )
    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_pgc_scz2022_prioritized_genes",
        fake_fetch_pgc_scz2022_prioritized_genes,
    )

    result = refresh_candidate_registry(
        output_file=output_file,
        work_dir=work_dir,
    )

    assert calls["opentargets"] == (
        work_dir.resolve() / "opentargets" / "schizophrenia_baseline.csv"
    )
    assert calls["pgc"] == work_dir.resolve() / "pgc" / "scz2022_prioritized_genes.csv"
    assert output_file.read_text(encoding="utf-8") == (
        work_dir.resolve() / "registry" / "candidate_gene_registry.csv"
    ).read_text(encoding="utf-8")
    assert result["published_output_file"] == str(output_file.resolve())
    assert result["registry_output_file"] == str(
        work_dir.resolve() / "registry" / "candidate_gene_registry.csv"
    )
    assert result["registry"]["artifact_name"] == "candidate_gene_registry.csv"


def test_refresh_candidate_registry_skips_pgc_when_requested(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_file = tmp_path / "published" / "candidate_gene_registry.csv"
    work_dir = tmp_path / "work"
    calls: dict[str, Path] = {}

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
            (
                "entity_id,entity_label,approved_name,generic_platform_baseline,"
                "opentargets_disease_id\n"
                "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
            ),
            encoding="utf-8",
        )
        return {"output_file": str(output_file), "row_count": 1}

    def fake_fetch_pgc_scz2022_prioritized_genes(*, output_file: Path) -> dict[str, object]:
        raise AssertionError("PGC fetch should not run when include_pgc=False")

    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_opentargets_baseline",
        fake_fetch_opentargets_baseline,
    )
    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_pgc_scz2022_prioritized_genes",
        fake_fetch_pgc_scz2022_prioritized_genes,
    )

    result = refresh_candidate_registry(
        output_file=output_file,
        work_dir=work_dir,
        include_pgc=False,
    )

    assert calls["opentargets"] == (
        work_dir.resolve() / "opentargets" / "schizophrenia_baseline.csv"
    )
    assert result["pgc"] is None
    assert output_file.exists()
    assert result["registry"]["pgc_file"] is None
