import json
from pathlib import Path

from scz_target_engine.atlas.ingest import refresh_atlas_candidate_registry
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


def test_refresh_atlas_candidate_registry_preserves_candidate_registry_output_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    legacy_output_file = tmp_path / "legacy" / "candidate_gene_registry.csv"
    legacy_work_dir = tmp_path / "legacy-work"
    atlas_output_file = tmp_path / "atlas" / "candidate_gene_registry.csv"
    atlas_work_dir = tmp_path / "atlas-work"
    atlas_raw_dir = tmp_path / "raw"

    def write_opentargets_rows(output_file: Path) -> None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(
            (
                "entity_id,entity_label,approved_name,generic_platform_baseline,"
                "opentargets_disease_id\n"
                "ENSG00000149295,DRD2,dopamine receptor D2,0.7446,MONDO_0005090\n"
            ),
            encoding="utf-8",
        )

    def write_pgc_rows(output_file: Path) -> None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(
            (
                "entity_id,entity_label,common_variant_support,pgc_scz2022_prioritised\n"
                "ENSG00000149295,DRD2,0.1875,1\n"
            ),
            encoding="utf-8",
        )

    def fake_fetch_opentargets_baseline(
        *,
        output_file: Path,
        disease_id: str | None,
        disease_query: str | None,
    ) -> dict[str, object]:
        assert disease_id is None
        assert disease_query == "schizophrenia"
        write_opentargets_rows(output_file)
        return {"output_file": str(output_file), "row_count": 1}

    def fake_fetch_pgc_scz2022_prioritized_genes(*, output_file: Path) -> dict[str, object]:
        write_pgc_rows(output_file)
        return {"output_file": str(output_file), "row_count": 1}

    def fake_fetch_atlas_opentargets_baseline(
        *,
        output_file: Path,
        disease_id: str | None,
        disease_query: str | None,
        raw_dir: Path | None,
        materialized_at: str | None,
    ) -> dict[str, object]:
        assert disease_id is None
        assert disease_query == "schizophrenia"
        write_opentargets_rows(output_file)
        raw_manifest_file = (
            (raw_dir or atlas_raw_dir)
            / "opentargets"
            / "schizophrenia-baseline"
            / "2026-03-30"
            / "manifest.json"
        )
        raw_manifest_file.parent.mkdir(parents=True, exist_ok=True)
        raw_manifest_file.write_text("{}", encoding="utf-8")
        return {
            "materialized_at": materialized_at or "2026-03-30",
            "raw_manifest_file": str(raw_manifest_file),
            "processed_output_file": str(output_file),
        }

    def fake_fetch_atlas_pgc_scz2022_prioritized_genes(
        *,
        output_file: Path,
        raw_dir: Path | None,
        materialized_at: str | None,
    ) -> dict[str, object]:
        write_pgc_rows(output_file)
        raw_manifest_file = (
            (raw_dir or atlas_raw_dir)
            / "pgc"
            / "scz2022-prioritized-genes"
            / "2026-03-30"
            / "manifest.json"
        )
        raw_manifest_file.parent.mkdir(parents=True, exist_ok=True)
        raw_manifest_file.write_text("{}", encoding="utf-8")
        return {
            "materialized_at": materialized_at or "2026-03-30",
            "raw_manifest_file": str(raw_manifest_file),
            "processed_output_file": str(output_file),
        }

    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_opentargets_baseline",
        fake_fetch_opentargets_baseline,
    )
    monkeypatch.setattr(
        "scz_target_engine.ingest.fetch_pgc_scz2022_prioritized_genes",
        fake_fetch_pgc_scz2022_prioritized_genes,
    )
    monkeypatch.setattr(
        "scz_target_engine.atlas.ingest.fetch_atlas_opentargets_baseline",
        fake_fetch_atlas_opentargets_baseline,
    )
    monkeypatch.setattr(
        "scz_target_engine.atlas.ingest.fetch_atlas_pgc_scz2022_prioritized_genes",
        fake_fetch_atlas_pgc_scz2022_prioritized_genes,
    )

    legacy_result = refresh_candidate_registry(
        output_file=legacy_output_file,
        work_dir=legacy_work_dir,
    )
    atlas_result = refresh_atlas_candidate_registry(
        output_file=atlas_output_file,
        work_dir=atlas_work_dir,
        raw_dir=atlas_raw_dir,
        materialized_at="2026-03-30",
    )

    assert atlas_output_file.read_text(encoding="utf-8") == legacy_output_file.read_text(
        encoding="utf-8"
    )
    assert atlas_result["registry"]["artifact_name"] == legacy_result["registry"]["artifact_name"]

    manifest = json.loads(
        Path(atlas_result["manifest_file"]).read_text(encoding="utf-8")
    )
    assert manifest["contract_version"] == "atlas-ingest-foundation/v1"
    assert manifest["raw_source_root"] == str(atlas_raw_dir.resolve())
    assert manifest["sources"]["opentargets"]["raw_manifest_file"].endswith("manifest.json")
    assert manifest["sources"]["pgc"]["raw_manifest_file"].endswith("manifest.json")
