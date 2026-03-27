import csv
from pathlib import Path

from scz_target_engine.sources.schema import (
    SCHEMANotFound,
    fetch_schema_rare_variant_support,
)


def fake_transport(url: str) -> object:
    if url.endswith("/api/search?q=TCF4"):
        return {
            "results": [
                {"label": "TCF4 (ENSG00000148737)", "url": "/gene/ENSG00000148737"},
                {"label": "TCF4 (ENSG00000196628)", "url": "/gene/ENSG00000196628"},
            ]
        }
    if url.endswith("/api/search?q=DRD2"):
        return {"results": [{"label": "DRD2", "url": "/gene/ENSG00000149295"}]}
    if url.endswith("/api/search?q=SETD1A"):
        return {"results": [{"label": "SETD1A", "url": "/gene/ENSG00000099381"}]}
    if url.endswith("/api/gene/ENSGEX0001"):
        raise SCHEMANotFound(url)
    if url.endswith("/api/gene/ENSG00000149295"):
        return {
            "gene": {
                "gene_id": "ENSG00000149295",
                "symbol": "DRD2",
                "name": "dopamine receptor D2",
                "hgnc_id": "HGNC:3023",
                "omim_id": "126450",
                "alias_symbols": ["D2R"],
                "gnomad_constraint": {"pLI": 0.74709, "oe_lof": 0.16972},
                "exac_constraint": {"pLI": 0.73326},
                "gene_results": {
                    "SCHEMA": {
                        "group_results": [
                            [
                                1,
                                4,
                                0,
                                0,
                                5,
                                22,
                                0.19,
                                1,
                                0.19,
                                None,
                                None,
                                None,
                                None,
                                0.19,
                                1,
                                1,
                                1,
                                0.912,
                                0.0204,
                                10.1,
                                0.0204,
                                10.1,
                                0.27,
                                2.47,
                            ]
                        ]
                    }
                },
            }
        }
    if url.endswith("/api/gene/ENSGEX0002"):
        raise SCHEMANotFound(url)
    if url.endswith("/api/gene/ENSGEX0003"):
        raise SCHEMANotFound(url)
    if url.endswith("/api/gene/ENSG00000099381"):
        return {
            "gene": {
                "gene_id": "ENSG00000099381",
                "symbol": "SETD1A",
                "name": "SET domain containing 1A, histone lysine methyltransferase",
                "hgnc_id": "HGNC:29010",
                "omim_id": "611052",
                "alias_symbols": ["KMT2F", "SET1A"],
                "gnomad_constraint": {"pLI": 1, "oe_lof": 0.059487},
                "exac_constraint": {"pLI": 1},
                "gene_results": {
                    "SCHEMA": {
                        "group_results": [
                            [
                                15,
                                3,
                                3,
                                4,
                                11,
                                10,
                                3.6e-07,
                                0.000588,
                                7.73e-09,
                                3,
                                None,
                                None,
                                1.45e-05,
                                2e-12,
                                3.62e-08,
                                20.1,
                                10.3,
                                4.42,
                                5.68,
                                108,
                                4.12,
                                29.3,
                                1.7,
                                11.6,
                            ]
                        ]
                    }
                },
            }
        }
    raise AssertionError(f"Unexpected URL: {url}")


def test_fetch_schema_rare_variant_support_writes_matched_gene_rows(tmp_path: Path) -> None:
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0001,DRD2\n"
            "ENSGEX0002,SETD1A\n"
        ),
        encoding="utf-8",
    )
    output_file = tmp_path / "schema.csv"

    metadata = fetch_schema_rare_variant_support(
        input_file=input_file,
        output_file=output_file,
        transport=fake_transport,
    )

    assert metadata["input_row_count"] == 2
    assert metadata["matched_gene_count"] == 2
    assert metadata["missing_gene_count"] == 0
    assert output_file.exists()
    assert output_file.with_suffix(".metadata.json").exists()

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["entity_label"] == "DRD2"
    assert rows[0]["schema_query_key"] == "entity_label"
    assert rows[0]["schema_or_ptv_upper_bound"] == "10.1"
    assert rows[0]["rare_variant_support"] == "0.0"

    assert rows[1]["entity_label"] == "SETD1A"
    assert rows[1]["schema_query"] == "SETD1A"
    assert rows[1]["schema_p_meta"] == "2e-12"
    assert rows[1]["schema_q_meta"] == "3.62e-08"
    assert rows[1]["rare_variant_support"] == "0.867278"


def test_fetch_schema_rare_variant_support_skips_ambiguous_symbol_hits(tmp_path: Path) -> None:
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0003,TCF4\n"
        ),
        encoding="utf-8",
    )
    output_file = tmp_path / "schema.csv"

    metadata = fetch_schema_rare_variant_support(
        input_file=input_file,
        output_file=output_file,
        transport=fake_transport,
    )

    assert metadata["matched_gene_count"] == 0
    assert metadata["missing_gene_count"] == 1

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == []
