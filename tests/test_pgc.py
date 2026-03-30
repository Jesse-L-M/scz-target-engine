import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

from scz_target_engine.atlas.sources import fetch_atlas_pgc_scz2022_prioritized_genes
from scz_target_engine.sources.pgc import fetch_pgc_scz2022_prioritized_genes


def make_sheet_xml(rows: list[list[int]]) -> str:
    row_xml: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cell_xml: list[str] = []
        for column_index, shared_string_index in enumerate(row):
            column_letter = chr(ord("A") + column_index)
            cell_xml.append(
                f'<c r="{column_letter}{row_index}" t="s"><v>{shared_string_index}</v></c>'
            )
        row_xml.append(f'<row r="{row_index}">{"".join(cell_xml)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(row_xml)}</sheetData>'
        "</worksheet>"
    )


def build_test_workbook_bytes() -> bytes:
    shared_strings: list[str] = []
    shared_string_index: dict[str, int] = {}

    def sst_index(value: str) -> int:
        if value not in shared_string_index:
            shared_string_index[value] = len(shared_strings)
            shared_strings.append(value)
        return shared_string_index[value]

    priority_values = [
        [
            "Index.SNP",
            "Ensembl.ID",
            "Symbol.ID",
            "gene_biotype",
            "FINEMAP.priority.gene",
            "SMR.priority.gene",
        ],
        [
            "rs1",
            "ENSG00000149295",
            "DRD2",
            "protein_coding",
            "1",
            "",
        ],
        [
            "rs2",
            "ENSG00000151067",
            "CACNA1C",
            "protein_coding",
            "",
            "1",
        ],
    ]
    criteria_values = [
        [
            "Index.SNP",
            "Ensembl.ID",
            "Symbol.ID",
            "gene_biotype",
            "FINEMAPk3.5",
            "SMRpsych",
            "sig.adultFUSION",
            "Prioritised",
        ],
        [
            "rs1",
            "ENSG00000149295",
            "DRD2",
            "protein_coding",
            "",
            "1",
            "1",
            "1",
        ],
        [
            "rs2",
            "ENSG00000151067",
            "CACNA1C",
            "protein_coding",
            "1",
            "",
            "",
            "1",
        ],
    ]

    priority_rows = [[sst_index(value) for value in row] for row in priority_values]
    criteria_rows = [[sst_index(value) for value in row] for row in criteria_values]

    shared_strings_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        f'count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">'
        + "".join(f"<si><t>{value}</t></si>" for value in shared_strings)
        + "</sst>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        '<sheet name="Extended.Data.Table.1" sheetId="1" r:id="rId1"/>'
        '<sheet name="ST12 all criteria" sheetId="2" r:id="rId2"/>'
        "</sheets>"
        "</workbook>"
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet2.xml"/>'
        "</Relationships>"
    )

    workbook_bytes = io.BytesIO()
    with ZipFile(workbook_bytes, "w") as zip_file:
        zip_file.writestr("xl/workbook.xml", workbook_xml)
        zip_file.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zip_file.writestr("xl/sharedStrings.xml", shared_strings_xml)
        zip_file.writestr("xl/worksheets/sheet1.xml", make_sheet_xml(priority_rows))
        zip_file.writestr("xl/worksheets/sheet2.xml", make_sheet_xml(criteria_rows))
    return workbook_bytes.getvalue()


def fake_json_transport(url: str) -> object:
    assert url.endswith("/articles/19426775")
    return {
        "files": [
            {
                "name": "scz2022-Extended-Data-Table1.xlsx",
                "download_url": "https://example.test/scz2022.xlsx",
            }
        ]
    }


def fake_bytes_transport(url: str) -> bytes:
    assert url == "https://example.test/scz2022.xlsx"
    return build_test_workbook_bytes()


def test_fetch_pgc_scz2022_prioritized_genes_writes_curated_gene_rows(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "pgc.csv"

    metadata = fetch_pgc_scz2022_prioritized_genes(
        output_file=output_file,
        json_transport=fake_json_transport,
        bytes_transport=fake_bytes_transport,
    )

    assert metadata["row_count"] == 2
    assert output_file.exists()
    assert output_file.with_suffix(".metadata.json").exists()

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["entity_label"] == "DRD2"
    assert rows[0]["common_variant_support"] == "0.1875"
    assert rows[0]["pgc_scz2022_prioritised"] == "1"
    assert rows[0]["pgc_scz2022_priority_index_snp_count"] == "1"
    assert rows[0]["pgc_scz2022_priority_index_snps_json"] == "[\"rs1\"]"
    assert rows[0]["pgc_scz2022_FINEMAP.priority.gene"] == "1"
    assert rows[0]["pgc_scz2022_SMRpsych"] == "1"

    assert rows[1]["entity_label"] == "CACNA1C"
    assert rows[1]["common_variant_support"] == "0.125"
    assert rows[1]["pgc_scz2022_SMR.priority.gene"] == "1"
    assert rows[1]["pgc_scz2022_FINEMAPk3.5"] == "1"


def test_fetch_atlas_pgc_scz2022_preserves_processed_output_and_stages_raw_release_artifacts(
    tmp_path: Path,
) -> None:
    legacy_output_file = tmp_path / "legacy.csv"
    atlas_output_file = tmp_path / "atlas.csv"

    legacy_metadata = fetch_pgc_scz2022_prioritized_genes(
        output_file=legacy_output_file,
        json_transport=fake_json_transport,
        bytes_transport=fake_bytes_transport,
    )
    atlas_metadata = fetch_atlas_pgc_scz2022_prioritized_genes(
        output_file=atlas_output_file,
        raw_dir=tmp_path / "raw",
        materialized_at="2026-03-30",
        json_transport=fake_json_transport,
        bytes_transport=fake_bytes_transport,
    )

    assert atlas_output_file.read_text(encoding="utf-8") == legacy_output_file.read_text(
        encoding="utf-8"
    )
    assert atlas_metadata["legacy_adapter_output"]["row_count"] == legacy_metadata["row_count"]

    manifest = json.loads(
        Path(atlas_metadata["raw_manifest_file"]).read_text(encoding="utf-8")
    )
    assert manifest["status"] == "completed"
    assert manifest["source_contract"]["source_name"] == "pgc"
    assert manifest["raw_artifact_count"] == 2
    artifact_names = [artifact["artifact_name"] for artifact in manifest["artifacts"]]
    assert artifact_names[0] == "001_figshare_article_19426775.json"
    assert artifact_names[1] == "002_scz2022-extended-data-table1.xlsx"
