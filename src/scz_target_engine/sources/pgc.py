from __future__ import annotations

import io
import json
from pathlib import Path
from statistics import mean
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from scz_target_engine.io import write_csv, write_json


PGC_SCZ2022_FIGSHARE_ARTICLE_ID = 19426775
PGC_SCZ2022_WORKBOOK_NAME = "scz2022-Extended-Data-Table1.xlsx"
FIGSHARE_API_BASE = "https://api.figshare.com/v2"

JsonTransport = Callable[[str], object]
BytesTransport = Callable[[str], bytes]

COMMON_VARIANT_CRITERIA = [
    "FINEMAP.priority.gene",
    "SMR.priority.gene",
    "FINEMAPk3.5",
    "nonsynPP0.10",
    "UTRPP0.10",
    "k3.5singleGene",
    "SMRpsych",
    "SMRfetal",
    "SMRblood",
    "SMRmap",
    "SMRsingleGene",
    "HI.C.SMR",
    "sig.adultFUSION",
    "sig.fetalFUSION",
    "sig.EpiXcan.gene.filtered",
    "sig.EpiXcan.trans.filtered",
]


class PGCError(RuntimeError):
    """Raised when official PGC or figshare content is missing or malformed."""


def live_json_transport(url: str) -> object:
    try:
        with urlopen(url) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise PGCError(f"PGC/figshare HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise PGCError(f"PGC/figshare connection error: {exc.reason}") from exc


def live_bytes_transport(url: str) -> bytes:
    try:
        with urlopen(url) as response:
            return response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise PGCError(f"PGC/figshare HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise PGCError(f"PGC/figshare connection error: {exc.reason}") from exc


def fetch_figshare_article(
    article_id: int,
    transport: JsonTransport,
) -> dict[str, object]:
    payload = transport(f"{FIGSHARE_API_BASE}/articles/{article_id}")
    if not isinstance(payload, dict):
        raise PGCError("Unexpected figshare article payload.")
    return payload


def select_figshare_file(
    article_payload: dict[str, object],
    filename: str,
) -> dict[str, object]:
    files = article_payload.get("files", [])
    for file_payload in files:
        if file_payload.get("name") == filename:
            return file_payload
    available = [str(file_payload.get("name")) for file_payload in files]
    raise PGCError(f"Could not find {filename} in figshare article files: {available}")


def parse_xlsx_sheet_names(zip_file: ZipFile) -> dict[str, str]:
    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    workbook_root = ET.fromstring(zip_file.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zip_file.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root
    }
    sheet_map: dict[str, str] = {}
    for sheet in workbook_root.find("a:sheets", ns):
        relationship_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        sheet_map[sheet.attrib["name"]] = f"xl/{rel_map[relationship_id]}"
    return sheet_map


def parse_shared_strings(zip_file: ZipFile) -> list[str]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    shared_strings = []
    try:
        root = ET.fromstring(zip_file.read("xl/sharedStrings.xml"))
    except KeyError:
        return shared_strings
    for string_item in root.findall("a:si", ns):
        pieces = [text_node.text or "" for text_node in string_item.iterfind(".//a:t", ns)]
        shared_strings.append("".join(pieces))
    return shared_strings


def excel_column_index(cell_reference: str) -> int:
    letters = "".join(character for character in cell_reference if character.isalpha())
    index = 0
    for character in letters:
        index = (index * 26) + (ord(character.upper()) - 64)
    return max(index - 1, 0)


def parse_sheet_rows(
    zip_file: ZipFile,
    sheet_path: str,
    shared_strings: list[str],
) -> list[dict[str, str]]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zip_file.read(sheet_path))
    sheet_data = root.find("a:sheetData", ns)
    if sheet_data is None:
        return []
    rows: list[list[str]] = []
    max_columns = 0
    for row_node in sheet_data:
        values: dict[int, str] = {}
        for cell in row_node.findall("a:c", ns):
            cell_reference = cell.attrib.get("r", "")
            column_index = excel_column_index(cell_reference)
            cell_type = cell.attrib.get("t")
            value_node = cell.find("a:v", ns)
            value_text = ""
            if value_node is not None:
                value_text = value_node.text or ""
                if cell_type == "s":
                    value_text = shared_strings[int(value_text)]
            values[column_index] = value_text
            max_columns = max(max_columns, column_index + 1)
        rows.append([values.get(index, "") for index in range(max_columns)])
    if not rows:
        return []
    header = rows[0]
    output_rows: list[dict[str, str]] = []
    for raw_values in rows[1:]:
        padded = raw_values + [""] * (len(header) - len(raw_values))
        output_rows.append({header[index]: padded[index] for index in range(len(header))})
    return output_rows


def as_binary(value: str) -> int:
    normalized = value.strip().upper()
    if normalized in {"1", "YES", "TRUE"}:
        return 1
    return 0


def aggregate_gene_support(
    priority_rows: list[dict[str, str]],
    criteria_rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    gene_map: dict[tuple[str, str], dict[str, object]] = {}
    priority_snp_sets: dict[tuple[str, str], set[str]] = {}

    def get_gene_record(entity_id: str, entity_label: str, gene_biotype: str) -> dict[str, object]:
        key = (entity_id, entity_label)
        if key not in gene_map:
            gene_map[key] = {
                "entity_id": entity_id,
                "entity_label": entity_label,
                "gene_biotype": gene_biotype,
                "pgc_scz2022_priority_index_snps_json": json.dumps([], sort_keys=True),
                "pgc_scz2022_priority_index_snp_count": 0,
                "pgc_scz2022_prioritised": 0,
                **{f"pgc_scz2022_{criterion}": 0 for criterion in COMMON_VARIANT_CRITERIA},
            }
        return gene_map[key]

    for row in priority_rows:
        entity_id = row.get("Ensembl.ID", "").strip()
        entity_label = row.get("Symbol.ID", "").strip()
        if not entity_id or not entity_label:
            continue
        record = get_gene_record(entity_id, entity_label, row.get("gene_biotype", "").strip())
        key = (entity_id, entity_label)
        priority_snp_sets.setdefault(key, set()).add(row.get("Index.SNP", "").strip())
        record["pgc_scz2022_FINEMAP.priority.gene"] = max(
            int(record["pgc_scz2022_FINEMAP.priority.gene"]),
            as_binary(row.get("FINEMAP.priority.gene", "")),
        )
        record["pgc_scz2022_SMR.priority.gene"] = max(
            int(record["pgc_scz2022_SMR.priority.gene"]),
            as_binary(row.get("SMR.priority.gene", "")),
        )

    for row in criteria_rows:
        entity_id = row.get("Ensembl.ID", "").strip()
        entity_label = row.get("Symbol.ID", "").strip()
        if not entity_id or not entity_label:
            continue
        record = get_gene_record(entity_id, entity_label, row.get("gene_biotype", "").strip())
        record["pgc_scz2022_prioritised"] = max(
            int(record["pgc_scz2022_prioritised"]),
            as_binary(row.get("Prioritised", "")),
        )
        for criterion in COMMON_VARIANT_CRITERIA:
            if criterion in {"FINEMAP.priority.gene", "SMR.priority.gene"}:
                continue
            record[f"pgc_scz2022_{criterion}"] = max(
                int(record[f"pgc_scz2022_{criterion}"]),
                as_binary(row.get(criterion, "")),
            )

    output_rows: list[dict[str, object]] = []
    for key, record in gene_map.items():
        criteria_values = [
            int(record[f"pgc_scz2022_{criterion}"])
            for criterion in COMMON_VARIANT_CRITERIA
        ]
        record["common_variant_support"] = round(mean(criteria_values), 6)
        snps = sorted(priority_snp_sets.get(key, set()))
        record["pgc_scz2022_priority_index_snp_count"] = len(snps)
        record["pgc_scz2022_priority_index_snps_json"] = json.dumps(snps)
        output_rows.append(record)

    output_rows.sort(
        key=lambda row: (
            -float(row["common_variant_support"]),
            -int(row["pgc_scz2022_prioritised"]),
            str(row["entity_label"]).lower(),
        )
    )
    return output_rows


def fetch_pgc_scz2022_prioritized_genes(
    output_file: Path,
    article_id: int = PGC_SCZ2022_FIGSHARE_ARTICLE_ID,
    workbook_name: str = PGC_SCZ2022_WORKBOOK_NAME,
    json_transport: JsonTransport | None = None,
    bytes_transport: BytesTransport | None = None,
) -> dict[str, object]:
    json_transport = json_transport or live_json_transport
    bytes_transport = bytes_transport or live_bytes_transport

    article_payload = fetch_figshare_article(article_id, json_transport)
    file_payload = select_figshare_file(article_payload, workbook_name)
    workbook_bytes = bytes_transport(str(file_payload["download_url"]))

    with ZipFile(io.BytesIO(workbook_bytes)) as zip_file:
        sheet_map = parse_xlsx_sheet_names(zip_file)
        shared_strings = parse_shared_strings(zip_file)
        priority_sheet_path = sheet_map.get("Extended.Data.Table.1")
        criteria_sheet_path = sheet_map.get("ST12 all criteria")
        if priority_sheet_path is None or criteria_sheet_path is None:
            raise PGCError(
                "Expected workbook sheets Extended.Data.Table.1 and ST12 all criteria."
            )
        priority_rows = parse_sheet_rows(
            zip_file,
            priority_sheet_path,
            shared_strings,
        )
        criteria_rows = parse_sheet_rows(
            zip_file,
            criteria_sheet_path,
            shared_strings,
        )

    output_rows = aggregate_gene_support(priority_rows, criteria_rows)
    fieldnames = [
        "entity_id",
        "entity_label",
        "gene_biotype",
        "common_variant_support",
        "pgc_scz2022_prioritised",
        "pgc_scz2022_priority_index_snp_count",
        "pgc_scz2022_priority_index_snps_json",
        *[f"pgc_scz2022_{criterion}" for criterion in COMMON_VARIANT_CRITERIA],
    ]
    write_csv(output_file, output_rows, fieldnames)

    metadata = {
        "source": "PGC scz2022 public release",
        "article_id": article_id,
        "workbook_name": workbook_name,
        "download_url": file_payload["download_url"],
        "output_file": str(output_file),
        "row_count": len(output_rows),
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata
