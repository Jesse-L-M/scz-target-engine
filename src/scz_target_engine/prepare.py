from __future__ import annotations

import json
from pathlib import Path

from scz_target_engine.io import read_csv_rows, write_csv, write_json


ENGINE_LAYER_COLUMNS = [
    "common_variant_support",
    "rare_variant_support",
    "cell_state_support",
    "developmental_regulatory_support",
    "tractability_compoundability",
    "generic_platform_baseline",
]


def normalize_key(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().upper()


def build_index(rows: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    by_id: dict[str, dict[str, str]] = {}
    by_label: dict[str, dict[str, str]] = {}
    for row in rows:
        entity_id = normalize_key(row.get("entity_id"))
        entity_label = normalize_key(row.get("entity_label"))
        if entity_id and entity_id not in by_id:
            by_id[entity_id] = row
        if entity_label and entity_label not in by_label:
            by_label[entity_label] = row
    return by_id, by_label


def match_row(
    seed_row: dict[str, str],
    source_by_id: dict[str, dict[str, str]],
    source_by_label: dict[str, dict[str, str]],
) -> tuple[dict[str, str] | None, str]:
    entity_id = normalize_key(seed_row.get("entity_id"))
    entity_label = normalize_key(seed_row.get("entity_label"))
    if entity_id and entity_id in source_by_id:
        return source_by_id[entity_id], "entity_id"
    if entity_label and entity_label in source_by_label:
        return source_by_label[entity_label], "entity_label"
    return None, ""


def merge_source_fields(
    destination: dict[str, str],
    source_row: dict[str, str],
    skip_fields: set[str],
) -> None:
    for key, value in source_row.items():
        if key in skip_fields:
            continue
        destination[key] = value


def prefer_nonempty(current_value: str, candidate_value: str) -> str:
    if candidate_value.strip():
        return candidate_value
    return current_value


def prepare_gene_table(
    seed_file: Path,
    output_file: Path,
    opentargets_file: Path | None = None,
    chembl_file: Path | None = None,
) -> dict[str, object]:
    seed_rows = read_csv_rows(seed_file)
    ot_rows = read_csv_rows(opentargets_file) if opentargets_file else []
    chembl_rows = read_csv_rows(chembl_file) if chembl_file else []

    ot_by_id, ot_by_label = build_index(ot_rows)
    chembl_by_id, chembl_by_label = build_index(chembl_rows)

    prepared_rows: list[dict[str, str]] = []
    ot_matches = 0
    chembl_matches = 0

    for seed_row in seed_rows:
        row = dict(seed_row)
        for layer_column in ENGINE_LAYER_COLUMNS:
            row.setdefault(layer_column, "")

        row["seed_entity_id"] = seed_row.get("entity_id", "")
        row["canonical_entity_id"] = seed_row.get("entity_id", "")
        row["source_present_opentargets"] = "False"
        row["source_present_chembl"] = "False"
        row["opentargets_match_key"] = ""
        row["chembl_match_key"] = ""

        provenance: list[str] = ["seed"]

        ot_row, ot_match_key = match_row(seed_row, ot_by_id, ot_by_label)
        if ot_row is not None:
            ot_matches += 1
            row["source_present_opentargets"] = "True"
            row["opentargets_match_key"] = ot_match_key
            row["generic_platform_baseline"] = ot_row.get("generic_platform_baseline", "")
            row["approved_name"] = prefer_nonempty(
                row.get("approved_name", ""),
                ot_row.get("approved_name", ""),
            )
            row["canonical_entity_id"] = ot_row.get("entity_id", row["canonical_entity_id"])
            merge_source_fields(
                row,
                ot_row,
                skip_fields={"entity_id", "entity_label", "approved_name", "generic_platform_baseline"},
            )
            provenance.append("opentargets")

        chembl_row, chembl_match_key = match_row(seed_row, chembl_by_id, chembl_by_label)
        if chembl_row is not None:
            chembl_matches += 1
            row["source_present_chembl"] = "True"
            row["chembl_match_key"] = chembl_match_key
            row["tractability_compoundability"] = chembl_row.get(
                "tractability_compoundability",
                "",
            )
            row["approved_name"] = prefer_nonempty(
                row.get("approved_name", ""),
                chembl_row.get("approved_name", ""),
            )
            merge_source_fields(
                row,
                chembl_row,
                skip_fields={"entity_id", "entity_label", "approved_name", "tractability_compoundability"},
            )
            provenance.append("chembl")

        row["provenance_sources_json"] = json.dumps(provenance)
        prepared_rows.append(row)

    preferred_field_order = [
        "entity_id",
        "canonical_entity_id",
        "entity_label",
        "approved_name",
        *ENGINE_LAYER_COLUMNS,
        "seed_entity_id",
        "source_present_opentargets",
        "opentargets_match_key",
        "source_present_chembl",
        "chembl_match_key",
        "provenance_sources_json",
    ]
    additional_fields = []
    seen = set(preferred_field_order)
    for row in prepared_rows:
        for key in row:
            if key not in seen:
                additional_fields.append(key)
                seen.add(key)

    fieldnames = preferred_field_order + additional_fields
    write_csv(output_file, prepared_rows, fieldnames)
    metadata = {
        "seed_file": str(seed_file),
        "output_file": str(output_file),
        "row_count": len(prepared_rows),
        "opentargets_file": str(opentargets_file) if opentargets_file else None,
        "chembl_file": str(chembl_file) if chembl_file else None,
        "opentargets_matches": ot_matches,
        "chembl_matches": chembl_matches,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata
