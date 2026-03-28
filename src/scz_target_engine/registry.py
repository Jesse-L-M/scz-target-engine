from __future__ import annotations

import json
import re
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

IDENTITY_SOURCE_ORDER = [
    "seed",
    "pgc",
    "schema",
    "psychencode",
    "opentargets",
    "chembl",
]

REGISTRY_SOURCE_ORDER = [
    "pgc",
    "opentargets",
]

DEFAULT_REGISTRY_ARTIFACT_NAME = "candidate_gene_registry.csv"


def normalize_key(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().upper()


def prefer_nonempty(current_value: str, candidate_value: str) -> str:
    if candidate_value.strip():
        return candidate_value
    return current_value


def fallback_registry_id(source: str, entity_label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", entity_label.strip().lower()).strip("-")
    if not slug:
        slug = "unlabeled"
    return f"registry:{source}:{slug}"


def build_empty_candidate() -> dict[str, object]:
    row: dict[str, object] = {
        "entity_id": "",
        "entity_label": "",
        "approved_name": "",
        "seed_entity_id": "",
        "registry_origin": "",
        "registry_source_count": 0,
        "registry_sources_json": "[]",
        "source_present_pgc": "False",
        "pgc_match_key": "",
        "source_present_schema": "False",
        "schema_match_key": "",
        "source_present_psychencode": "False",
        "psychencode_match_key": "",
        "source_present_opentargets": "False",
        "opentargets_match_key": "",
        "source_present_chembl": "False",
        "chembl_match_key": "",
    }
    for layer_column in ENGINE_LAYER_COLUMNS:
        row[layer_column] = ""
    return {
        "row": row,
        "source_rows": {},
        "match_keys": {},
    }


def find_candidate(
    source_row: dict[str, str],
    by_id: dict[str, dict[str, object]],
    by_label: dict[str, list[dict[str, object]]],
) -> tuple[dict[str, object] | None, str]:
    entity_id = normalize_key(source_row.get("entity_id"))
    entity_label = normalize_key(source_row.get("entity_label"))
    if entity_id and entity_id in by_id:
        return by_id[entity_id], "entity_id"
    if entity_label and entity_label in by_label:
        label_candidates = by_label[entity_label]
        compatible_candidates = [
            candidate
            for candidate in label_candidates
            if candidate_supports_label_match(candidate, source_row)
        ]
        if len(compatible_candidates) == 1:
            return compatible_candidates[0], "entity_label"
    if entity_id:
        return None, "entity_id"
    if entity_label:
        return None, "entity_label"
    return None, ""


def index_candidate(
    candidate: dict[str, object],
    by_id: dict[str, dict[str, object]],
    by_label: dict[str, list[dict[str, object]]],
) -> None:
    row = candidate["row"]
    entity_id = normalize_key(str(row.get("entity_id", "")))
    entity_label = normalize_key(str(row.get("entity_label", "")))
    if entity_id and entity_id not in by_id:
        by_id[entity_id] = candidate
    if entity_label:
        label_candidates = by_label.setdefault(entity_label, [])
        if candidate not in label_candidates:
            label_candidates.append(candidate)


def candidate_source_entity_ids(candidate: dict[str, object]) -> set[str]:
    source_rows = candidate["source_rows"]
    return {
        source_row.get("entity_id", "").strip()
        for source_row in source_rows.values()
        if source_row.get("entity_id", "").strip()
    }


def candidate_supports_label_match(
    candidate: dict[str, object],
    source_row: dict[str, str],
) -> bool:
    source_entity_id = source_row.get("entity_id", "").strip()
    known_source_entity_ids = candidate_source_entity_ids(candidate)
    if source_entity_id:
        return not known_source_entity_ids or known_source_entity_ids == {source_entity_id}
    return len(known_source_entity_ids) <= 1


def merge_source_row(
    destination: dict[str, object],
    source_row: dict[str, str],
) -> None:
    destination["entity_label"] = prefer_nonempty(
        str(destination.get("entity_label", "")),
        source_row.get("entity_label", ""),
    )
    destination["approved_name"] = prefer_nonempty(
        str(destination.get("approved_name", "")),
        source_row.get("approved_name", ""),
    )
    for key, value in source_row.items():
        if key in {"entity_id", "entity_label", "approved_name"}:
            continue
        destination[key] = prefer_nonempty(str(destination.get(key, "")), value)


def build_provenance_entry(
    source: str,
    source_row: dict[str, str] | None,
    match_key: str,
) -> dict[str, object]:
    if source_row is None:
        return {
            "source": source,
            "matched": False,
            "entity_id": None,
            "entity_label": None,
            "match_key": None,
            "match_status": None,
        }
    entity_id = source_row.get("entity_id", "").strip() or None
    entity_label = source_row.get("entity_label", "").strip() or None
    return {
        "source": source,
        "matched": True,
        "entity_id": entity_id,
        "entity_label": entity_label,
        "match_key": match_key or None,
        "match_status": "matched",
    }


def classify_match_confidence(
    primary_gene_id: str,
    source_rows: dict[str, dict[str, str]],
) -> str:
    matched_sources = [
        source
        for source in REGISTRY_SOURCE_ORDER
        if source in source_rows
    ]
    if not matched_sources:
        return "seed_only"

    conflicting_ids = [
        source_row.get("entity_id", "").strip()
        for source_row in source_rows.values()
        if source_row.get("entity_id", "").strip()
        and source_row.get("entity_id", "").strip() != primary_gene_id
    ]
    if conflicting_ids:
        return "source_conflict"

    confirming_sources = [
        source
        for source in matched_sources
        if source_rows[source].get("entity_id", "").strip() == primary_gene_id
    ]
    if len(confirming_sources) >= 2:
        return "id_confirmed"

    if all(not source_rows[source].get("entity_id", "").strip() for source in matched_sources):
        return "source_matched"

    return "source_confirmed"


def build_fieldnames(rows: list[dict[str, object]]) -> list[str]:
    preferred_field_order = [
        "entity_id",
        "primary_gene_id",
        "canonical_entity_id",
        "entity_label",
        "approved_name",
        *ENGINE_LAYER_COLUMNS,
        "registry_origin",
        "registry_source_count",
        "registry_sources_json",
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
    additional_fields: list[str] = []
    seen = set(preferred_field_order)
    for row in rows:
        for key in row:
            if key not in seen:
                additional_fields.append(key)
                seen.add(key)
    return preferred_field_order + additional_fields


def finalize_candidate(candidate: dict[str, object]) -> dict[str, object]:
    row = candidate["row"]
    source_rows = candidate["source_rows"]
    match_keys = candidate["match_keys"]
    primary_gene_id = str(row.get("entity_id", "")).strip()
    matched_sources = [
        source
        for source in REGISTRY_SOURCE_ORDER
        if source in source_rows
    ]

    row["primary_gene_id"] = primary_gene_id
    row["canonical_entity_id"] = primary_gene_id
    row["registry_origin"] = "+".join(matched_sources)
    row["registry_source_count"] = len(matched_sources)
    row["registry_sources_json"] = json.dumps(matched_sources)
    row["match_confidence"] = classify_match_confidence(primary_gene_id, source_rows)

    source_entity_ids = {}
    provenance_entries = []
    matched_provenance_sources = []
    for source in IDENTITY_SOURCE_ORDER:
        source_row = source_rows.get(source)
        if source == "seed":
            source_entity_ids[source] = None
            provenance_entries.append(build_provenance_entry(source, None, ""))
            continue
        source_entity_id = None
        if source_row is not None:
            source_entity_id = source_row.get("entity_id", "").strip() or None
            matched_provenance_sources.append(source)
        source_entity_ids[source] = source_entity_id
        provenance_entries.append(
            build_provenance_entry(
                source,
                source_row,
                match_keys.get(source, ""),
            )
        )

    row["source_entity_ids_json"] = json.dumps(source_entity_ids)
    row["match_provenance_json"] = json.dumps(provenance_entries)
    row["provenance_sources_json"] = json.dumps(matched_provenance_sources)
    return row


def build_candidate_gene_registry(
    output_file: Path,
    pgc_file: Path | None = None,
    opentargets_file: Path | None = None,
) -> dict[str, object]:
    if pgc_file is None and opentargets_file is None:
        raise ValueError("Provide at least one non-seed source file for the candidate registry.")

    source_rows_by_name = {
        "pgc": read_csv_rows(pgc_file) if pgc_file else [],
        "opentargets": read_csv_rows(opentargets_file) if opentargets_file else [],
    }

    candidates: list[dict[str, object]] = []
    by_id: dict[str, dict[str, object]] = {}
    by_label: dict[str, list[dict[str, object]]] = {}

    for source in REGISTRY_SOURCE_ORDER:
        for source_row in source_rows_by_name[source]:
            candidate, match_key = find_candidate(source_row, by_id, by_label)
            if candidate is None:
                candidate = build_empty_candidate()
                candidate_row = candidate["row"]
                entity_id = source_row.get("entity_id", "").strip()
                entity_label = source_row.get("entity_label", "").strip()
                candidate_row["entity_id"] = entity_id or fallback_registry_id(source, entity_label)
                candidate_row["entity_label"] = entity_label
                candidates.append(candidate)
                index_candidate(candidate, by_id, by_label)

            candidate_row = candidate["row"]
            source_rows = candidate["source_rows"]
            match_keys = candidate["match_keys"]
            entity_id = source_row.get("entity_id", "").strip()
            if (
                not str(candidate_row.get("entity_id", "")).strip()
                or str(candidate_row.get("entity_id", "")).startswith("registry:")
            ) and entity_id:
                candidate_row["entity_id"] = entity_id
            elif not str(candidate_row.get("entity_id", "")).strip():
                entity_label = source_row.get("entity_label", "").strip()
                candidate_row["entity_id"] = entity_id or fallback_registry_id(source, entity_label)
            merge_source_row(candidate_row, source_row)
            source_rows[source] = source_row
            match_keys[source] = match_key or "entity_id"
            candidate_row[f"source_present_{source}"] = "True"
            candidate_row[f"{source}_match_key"] = match_key or "entity_id"
            index_candidate(candidate, by_id, by_label)

    finalized_rows = [finalize_candidate(candidate) for candidate in candidates]
    finalized_rows.sort(
        key=lambda row: (
            -int(row["registry_source_count"]),
            -float(str(row.get("common_variant_support", "") or 0.0)),
            -float(str(row.get("generic_platform_baseline", "") or 0.0)),
            str(row.get("entity_label", "")).lower(),
        )
    )

    write_csv(output_file, finalized_rows, build_fieldnames(finalized_rows))
    metadata = {
        "output_file": str(output_file),
        "row_count": len(finalized_rows),
        "pgc_file": str(pgc_file) if pgc_file else None,
        "pgc_row_count": len(source_rows_by_name["pgc"]),
        "pgc_matches": sum(
            1
            for row in finalized_rows
            if row["source_present_pgc"] == "True"
            and row["source_present_opentargets"] == "True"
        ),
        "opentargets_file": str(opentargets_file) if opentargets_file else None,
        "opentargets_row_count": len(source_rows_by_name["opentargets"]),
        "multi_source_row_count": sum(
            1
            for row in finalized_rows
            if int(row["registry_source_count"]) > 1
        ),
        "source_conflict_row_count": sum(
            1
            for row in finalized_rows
            if row["match_confidence"] == "source_conflict"
        ),
        "artifact_name": output_file.name,
        "registry_artifact": output_file.name,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata


def build_candidate_registry(
    *,
    opentargets_file: Path | None = None,
    output_file: Path,
    pgc_file: Path | None = None,
) -> dict[str, object]:
    return build_candidate_gene_registry(
        output_file=output_file,
        pgc_file=pgc_file,
        opentargets_file=opentargets_file,
    )
