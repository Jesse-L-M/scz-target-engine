from __future__ import annotations

import json
from pathlib import Path
import shutil

from scz_target_engine.io import read_csv_rows, write_csv, write_json
from scz_target_engine.sources.chembl import fetch_chembl_tractability
from scz_target_engine.sources.opentargets import fetch_opentargets_baseline
from scz_target_engine.sources.pgc import fetch_pgc_scz2022_prioritized_genes
from scz_target_engine.sources.psychencode import fetch_psychencode_support
from scz_target_engine.sources.schema import fetch_schema_rare_variant_support


ENGINE_LAYER_COLUMNS = [
    "common_variant_support",
    "rare_variant_support",
    "cell_state_support",
    "developmental_regulatory_support",
    "tractability_compoundability",
    "generic_platform_baseline",
]

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXAMPLE_GENE_SEED_FILE = REPO_ROOT / "examples" / "v0" / "input" / "gene_seed.csv"
DEFAULT_EXAMPLE_GENE_OUTPUT_FILE = REPO_ROOT / "examples" / "v0" / "input" / "gene_evidence.csv"
DEFAULT_EXAMPLE_GENE_WORK_DIR = REPO_ROOT / "data" / "processed" / "example_gene_workflow"
DEFAULT_EXAMPLE_GENE_DISEASE_QUERY = "schizophrenia"


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
    pgc_file: Path | None = None,
    schema_file: Path | None = None,
    psychencode_file: Path | None = None,
    opentargets_file: Path | None = None,
    chembl_file: Path | None = None,
) -> dict[str, object]:
    seed_rows = read_csv_rows(seed_file)
    pgc_rows = read_csv_rows(pgc_file) if pgc_file else []
    schema_rows = read_csv_rows(schema_file) if schema_file else []
    psychencode_rows = read_csv_rows(psychencode_file) if psychencode_file else []
    ot_rows = read_csv_rows(opentargets_file) if opentargets_file else []
    chembl_rows = read_csv_rows(chembl_file) if chembl_file else []

    pgc_by_id, pgc_by_label = build_index(pgc_rows)
    schema_by_id, schema_by_label = build_index(schema_rows)
    psychencode_by_id, psychencode_by_label = build_index(psychencode_rows)
    ot_by_id, ot_by_label = build_index(ot_rows)
    chembl_by_id, chembl_by_label = build_index(chembl_rows)

    prepared_rows: list[dict[str, str]] = []
    pgc_matches = 0
    schema_matches = 0
    psychencode_matches = 0
    ot_matches = 0
    chembl_matches = 0

    for seed_row in seed_rows:
        row = dict(seed_row)
        for layer_column in ENGINE_LAYER_COLUMNS:
            row.setdefault(layer_column, "")

        row["seed_entity_id"] = seed_row.get("entity_id", "")
        row["canonical_entity_id"] = seed_row.get("entity_id", "")
        row["source_present_pgc"] = "False"
        row["source_present_schema"] = "False"
        row["source_present_psychencode"] = "False"
        row["source_present_opentargets"] = "False"
        row["source_present_chembl"] = "False"
        row["pgc_match_key"] = ""
        row["schema_match_key"] = ""
        row["psychencode_match_key"] = ""
        row["opentargets_match_key"] = ""
        row["chembl_match_key"] = ""

        provenance: list[str] = ["seed"]

        pgc_row, pgc_match_key = match_row(seed_row, pgc_by_id, pgc_by_label)
        if pgc_row is not None:
            pgc_matches += 1
            row["source_present_pgc"] = "True"
            row["pgc_match_key"] = pgc_match_key
            row["common_variant_support"] = pgc_row.get("common_variant_support", "")
            row["canonical_entity_id"] = pgc_row.get("entity_id", row["canonical_entity_id"])
            merge_source_fields(
                row,
                pgc_row,
                skip_fields={"entity_id", "entity_label", "common_variant_support"},
            )
            provenance.append("pgc")

        schema_row, schema_match_key = match_row(seed_row, schema_by_id, schema_by_label)
        if schema_row is not None:
            schema_matches += 1
            row["source_present_schema"] = "True"
            row["schema_match_key"] = schema_match_key
            row["rare_variant_support"] = schema_row.get("rare_variant_support", "")
            row["approved_name"] = prefer_nonempty(
                row.get("approved_name", ""),
                schema_row.get("approved_name", ""),
            )
            if schema_row.get("schema_match_status") == "matched":
                row["canonical_entity_id"] = schema_row.get(
                    "entity_id",
                    row["canonical_entity_id"],
                )
            merge_source_fields(
                row,
                schema_row,
                skip_fields={"entity_id", "entity_label", "approved_name", "rare_variant_support"},
            )
            provenance.append("schema")

        psychencode_row, psychencode_match_key = match_row(
            seed_row,
            psychencode_by_id,
            psychencode_by_label,
        )
        if psychencode_row is not None:
            psychencode_matches += 1
            row["source_present_psychencode"] = "True"
            row["psychencode_match_key"] = psychencode_match_key
            row["cell_state_support"] = psychencode_row.get("cell_state_support", "")
            row["developmental_regulatory_support"] = psychencode_row.get(
                "developmental_regulatory_support",
                "",
            )
            row["approved_name"] = prefer_nonempty(
                row.get("approved_name", ""),
                psychencode_row.get("approved_name", ""),
            )
            merge_source_fields(
                row,
                psychencode_row,
                skip_fields={
                    "entity_id",
                    "entity_label",
                    "approved_name",
                    "cell_state_support",
                    "developmental_regulatory_support",
                },
            )
            provenance.append("psychencode")

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
        "pgc_file": str(pgc_file) if pgc_file else None,
        "schema_file": str(schema_file) if schema_file else None,
        "psychencode_file": str(psychencode_file) if psychencode_file else None,
        "opentargets_file": str(opentargets_file) if opentargets_file else None,
        "chembl_file": str(chembl_file) if chembl_file else None,
        "pgc_matches": pgc_matches,
        "schema_matches": schema_matches,
        "psychencode_matches": psychencode_matches,
        "opentargets_matches": ot_matches,
        "chembl_matches": chembl_matches,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata


def refresh_example_gene_table(
    seed_file: Path | None = None,
    output_file: Path | None = None,
    work_dir: Path | None = None,
    disease_id: str | None = None,
    disease_query: str | None = None,
    overrides_file: Path | None = None,
) -> dict[str, object]:
    resolved_seed_file = (seed_file or DEFAULT_EXAMPLE_GENE_SEED_FILE).resolve()
    resolved_output_file = (output_file or DEFAULT_EXAMPLE_GENE_OUTPUT_FILE).resolve()
    resolved_work_dir = (work_dir or DEFAULT_EXAMPLE_GENE_WORK_DIR).resolve()
    resolved_disease_query = disease_query
    if disease_id is None and resolved_disease_query is None:
        resolved_disease_query = DEFAULT_EXAMPLE_GENE_DISEASE_QUERY

    pgc_file = resolved_work_dir / "pgc" / "scz2022_prioritized_genes.csv"
    schema_file = resolved_work_dir / "schema" / "example_rare_variant_support.csv"
    psychencode_file = resolved_work_dir / "psychencode" / "example_support.csv"
    opentargets_file = resolved_work_dir / "opentargets" / "schizophrenia_baseline.csv"
    chembl_file = resolved_work_dir / "chembl" / "example_tractability.csv"
    curated_file = resolved_work_dir / "curated" / "example_gene_evidence.csv"
    for candidate in (
        pgc_file,
        schema_file,
        psychencode_file,
        opentargets_file,
        chembl_file,
        curated_file,
    ):
        candidate.parent.mkdir(parents=True, exist_ok=True)

    pgc_metadata = fetch_pgc_scz2022_prioritized_genes(output_file=pgc_file)
    schema_metadata = fetch_schema_rare_variant_support(
        input_file=resolved_seed_file,
        output_file=schema_file,
        overrides_file=overrides_file.resolve() if overrides_file else None,
    )
    psychencode_metadata = fetch_psychencode_support(
        input_file=resolved_seed_file,
        output_file=psychencode_file,
    )
    opentargets_metadata = fetch_opentargets_baseline(
        output_file=opentargets_file,
        disease_id=disease_id,
        disease_query=resolved_disease_query,
    )
    chembl_metadata = fetch_chembl_tractability(
        input_file=resolved_seed_file,
        output_file=chembl_file,
    )
    prepare_metadata = prepare_gene_table(
        seed_file=resolved_seed_file,
        output_file=curated_file,
        pgc_file=pgc_file,
        schema_file=schema_file,
        psychencode_file=psychencode_file,
        opentargets_file=opentargets_file,
        chembl_file=chembl_file,
    )

    resolved_output_file.parent.mkdir(parents=True, exist_ok=True)
    if curated_file != resolved_output_file:
        shutil.copyfile(curated_file, resolved_output_file)

    return {
        "seed_file": str(resolved_seed_file),
        "work_dir": str(resolved_work_dir),
        "published_output_file": str(resolved_output_file),
        "curated_output_file": str(curated_file),
        "pgc": pgc_metadata,
        "schema": schema_metadata,
        "psychencode": psychencode_metadata,
        "opentargets": opentargets_metadata,
        "chembl": chembl_metadata,
        "prepare": prepare_metadata,
    }
