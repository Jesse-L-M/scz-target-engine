from __future__ import annotations

import csv
import gzip
import json
import math
import re
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

from scz_target_engine.io import read_csv_rows, write_csv, write_json


SCHEMA_RESULTS_BROWSER_URL = "https://schema.broadinstitute.org"
SCHEMA_GENE_RESULTS_TSV_URL = (
    "https://atgu-exome-browser-data.s3.amazonaws.com/SCHEMA/SCHEMA_gene_results.tsv.bgz"
)
DEFAULT_SCHEMA_ALIAS_OVERRIDES_FILE = (
    Path(__file__).resolve().parents[3] / "config" / "schema_alias_overrides.csv"
)

JsonTransport = Callable[[str], object]
BytesTransport = Callable[[str], bytes]

SCHEMA_GENE_RESULT_FIELDS = [
    "Case PTV",
    "Ctrl PTV",
    "Case mis3",
    "Ctrl mis3",
    "Case mis2",
    "Ctrl mis2",
    "P ca/co (Class 1)",
    "P ca/co (Class 2)",
    "P ca/co (comb)",
    "De novo PTV",
    "De novo mis3",
    "De novo mis2",
    "P de novo",
    "P meta",
    "Q meta",
    "OR (PTV)",
    "OR (Class I)",
    "OR (Class II)",
    "OR (PTV) lower bound",
    "OR (PTV) upper bound",
    "OR (Class I) lower bound",
    "OR (Class I) upper bound",
    "OR (Class II) lower bound",
    "OR (Class II) upper bound",
]


class SCHEMAError(RuntimeError):
    """Raised when the SCHEMA public API returns an unexpected response."""


class SCHEMANotFound(SCHEMAError):
    """Raised when a gene query is not found in the SCHEMA browser."""


def normalize_field_name(field_name: str) -> str:
    normalized = field_name.lower()
    normalized = normalized.replace("ca/co", "ca_co")
    normalized = normalized.replace("class i", "class_i")
    normalized = normalized.replace("class ii", "class_ii")
    normalized = normalized.replace("class 1", "class_1")
    normalized = normalized.replace("class 2", "class_2")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


SCHEMA_FIELD_KEY_MAP = {
    field_name: f"schema_{normalize_field_name(field_name)}"
    for field_name in SCHEMA_GENE_RESULT_FIELDS
}


def live_json_transport(url: str) -> object:
    try:
        with urlopen(url) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 404:
            raise SCHEMANotFound(f"SCHEMA gene not found: {url}") from exc
        raise SCHEMAError(f"SCHEMA HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise SCHEMAError(f"SCHEMA connection error: {exc.reason}") from exc


def live_bytes_transport(url: str) -> bytes:
    try:
        with urlopen(url) as response:
            return response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SCHEMAError(f"SCHEMA HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise SCHEMAError(f"SCHEMA connection error: {exc.reason}") from exc


def build_gene_url(query: str) -> str:
    return f"{SCHEMA_RESULTS_BROWSER_URL}/api/gene/{quote(query, safe='')}"


def build_search_url(query: str) -> str:
    return f"{SCHEMA_RESULTS_BROWSER_URL}/api/search?{urlencode({'q': query})}"


def fetch_gene_payload(
    query: str,
    transport: JsonTransport,
) -> dict[str, object]:
    payload = transport(build_gene_url(query))
    if not isinstance(payload, dict) or "gene" not in payload:
        raise SCHEMAError("Unexpected SCHEMA gene payload.")
    gene = payload["gene"]
    if not isinstance(gene, dict):
        raise SCHEMAError("SCHEMA gene payload did not include a gene object.")
    return gene


def search_gene_id(
    query: str,
    transport: JsonTransport,
) -> str | None:
    payload = transport(build_search_url(query))
    if not isinstance(payload, dict):
        raise SCHEMAError("Unexpected SCHEMA search payload.")
    results = payload.get("results", [])
    if not isinstance(results, list) or not results:
        return None

    def primary_label(result: dict[str, object]) -> str:
        label = str(result.get("label", "")).strip()
        return label.split(" (", 1)[0]

    exact_matches = [
        result
        for result in results
        if primary_label(result).upper() == query.strip().upper()
    ]
    if len(exact_matches) > 1:
        return None
    if exact_matches:
        selected = exact_matches[0]
    elif len(results) == 1:
        selected = results[0]
    else:
        return None
    url = str(selected.get("url", ""))
    if not url.startswith("/gene/"):
        raise SCHEMAError(f"Unexpected SCHEMA search result URL: {url}")
    return url.removeprefix("/gene/")


def extract_schema_gene_results(gene_payload: dict[str, object]) -> dict[str, object] | None:
    gene_results = gene_payload.get("gene_results", {})
    if not isinstance(gene_results, dict):
        return None
    schema_results = gene_results.get("SCHEMA")
    if not isinstance(schema_results, dict):
        return None
    group_results = schema_results.get("group_results", [])
    if not group_results:
        return None
    first_group = group_results[0]
    if not isinstance(first_group, list):
        raise SCHEMAError("Unexpected SCHEMA group result shape.")
    if len(first_group) != len(SCHEMA_GENE_RESULT_FIELDS):
        raise SCHEMAError(
            "Unexpected SCHEMA field count "
            f"{len(first_group)}; expected {len(SCHEMA_GENE_RESULT_FIELDS)}."
        )
    return {
        field_name: first_group[index]
        for index, field_name in enumerate(SCHEMA_GENE_RESULT_FIELDS)
    }


def load_alias_overrides(overrides_file: Path | None) -> dict[str, dict[str, str]]:
    if overrides_file is None:
        candidate = DEFAULT_SCHEMA_ALIAS_OVERRIDES_FILE
        if not candidate.exists():
            return {}
        overrides_file = candidate
    if not overrides_file.exists():
        raise FileNotFoundError(f"SCHEMA alias overrides file not found: {overrides_file}")

    overrides: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(overrides_file):
        key = row.get("entity_label", "").strip().upper()
        if key:
            overrides[key] = row
    return overrides


def load_bulk_gene_results(
    transport: BytesTransport,
) -> dict[str, dict[str, object]]:
    payload = gzip.decompress(transport(SCHEMA_GENE_RESULTS_TSV_URL)).decode(
        "utf-8",
        errors="replace",
    )
    reader = csv.DictReader(payload.splitlines(), delimiter="\t")
    bulk_results: dict[str, dict[str, object]] = {}
    for row in reader:
        gene_id = row.get("gene_id", "").strip()
        if not gene_id or row.get("group") != "meta":
            continue
        bulk_results[gene_id] = {
            field_name: row.get(field_name, "")
            for field_name in SCHEMA_GENE_RESULT_FIELDS
        }
    return bulk_results


def as_float(value: object) -> float | None:
    if value in {None, "", "NA"}:
        return None
    return float(value)


def pvalue_signal(p_value: float | None, scale: float) -> float:
    if p_value is None or p_value <= 0:
        return 0.0
    return min(-math.log10(p_value) / scale, 1.0)


def positive_or_signal(or_value: float | None) -> float:
    if or_value is None or or_value <= 1.0:
        return 0.0
    return min(math.log2(or_value) / 4.0, 1.0)


def compute_rare_variant_support(schema_result: dict[str, object]) -> dict[str, float]:
    q_meta_signal = pvalue_signal(as_float(schema_result.get("Q meta")), scale=8.0)
    p_meta_signal = pvalue_signal(as_float(schema_result.get("P meta")), scale=12.0)
    significance_signal = (0.65 * q_meta_signal) + (0.35 * p_meta_signal)

    or_signals = [
        positive_or_signal(as_float(schema_result.get("OR (PTV)"))),
        positive_or_signal(as_float(schema_result.get("OR (Class I)"))),
        positive_or_signal(as_float(schema_result.get("OR (Class II)"))),
    ]
    effect_signal = (0.6 * max(or_signals)) + (0.4 * (sum(or_signals) / len(or_signals)))

    return {
        "schema_significance_signal": round(significance_signal, 6),
        "schema_effect_signal": round(effect_signal, 6),
        "rare_variant_support": round(significance_signal * effect_signal, 6),
    }


def resolve_gene_query_candidates(input_row: dict[str, str]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    entity_id = input_row.get("entity_id", "").strip()
    entity_label = input_row.get("entity_label", "").strip()
    if entity_id:
        candidates.append(("entity_id", entity_id))
    if entity_label and entity_label.upper() != entity_id.upper():
        candidates.append(("entity_label", entity_label))
    return candidates


def fetch_schema_rare_variant_support(
    input_file: Path,
    output_file: Path,
    limit: int | None = None,
    transport: JsonTransport | None = None,
    bytes_transport: BytesTransport | None = None,
    overrides_file: Path | None = None,
) -> dict[str, object]:
    transport = transport or live_json_transport
    bytes_transport = bytes_transport or live_bytes_transport
    input_rows = read_csv_rows(input_file)
    if limit is not None:
        input_rows = input_rows[:limit]

    alias_overrides = load_alias_overrides(overrides_file)
    bulk_results_cache: dict[str, dict[str, object]] | None = None
    query_cache: dict[str, dict[str, object] | None] = {}
    output_rows: list[dict[str, object]] = []
    matched_gene_count = 0
    missing_gene_count = 0
    alias_override_match_count = 0
    bulk_fallback_match_count = 0

    for input_row in input_rows:
        resolved_gene: dict[str, object] | None = None
        schema_result: dict[str, object] | None = None
        resolved_query = ""
        resolved_query_key = ""
        override_note = ""
        override_source = ""
        input_label = input_row.get("entity_label", "").strip()
        override_row = alias_overrides.get(input_label.upper())

        if override_row is not None:
            override_gene_id = override_row.get("schema_gene_id", "").strip()
            if override_gene_id:
                resolved_query = input_label
                resolved_query_key = "alias_override"
                override_note = override_row.get("override_note", "")
                override_source = override_row.get("override_source", "")
                try:
                    resolved_gene = fetch_gene_payload(override_gene_id, transport)
                    schema_result = extract_schema_gene_results(resolved_gene)
                    if schema_result is not None:
                        alias_override_match_count += 1
                except SCHEMANotFound:
                    if bulk_results_cache is None:
                        bulk_results_cache = load_bulk_gene_results(bytes_transport)
                    schema_result = bulk_results_cache.get(override_gene_id)
                    if schema_result is not None:
                        resolved_gene = {
                            "gene_id": override_gene_id,
                            "symbol": override_row.get("schema_official_symbol", ""),
                            "name": override_row.get("approved_name", ""),
                            "hgnc_id": override_row.get("hgnc_id", ""),
                            "omim_id": override_row.get("omim_id", ""),
                            "alias_symbols": [],
                            "gnomad_constraint": {},
                            "exac_constraint": {},
                        }
                        resolved_query_key = "alias_override_bulk_fallback"
                        alias_override_match_count += 1
                        bulk_fallback_match_count += 1

        if resolved_gene is None:
            for query_key, query_value in resolve_gene_query_candidates(input_row):
                if query_value not in query_cache:
                    try:
                        resolved_query = (
                            search_gene_id(query_value, transport)
                            if query_key == "entity_label"
                            else query_value
                        )
                        if not resolved_query:
                            query_cache[query_value] = None
                        else:
                            query_cache[query_value] = fetch_gene_payload(
                                resolved_query,
                                transport,
                            )
                    except SCHEMANotFound:
                        query_cache[query_value] = None
                if query_cache[query_value] is not None:
                    resolved_gene = query_cache[query_value]
                    resolved_query = query_value
                    resolved_query_key = query_key
                    break

        if resolved_gene is None:
            missing_gene_count += 1
            continue

        if schema_result is None:
            schema_result = extract_schema_gene_results(resolved_gene)
        if schema_result is None:
            missing_gene_count += 1
            continue

        matched_gene_count += 1
        support_metrics = compute_rare_variant_support(schema_result)
        alias_symbols = resolved_gene.get("alias_symbols") or []
        gnomad_constraint = resolved_gene.get("gnomad_constraint") or {}
        exac_constraint = resolved_gene.get("exac_constraint") or {}

        output_row: dict[str, object] = {
            "entity_id": str(resolved_gene.get("gene_id", input_row.get("entity_id", ""))),
            "entity_label": input_row.get("entity_label", "").strip()
            or str(resolved_gene.get("symbol", "")),
            "approved_name": str(resolved_gene.get("name", input_row.get("approved_name", ""))),
            "rare_variant_support": support_metrics["rare_variant_support"],
            "schema_match_status": "matched",
            "schema_query": resolved_query,
            "schema_query_key": resolved_query_key,
            "schema_override_note": override_note,
            "schema_override_source": override_source,
            "schema_group": "meta",
            "schema_official_symbol": str(resolved_gene.get("symbol", "") or ""),
            "schema_hgnc_id": str(resolved_gene.get("hgnc_id", "") or ""),
            "schema_omim_id": str(resolved_gene.get("omim_id", "") or ""),
            "schema_alias_symbols_json": json.dumps(alias_symbols, sort_keys=True),
            "schema_gnomad_pli": gnomad_constraint.get("pLI", ""),
            "schema_gnomad_oe_lof": gnomad_constraint.get("oe_lof", ""),
            "schema_exac_pli": exac_constraint.get("pLI", ""),
            **support_metrics,
        }
        for field_name, value in schema_result.items():
            output_row[SCHEMA_FIELD_KEY_MAP[field_name]] = value
        output_rows.append(output_row)

    preferred_field_order = [
        "entity_id",
        "entity_label",
        "approved_name",
        "rare_variant_support",
        "schema_match_status",
        "schema_query_key",
        "schema_query",
        "schema_override_note",
        "schema_override_source",
        "schema_group",
        "schema_official_symbol",
        "schema_significance_signal",
        "schema_effect_signal",
        "schema_hgnc_id",
        "schema_omim_id",
        "schema_alias_symbols_json",
        "schema_gnomad_pli",
        "schema_gnomad_oe_lof",
        "schema_exac_pli",
        *[SCHEMA_FIELD_KEY_MAP[field_name] for field_name in SCHEMA_GENE_RESULT_FIELDS],
    ]
    write_csv(output_file, output_rows, preferred_field_order)

    metadata = {
        "source": "SCHEMA results browser gene API",
        "source_url": SCHEMA_RESULTS_BROWSER_URL,
        "gene_results_tsv_url": SCHEMA_GENE_RESULTS_TSV_URL,
        "input_file": str(input_file),
        "output_file": str(output_file),
        "input_row_count": len(input_rows),
        "row_count": len(output_rows),
        "matched_gene_count": matched_gene_count,
        "missing_gene_count": missing_gene_count,
        "alias_override_count": len(alias_overrides),
        "alias_override_match_count": alias_override_match_count,
        "bulk_fallback_match_count": bulk_fallback_match_count,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata
