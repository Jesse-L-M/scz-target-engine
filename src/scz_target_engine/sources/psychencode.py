from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile

from scz_target_engine.io import read_csv_rows, write_csv, write_json


BRAINSCOPE_RESOURCE_URL = "https://brainscope.gersteinlab.org"
BRAINSCOPE_DEG_COMBINED_URL = (
    f"{BRAINSCOPE_RESOURCE_URL}/data/DEG-combined/Schizophrenia_DEGcombined.csv"
)
BRAINSCOPE_GRN_ZIP_URL = f"{BRAINSCOPE_RESOURCE_URL}/GRNs.zip"
PSYCHENCODE_MATCH_RULE = (
    "Exact official gene-symbol match only against BrainSCOPE DEG `gene` and GRN `TG` "
    "columns. Do not infer aliases without a curated, source-backed one-to-one exception."
)
PSYCHENCODE_MODULE_MIN_MEMBER_GENE_COUNT = 2

TextTransport = Callable[[str], str]
BytesTransport = Callable[[str], bytes]


class PsychENCODEError(RuntimeError):
    """Raised when official BrainSCOPE source content is missing or malformed."""


def live_text_transport(url: str) -> str:
    try:
        with urlopen(url) as response:
            return response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise PsychENCODEError(f"BrainSCOPE HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise PsychENCODEError(f"BrainSCOPE connection error: {exc.reason}") from exc


def live_bytes_transport(url: str) -> bytes:
    try:
        with urlopen(url) as response:
            return response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise PsychENCODEError(f"BrainSCOPE HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise PsychENCODEError(f"BrainSCOPE connection error: {exc.reason}") from exc


def normalize_gene_key(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().upper()


def as_float(value: str | None) -> float | None:
    if value in {None, "", "NA"}:
        return None
    return float(value)


def pvalue_signal(p_value: float | None, scale: float) -> float:
    if p_value is None or p_value <= 0:
        return 0.0
    return min(-math.log10(max(p_value, 1e-300)) / scale, 1.0)


def magnitude_signal(value: float | None, scale: float) -> float:
    if value is None:
        return 0.0
    return min(abs(value) / scale, 1.0)


def edge_weight_signal(value: float | None, scale: float) -> float:
    if value is None or value <= 0:
        return 0.0
    return min(math.log10(value + 1.0) / scale, 1.0)


def mean_or_zero(values: list[float]) -> float:
    if not values:
        return 0.0
    return mean(values)


def compute_deg_support(rows: list[dict[str, str]]) -> dict[str, object]:
    scored_rows: list[dict[str, object]] = []
    min_padj: float | None = None
    min_pvalue: float | None = None
    max_abs_log2_fold_change = 0.0

    for row in rows:
        pvalue = as_float(row.get("pvalue"))
        padj = as_float(row.get("padj"))
        significance_value = padj if padj is not None else pvalue
        significance = pvalue_signal(significance_value, scale=4.0)
        abs_log2_fold_change = abs(as_float(row.get("log2FoldChange")) or 0.0)
        effect = magnitude_signal(abs_log2_fold_change, scale=1.5)
        row_score = significance * effect
        cell_type = row.get("cell_type", "").strip()
        scored_rows.append(
            {
                "cell_type": cell_type,
                "row_score": row_score,
                "significance": significance,
                "effect": effect,
                "pvalue": pvalue,
                "padj": padj,
                "abs_log2_fold_change": abs_log2_fold_change,
            }
        )
        if pvalue is not None:
            min_pvalue = pvalue if min_pvalue is None else min(min_pvalue, pvalue)
        if padj is not None:
            min_padj = padj if min_padj is None else min(min_padj, padj)
        max_abs_log2_fold_change = max(max_abs_log2_fold_change, abs_log2_fold_change)

    if not scored_rows:
        return {
            "cell_state_support": 0.0,
            "psychencode_deg_strength_signal": 0.0,
            "psychencode_deg_breadth_signal": 0.0,
            "psychencode_deg_row_count": 0,
            "psychencode_deg_cell_type_count": 0,
            "psychencode_deg_best_pvalue": "",
            "psychencode_deg_best_padj": "",
            "psychencode_deg_max_abs_log2_fold_change": "",
            "psychencode_deg_top_cell_types_json": json.dumps([], sort_keys=True),
        }

    best_by_cell_type: dict[str, dict[str, object]] = {}
    for scored_row in scored_rows:
        cell_type = str(scored_row["cell_type"])
        if not cell_type:
            continue
        current = best_by_cell_type.get(cell_type)
        if current is None or float(scored_row["row_score"]) > float(current["row_score"]):
            best_by_cell_type[cell_type] = scored_row

    ranked_cell_types = sorted(
        best_by_cell_type.values(),
        key=lambda row: float(row["row_score"]),
        reverse=True,
    )
    top_scores = [float(row["row_score"]) for row in ranked_cell_types[:3]]
    best_score = top_scores[0] if top_scores else 0.0
    mean_top_score = mean_or_zero(top_scores)
    strength_signal = (0.6 * best_score) + (0.4 * mean_top_score)
    breadth_hits = sum(score >= 0.08 for score in (row["row_score"] for row in ranked_cell_types))
    breadth_signal = min(breadth_hits / 6.0, 1.0)
    support = round((0.8 * strength_signal) + (0.2 * breadth_signal), 6)

    top_cell_types = [
        {
            "cell_type": row["cell_type"],
            "row_score": round(float(row["row_score"]), 6),
            "pvalue": row["pvalue"],
            "padj": row["padj"],
            "abs_log2_fold_change": round(float(row["abs_log2_fold_change"]), 6),
        }
        for row in ranked_cell_types[:5]
    ]
    return {
        "cell_state_support": support,
        "psychencode_deg_strength_signal": round(strength_signal, 6),
        "psychencode_deg_breadth_signal": round(breadth_signal, 6),
        "psychencode_deg_row_count": len(scored_rows),
        "psychencode_deg_cell_type_count": len(best_by_cell_type),
        "psychencode_deg_best_pvalue": min_pvalue if min_pvalue is not None else "",
        "psychencode_deg_best_padj": min_padj if min_padj is not None else "",
        "psychencode_deg_max_abs_log2_fold_change": round(max_abs_log2_fold_change, 6),
        "psychencode_deg_top_cell_types_json": json.dumps(top_cell_types, sort_keys=True),
    }


def parse_grn_members(
    zip_bytes: bytes,
    allowed_gene_keys: set[str],
) -> tuple[dict[str, list[dict[str, str]]], int]:
    gene_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    with ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        member_names = sorted(
            name
            for name in zip_file.namelist()
            if name.endswith("_GRN.txt")
        )
        if not member_names:
            raise PsychENCODEError("BrainSCOPE GRN zip did not contain *_GRN.txt members.")
        for member_name in member_names:
            with zip_file.open(member_name) as handle:
                text_handle = io.TextIOWrapper(handle, encoding="utf-8", errors="replace")
                reader = csv.DictReader(text_handle, delimiter="\t")
                for row in reader:
                    gene_key = normalize_gene_key(row.get("TG"))
                    if gene_key in allowed_gene_keys:
                        gene_rows[gene_key].append(row)
    return gene_rows, len(member_names)


def compute_grn_support(rows: list[dict[str, str]]) -> dict[str, object]:
    if not rows:
        return {
            "developmental_regulatory_support": 0.0,
            "psychencode_grn_strength_signal": 0.0,
            "psychencode_grn_breadth_signal": 0.0,
            "psychencode_grn_tf_diversity_signal": 0.0,
            "psychencode_grn_edge_count_signal": 0.0,
            "psychencode_grn_edge_count": 0,
            "psychencode_grn_unique_tf_count": 0,
            "psychencode_grn_cell_type_count": 0,
            "psychencode_grn_max_edge_weight": "",
            "psychencode_grn_top_cell_types_json": json.dumps([], sort_keys=True),
            "psychencode_grn_top_tfs_json": json.dumps([], sort_keys=True),
            "psychencode_grn_regulation_breakdown_json": json.dumps({}, sort_keys=True),
        }

    cell_type_weights: dict[str, list[float]] = defaultdict(list)
    cell_type_tfs: dict[str, set[str]] = defaultdict(set)
    tf_weights: dict[str, list[float]] = defaultdict(list)
    regulation_counts: Counter[str] = Counter()
    edge_weights: list[float] = []

    for row in rows:
        edge_weight = as_float(row.get("edgeWeight")) or 0.0
        cell_type = row.get("celltype", "").strip()
        tf_name = row.get("TF", "").strip()
        regulation = row.get("Regulation", "").strip() or "Unknown"
        if cell_type:
            cell_type_weights[cell_type].append(edge_weight)
            if tf_name:
                cell_type_tfs[cell_type].add(tf_name)
        if tf_name:
            tf_weights[tf_name].append(edge_weight)
        regulation_counts[regulation] += 1
        edge_weights.append(edge_weight)

    def cell_type_score(cell_type: str) -> float:
        weights = cell_type_weights[cell_type]
        max_signal = edge_weight_signal(max(weights), scale=1.6)
        mean_top_signal = edge_weight_signal(
            mean_or_zero(sorted(weights, reverse=True)[:5]),
            scale=1.3,
        )
        tf_signal = min(math.log10(len(cell_type_tfs[cell_type]) + 1) / 2.0, 1.0)
        return (0.5 * max_signal) + (0.3 * mean_top_signal) + (0.2 * tf_signal)

    ranked_cell_types = sorted(
        (
            {
                "cell_type": cell_type,
                "score": cell_type_score(cell_type),
                "edge_count": len(cell_type_weights[cell_type]),
                "max_edge_weight": max(cell_type_weights[cell_type]),
                "unique_tf_count": len(cell_type_tfs[cell_type]),
            }
            for cell_type in cell_type_weights
        ),
        key=lambda row: float(row["score"]),
        reverse=True,
    )
    cell_type_scores = [float(row["score"]) for row in ranked_cell_types[:3]]
    top_score = float(ranked_cell_types[0]["score"]) if ranked_cell_types else 0.0
    mean_top_score = mean_or_zero(cell_type_scores)
    strength_signal = (0.6 * top_score) + (0.4 * mean_top_score)
    breadth_signal = min(len(cell_type_weights) / 12.0, 1.0)
    tf_diversity_signal = min(math.log10(len(tf_weights) + 1) / 2.5, 1.0)
    edge_count_signal = min(math.log10(len(edge_weights) + 1) / 4.0, 1.0)
    support = round(
        (0.55 * strength_signal)
        + (0.15 * breadth_signal)
        + (0.15 * tf_diversity_signal)
        + (0.15 * edge_count_signal),
        6,
    )

    top_cell_types = [
        {
            "cell_type": row["cell_type"],
            "score": round(float(row["score"]), 6),
            "edge_count": int(row["edge_count"]),
            "max_edge_weight": round(float(row["max_edge_weight"]), 6),
            "unique_tf_count": int(row["unique_tf_count"]),
        }
        for row in ranked_cell_types[:5]
    ]
    ranked_tfs = sorted(
        (
            {
                "tf": tf_name,
                "edge_count": len(weights),
                "max_edge_weight": max(weights),
                "mean_edge_weight": mean_or_zero(weights),
            }
            for tf_name, weights in tf_weights.items()
        ),
        key=lambda row: (float(row["max_edge_weight"]), int(row["edge_count"])),
        reverse=True,
    )
    top_tfs = [
        {
            "tf": row["tf"],
            "edge_count": int(row["edge_count"]),
            "max_edge_weight": round(float(row["max_edge_weight"]), 6),
            "mean_edge_weight": round(float(row["mean_edge_weight"]), 6),
        }
        for row in ranked_tfs[:10]
    ]
    return {
        "developmental_regulatory_support": support,
        "psychencode_grn_strength_signal": round(strength_signal, 6),
        "psychencode_grn_breadth_signal": round(breadth_signal, 6),
        "psychencode_grn_tf_diversity_signal": round(tf_diversity_signal, 6),
        "psychencode_grn_edge_count_signal": round(edge_count_signal, 6),
        "psychencode_grn_edge_count": len(edge_weights),
        "psychencode_grn_unique_tf_count": len(tf_weights),
        "psychencode_grn_cell_type_count": len(cell_type_weights),
        "psychencode_grn_max_edge_weight": round(max(edge_weights), 6) if edge_weights else "",
        "psychencode_grn_top_cell_types_json": json.dumps(top_cell_types, sort_keys=True),
        "psychencode_grn_top_tfs_json": json.dumps(top_tfs, sort_keys=True),
        "psychencode_grn_regulation_breakdown_json": json.dumps(
            dict(sorted(regulation_counts.items())),
            sort_keys=True,
        ),
    }


def fetch_psychencode_support(
    input_file: Path,
    output_file: Path,
    limit: int | None = None,
    text_transport: TextTransport | None = None,
    bytes_transport: BytesTransport | None = None,
) -> dict[str, object]:
    text_transport = text_transport or live_text_transport
    bytes_transport = bytes_transport or live_bytes_transport

    input_rows = read_csv_rows(input_file)
    if limit is not None:
        input_rows = input_rows[:limit]

    input_by_gene = {
        normalize_gene_key(row.get("entity_label")): row
        for row in input_rows
        if normalize_gene_key(row.get("entity_label"))
    }
    if not input_by_gene:
        raise PsychENCODEError("Input file did not contain entity_label values.")

    deg_text = text_transport(BRAINSCOPE_DEG_COMBINED_URL)
    deg_rows_by_gene: dict[str, list[dict[str, str]]] = defaultdict(list)
    deg_reader = csv.DictReader(io.StringIO(deg_text))
    for row in deg_reader:
        gene_key = normalize_gene_key(row.get("gene"))
        if gene_key in input_by_gene:
            deg_rows_by_gene[gene_key].append(row)

    grn_rows_by_gene, grn_member_count = parse_grn_members(
        bytes_transport(BRAINSCOPE_GRN_ZIP_URL),
        set(input_by_gene),
    )

    output_rows: list[dict[str, object]] = []
    deg_match_count = 0
    grn_match_count = 0
    unmatched_genes: list[dict[str, str]] = []

    for gene_key, input_row in input_by_gene.items():
        deg_rows = deg_rows_by_gene.get(gene_key, [])
        grn_rows = grn_rows_by_gene.get(gene_key, [])
        if not deg_rows and not grn_rows:
            unmatched_genes.append(
                {
                    "entity_id": input_row.get("entity_id", "").strip(),
                    "entity_label": input_row.get("entity_label", "").strip(),
                    "approved_name": input_row.get("approved_name", "").strip(),
                    "psychencode_match_status": "absent_from_deg_and_grn",
                    "reason": (
                        "No exact BrainSCOPE schizophrenia DEG `gene` or GRN `TG` symbol "
                        "matched this input gene."
                    ),
                }
            )
            continue

        deg_support = compute_deg_support(deg_rows)
        grn_support = compute_grn_support(grn_rows)
        if deg_rows:
            deg_match_count += 1
        if grn_rows:
            grn_match_count += 1

        if deg_rows and grn_rows:
            match_status = "matched_deg_and_grn"
        elif deg_rows:
            match_status = "matched_deg_only"
        else:
            match_status = "matched_grn_only"

        output_rows.append(
            {
                "entity_id": input_row.get("entity_id", "").strip(),
                "entity_label": input_row.get("entity_label", "").strip(),
                "approved_name": input_row.get("approved_name", "").strip(),
                "cell_state_support": deg_support["cell_state_support"],
                "developmental_regulatory_support": grn_support[
                    "developmental_regulatory_support"
                ],
                "psychencode_match_status": match_status,
                **deg_support,
                **grn_support,
            }
        )

    output_rows.sort(key=lambda row: str(row["entity_label"]).lower())
    fieldnames = [
        "entity_id",
        "entity_label",
        "approved_name",
        "cell_state_support",
        "developmental_regulatory_support",
        "psychencode_match_status",
        "psychencode_deg_strength_signal",
        "psychencode_deg_breadth_signal",
        "psychencode_deg_row_count",
        "psychencode_deg_cell_type_count",
        "psychencode_deg_best_pvalue",
        "psychencode_deg_best_padj",
        "psychencode_deg_max_abs_log2_fold_change",
        "psychencode_deg_top_cell_types_json",
        "psychencode_grn_strength_signal",
        "psychencode_grn_breadth_signal",
        "psychencode_grn_tf_diversity_signal",
        "psychencode_grn_edge_count_signal",
        "psychencode_grn_edge_count",
        "psychencode_grn_unique_tf_count",
        "psychencode_grn_cell_type_count",
        "psychencode_grn_max_edge_weight",
        "psychencode_grn_top_cell_types_json",
        "psychencode_grn_top_tfs_json",
        "psychencode_grn_regulation_breakdown_json",
    ]
    write_csv(output_file, output_rows, fieldnames)

    metadata = {
        "source": "BrainSCOPE / PsychENCODE public resources",
        "resource_url": BRAINSCOPE_RESOURCE_URL,
        "deg_combined_url": BRAINSCOPE_DEG_COMBINED_URL,
        "grn_zip_url": BRAINSCOPE_GRN_ZIP_URL,
        "matching_rule": PSYCHENCODE_MATCH_RULE,
        "input_file": str(input_file),
        "output_file": str(output_file),
        "input_row_count": len(input_rows),
        "unique_input_gene_count": len(input_by_gene),
        "row_count": len(output_rows),
        "matched_gene_count": len(output_rows),
        "unmatched_gene_count": len(unmatched_genes),
        "unmatched_gene_labels": [
            unmatched_gene["entity_label"] for unmatched_gene in unmatched_genes
        ],
        "unmatched_genes": unmatched_genes,
        "deg_match_count": deg_match_count,
        "grn_match_count": grn_match_count,
        "grn_member_count": grn_member_count,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata


def slugify_module_key(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "unknown"


def mean_present_gene_score(row: dict[str, str], columns: tuple[str, ...]) -> float:
    values = [
        score
        for score in (as_float(row.get(column)) for column in columns)
        if score is not None
    ]
    if not values:
        return 0.0
    return mean(values)


def score_deg_row_for_module(row: dict[str, str]) -> float:
    pvalue = as_float(row.get("pvalue"))
    padj = as_float(row.get("padj"))
    significance_value = padj if padj is not None else pvalue
    significance = pvalue_signal(significance_value, scale=4.0)
    abs_log2_fold_change = abs(as_float(row.get("log2FoldChange")) or 0.0)
    effect = magnitude_signal(abs_log2_fold_change, scale=1.5)
    return significance * effect


def parse_grn_rows_by_cell_type(
    zip_bytes: bytes,
    allowed_gene_keys: set[str],
) -> tuple[dict[str, list[dict[str, str]]], int]:
    cell_type_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    with ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        member_names = sorted(
            name
            for name in zip_file.namelist()
            if name.endswith("_GRN.txt")
        )
        if not member_names:
            raise PsychENCODEError("BrainSCOPE GRN zip did not contain *_GRN.txt members.")
        for member_name in member_names:
            with zip_file.open(member_name) as handle:
                text_handle = io.TextIOWrapper(handle, encoding="utf-8", errors="replace")
                reader = csv.DictReader(text_handle, delimiter="\t")
                for row in reader:
                    gene_key = normalize_gene_key(row.get("TG"))
                    cell_type = row.get("celltype", "").strip()
                    if gene_key in allowed_gene_keys and cell_type:
                        cell_type_rows[cell_type].append(row)
    return cell_type_rows, len(member_names)


def build_module_member_gene_entries(
    member_gene_keys: set[str],
    gene_rows_by_key: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    entries = []
    for gene_key in member_gene_keys:
        gene_row = gene_rows_by_key[gene_key]
        genetic_score = mean_present_gene_score(
            gene_row,
            ("common_variant_support", "rare_variant_support"),
        )
        entries.append(
            {
                "entity_id": gene_row.get("entity_id", "").strip(),
                "entity_label": gene_row.get("entity_label", "").strip(),
                "approved_name": gene_row.get("approved_name", "").strip(),
                "genetic_score": genetic_score,
            }
        )
    return sorted(
        entries,
        key=lambda entry: (
            -float(entry["genetic_score"]),
            str(entry["entity_label"]),
            str(entry["entity_id"]),
        ),
    )


def compute_module_gene_enrichment(member_gene_entries: list[dict[str, object]]) -> float:
    if not member_gene_entries:
        return 0.0
    genetic_scores = [float(entry["genetic_score"]) for entry in member_gene_entries]
    top_mean = mean_or_zero(genetic_scores[:5])
    breadth = sum(score >= 0.2 for score in genetic_scores) / len(genetic_scores)
    return round((0.75 * top_mean) + (0.25 * breadth), 6)


def compute_module_cell_state_specificity(
    deg_rows: list[dict[str, str]],
) -> tuple[float, list[dict[str, object]]]:
    best_by_gene: dict[str, dict[str, object]] = {}
    for row in deg_rows:
        gene_key = normalize_gene_key(row.get("gene"))
        if not gene_key:
            continue
        gene_score = score_deg_row_for_module(row)
        candidate = {
            "entity_label": row.get("gene", "").strip(),
            "row_score": gene_score,
            "pvalue": as_float(row.get("pvalue")),
            "padj": as_float(row.get("padj")),
            "abs_log2_fold_change": abs(as_float(row.get("log2FoldChange")) or 0.0),
        }
        current = best_by_gene.get(gene_key)
        if current is None or float(candidate["row_score"]) > float(current["row_score"]):
            best_by_gene[gene_key] = candidate

    ranked_entries = sorted(
        best_by_gene.values(),
        key=lambda entry: (
            -float(entry["row_score"]),
            str(entry["entity_label"]),
        ),
    )
    if not ranked_entries:
        return 0.0, []

    top_scores = [float(entry["row_score"]) for entry in ranked_entries[:5]]
    best_score = top_scores[0] if top_scores else 0.0
    strength = (0.6 * best_score) + (0.4 * mean_or_zero(top_scores))
    breadth = min(len(ranked_entries) / 6.0, 1.0)
    support = round((0.8 * strength) + (0.2 * breadth), 6)

    top_gene_entries = [
        {
            "entity_label": entry["entity_label"],
            "row_score": round(float(entry["row_score"]), 6),
            "pvalue": entry["pvalue"],
            "padj": entry["padj"],
            "abs_log2_fold_change": round(float(entry["abs_log2_fold_change"]), 6),
        }
        for entry in ranked_entries[:10]
    ]
    return support, top_gene_entries


def compute_module_regulatory_relevance(
    grn_rows: list[dict[str, str]],
) -> tuple[float, list[dict[str, object]], int, int]:
    if not grn_rows:
        return 0.0, [], 0, 0

    edge_weights: list[float] = []
    tf_weights: dict[str, list[float]] = defaultdict(list)
    target_gene_keys: set[str] = set()
    for row in grn_rows:
        edge_weight = as_float(row.get("edgeWeight")) or 0.0
        tf_name = row.get("TF", "").strip()
        target_gene_key = normalize_gene_key(row.get("TG"))
        if target_gene_key:
            target_gene_keys.add(target_gene_key)
        edge_weights.append(edge_weight)
        if tf_name:
            tf_weights[tf_name].append(edge_weight)

    top_edge_weights = sorted(edge_weights, reverse=True)[:10]
    max_signal = edge_weight_signal(max(edge_weights), scale=1.6)
    mean_top_signal = edge_weight_signal(mean_or_zero(top_edge_weights), scale=1.3)
    strength = (0.6 * max_signal) + (0.4 * mean_top_signal)
    breadth = min(len(target_gene_keys) / 8.0, 1.0)
    tf_diversity = min(math.log10(len(tf_weights) + 1) / 2.0, 1.0)
    support = round((0.5 * strength) + (0.25 * breadth) + (0.25 * tf_diversity), 6)

    ranked_tfs = sorted(
        (
            {
                "tf": tf_name,
                "edge_count": len(weights),
                "max_edge_weight": max(weights),
                "mean_edge_weight": mean_or_zero(weights),
            }
            for tf_name, weights in tf_weights.items()
        ),
        key=lambda entry: (
            -float(entry["max_edge_weight"]),
            -int(entry["edge_count"]),
            str(entry["tf"]),
        ),
    )
    top_tfs = [
        {
            "tf": entry["tf"],
            "edge_count": int(entry["edge_count"]),
            "max_edge_weight": round(float(entry["max_edge_weight"]), 6),
            "mean_edge_weight": round(float(entry["mean_edge_weight"]), 6),
        }
        for entry in ranked_tfs[:10]
    ]
    return support, top_tfs, len(target_gene_keys), len(tf_weights)


def fetch_psychencode_module_table(
    input_file: Path,
    output_file: Path,
    limit: int | None = None,
    text_transport: TextTransport | None = None,
    bytes_transport: BytesTransport | None = None,
) -> dict[str, object]:
    text_transport = text_transport or live_text_transport
    bytes_transport = bytes_transport or live_bytes_transport

    input_rows = read_csv_rows(input_file)
    if limit is not None:
        input_rows = input_rows[:limit]

    gene_rows_by_key = {
        normalize_gene_key(row.get("entity_label")): row
        for row in input_rows
        if normalize_gene_key(row.get("entity_label"))
    }
    if not gene_rows_by_key:
        raise PsychENCODEError("Input file did not contain entity_label values.")

    deg_text = text_transport(BRAINSCOPE_DEG_COMBINED_URL)
    deg_rows_by_cell_type: dict[str, list[dict[str, str]]] = defaultdict(list)
    deg_reader = csv.DictReader(io.StringIO(deg_text))
    for row in deg_reader:
        gene_key = normalize_gene_key(row.get("gene"))
        cell_type = row.get("cell_type", "").strip()
        if gene_key in gene_rows_by_key and cell_type:
            deg_rows_by_cell_type[cell_type].append(row)

    grn_rows_by_cell_type, grn_member_count = parse_grn_rows_by_cell_type(
        bytes_transport(BRAINSCOPE_GRN_ZIP_URL),
        set(gene_rows_by_key),
    )

    output_rows: list[dict[str, object]] = []
    skipped_cell_types: list[str] = []
    candidate_cell_types = sorted(set(deg_rows_by_cell_type) | set(grn_rows_by_cell_type))
    for cell_type in candidate_cell_types:
        deg_rows = deg_rows_by_cell_type.get(cell_type, [])
        grn_rows = grn_rows_by_cell_type.get(cell_type, [])
        member_gene_keys = {
            normalize_gene_key(row.get("gene"))
            for row in deg_rows
            if normalize_gene_key(row.get("gene")) in gene_rows_by_key
        }
        member_gene_keys.update(
            normalize_gene_key(row.get("TG"))
            for row in grn_rows
            if normalize_gene_key(row.get("TG")) in gene_rows_by_key
        )
        if len(member_gene_keys) < PSYCHENCODE_MODULE_MIN_MEMBER_GENE_COUNT:
            skipped_cell_types.append(cell_type)
            continue

        member_gene_entries = build_module_member_gene_entries(member_gene_keys, gene_rows_by_key)
        cell_state_specificity, top_deg_gene_entries = compute_module_cell_state_specificity(
            deg_rows
        )
        developmental_regulatory_relevance, top_tfs, grn_target_gene_count, unique_tf_count = (
            compute_module_regulatory_relevance(grn_rows)
        )
        entity_id = f"psychencode:{slugify_module_key(cell_type)}"
        output_rows.append(
            {
                "entity_id": entity_id,
                "entity_label": f"BrainSCOPE {cell_type}",
                "member_gene_genetic_enrichment": compute_module_gene_enrichment(
                    member_gene_entries
                ),
                "cell_state_specificity": cell_state_specificity,
                "developmental_regulatory_relevance": developmental_regulatory_relevance,
                "module_source": "BrainSCOPE / PsychENCODE",
                "psychencode_module_cell_type": cell_type,
                "psychencode_module_member_gene_count": len(member_gene_entries),
                "psychencode_module_deg_gene_count": len(
                    {
                        normalize_gene_key(row.get("gene"))
                        for row in deg_rows
                        if normalize_gene_key(row.get("gene"))
                    }
                ),
                "psychencode_module_grn_target_gene_count": grn_target_gene_count,
                "psychencode_module_grn_edge_count": len(grn_rows),
                "psychencode_module_unique_tf_count": unique_tf_count,
                "psychencode_module_member_genes_json": json.dumps(
                    [entry["entity_label"] for entry in member_gene_entries],
                    sort_keys=True,
                ),
                "psychencode_module_top_member_genes_json": json.dumps(
                    [
                        {
                            "entity_id": entry["entity_id"],
                            "entity_label": entry["entity_label"],
                            "approved_name": entry["approved_name"],
                            "genetic_score": round(float(entry["genetic_score"]), 6),
                        }
                        for entry in member_gene_entries[:10]
                    ],
                    sort_keys=True,
                ),
                "psychencode_module_top_deg_genes_json": json.dumps(
                    top_deg_gene_entries,
                    sort_keys=True,
                ),
                "psychencode_module_top_tfs_json": json.dumps(top_tfs, sort_keys=True),
            }
        )

    output_rows.sort(key=lambda row: str(row["entity_label"]).lower())
    fieldnames = [
        "entity_id",
        "entity_label",
        "member_gene_genetic_enrichment",
        "cell_state_specificity",
        "developmental_regulatory_relevance",
        "module_source",
        "psychencode_module_cell_type",
        "psychencode_module_member_gene_count",
        "psychencode_module_deg_gene_count",
        "psychencode_module_grn_target_gene_count",
        "psychencode_module_grn_edge_count",
        "psychencode_module_unique_tf_count",
        "psychencode_module_member_genes_json",
        "psychencode_module_top_member_genes_json",
        "psychencode_module_top_deg_genes_json",
        "psychencode_module_top_tfs_json",
    ]
    write_csv(output_file, output_rows, fieldnames)

    metadata = {
        "source": "BrainSCOPE / PsychENCODE public resources",
        "resource_url": BRAINSCOPE_RESOURCE_URL,
        "deg_combined_url": BRAINSCOPE_DEG_COMBINED_URL,
        "grn_zip_url": BRAINSCOPE_GRN_ZIP_URL,
        "input_file": str(input_file),
        "output_file": str(output_file),
        "input_gene_count": len(gene_rows_by_key),
        "row_count": len(output_rows),
        "candidate_cell_type_count": len(candidate_cell_types),
        "deg_cell_type_count": len(deg_rows_by_cell_type),
        "grn_cell_type_count": len(grn_rows_by_cell_type),
        "grn_member_count": grn_member_count,
        "minimum_member_gene_count": PSYCHENCODE_MODULE_MIN_MEMBER_GENE_COUNT,
        "skipped_cell_type_count": len(skipped_cell_types),
        "skipped_cell_types": skipped_cell_types,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata
