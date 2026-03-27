from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from scz_target_engine.io import read_csv_rows, write_csv, write_json


CHEMBL_API_BASE = "https://www.ebi.ac.uk/chembl/api/data"

JsonTransport = Callable[[str], dict[str, object]]


class ChEMBLError(RuntimeError):
    """Raised when the ChEMBL API returns an unexpected response."""


def live_json_transport(url: str) -> dict[str, object]:
    try:
        with urlopen(url) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ChEMBLError(f"ChEMBL HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise ChEMBLError(f"ChEMBL connection error: {exc.reason}") from exc


def build_url(endpoint: str, **params: object) -> str:
    cleaned = {
        key: value
        for key, value in params.items()
        if value is not None
    }
    query = urlencode(cleaned)
    return f"{CHEMBL_API_BASE}/{endpoint}.json?{query}" if query else f"{CHEMBL_API_BASE}/{endpoint}.json"


def extract_gene_symbols(target: dict[str, object]) -> set[str]:
    symbols: set[str] = set()
    for component in target.get("target_components", []):
        for synonym in component.get("target_component_synonyms", []):
            if synonym.get("syn_type") == "GENE_SYMBOL" and synonym.get("component_synonym"):
                symbols.add(str(synonym["component_synonym"]).upper())
    return symbols


def target_type_priority(target_type: str) -> int:
    priorities = {
        "SINGLE PROTEIN": 5,
        "PROTEIN FAMILY": 4,
        "PROTEIN COMPLEX": 3,
        "PROTEIN COMPLEX GROUP": 2,
        "PROTEIN-PROTEIN INTERACTION": 1,
    }
    return priorities.get(target_type.upper(), 0)


def search_targets_by_symbol(
    gene_symbol: str,
    transport: JsonTransport,
    limit: int = 20,
) -> list[dict[str, object]]:
    url = build_url("target/search", q=gene_symbol, limit=limit)
    payload = transport(url)
    return list(payload.get("targets", []))


def select_best_target(
    gene_symbol: str,
    targets: list[dict[str, object]],
) -> tuple[dict[str, object] | None, dict[str, object]]:
    upper_symbol = gene_symbol.upper()
    candidates = []
    for target in targets:
        symbols = extract_gene_symbols(target)
        human = str(target.get("organism", "")).lower() == "homo sapiens"
        exact_symbol = upper_symbol in symbols
        candidates.append(
            {
                "target": target,
                "human": human,
                "exact_symbol": exact_symbol,
                "target_type_priority": target_type_priority(str(target.get("target_type", ""))),
                "search_score": float(target.get("score", 0.0)),
            }
        )

    exact_human = [item for item in candidates if item["human"] and item["exact_symbol"]]
    if exact_human:
        exact_human.sort(
            key=lambda item: (
                -int(item["exact_symbol"]),
                -int(item["human"]),
                -int(item["target_type_priority"]),
                -float(item["search_score"]),
                str(item["target"].get("pref_name", "")).lower(),
            )
        )
        return exact_human[0]["target"], {
            "match_status": "matched_exact_human_gene_symbol",
            "candidate_count": len(exact_human),
        }

    return None, {
        "match_status": "no_exact_human_gene_symbol_match",
        "candidate_count": len(candidates),
    }


def fetch_target_details(
    target_chembl_id: str,
    transport: JsonTransport,
) -> dict[str, object]:
    url = build_url(f"target/{target_chembl_id}")
    return transport(url)


def fetch_activity_stats(
    target_chembl_id: str,
    transport: JsonTransport,
) -> dict[str, object]:
    url = build_url("activity", target_chembl_id=target_chembl_id, limit=1)
    payload = transport(url)
    page_meta = payload.get("page_meta", {})
    first = payload.get("activities", [])
    return {
        "activity_count": int(page_meta.get("total_count", 0)),
        "first_activity_type": first[0].get("standard_type") if first else None,
    }


def fetch_mechanism_stats(
    target_chembl_id: str,
    transport: JsonTransport,
    limit: int = 1000,
) -> dict[str, object]:
    url = build_url("mechanism", target_chembl_id=target_chembl_id, limit=limit)
    payload = transport(url)
    mechanisms = payload.get("mechanisms", [])
    page_meta = payload.get("page_meta", {})
    max_phase = max(
        (int(mechanism.get("max_phase", 0) or 0) for mechanism in mechanisms),
        default=0,
    )
    action_types = sorted(
        {
            str(mechanism["action_type"])
            for mechanism in mechanisms
            if mechanism.get("action_type")
        }
    )
    return {
        "mechanism_count": int(page_meta.get("total_count", 0)),
        "mechanism_rows_fetched": len(mechanisms),
        "mechanism_result_truncated": int(page_meta.get("total_count", 0)) > len(mechanisms),
        "max_phase": max_phase,
        "action_types": action_types,
    }


def compute_match_confidence(target: dict[str, object]) -> float:
    human = str(target.get("organism", "")).lower() == "homo sapiens"
    type_score = target_type_priority(str(target.get("target_type", "")))
    if human and type_score >= 5:
        return 1.0
    if human and type_score >= 3:
        return 0.85
    if human:
        return 0.7
    return 0.0


def target_type_signal(target_type: str) -> float:
    mapping = {
        "SINGLE PROTEIN": 1.0,
        "PROTEIN FAMILY": 0.8,
        "PROTEIN COMPLEX": 0.55,
        "PROTEIN COMPLEX GROUP": 0.45,
        "PROTEIN-PROTEIN INTERACTION": 0.35,
    }
    return mapping.get(target_type.upper(), 0.25)


def activity_signal(activity_count: int) -> float:
    if activity_count <= 0:
        return 0.0
    return min(math.log10(activity_count + 1) / 4.5, 1.0)


def mechanism_signal(mechanism_count: int) -> float:
    if mechanism_count <= 0:
        return 0.0
    return min(math.log10(mechanism_count + 1) / 2.0, 1.0)


def compute_tractability_compoundability(
    target_type: str,
    activity_count: int,
    mechanism_count: int,
    max_phase: int,
) -> float:
    phase_signal = max(0.0, min(max_phase / 4.0, 1.0))
    score = (
        (0.40 * phase_signal)
        + (0.35 * activity_signal(activity_count))
        + (0.15 * mechanism_signal(mechanism_count))
        + (0.10 * target_type_signal(target_type))
    )
    return round(score, 6)


def fetch_chembl_tractability(
    input_file: Path,
    output_file: Path,
    limit: int | None = None,
    transport: JsonTransport | None = None,
) -> dict[str, object]:
    transport = transport or live_json_transport
    input_rows = read_csv_rows(input_file)
    if limit is not None:
        input_rows = input_rows[:limit]

    cache: dict[str, dict[str, object]] = {}
    output_rows: list[dict[str, object]] = []

    for row in input_rows:
        entity_id = row.get("entity_id", "").strip()
        gene_symbol = row.get("entity_label", "").strip()
        approved_name = row.get("approved_name", "").strip()
        if not entity_id or not gene_symbol:
            raise ValueError("ChEMBL input rows require entity_id and entity_label.")

        if gene_symbol not in cache:
            targets = search_targets_by_symbol(gene_symbol, transport)
            selected, match_meta = select_best_target(gene_symbol, targets)
            cached: dict[str, object] = {
                "chembl_match_status": match_meta["match_status"],
                "chembl_search_candidate_count": match_meta["candidate_count"],
                "chembl_target_chembl_id": "",
                "chembl_pref_name": "",
                "chembl_target_type": "",
                "chembl_organism": "",
                "chembl_match_confidence": 0.0,
                "chembl_activity_count": 0,
                "chembl_mechanism_count": 0,
                "chembl_max_phase": 0,
                "chembl_mechanism_rows_fetched": 0,
                "chembl_mechanism_result_truncated": False,
                "chembl_action_types_json": "[]",
                "tractability_compoundability": 0.0,
            }
            if selected is not None:
                target_chembl_id = str(selected["target_chembl_id"])
                details = fetch_target_details(target_chembl_id, transport)
                activities = fetch_activity_stats(target_chembl_id, transport)
                mechanisms = fetch_mechanism_stats(target_chembl_id, transport)
                cached.update(
                    {
                        "chembl_target_chembl_id": target_chembl_id,
                        "chembl_pref_name": str(details.get("pref_name", "")),
                        "chembl_target_type": str(details.get("target_type", "")),
                        "chembl_organism": str(details.get("organism", "")),
                        "chembl_match_confidence": compute_match_confidence(details),
                        "chembl_activity_count": activities["activity_count"],
                        "chembl_mechanism_count": mechanisms["mechanism_count"],
                        "chembl_max_phase": mechanisms["max_phase"],
                        "chembl_mechanism_rows_fetched": mechanisms["mechanism_rows_fetched"],
                        "chembl_mechanism_result_truncated": mechanisms[
                            "mechanism_result_truncated"
                        ],
                        "chembl_action_types_json": json.dumps(
                            mechanisms["action_types"],
                            sort_keys=True,
                        ),
                        "tractability_compoundability": compute_tractability_compoundability(
                            str(details.get("target_type", "")),
                            int(activities["activity_count"]),
                            int(mechanisms["mechanism_count"]),
                            int(mechanisms["max_phase"]),
                        ),
                    }
                )
            cache[gene_symbol] = cached

        output_row = {
            "entity_id": entity_id,
            "entity_label": gene_symbol,
            "approved_name": approved_name,
            **cache[gene_symbol],
        }
        output_rows.append(output_row)

    fieldnames = [
        "entity_id",
        "entity_label",
        "approved_name",
        "chembl_match_status",
        "chembl_search_candidate_count",
        "chembl_target_chembl_id",
        "chembl_pref_name",
        "chembl_target_type",
        "chembl_organism",
        "chembl_match_confidence",
        "chembl_activity_count",
        "chembl_mechanism_count",
        "chembl_max_phase",
        "chembl_mechanism_rows_fetched",
        "chembl_mechanism_result_truncated",
        "chembl_action_types_json",
        "tractability_compoundability",
    ]
    write_csv(output_file, output_rows, fieldnames)
    metadata = {
        "source": "ChEMBL web services",
        "api_base": CHEMBL_API_BASE,
        "input_file": str(input_file),
        "output_file": str(output_file),
        "row_count": len(output_rows),
        "unique_symbols": len(cache),
        "limit": limit,
    }
    write_json(output_file.with_suffix(".metadata.json"), metadata)
    return metadata
