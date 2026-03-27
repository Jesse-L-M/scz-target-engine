from __future__ import annotations

import json
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scz_target_engine.io import write_csv, write_json


OPEN_TARGETS_GRAPHQL_URL = "https://api.platform.opentargets.org/api/v4/graphql"

GraphQLTransport = Callable[[str, dict[str, object]], dict[str, object]]


class OpenTargetsError(RuntimeError):
    """Raised when the Open Targets API returns an unexpected response."""


def live_graphql_transport(
    query: str,
    variables: dict[str, object],
    api_url: str = OPEN_TARGETS_GRAPHQL_URL,
) -> dict[str, object]:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request = Request(
        api_url,
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise OpenTargetsError(f"Open Targets HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise OpenTargetsError(f"Open Targets connection error: {exc.reason}") from exc

    if "errors" in data:
        raise OpenTargetsError(json.dumps(data["errors"], indent=2))
    if "data" not in data:
        raise OpenTargetsError("Open Targets response did not include a data payload.")
    return data["data"]


def fetch_meta(transport: GraphQLTransport) -> dict[str, object]:
    query = """
    query Meta {
      meta {
        name
        product
        apiVersion { x y z suffix }
        dataVersion { year month iteration }
      }
    }
    """
    return transport(query, {})["meta"]


def search_disease(
    disease_query: str,
    transport: GraphQLTransport,
) -> dict[str, str]:
    query = """
    query SearchDisease($queryString: String!) {
      search(queryString: $queryString, entityNames: ["disease"], page: {index: 0, size: 20}) {
        hits {
          id
          object {
            ... on Disease {
              id
              name
            }
          }
        }
      }
    }
    """
    payload = transport(query, {"queryString": disease_query})
    hits = payload["search"]["hits"]
    if not hits:
        raise OpenTargetsError(f"No disease hits found for query: {disease_query}")

    exact_matches = [
        hit["object"]
        for hit in hits
        if hit["object"]["name"].strip().lower() == disease_query.strip().lower()
    ]
    if exact_matches:
        return {"id": exact_matches[0]["id"], "name": exact_matches[0]["name"]}

    first = hits[0]["object"]
    return {"id": first["id"], "name": first["name"]}


def fetch_disease_associations(
    disease_id: str,
    transport: GraphQLTransport,
    page_size: int = 500,
    max_pages: int | None = None,
) -> tuple[dict[str, str], list[dict[str, object]]]:
    query = """
    query DiseaseAssociations($diseaseId: String!, $index: Int!, $size: Int!) {
      disease(efoId: $diseaseId) {
        id
        name
        associatedTargets(page: {index: $index, size: $size}) {
          count
          rows {
            score
            datatypeScores {
              id
              score
            }
            target {
              id
              approvedSymbol
              approvedName
            }
          }
        }
      }
    }
    """

    all_rows: list[dict[str, object]] = []
    disease_identity: dict[str, str] | None = None
    index = 0
    total_count = None

    while True:
        payload = transport(
            query,
            {"diseaseId": disease_id, "index": index, "size": page_size},
        )
        disease = payload.get("disease")
        if disease is None:
            raise OpenTargetsError(f"Disease ID not found in Open Targets: {disease_id}")

        disease_identity = {"id": disease["id"], "name": disease["name"]}
        associations = disease["associatedTargets"]
        total_count = associations["count"]
        rows = associations["rows"]
        all_rows.extend(rows)

        index += 1
        if len(all_rows) >= total_count:
            break
        if max_pages is not None and index >= max_pages:
            break
        if not rows:
            break

    if disease_identity is None:
        raise OpenTargetsError("Open Targets returned no disease identity.")

    return disease_identity, all_rows


def flatten_association_rows(
    disease_identity: dict[str, str],
    meta: dict[str, object],
    rows: list[dict[str, object]],
) -> tuple[list[str], list[dict[str, object]]]:
    datatype_keys = sorted(
        {
            score["id"]
            for row in rows
            for score in row.get("datatypeScores", [])
        }
    )
    api_version = meta["apiVersion"]
    data_version = meta["dataVersion"]
    api_version_str = ".".join(
        str(api_version[key]) for key in ("x", "y", "z") if api_version.get(key)
    )
    if api_version.get("suffix"):
        api_version_str = f"{api_version_str}{api_version['suffix']}"
    data_version_str = f"{data_version['year']}.{data_version['month']}"
    if data_version.get("iteration"):
        data_version_str = f"{data_version_str}.{data_version['iteration']}"

    output_rows: list[dict[str, object]] = []
    for row in rows:
        datatype_map = {
            score["id"]: score["score"]
            for score in row.get("datatypeScores", [])
        }
        target = row["target"]
        entity_label = target.get("approvedSymbol") or target.get("approvedName") or target["id"]
        output_row: dict[str, object] = {
            "entity_id": target["id"],
            "entity_label": entity_label,
            "approved_name": target.get("approvedName") or "",
            "generic_platform_baseline": round(float(row["score"]), 12),
            "opentargets_disease_id": disease_identity["id"],
            "opentargets_disease_name": disease_identity["name"],
            "opentargets_api_version": api_version_str,
            "opentargets_data_version": data_version_str,
            "opentargets_datatype_scores_json": json.dumps(
                datatype_map,
                sort_keys=True,
            ),
        }
        for datatype_key in datatype_keys:
            output_row[f"opentargets_datatype_{datatype_key}"] = datatype_map.get(
                datatype_key,
                "",
            )
        output_rows.append(output_row)

    output_rows.sort(
        key=lambda row: (
            -float(row["generic_platform_baseline"]),
            str(row["entity_label"]).lower(),
        )
    )

    fieldnames = [
        "entity_id",
        "entity_label",
        "approved_name",
        "generic_platform_baseline",
        "opentargets_disease_id",
        "opentargets_disease_name",
        "opentargets_api_version",
        "opentargets_data_version",
        "opentargets_datatype_scores_json",
        *[f"opentargets_datatype_{key}" for key in datatype_keys],
    ]
    return fieldnames, output_rows


def fetch_opentargets_baseline(
    output_file: Path,
    disease_id: str | None = None,
    disease_query: str | None = None,
    page_size: int = 500,
    max_pages: int | None = None,
    transport: GraphQLTransport | None = None,
) -> dict[str, object]:
    if disease_id is None and disease_query is None:
        raise ValueError("Provide either disease_id or disease_query.")

    transport = transport or live_graphql_transport
    meta = fetch_meta(transport)

    if disease_id is None:
        resolved = search_disease(disease_query or "", transport)
        disease_id = resolved["id"]
    disease_identity, rows = fetch_disease_associations(
        disease_id=disease_id,
        transport=transport,
        page_size=page_size,
        max_pages=max_pages,
    )
    fieldnames, flat_rows = flatten_association_rows(disease_identity, meta, rows)
    write_csv(output_file, flat_rows, fieldnames)

    metadata_file = output_file.with_suffix(".metadata.json")
    metadata = {
        "source": "Open Targets GraphQL API",
        "api_url": OPEN_TARGETS_GRAPHQL_URL,
        "disease": disease_identity,
        "row_count": len(flat_rows),
        "page_size": page_size,
        "max_pages": max_pages,
        "api_version": meta["apiVersion"],
        "data_version": meta["dataVersion"],
        "output_file": str(output_file),
    }
    write_json(metadata_file, metadata)
    return metadata
