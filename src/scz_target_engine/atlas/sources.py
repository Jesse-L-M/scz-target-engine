from __future__ import annotations

from pathlib import Path
import re

from scz_target_engine.atlas.contracts import (
    ATLAS_SOURCE_CONTRACT_VERSION,
    get_atlas_source_contract,
)
from scz_target_engine.atlas.staging import RawArtifactRecorder, slugify
from scz_target_engine.sources.opentargets import (
    GraphQLTransport,
    fetch_opentargets_baseline,
    live_graphql_transport,
)
from scz_target_engine.sources.pgc import (
    PGC_SCZ2022_FIGSHARE_ARTICLE_ID,
    PGC_SCZ2022_WORKBOOK_NAME,
    BytesTransport as PGCBytesTransport,
    JsonTransport as PGCJsonTransport,
    fetch_pgc_scz2022_prioritized_genes,
    live_bytes_transport as live_pgc_bytes_transport,
    live_json_transport as live_pgc_json_transport,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_RAW_SOURCE_DIR = REPO_ROOT / "data" / "raw" / "sources"
_QUERY_NAME_PATTERN = re.compile(r"query\s+([A-Za-z0-9_]+)")


def _graphql_query_name(query: str) -> str:
    match = _QUERY_NAME_PATTERN.search(query)
    if match is None:
        return "graphql_query"
    return match.group(1)


def _opentargets_dataset_slug(
    disease_id: str | None,
    disease_query: str | None,
) -> str:
    if disease_id:
        return f"{slugify(disease_id)}-baseline"
    if disease_query:
        return f"{slugify(disease_query)}-baseline"
    return "baseline"


def _build_pgc_workbook_artifact_name(workbook_name: str, call_index: int) -> str:
    suffix = Path(workbook_name).suffix or ".bin"
    stem = Path(workbook_name).stem or "workbook"
    return f"{call_index:03d}_{slugify(stem)}{suffix}"


def fetch_atlas_opentargets_baseline(
    output_file: Path,
    disease_id: str | None = None,
    disease_query: str | None = None,
    page_size: int = 500,
    max_pages: int | None = None,
    raw_dir: Path | None = None,
    materialized_at: str | None = None,
    transport: GraphQLTransport | None = None,
) -> dict[str, object]:
    output_file = output_file.resolve()
    contract = get_atlas_source_contract("opentargets")
    recorder = RawArtifactRecorder(
        contract=contract,
        raw_root=raw_dir or DEFAULT_ATLAS_RAW_SOURCE_DIR,
        dataset_slug=_opentargets_dataset_slug(disease_id, disease_query),
        materialized_at=materialized_at,
    )
    base_transport = transport or live_graphql_transport
    request_count = 0

    def staged_transport(query: str, variables: dict[str, object]) -> dict[str, object]:
        nonlocal request_count
        payload = base_transport(query, variables)
        request_count += 1
        query_name = _graphql_query_name(query)
        artifact_name = f"{request_count:03d}_{slugify(query_name)}.json"
        if query_name == "DiseaseAssociations":
            artifact_name = (
                f"{request_count:03d}_{slugify(query_name)}_page_"
                f"{int(variables.get('index', 0)):04d}.json"
            )
        recorder.stage_json(
            artifact_name=artifact_name,
            payload={
                "query_name": query_name,
                "variables": variables,
                "response": payload,
            },
        )
        return payload

    request_metadata = {
        "disease_id": disease_id,
        "disease_query": disease_query,
        "page_size": page_size,
        "max_pages": max_pages,
        "output_file": str(output_file),
    }

    try:
        legacy_metadata = fetch_opentargets_baseline(
            output_file=output_file,
            disease_id=disease_id,
            disease_query=disease_query,
            page_size=page_size,
            max_pages=max_pages,
            transport=staged_transport,
        )
    except Exception as exc:
        recorder.write_manifest(
            request_metadata=request_metadata,
            processed_artifacts=[
                output_file,
                output_file.with_suffix(".metadata.json"),
            ],
            status="failed",
            error=f"{exc.__class__.__name__}: {exc}",
        )
        raise

    manifest_file = recorder.write_manifest(
        request_metadata=request_metadata,
        processed_artifacts=[
            output_file,
            output_file.with_suffix(".metadata.json"),
        ],
        status="completed",
        upstream_metadata=legacy_metadata,
    )
    return {
        "contract_version": ATLAS_SOURCE_CONTRACT_VERSION,
        "source_contract": contract.to_dict(),
        "materialized_at": recorder.materialized_at,
        "raw_stage_dir": str(recorder.stage_dir),
        "raw_manifest_file": str(manifest_file),
        "raw_artifact_count": len(recorder.artifacts),
        "processed_output_file": str(output_file),
        "processed_metadata_file": str(output_file.with_suffix(".metadata.json")),
        "legacy_adapter_output": legacy_metadata,
    }


def fetch_atlas_pgc_scz2022_prioritized_genes(
    output_file: Path,
    article_id: int = PGC_SCZ2022_FIGSHARE_ARTICLE_ID,
    workbook_name: str = PGC_SCZ2022_WORKBOOK_NAME,
    raw_dir: Path | None = None,
    materialized_at: str | None = None,
    json_transport: PGCJsonTransport | None = None,
    bytes_transport: PGCBytesTransport | None = None,
) -> dict[str, object]:
    output_file = output_file.resolve()
    contract = get_atlas_source_contract("pgc_scz2022")
    recorder = RawArtifactRecorder(
        contract=contract,
        raw_root=raw_dir or DEFAULT_ATLAS_RAW_SOURCE_DIR,
        dataset_slug=contract.dataset_name,
        materialized_at=materialized_at,
    )
    base_json_transport = json_transport or live_pgc_json_transport
    base_bytes_transport = bytes_transport or live_pgc_bytes_transport
    request_count = 0

    def staged_json_transport(url: str) -> object:
        nonlocal request_count
        payload = base_json_transport(url)
        request_count += 1
        recorder.stage_json(
            artifact_name=f"{request_count:03d}_figshare_article_{article_id}.json",
            payload={
                "url": url,
                "response": payload,
            },
        )
        return payload

    def staged_bytes_transport(url: str) -> bytes:
        nonlocal request_count
        payload = base_bytes_transport(url)
        request_count += 1
        recorder.stage_bytes(
            artifact_name=_build_pgc_workbook_artifact_name(workbook_name, request_count),
            payload=payload,
            media_type=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            extra_metadata={"url": url},
        )
        return payload

    request_metadata = {
        "article_id": article_id,
        "workbook_name": workbook_name,
        "output_file": str(output_file),
    }

    try:
        legacy_metadata = fetch_pgc_scz2022_prioritized_genes(
            output_file=output_file,
            article_id=article_id,
            workbook_name=workbook_name,
            json_transport=staged_json_transport,
            bytes_transport=staged_bytes_transport,
        )
    except Exception as exc:
        recorder.write_manifest(
            request_metadata=request_metadata,
            processed_artifacts=[
                output_file,
                output_file.with_suffix(".metadata.json"),
            ],
            status="failed",
            error=f"{exc.__class__.__name__}: {exc}",
        )
        raise

    manifest_file = recorder.write_manifest(
        request_metadata=request_metadata,
        processed_artifacts=[
            output_file,
            output_file.with_suffix(".metadata.json"),
        ],
        status="completed",
        upstream_metadata=legacy_metadata,
    )
    return {
        "contract_version": ATLAS_SOURCE_CONTRACT_VERSION,
        "source_contract": contract.to_dict(),
        "materialized_at": recorder.materialized_at,
        "raw_stage_dir": str(recorder.stage_dir),
        "raw_manifest_file": str(manifest_file),
        "raw_artifact_count": len(recorder.artifacts),
        "processed_output_file": str(output_file),
        "processed_metadata_file": str(output_file.with_suffix(".metadata.json")),
        "legacy_adapter_output": legacy_metadata,
    }
