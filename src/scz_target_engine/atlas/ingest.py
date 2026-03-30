from __future__ import annotations

from pathlib import Path
import shutil

from scz_target_engine.atlas.contracts import ATLAS_INGEST_CONTRACT_VERSION
from scz_target_engine.atlas.sources import (
    DEFAULT_ATLAS_RAW_SOURCE_DIR,
    fetch_atlas_opentargets_baseline,
    fetch_atlas_pgc_scz2022_prioritized_genes,
)
from scz_target_engine.ingest import DEFAULT_DISEASE_QUERY
from scz_target_engine.io import write_json
from scz_target_engine.registry import (
    DEFAULT_REGISTRY_ARTIFACT_NAME,
    build_candidate_gene_registry,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_FULL_UNIVERSE_WORK_DIR = (
    REPO_ROOT / "data" / "processed" / "atlas" / "full_universe_ingest"
)
DEFAULT_ATLAS_CANDIDATE_REGISTRY_OUTPUT_FILE = (
    DEFAULT_ATLAS_FULL_UNIVERSE_WORK_DIR / "registry" / DEFAULT_REGISTRY_ARTIFACT_NAME
)


def refresh_atlas_candidate_gene_registry(
    output_file: Path | None = None,
    work_dir: Path | None = None,
    raw_dir: Path | None = None,
    materialized_at: str | None = None,
    disease_id: str | None = None,
    disease_query: str | None = None,
    include_pgc: bool = True,
) -> dict[str, object]:
    resolved_output_file = (
        output_file or DEFAULT_ATLAS_CANDIDATE_REGISTRY_OUTPUT_FILE
    ).resolve()
    resolved_work_dir = (work_dir or DEFAULT_ATLAS_FULL_UNIVERSE_WORK_DIR).resolve()
    resolved_raw_dir = (raw_dir or DEFAULT_ATLAS_RAW_SOURCE_DIR).resolve()
    resolved_disease_query = disease_query
    if disease_id is None and resolved_disease_query is None:
        resolved_disease_query = DEFAULT_DISEASE_QUERY

    opentargets_file = resolved_work_dir / "opentargets" / "schizophrenia_baseline.csv"
    pgc_file = resolved_work_dir / "pgc" / "scz2022_prioritized_genes.csv"
    registry_file = resolved_work_dir / "registry" / DEFAULT_REGISTRY_ARTIFACT_NAME

    for candidate in (opentargets_file, pgc_file, registry_file):
        candidate.parent.mkdir(parents=True, exist_ok=True)

    pgc_metadata = None
    if include_pgc:
        pgc_metadata = fetch_atlas_pgc_scz2022_prioritized_genes(
            output_file=pgc_file,
            raw_dir=resolved_raw_dir,
            materialized_at=materialized_at,
        )

    opentargets_metadata = fetch_atlas_opentargets_baseline(
        output_file=opentargets_file,
        disease_id=disease_id,
        disease_query=resolved_disease_query,
        raw_dir=resolved_raw_dir,
        materialized_at=materialized_at,
    )
    registry_metadata = build_candidate_gene_registry(
        output_file=registry_file,
        pgc_file=pgc_file if include_pgc else None,
        opentargets_file=opentargets_file,
    )

    resolved_output_file.parent.mkdir(parents=True, exist_ok=True)
    if registry_file != resolved_output_file:
        shutil.copyfile(registry_file, resolved_output_file)
        shutil.copyfile(
            registry_file.with_suffix(".metadata.json"),
            resolved_output_file.with_suffix(".metadata.json"),
        )

    manifest_file = resolved_work_dir / "atlas" / "candidate_registry_ingest_manifest.json"
    manifest_materialized_at = (
        materialized_at
        or str(opentargets_metadata["materialized_at"])
        or (str(pgc_metadata["materialized_at"]) if pgc_metadata else None)
    )
    manifest = {
        "contract_version": ATLAS_INGEST_CONTRACT_VERSION,
        "materialized_at": manifest_materialized_at,
        "scope_boundary": (
            "Stages raw source artifacts under data/raw/sources and rebuilds the "
            "candidate registry from the current processed source adapters. It does "
            "not implement consortium-dump parsing or replace the current scoring path."
        ),
        "raw_source_root": str(resolved_raw_dir),
        "work_dir": str(resolved_work_dir),
        "published_output_file": str(resolved_output_file),
        "registry_output_file": str(registry_file),
        "sources": {
            "pgc": pgc_metadata,
            "opentargets": opentargets_metadata,
        },
        "registry": registry_metadata,
    }
    write_json(manifest_file, manifest)

    return {
        "contract_version": ATLAS_INGEST_CONTRACT_VERSION,
        "materialized_at": manifest_materialized_at,
        "raw_source_root": str(resolved_raw_dir),
        "work_dir": str(resolved_work_dir),
        "published_output_file": str(resolved_output_file),
        "registry_output_file": str(registry_file),
        "manifest_file": str(manifest_file),
        "pgc": pgc_metadata,
        "opentargets": opentargets_metadata,
        "registry": registry_metadata,
    }


def refresh_atlas_candidate_registry(
    output_file: Path | None = None,
    work_dir: Path | None = None,
    raw_dir: Path | None = None,
    materialized_at: str | None = None,
    disease_id: str | None = None,
    disease_query: str | None = None,
    include_pgc: bool = True,
) -> dict[str, object]:
    return refresh_atlas_candidate_gene_registry(
        output_file=output_file,
        work_dir=work_dir,
        raw_dir=raw_dir,
        materialized_at=materialized_at,
        disease_id=disease_id,
        disease_query=disease_query,
        include_pgc=include_pgc,
    )
