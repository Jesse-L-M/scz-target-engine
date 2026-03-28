from __future__ import annotations

import shutil
from pathlib import Path

from scz_target_engine.registry import (
    DEFAULT_REGISTRY_ARTIFACT_NAME,
    build_candidate_gene_registry,
)
from scz_target_engine.sources.opentargets import fetch_opentargets_baseline
from scz_target_engine.sources.pgc import fetch_pgc_scz2022_prioritized_genes


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FULL_UNIVERSE_WORK_DIR = REPO_ROOT / "data" / "processed" / "full_universe_ingest"
DEFAULT_CANDIDATE_REGISTRY_OUTPUT_FILE = (
    DEFAULT_FULL_UNIVERSE_WORK_DIR / "registry" / DEFAULT_REGISTRY_ARTIFACT_NAME
)
DEFAULT_DISEASE_QUERY = "schizophrenia"


def refresh_candidate_gene_registry(
    output_file: Path | None = None,
    work_dir: Path | None = None,
    disease_id: str | None = None,
    disease_query: str | None = None,
    include_pgc: bool = True,
) -> dict[str, object]:
    resolved_output_file = (
        output_file or DEFAULT_CANDIDATE_REGISTRY_OUTPUT_FILE
    ).resolve()
    resolved_work_dir = (work_dir or DEFAULT_FULL_UNIVERSE_WORK_DIR).resolve()
    resolved_disease_query = disease_query
    if disease_id is None and resolved_disease_query is None:
        resolved_disease_query = DEFAULT_DISEASE_QUERY

    opentargets_file = resolved_work_dir / "opentargets" / "schizophrenia_baseline.csv"
    pgc_file = resolved_work_dir / "pgc" / "scz2022_prioritized_genes.csv"
    registry_file = resolved_work_dir / "registry" / DEFAULT_REGISTRY_ARTIFACT_NAME

    for candidate in (pgc_file, opentargets_file, registry_file):
        candidate.parent.mkdir(parents=True, exist_ok=True)

    pgc_metadata = None
    if include_pgc:
        pgc_metadata = fetch_pgc_scz2022_prioritized_genes(output_file=pgc_file)
    opentargets_metadata = fetch_opentargets_baseline(
        output_file=opentargets_file,
        disease_id=disease_id,
        disease_query=resolved_disease_query,
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

    return {
        "work_dir": str(resolved_work_dir),
        "published_output_file": str(resolved_output_file),
        "registry_output_file": str(registry_file),
        "pgc": pgc_metadata,
        "opentargets": opentargets_metadata,
        "registry": registry_metadata,
    }


def refresh_candidate_registry(
    output_file: Path | None = None,
    work_dir: Path | None = None,
    disease_id: str | None = None,
    disease_query: str | None = None,
    include_pgc: bool = True,
) -> dict[str, object]:
    return refresh_candidate_gene_registry(
        output_file=output_file,
        work_dir=work_dir,
        disease_id=disease_id,
        disease_query=disease_query,
        include_pgc=include_pgc,
    )
