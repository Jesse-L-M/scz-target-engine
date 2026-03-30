from __future__ import annotations

from dataclasses import asdict, dataclass


ATLAS_SOURCE_CONTRACT_VERSION = "atlas-source-contract/v1"
ATLAS_INGEST_CONTRACT_VERSION = "atlas-ingest-foundation/v1"
ATLAS_TAXONOMY_CONTRACT_VERSION = "atlas-context-taxonomy/v1"
ATLAS_TENSOR_CONTRACT_VERSION = "atlas-evidence-tensor/v1"


@dataclass(frozen=True)
class AtlasSourceContract:
    adapter_name: str
    source_name: str
    dataset_name: str
    processed_artifact_name: str
    upstream_artifact_kind: str
    preserved_output_contract: str
    scope_boundary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


OPENTARGETS_BASELINE_CONTRACT = AtlasSourceContract(
    adapter_name="atlas.sources.opentargets",
    source_name="opentargets",
    dataset_name="schizophrenia_baseline",
    processed_artifact_name="schizophrenia_baseline.csv",
    upstream_artifact_kind="graphql_request_response_pages",
    preserved_output_contract="scz_target_engine.sources.opentargets.fetch_opentargets_baseline",
    scope_boundary=(
        "Stages provenance-bearing API request and response captures alongside the "
        "existing processed Open Targets baseline CSV. It does not implement raw "
        "consortium-dump ingestion or replace the current scoring inputs."
    ),
)

PGC_SCZ2022_CONTRACT = AtlasSourceContract(
    adapter_name="atlas.sources.pgc_scz2022",
    source_name="pgc",
    dataset_name="scz2022_prioritized_genes",
    processed_artifact_name="scz2022_prioritized_genes.csv",
    upstream_artifact_kind="release_metadata_plus_workbook_bytes",
    preserved_output_contract="scz_target_engine.sources.pgc.fetch_pgc_scz2022_prioritized_genes",
    scope_boundary=(
        "Stages the figshare release metadata and workbook download that feed the "
        "existing processed PGC prioritized-gene CSV. It does not implement raw "
        "consortium-dump ingestion or change scoring semantics."
    ),
)

ATLAS_SOURCE_CONTRACTS = {
    "opentargets": OPENTARGETS_BASELINE_CONTRACT,
    "pgc_scz2022": PGC_SCZ2022_CONTRACT,
}


def get_atlas_source_contract(contract_name: str) -> AtlasSourceContract:
    try:
        return ATLAS_SOURCE_CONTRACTS[contract_name]
    except KeyError as exc:
        available = ", ".join(sorted(ATLAS_SOURCE_CONTRACTS))
        raise KeyError(
            f"Unknown atlas source contract {contract_name!r}. Available: {available}"
        ) from exc
