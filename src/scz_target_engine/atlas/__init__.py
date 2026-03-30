"""Atlas-facing source staging and ingest foundations."""

from scz_target_engine.atlas.contracts import (
    ATLAS_CONVERGENCE_CONTRACT_VERSION,
    ATLAS_INGEST_CONTRACT_VERSION,
    ATLAS_MECHANISTIC_AXES_CONTRACT_VERSION,
    ATLAS_SOURCE_CONTRACTS,
    ATLAS_SOURCE_CONTRACT_VERSION,
    ATLAS_TAXONOMY_CONTRACT_VERSION,
    ATLAS_TENSOR_CONTRACT_VERSION,
)
from scz_target_engine.atlas.convergence import (
    DEFAULT_ATLAS_CONVERGENCE_OUTPUT_DIR,
    materialize_convergence_hubs,
)
from scz_target_engine.atlas.ingest import (
    DEFAULT_ATLAS_CANDIDATE_REGISTRY_OUTPUT_FILE,
    DEFAULT_ATLAS_FULL_UNIVERSE_WORK_DIR,
    refresh_atlas_candidate_gene_registry,
    refresh_atlas_candidate_registry,
)
from scz_target_engine.atlas.mechanistic_axes import (
    DEFAULT_ATLAS_MECHANISTIC_AXES_OUTPUT_DIR,
    materialize_mechanistic_axes,
)
from scz_target_engine.atlas.sources import (
    DEFAULT_ATLAS_RAW_SOURCE_DIR,
    fetch_atlas_opentargets_baseline,
    fetch_atlas_pgc_scz2022_prioritized_genes,
)
from scz_target_engine.atlas.taxonomy import (
    DEFAULT_ATLAS_CURATED_DIR,
    DEFAULT_ATLAS_TAXONOMY_OUTPUT_DIR,
    materialize_atlas_taxonomy,
)
from scz_target_engine.atlas.tensor import (
    DEFAULT_ATLAS_TENSOR_OUTPUT_DIR,
    materialize_atlas_tensor,
)

__all__ = [
    "ATLAS_CONVERGENCE_CONTRACT_VERSION",
    "ATLAS_INGEST_CONTRACT_VERSION",
    "ATLAS_MECHANISTIC_AXES_CONTRACT_VERSION",
    "ATLAS_SOURCE_CONTRACTS",
    "ATLAS_SOURCE_CONTRACT_VERSION",
    "ATLAS_TAXONOMY_CONTRACT_VERSION",
    "ATLAS_TENSOR_CONTRACT_VERSION",
    "DEFAULT_ATLAS_CURATED_DIR",
    "DEFAULT_ATLAS_CANDIDATE_REGISTRY_OUTPUT_FILE",
    "DEFAULT_ATLAS_FULL_UNIVERSE_WORK_DIR",
    "DEFAULT_ATLAS_RAW_SOURCE_DIR",
    "DEFAULT_ATLAS_CONVERGENCE_OUTPUT_DIR",
    "DEFAULT_ATLAS_MECHANISTIC_AXES_OUTPUT_DIR",
    "DEFAULT_ATLAS_TAXONOMY_OUTPUT_DIR",
    "DEFAULT_ATLAS_TENSOR_OUTPUT_DIR",
    "fetch_atlas_opentargets_baseline",
    "fetch_atlas_pgc_scz2022_prioritized_genes",
    "materialize_convergence_hubs",
    "materialize_mechanistic_axes",
    "materialize_atlas_taxonomy",
    "materialize_atlas_tensor",
    "refresh_atlas_candidate_gene_registry",
    "refresh_atlas_candidate_registry",
]
