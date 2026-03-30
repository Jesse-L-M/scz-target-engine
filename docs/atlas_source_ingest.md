# Atlas Source Ingest Foundation

Atlas source ingest is now an additive layer on top of the current source adapters.
Its job is to stage provenance-bearing raw source artifacts without changing the
processed outputs that the current engine path consumes.

## Current Scope

Implemented atlas-facing adapters:

- `atlas sources opentargets`
- `atlas sources pgc scz2022`

Implemented atlas ingest wrapper:

- `atlas ingest candidate-registry`

Not implemented:

- raw consortium-dump parsing
- scoring-path rewrites
- replacement of the current processed source adapter outputs
- convergence-hub or mechanistic-axis work

## Source Adapter Contract

Each atlas source adapter preserves the corresponding legacy processed adapter contract
and adds a staged raw-source contract.

Processed-output preservation:

- `atlas sources opentargets` preserves the CSV and metadata contract from `fetch_opentargets_baseline`
- `atlas sources pgc scz2022` preserves the CSV and metadata contract from `fetch_pgc_scz2022_prioritized_genes`

Raw-stage additions:

- raw artifacts are written under `data/raw/sources/{source}/{dataset}/{materialized_at}/`
- each run writes `manifest.json`
- the manifest records:
  - the atlas source contract version
  - the source and dataset identity
  - requested parameters
  - staged raw artifact paths, digests, media types, and byte sizes
  - downstream processed artifact paths
  - status and any adapter error

Current raw artifact examples:

- `Open Targets`: staged GraphQL request/response pages for `Meta`, `SearchDisease`, and paged `DiseaseAssociations`
- `PGC scz2022`: staged figshare article metadata JSON plus the downloaded workbook bytes

## Ingest Foundation Contract

`atlas ingest candidate-registry` is the current end-to-end example path for atlas.
It:

- stages raw `Open Targets` and optional `PGC` artifacts
- materializes processed source CSVs in its work directory
- rebuilds `candidate_gene_registry.csv` via the existing registry builder
- writes `candidate_registry_ingest_manifest.json` to record the staged-source inputs and registry output

This keeps the candidate-registry contract aligned with the current engine path while
giving later atlas work a raw-source provenance layer to build on.

## Tensor Layer

Atlas now also exposes additive builders on top of the ingest manifest:

- `atlas build taxonomy`
- `atlas build tensor`

Those builders consume the staged-source manifest plus processed source tables and
materialize:

- context taxonomies for atlas-relevant dimensions
- provenance bundles that point back to processed files and staged raw artifact identity
- conservative cross-source alignments
- explicit `observed`, `missingness`, `conflict`, and `uncertainty` tensor rows

They still do not:

- rewrite scoring
- replace the current source adapter outputs used by scoring
- choose a winner when cross-source IDs conflict
- perform mechanistic-axis or convergence-hub work

## Boundary

This foundation is deliberately narrow.

- It captures adapter-backed raw requests and downloads.
- It does not yet parse raw consortium dumps into scored evidence tables.
- It does not alter `registry refresh`, `prepare example-inputs`, or scoring behavior.

Use the legacy source and prep flows when you need the current scoring path. Use the
atlas adapters when you need staged raw provenance for future atlas ingest work.
