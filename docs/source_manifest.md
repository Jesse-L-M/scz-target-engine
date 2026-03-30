# Source Manifest

`v0` scoring currently has two ingestion layers:

- processed source tables and a non-seed candidate registry
- curated evidence tables that feed the current scoring build

Atlas now adds an additive raw-source staging foundation for selected adapter-backed
pulls under `data/raw/sources/`, but that foundation does not replace the scoring
path above.
Raw consortium dump parsing is still deferred. The scoring engine and its failure conditions still need to stabilize before raw-source parsing grows the scope.

These sources currently feed one shared schizophrenia `v0` scaffold plus an additive `v1` decision-vector layer. `v1` now combines the original human-support, biology-context, and intervention-readiness heads with numeric PR7-backed failure, directionality, and subgroup heads for gene targets, while shared `v0` outputs remain unchanged.

## Target V0 Sources

### PGC

- Role: common-variant signal
- Output into engine: `common_variant_support`
- Prepared primitive fields:
  - `gene_biotype`
  - `pgc_scz2022_prioritised`
  - `pgc_scz2022_priority_index_snp_count`
  - `pgc_scz2022_priority_index_snps_json`
  - `pgc_scz2022_*` prioritization criteria
- Notes: manual or scripted upstream harmonization into gene-level normalized support
- Status: live `scz2022` prioritized-gene importer implemented; can now feed the non-seed candidate registry

### SCHEMA

- Role: rare-variant signal
- Output into engine: `rare_variant_support`
- Prepared primitive fields:
  - significance and effect primitives such as `schema_significance_signal` and `schema_effect_signal`
  - published burden-class and odds-ratio fields under the `schema_*` prefix
  - provenance primitives such as `schema_match_status`, `schema_query`, and override metadata
- Notes: upstream harmonization should preserve burden direction and study provenance where possible
- Status: live shortlist-oriented gene fetcher implemented via the official SCHEMA results browser API, with a curated alias override layer for ambiguous symbols
- Current scope note: still joins through the seed-driven example prep flow, not the non-seed registry

### PsychENCODE

- Role: cell-state and developmental/regulatory evidence, plus module definitions
- Output into engine:
  - `cell_state_support`
  - `developmental_regulatory_support`
  - module tables
- Prepared primitive fields:
  - DEG primitives under `psychencode_deg_*`
  - GRN primitives under `psychencode_grn_*`
  - `psychencode_match_status`
- Status: live shortlist-oriented BrainSCOPE importer implemented for schizophrenia DEG plus adult cell-type GRN support, plus a live cell-type module derivation step backed by the same sources
- Notes: `v0` currently uses the regulatory half of this layer from BrainSCOPE GRNs; a separate developmental source is still open work
- Current scope note: still joins through the seed-driven example prep flow, not the non-seed registry

### Open Targets

- Role: generic platform baseline and auxiliary target context
- Output into engine:
  - `generic_platform_baseline`
- Prepared primitive fields:
  - disease and version metadata under the `opentargets_*` prefix
  - `opentargets_datatype_scores_json`
  - flattened datatype vector columns such as `opentargets_datatype_genetic_association`
- Notes: used as a comparison source, not as the source of truth
- Status: a live GraphQL fetcher is implemented for disease-scoped baseline pulls and now serves as the primary row-expansion source for the non-seed candidate registry

### ChEMBL

- Role: tractability and compoundability context
- Output into engine:
  - `tractability_compoundability`
- Prepared primitive fields:
  - `chembl_activity_count`
  - `chembl_mechanism_count`
  - `chembl_max_phase`
  - `chembl_action_types_json`
  - target and match metadata under the `chembl_*` prefix
- Status: shortlist-oriented live fetcher implemented
- Current scope note: still joins through the seed-driven example prep flow, not the non-seed registry

## Deferred for V0.1

- DGIdb
- Stanley resources
- richer failure-history formalization
- raw download and transform helpers

## Upstream Contract

The upstream ingestion layer should output:

- processed source tables with normalized scores in `[0, 1]`, explicit provenance, and stable entity IDs
- an explicit candidate registry artifact before scoring
- curated evidence tables only after source joins and identity reconciliation are complete

## Atlas Raw-Source Staging Contract

Atlas now exposes additive source adapters for:

- `Open Targets` schizophrenia baseline pulls
- `PGC scz2022` prioritized-gene pulls

Those atlas adapters preserve the legacy processed CSV outputs and add:

- a provenance-bearing raw stage root under `data/raw/sources/{source}/{dataset}/{materialized_at}/`
- a `manifest.json` sidecar describing the source contract, requested parameters, staged raw artifacts, and downstream processed artifact paths
- explicit scope boundaries stating that staged request/download capture is implemented, while full consortium-dump parsing remains future work

The current atlas example ingest path is `atlas ingest candidate-registry`. It stages
raw `Open Targets` and optional `PGC` artifacts, writes processed source tables under
its work directory, and rebuilds the same candidate-registry contract via the existing
registry builder.

Prepared gene tables keep source-owned primitives as first-class columns. They separate:

- rolled-up `v0` layer fields
- metadata and provenance fields
- primitive source groups for `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL`

## Benchmark Snapshot Contract

`PR9A` freezes the benchmark protocol in [docs/benchmarking.md](benchmarking.md).

For time-sliced benchmarking, the current evidence sources are treated as release-scoped rather than row-dated:

- `PGC`
- `SCHEMA`
- `PsychENCODE`
- `Open Targets`
- `ChEMBL`

That means benchmark snapshots must either:

- use a release or archived extract materialized on or before the benchmark `as_of_date`
- or exclude the source from that snapshot

The protocol does not allow current-source pulls to be projected backward into older snapshots.
Undated or ambiguously dated evidence is excluded by default rather than admitted with warnings.

`PR9B` now materializes this explicitly in snapshot artifacts:

- every frozen source gets one `SourceSnapshot` entry
- included sources are backed by an archived descriptor plus a verified archive file digest
- excluded sources set `included = false` and record a concrete `exclusion_reason`

`PR9C` now consumes those archived inputs in the runner:

- only `available_now` baselines listed in the snapshot manifest execute
- the runner emits `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload`
- there is still no fallback from missing archived inputs to current live source data

The checked-in deterministic fixture flow for this behavior lives under
`data/benchmark/fixtures/scz_small/`, with canonical generated outputs under
`data/benchmark/generated/scz_small/`.
