# Source Manifest

`v0` starts from curated evidence tables, not raw consortium dumps.

That is deliberate. The scoring engine and its failure conditions need to stabilize before raw-source parsing grows the scope.

These sources currently feed one shared schizophrenia `v0` scaffold. They do not yet back separate scoring heads for acute positive symptoms, relapse prevention, treatment-resistant subtypes, negative symptoms, cognition, CHR / transition prevention, or durable recovery relevance.

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
- Status: live `scz2022` prioritized-gene importer implemented

### SCHEMA

- Role: rare-variant signal
- Output into engine: `rare_variant_support`
- Prepared primitive fields:
  - significance and effect primitives such as `schema_significance_signal` and `schema_effect_signal`
  - published burden-class and odds-ratio fields under the `schema_*` prefix
  - provenance primitives such as `schema_match_status`, `schema_query`, and override metadata
- Notes: upstream harmonization should preserve burden direction and study provenance where possible
- Status: live shortlist-oriented gene fetcher implemented via the official SCHEMA results browser API, with a curated alias override layer for ambiguous symbols

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

### Open Targets

- Role: generic platform baseline and auxiliary target context
- Output into engine:
  - `generic_platform_baseline`
- Prepared primitive fields:
  - disease and version metadata under the `opentargets_*` prefix
  - `opentargets_datatype_scores_json`
  - flattened datatype vector columns such as `opentargets_datatype_genetic_association`
- Notes: used as a comparison source, not as the source of truth
- Status: a live GraphQL fetcher is implemented for disease-scoped baseline pulls

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

## Deferred for V0.1

- DGIdb
- Stanley resources
- richer failure-history formalization
- raw download and transform helpers

## Upstream Contract

The upstream ingestion layer should output curated CSV tables with normalized scores in `[0, 1]`, explicit provenance, and stable entity IDs.

Prepared gene tables keep those source-owned primitives as first-class columns. They separate:

- rolled-up `v0` layer fields
- metadata and provenance fields
- primitive source groups for `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL`
