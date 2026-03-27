# Source Manifest

`v0` starts from curated evidence tables, not raw consortium dumps.

That is deliberate. The scoring engine and its failure conditions need to stabilize before raw-source parsing grows the scope.

## Target V0 Sources

### PGC

- Role: common-variant signal
- Output into engine: `common_variant_support`
- Notes: manual or scripted upstream harmonization into gene-level normalized support
- Status: live `scz2022` prioritized-gene importer implemented

### SCHEMA

- Role: rare-variant signal
- Output into engine: `rare_variant_support`
- Notes: upstream harmonization should preserve burden direction and study provenance where possible
- Status: live shortlist-oriented gene fetcher implemented via the official SCHEMA results browser API

### PsychENCODE

- Role: cell-state and developmental/regulatory evidence, plus module definitions
- Output into engine:
  - `cell_state_support`
  - `developmental_regulatory_support`
  - module tables

### Open Targets

- Role: generic platform baseline and auxiliary target context
- Output into engine:
  - `generic_platform_baseline`
- Notes: used as a comparison source, not as the source of truth
- Status: a live GraphQL fetcher is implemented for disease-scoped baseline pulls

### ChEMBL

- Role: tractability and compoundability context
- Output into engine:
  - `tractability_compoundability`
- Status: shortlist-oriented live fetcher implemented

## Deferred for V0.1

- DGIdb
- Stanley resources
- richer failure-history formalization
- raw download and transform helpers

## Upstream Contract

The upstream ingestion layer should output curated CSV tables with normalized scores in `[0, 1]`, explicit provenance, and stable entity IDs.
