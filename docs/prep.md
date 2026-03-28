# Prep And Registry

The engine needs two different prep layers now:

- a non-seed candidate registry that can grow beyond the curated shortlist
- the existing seed-driven example prep flow that still feeds the checked-in fixture tables

## Candidate Registry Prep

`build-candidate-registry` joins processed full-universe-capable source tables:

- `Open Targets` schizophrenia baseline output
- optional `PGC scz2022` prioritized-gene output

It emits a candidate registry CSV with:

- `entity_id`
- `primary_gene_id`
- deprecated `canonical_entity_id` compatibility alias
- `entity_label`
- `approved_name`
- any currently available layer columns such as `common_variant_support` and `generic_platform_baseline`
- `registry_origin`
- `registry_source_count`
- `registry_sources_json`
- `seed_entity_id` kept blank to preserve the PR2-style column contract without inventing a seed driver
- `source_entity_ids_json`
- `match_confidence`
- `match_provenance_json`
- `provenance_sources_json`
- source presence flags and match keys

The default end-to-end wrapper is `refresh-candidate-registry`. It:

- fetches `Open Targets` schizophrenia baseline rows
- fetches `PGC scz2022` prioritized genes unless `--skip-pgc` is passed
- writes those processed source tables under `data/processed/full_universe_ingest/`
- builds `data/processed/full_universe_ingest/registry/candidate_gene_registry.csv`

## Registry Identity Contract

The registry keeps the same core provenance fields introduced for prepared gene tables:

- `entity_id` and `primary_gene_id` resolve to the current registry row identity
- `source_entity_ids_json` records source-side IDs for `pgc`, `opentargets`, and the still-empty future source slots
- `match_provenance_json` preserves ordered per-source provenance with:
  - `source`
  - `matched`
  - `entity_id`
  - `entity_label`
  - `match_key`
  - `match_status`
- `provenance_sources_json` records the matched non-seed sources in stable order

Registry `match_confidence` values mean:

- `id_confirmed`: multiple sources matched the row on stable IDs without conflict
- `source_confirmed`: one or more non-seed sources back the row without an ID conflict, but the row is not independently ID-confirmed across multiple sources
- `source_conflict`: matched sources carried different IDs for the merged row
- `source_matched`: fallback when a matched source did not carry a confirming ID

## Gene Table Prep

`prepare-gene-table` remains the seed-driven prep step for the checked-in example workflow. It joins:

- a seed gene list or draft gene evidence table
- optional `PGC scz2022` prioritized-gene output
- optional `SCHEMA` rare-variant output
- optional `PsychENCODE / BrainSCOPE` DEG + GRN output
- optional `Open Targets` baseline output
- optional `ChEMBL` tractability output

It emits an engine-ready CSV with a stable column contract:

- rolled-up `v0` layer fields always present
- metadata and provenance fields grouped ahead of source primitives
- source-owned primitive fields grouped by source
- deprecated `canonical_entity_id` compatibility alias resolved from `primary_gene_id`

### Prepared Gene Column Groups

The prepared gene table is not a random dump of whatever the adapters emitted. It is ordered in three layers:

1. identity and labels
2. rolled-up `v0` layer fields
3. metadata / provenance fields
4. primitive source-field groups

Rolled-up `v0` layer fields:

- `common_variant_support`
- `rare_variant_support`
- `cell_state_support`
- `developmental_regulatory_support`
- `tractability_compoundability`
- `generic_platform_baseline`

Metadata / provenance fields:

- `primary_gene_id`
- `canonical_entity_id`
- `seed_entity_id`
- `source_entity_ids_json`
- `match_confidence`
- `match_provenance_json`
- `provenance_sources_json`
- source presence flags such as `source_present_pgc`
- source match keys such as `pgc_match_key`

Primitive source-field groups:

- `PGC`
  - `gene_biotype`
  - `pgc_scz2022_prioritised`
  - `pgc_scz2022_priority_index_snp_count`
  - `pgc_scz2022_priority_index_snps_json`
  - the `pgc_scz2022_*` prioritization criteria vector
- `SCHEMA`
  - significance and effect primitives such as `schema_significance_signal` and `schema_effect_signal`
  - burden-class, burden-count, and odds-ratio fields under `schema_*`
  - provenance primitives such as `schema_match_status`, `schema_query`, and override metadata
- `PsychENCODE / BrainSCOPE`
  - DEG primitives under `psychencode_deg_*`
  - GRN primitives under `psychencode_grn_*`
  - `psychencode_match_status`
- `Open Targets`
  - disease/version metadata under `opentargets_*`
  - `opentargets_datatype_scores_json`
  - flattened datatype vector fields such as `opentargets_datatype_clinical`
- `ChEMBL`
  - target/match metadata under `chembl_*`
  - `chembl_activity_count`
  - `chembl_mechanism_count`
  - `chembl_max_phase`
  - `chembl_action_types_json`

For the seed-driven phase:

- `entity_id` and `primary_gene_id` resolve to the immutable row identity
- `seed_entity_id` records the literal seed-row ID that drove the row
- later source rows can enrich the row but do not replace the seed-driven row count

## Module Table Prep

`fetch-psychencode-modules` still derives a source-backed module table from:

- a curated gene evidence table
- live `PsychENCODE / BrainSCOPE` schizophrenia DEG rows
- live `PsychENCODE / BrainSCOPE` adult cell-type GRNs

It emits an engine-ready module CSV with:

- `entity_id` keyed as `psychencode:{cell_type_slug}`
- `entity_label` as `BrainSCOPE {cell_type}`
- required module scoring columns:
  - `member_gene_genetic_enrichment`
  - `cell_state_specificity`
  - `developmental_regulatory_relevance`
- source context columns such as member-gene counts, top genes, and top TFs

`v0` still keeps module derivation deliberately narrow:

- modules are BrainSCOPE cell-type modules
- module membership is driven by the current curated gene table, not all genes in the universe
- cell types with fewer than `2` matched member genes are dropped

## Workflow Wrappers

`refresh-candidate-registry` is the repo-native non-seed ingest wrapper. It:

- fetches processed full-universe-capable source tables
- writes them under `data/processed/full_universe_ingest/`
- publishes `candidate_gene_registry.csv`

`refresh-example-gene-table` is the repo-native example fixture wrapper. It:

- reads `examples/v0/input/gene_seed.csv`
- fetches live shortlist-compatible source tables
- writes those processed tables under `data/processed/example_gene_workflow/`
- prepares `data/processed/example_gene_workflow/curated/example_gene_evidence.csv`
- publishes the fixture snapshot to `examples/v0/input/gene_evidence.csv`

`refresh-example-module-table` is the matching module fixture wrapper. It:

- reads `examples/v0/input/gene_evidence.csv` by default
- derives `PsychENCODE / BrainSCOPE` cell-type modules
- writes the source-backed module snapshot under `data/processed/example_module_workflow/`
- publishes the fixture snapshot to `examples/v0/input/module_evidence.csv`

`refresh-example-inputs` runs the gene fixture wrapper first and then the module fixture wrapper.

## Join Rules

For the candidate registry:

1. match by `entity_id` first
2. if that fails, match by `entity_label` exactly, case-insensitive, only when that label still resolves to one compatible candidate
3. expand the registry when a non-seed source row does not match an existing candidate
4. keep source-side IDs in `source_entity_ids_json`
5. keep ordered provenance in `match_provenance_json`

For `prepare-gene-table`:

1. match by `entity_id` first
2. if that fails, match by `entity_label` exactly, case-insensitive
3. keep the seed row as the row driver; do not expand or drop rows
4. keep the primary row identity stable; do not let later sources overwrite it
5. store source-side IDs in `source_entity_ids_json`
6. store ordered provenance details in `match_provenance_json` and compatibility source order in `provenance_sources_json`

## Pass-Through Rule

`prepare-gene-table` keeps source primitives unless there is a documented reason not to.

- if a source emits a `pgc_*`, `schema_*`, `psychencode_*`, `opentargets_*`, or `chembl_*` field, prep preserves it in the prepared CSV
- known primitive groups are ordered explicitly
- any remaining passthrough columns are still preserved after the declared contract fields

## Why This Matters

This keeps the example workflow fast and deterministic while separating it from the real architecture shift: non-seed source pulls can now create an explicit candidate registry before later scoring or broader source coverage is added.

## Example Workflows

Build the non-seed candidate registry:

```bash
uv run scz-target-engine refresh-candidate-registry
```

Or run the non-seed steps manually:

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/full_universe_ingest/opentargets/schizophrenia_baseline.csv

uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/full_universe_ingest/pgc/scz2022_prioritized_genes.csv

uv run scz-target-engine build-candidate-registry \
  --opentargets-file data/processed/full_universe_ingest/opentargets/schizophrenia_baseline.csv \
  --pgc-file data/processed/full_universe_ingest/pgc/scz2022_prioritized_genes.csv \
  --output-file data/processed/full_universe_ingest/registry/candidate_gene_registry.csv
```

Refresh the checked-in example gene and module fixtures:

```bash
uv run scz-target-engine refresh-example-inputs
```
