# Example Table Prep

The engine does not want raw source exports forever. It wants curated evidence tables with clear ownership of each column.

## Gene Table Prep

`prepare-gene-table` joins:

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

## Identity Contract

For the current seed-driven phase:

- `entity_id` and `primary_gene_id` resolve to the immutable row identity
- `seed_entity_id` records the literal seed-row ID that drove the row
- `source_entity_ids_json` records the matched ID from each source, or `null` when that source did not match
- `match_provenance_json` preserves ordered per-source provenance with:
  - `source`
  - `matched`
  - `entity_id`
  - `entity_label`
  - `match_key`
  - `match_status`
- `canonical_entity_id` is kept only as a deprecated compatibility alias and always resolves from `primary_gene_id`

`match_confidence` is a compact summary of how strongly the prepared row identity is supported:

- `seed_only`: no external source matched the row
- `id_confirmed`: at least one external source matched on `entity_id` and no source produced a conflicting ID
- `source_confirmed`: external sources matched without conflict, but confirmation depends on non-`entity_id` evidence
- `source_conflict`: one or more matched sources carried a different source-side ID than the primary row identity
- `source_matched`: fallback for a matched source row that did not carry a confirming ID

## Module Table Prep

`fetch-psychencode-modules` derives a source-backed module table from:

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

`v0` keeps the module derivation deliberately narrow:

- modules are BrainSCOPE cell-type modules
- module membership is driven by the current curated gene table, not all genes in the universe
- cell types with fewer than `2` matched member genes are dropped

## Example Workflow Wrappers

`refresh-example-gene-table` is the repo-native example workflow wrapper. It:

- reads `examples/v0/input/gene_seed.csv`
- fetches live `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL` tables
- writes those source snapshots under `data/processed/example_gene_workflow/`
- prepares `data/processed/example_gene_workflow/curated/example_gene_evidence.csv`
- publishes the curated snapshot to `examples/v0/input/gene_evidence.csv`

`refresh-example-module-table` is the matching wrapper for modules. It:

- reads `examples/v0/input/gene_evidence.csv` by default
- derives `PsychENCODE / BrainSCOPE` cell-type modules
- writes the source-backed module snapshot under `data/processed/example_module_workflow/`
- publishes the curated snapshot to `examples/v0/input/module_evidence.csv`

`refresh-example-inputs` runs the gene wrapper first and then the module wrapper, so the checked-in example inputs stay aligned.

## Join Rules

For each source:

1. match by `entity_id` first
2. if that fails, match by `entity_label` exactly, case-insensitive
3. keep the seed row as the row driver, do not expand or drop rows
4. keep the primary row identity stable; do not let later sources overwrite it
5. store source-side IDs in `source_entity_ids_json` instead of using join order as identity
6. store ordered provenance details in `match_provenance_json` and compatibility source order in `provenance_sources_json`

## Pass-Through Rule

`prepare-gene-table` keeps source primitives unless there is a documented reason not to.

- if a source emits a `pgc_*`, `schema_*`, `psychencode_*`, `opentargets_*`, or `chembl_*` field, prep preserves it in the prepared CSV
- known primitive groups are ordered explicitly
- any remaining passthrough columns are still preserved after the declared contract fields

## Why This Matters

This keeps source fetchers honest. `Open Targets` and `ChEMBL` stay as upstream adapters. The prep layer is where they become engine input.

## Example Workflow

Refresh the checked-in example gene and module tables:

```bash
uv run scz-target-engine refresh-example-inputs
```

Or run the steps manually:

```bash
uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/pgc/scz2022_prioritized_genes.csv

uv run scz-target-engine fetch-schema \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv

uv run scz-target-engine fetch-psychencode \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/psychencode/example_support.csv

uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/opentargets/schizophrenia_baseline.csv

uv run scz-target-engine fetch-chembl \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/chembl/example_tractability.csv

uv run scz-target-engine prepare-gene-table \
  --seed-file examples/v0/input/gene_seed.csv \
  --pgc-file data/processed/pgc/scz2022_prioritized_genes.csv \
  --schema-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv \
  --psychencode-file data/processed/example_gene_workflow/psychencode/example_support.csv \
  --opentargets-file data/processed/opentargets/schizophrenia_baseline.csv \
  --chembl-file data/processed/example_gene_workflow/chembl/example_tractability.csv \
  --output-file data/processed/example_gene_workflow/curated/example_gene_evidence.csv

uv run scz-target-engine fetch-psychencode-modules \
  --input-file data/processed/example_gene_workflow/curated/example_gene_evidence.csv \
  --output-file data/processed/example_module_workflow/psychencode/example_module_evidence.csv
```
