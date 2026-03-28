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

It emits an engine-ready CSV with:

- required engine layer columns always present
- source-owned columns merged in
- `primary_gene_id`
- `seed_entity_id`
- `source_entity_ids_json`
- `match_confidence`
- `match_provenance_json`
- source match keys
- source presence flags
- source-specific match status fields where available
- deprecated `canonical_entity_id` compatibility alias resolved from `primary_gene_id`

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
