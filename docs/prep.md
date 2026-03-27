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
- `canonical_entity_id`
- source match keys
- source presence flags
- provenance JSON

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
3. if `PGC` matches, use its `entity_id` as `canonical_entity_id`
4. if `SCHEMA` matches with a confirmed source match, overwrite `canonical_entity_id` with its `entity_id`
5. if `Open Targets` matches, overwrite `canonical_entity_id` with its `entity_id`
6. keep the seed row as the row driver, do not expand or drop rows

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
