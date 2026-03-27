# Gene Table Prep

The engine does not want raw source exports forever. It wants a curated gene evidence table with clear ownership of each column.

## Current Commands

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

`refresh-example-gene-table` is the repo-native example workflow wrapper. It:

- reads `examples/v0/input/gene_seed.csv`
- fetches live `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL` tables
- writes those source snapshots under `data/processed/example_gene_workflow/`
- prepares `data/processed/example_gene_workflow/curated/example_gene_evidence.csv`
- publishes the curated snapshot to `examples/v0/input/gene_evidence.csv`

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

Refresh the checked-in example gene table:

```bash
uv run scz-target-engine refresh-example-gene-table
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
```
