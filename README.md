# Schizophrenia Target Engine

This repo builds `target_engine_v0`, a public-data-first prioritization engine for schizophrenia targets and modules.

The point is not to rank genes by vibes. The point is to force a hard scoring contract, run stability checks that can fail, and publish both winners and kill cards.

## V0 Scope

- Schizophrenia core only
- TRS as annotation, not as a separately scored output
- Separate gene and module leaderboards
- Warning overlays for prior failure history and evidence gaps
- Stability checks:
  - leave-one-layer-out
  - `+/- 20%` weight perturbations
  - decision-grade threshold of `>= 70%` survival across sensitivity runs

## Current State

This repo currently implements:

- a manifest-driven scoring engine for curated evidence tables
- stability analysis and baseline comparisons
- markdown and CSV report generation
- a seed-only example gene shortlist plus a checked-in curated gene table refreshed from live source adapters
- illustrative module inputs for end-to-end verification
- a real `Open Targets` baseline fetcher via the official GraphQL API
- a real `ChEMBL` tractability fetcher for shortlist genes
- a real `PGC` schizophrenia prioritized-gene fetcher from the official `scz2022` release
- a real `SCHEMA` rare-variant fetcher for shortlist genes via the official results browser API
  - with a checked-in curated alias override layer for unresolved symbols
- a real `PsychENCODE / BrainSCOPE` shortlist importer for schizophrenia DEG and adult cell-type GRN support

This repo does not yet implement raw-source ingestion from consortium dumps. That is the next layer. V0 starts from curated tables with normalized layer scores in `[0, 1]`.

## Quickstart

Refresh the example gene table from the live source adapters:

```bash
uv run scz-target-engine refresh-example-gene-table
```

Then run the example build:

```bash
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output
```

`examples/v0/input/gene_evidence.csv` is a generated snapshot from that refresh command. `examples/v0/input/module_evidence.csv` remains illustrative example input until the module workflow gets the same live-source treatment.

Fetch a real `Open Targets` schizophrenia baseline table:

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/opentargets/schizophrenia_baseline.csv
```

Fetch `ChEMBL` tractability context for a shortlist:

```bash
uv run scz-target-engine fetch-chembl \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/chembl/example_tractability.csv
```

Fetch `PGC` schizophrenia common-variant gene support:

```bash
uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/pgc/scz2022_prioritized_genes.csv
```

Fetch `SCHEMA` schizophrenia rare-variant gene support for a shortlist:

```bash
uv run scz-target-engine fetch-schema \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv
```

Fetch `PsychENCODE / BrainSCOPE` schizophrenia DEG and GRN support for a shortlist:

```bash
uv run scz-target-engine fetch-psychencode \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/psychencode/example_support.csv
```

Prepare an engine-ready gene table from joined source outputs:

```bash
uv run scz-target-engine prepare-gene-table \
  --seed-file examples/v0/input/gene_seed.csv \
  --pgc-file data/processed/pgc/scz2022_prioritized_genes.csv \
  --schema-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv \
  --psychencode-file data/processed/example_gene_workflow/psychencode/example_support.csv \
  --opentargets-file data/processed/opentargets/schizophrenia_baseline.csv \
  --chembl-file data/processed/example_gene_workflow/chembl/example_tractability.csv \
  --output-file data/processed/example_gene_workflow/curated/example_gene_evidence.csv
```

Validate only:

```bash
uv run scz-target-engine validate \
  --config config/v0.toml \
  --input-dir examples/v0/input
```

Run tests:

```bash
uv run --group dev pytest
```

## Repo Layout

- [config/v0.toml](config/v0.toml): scoring and build config
- [docs/scoring_contract.md](docs/scoring_contract.md): methodological contract for `v0`
- [docs/source_manifest.md](docs/source_manifest.md): source roles and intended upstream inputs
- [docs/opentargets.md](docs/opentargets.md): Open Targets fetch contract
- [docs/chembl.md](docs/chembl.md): ChEMBL fetch contract
- [docs/pgc.md](docs/pgc.md): PGC scz2022 fetch contract
- [docs/schema.md](docs/schema.md): SCHEMA fetch contract
- [docs/psychencode.md](docs/psychencode.md): PsychENCODE / BrainSCOPE fetch contract
- [docs/prep.md](docs/prep.md): source join and curation contract
- [examples/v0/input](examples/v0/input): seed shortlist, curated gene snapshot, and example module inputs
- [src/scz_target_engine](src/scz_target_engine): scoring engine

## Input Tables

### Gene Evidence

Required columns:

- `entity_id`
- `entity_label`
- `common_variant_support`
- `rare_variant_support`
- `cell_state_support`
- `developmental_regulatory_support`
- `tractability_compoundability`
- `generic_platform_baseline`

Optional free-text columns are preserved in reports.

### Module Evidence

Required columns:

- `entity_id`
- `entity_label`
- `member_gene_genetic_enrichment`
- `cell_state_specificity`
- `developmental_regulatory_relevance`

### Warning Overlays

Required columns:

- `entity_type`
- `entity_id`
- `severity`
- `warning_kind`
- `warning_text`

## Design Principle

If the output cannot tell a skeptical researcher what to chase, what to ignore, and how fragile that conclusion is, it is not good enough.
