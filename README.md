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
- synthetic example inputs for end-to-end verification
- a real `Open Targets` baseline fetcher via the official GraphQL API
- a real `ChEMBL` tractability fetcher for shortlist genes

This repo does not yet implement raw-source ingestion from consortium dumps. That is the next layer. V0 starts from curated tables with normalized layer scores in `[0, 1]`.

## Quickstart

Run the example build:

```bash
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output
```

Fetch a real `Open Targets` schizophrenia baseline table:

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/opentargets/schizophrenia_baseline.csv
```

Fetch `ChEMBL` tractability context for a shortlist:

```bash
uv run scz-target-engine fetch-chembl \
  --input-file examples/v0/input/gene_evidence.csv \
  --output-file data/processed/chembl/example_tractability.csv \
  --limit 10
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

- [config/v0.toml](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/config/v0.toml): scoring and build config
- [docs/scoring_contract.md](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/docs/scoring_contract.md): methodological contract for `v0`
- [docs/source_manifest.md](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/docs/source_manifest.md): source roles and intended upstream inputs
- [docs/opentargets.md](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/docs/opentargets.md): Open Targets fetch contract
- [docs/chembl.md](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/docs/chembl.md): ChEMBL fetch contract
- [examples/v0/input](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/examples/v0/input): synthetic example inputs
- [src/scz_target_engine](/Users/jessemerrigan/conductor/workspaces/scz-target-engine/santiago-v1/src/scz_target_engine): scoring engine

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
