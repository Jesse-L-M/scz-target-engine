# Schizophrenia Target Engine

A transparent `v0` public-evidence prioritization scaffold for schizophrenia-oriented drug targets and biological modules, built on publicly available genomic and transcriptomic data.

The core challenge in target selection is separating reproducible biological signal from accumulated intuition. This engine makes the scoring contract explicit, runs quantitative stability analyses, and publishes report cards that show where the current public evidence looks promising, fragile, or insufficient.

`v0` is not a validated target decision system. It does not justify `advance / do not advance` claims, it does not yet score symptom-domain or illness-stage use cases separately, and its warning overlays do not change numeric rank.

## V0 Scope

- Schizophrenia core only
- Treatment-resistant schizophrenia annotated but not separately scored
- Independent gene-level and module-level leaderboards
- Warning overlays for prior clinical failure history and evidence gaps
- Ontology vocabulary documented but not separately scored yet
- Seed-linked example workflow; not yet seed-independent
- Stability analysis:
  - Leave-one-layer-out ablation
  - `+/- 20%` weight perturbation
  - Heuristic-stability threshold: `>= 70%` survival across sensitivity runs

## Current State

The engine currently implements:

- Manifest-driven scoring for curated evidence tables
- Stability analysis and baseline comparisons
- Markdown and CSV report generation
- A seed gene shortlist with a checked-in curated gene table refreshed from live source adapters
- A checked-in curated module table derived from `PsychENCODE / BrainSCOPE` cell-type DEG and GRN assets
- Live data fetchers:
  - `Open Targets` schizophrenia baseline via the official GraphQL API
  - `ChEMBL` tractability annotation for shortlist genes
  - `PGC` schizophrenia prioritised genes from the `scz2022` release
  - `SCHEMA` rare-variant support via the official results browser API, with a curated alias override layer for unresolved symbols
  - `PsychENCODE / BrainSCOPE` schizophrenia DEG and adult cell-type GRN support
  - `PsychENCODE / BrainSCOPE` source-backed cell-type module derivation

Raw-source ingestion from consortium data dumps is not yet implemented. V0 operates from curated tables with normalised layer scores in `[0, 1]`.

## Claim Boundaries

- `v0` is infrastructure, not the full target-engine vision.
- `v0` is a public-evidence prioritization scaffold, not a validated decision authority.
- `v0` does not yet score relapse prevention, negative symptoms, cognition, CHR/transition prevention, or durable recovery relevance separately.
- `v0` is not yet seed-independent.
- Warning overlays remain reporting-only.
- Config naming note: `stability.heuristic_stability_threshold` is the preferred key. The legacy `stability.decision_grade_threshold` alias is still accepted temporarily for compatibility.

See [docs/claim.md](docs/claim.md) for the current claim boundary, and [docs/ontology.md](docs/ontology.md) for the initial domain/stage vocabulary that `v0` does not separate yet.

## Quickstart

Refresh the example gene and module tables from the live source adapters:

```bash
uv run scz-target-engine refresh-example-inputs
```

Then run the example build:

```bash
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output
```

`examples/v0/input/gene_evidence.csv` and `examples/v0/input/module_evidence.csv` are generated snapshots from that refresh flow.

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

Fetch `PsychENCODE / BrainSCOPE` source-backed module evidence from a curated gene table:

```bash
uv run scz-target-engine fetch-psychencode-modules \
  --input-file examples/v0/input/gene_evidence.csv \
  --output-file data/processed/example_module_workflow/psychencode/example_module_evidence.csv
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
- [docs/claim.md](docs/claim.md): current capability and claim boundary for `v0`
- [docs/ontology.md](docs/ontology.md): initial domain/stage vocabulary not yet separately scored
- [docs/scoring_contract.md](docs/scoring_contract.md): methodological contract for `v0`
- [docs/source_manifest.md](docs/source_manifest.md): source roles and intended upstream inputs
- [docs/opentargets.md](docs/opentargets.md): Open Targets fetch contract
- [docs/chembl.md](docs/chembl.md): ChEMBL fetch contract
- [docs/pgc.md](docs/pgc.md): PGC scz2022 fetch contract
- [docs/schema.md](docs/schema.md): SCHEMA fetch contract
- [docs/psychencode.md](docs/psychencode.md): PsychENCODE / BrainSCOPE fetch contract
- [docs/prep.md](docs/prep.md): source join and curation contract
- [examples/v0/input](examples/v0/input): seed shortlist plus curated gene and module snapshots
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

The output must give a researcher a transparent basis for comparing public evidence, spotting fragile rankings, and deciding what warrants deeper domain-specific review.
