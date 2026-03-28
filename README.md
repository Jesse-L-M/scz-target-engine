# Schizophrenia Target Engine

A transparent `v0` public-evidence prioritization scaffold for schizophrenia-oriented drug targets and biological modules, built on publicly available genomic and transcriptomic data.

The core challenge in target selection is separating reproducible biological signal from accumulated intuition. This engine makes the scoring contract explicit, runs quantitative stability analyses, and publishes report cards that show where the current public evidence looks promising, fragile, or insufficient.

`v0` is not a validated target decision system. It does not justify `advance / do not advance` claims, and its warning overlays do not change numeric rank. The build now also emits an additive `v1` decision-vector layer with domain/stage heads, while leaving all existing `v0` numeric outputs unchanged.

## V0 Scope

- Schizophrenia core only
- Treatment-resistant schizophrenia annotated but not separately scored
- Independent gene-level and module-level leaderboards
- Warning overlays for prior clinical failure history and evidence gaps
- Ontology vocabulary documented and exposed through additive `v1` domain/stage heads
- Non-seed candidate-registry ingest plus a seed-linked example fixture workflow
- Stability analysis:
  - Leave-one-layer-out ablation
  - `+/- 20%` weight perturbation
  - Heuristic-stability threshold: `>= 70%` survival across sensitivity runs

## Current State

The engine currently implements:

- Manifest-driven scoring for curated evidence tables
- Stability analysis and baseline comparisons
- Markdown and CSV report generation
- Scoring-neutral target-ledger JSON outputs with structural failure history and directionality hypotheses
- Additive `v1` decision vectors plus per-domain/per-stage ranking artifacts
- Explicit prepared-gene identity contract with stable primary IDs and per-source provenance
- Prepared gene tables that keep primitive `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL` source fields as first-class columns alongside the stable rolled-up `v0` layer inputs
- A non-seed candidate registry built from `Open Targets` baseline pulls plus optional `PGC` support
- Implementation-ready ontology plus a checked-in program-history, failure-taxonomy, and directionality-hypothesis substrate for later domain-aware reasoning
- A seed gene shortlist with a checked-in curated gene table refreshed from live source adapters as a fixture path
- A checked-in module fixture path rebuilt from the full-universe candidate registry plus `PsychENCODE / BrainSCOPE` cell-type DEG and GRN assets
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
- `v1` decision vectors are an explicit multi-head output layer, not a validated clinical advancement authority.
- `v1` domain/stage scores currently rely on human-support, biology-context, and intervention-readiness heads; PR7-backed failure, directionality, and subgroup heads remain explicit unscored placeholders on top of the landed substrate.
- `v0` now has a non-seed ingest path and a full-universe module-prep path, but gene prep and end-to-end scoring are not yet fully seed-independent.
- Warning overlays remain reporting-only.
- Program-history, failure-taxonomy, and directionality-hypothesis artifacts now emit structural target ledgers, but they still do not affect numeric scoring.
- Config naming note: `stability.heuristic_stability_threshold` is the preferred key. The legacy `stability.decision_grade_threshold` alias is still accepted temporarily for compatibility.

See [docs/claim.md](docs/claim.md) for the current claim boundary, [docs/ontology.md](docs/ontology.md) for the implementation-ready domain and stage vocabulary, and [docs/program_history.md](docs/program_history.md) for the curated program-history substrate that remains scoring-neutral in `v0`.
See [docs/ledger_contract.md](docs/ledger_contract.md) for the target-ledger output contract.

## Quickstart

Build the non-seed candidate registry:

```bash
uv run scz-target-engine refresh-candidate-registry
```

That writes `data/processed/full_universe_ingest/registry/candidate_gene_registry.csv`.

Refresh the example gene and module tables from the live source adapters:

```bash
uv run scz-target-engine refresh-example-inputs
```

`refresh-example-inputs` still publishes the checked-in example fixtures; the module side
now rebuilds from the non-seed candidate registry while the gene side remains seed-driven.

Then run the example build:

```bash
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output
```

`examples/v0/input/gene_evidence.csv` and `examples/v0/input/module_evidence.csv` are generated fixture snapshots from that refresh flow.
The build now also emits `gene_target_ledgers.json`, a structured per-target artifact that stays scoring-neutral in `v0`.

The build now also emits:

- `decision_vectors_v1.json`: nested per-entity `v1` decision vectors with named head fields, a keyed `decision_vector` object, and domain/stage scores
- `domain_head_rankings_v1.csv`: per-domain/per-stage `v1` ranking rows with side-by-side `heuristic_score_v0` comparison fields

Build the registry manually from processed full-universe-capable sources:

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

Fetch `PsychENCODE / BrainSCOPE` source-backed module evidence from the candidate registry
or another provenance-bearing candidate input:

```bash
uv run scz-target-engine fetch-psychencode-modules \
  --input-file data/processed/full_universe_ingest/registry/candidate_gene_registry.csv \
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
- [docs/ontology.md](docs/ontology.md): implementation-ready domain/stage vocabulary consumed by the additive `v1` head layer
- [docs/program_history.md](docs/program_history.md): curated landmark program-history schema and curation rules
- [docs/scoring_contract.md](docs/scoring_contract.md): methodological contract for `v0`
- [docs/ledger_contract.md](docs/ledger_contract.md): structured failure and directionality ledger contract
- [docs/source_manifest.md](docs/source_manifest.md): source roles and intended upstream inputs
- [docs/opentargets.md](docs/opentargets.md): Open Targets fetch contract
- [docs/chembl.md](docs/chembl.md): ChEMBL fetch contract
- [docs/pgc.md](docs/pgc.md): PGC scz2022 fetch contract
- [docs/schema.md](docs/schema.md): SCHEMA fetch contract
- [docs/psychencode.md](docs/psychencode.md): PsychENCODE / BrainSCOPE fetch contract
- [docs/prep.md](docs/prep.md): source join and curation contract
- [data/curated/program_history](data/curated/program_history): landmark program-history substrate with source URLs and failure-taxonomy labels
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

Prepared gene tables also carry:

- rolled-up `v0` layer inputs that remain numerically stable
- identity and provenance metadata such as `primary_gene_id`, `seed_entity_id`,
  `source_entity_ids_json`, `match_confidence`, and `match_provenance_json`
- primitive source-field groups for `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL`

The legacy `canonical_entity_id` column is kept temporarily as a deprecated alias to
`primary_gene_id`. See [docs/prep.md](docs/prep.md) for the prepared-table contract.

The non-seed candidate registry uses the same provenance fields, keeps `seed_entity_id` blank,
and records which full-universe-capable sources currently back each candidate row.

### Module Evidence

Required columns:

- `entity_id`
- `entity_label`
- `member_gene_genetic_enrichment`
- `cell_state_specificity`
- `developmental_regulatory_relevance`

Prepared module tables also carry admissibility and provenance context such as
`psychencode_module_genetically_supported_gene_count`,
`psychencode_module_member_source_breakdown_json`, and
`psychencode_module_admissibility_json`.

### Warning Overlays

Required columns:

- `entity_type`
- `entity_id`
- `severity`
- `warning_kind`
- `warning_text`

## Design Principle

The output must give a researcher a transparent basis for comparing public evidence, spotting fragile rankings, and deciding what warrants deeper domain-specific review.
