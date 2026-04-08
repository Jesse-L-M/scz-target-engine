# Schizophrenia Target Engine

Transparent, reproducible ranking of schizophrenia drug targets from public evidence.

## The Problem

Target selection in drug discovery is often driven by accumulated intuition — who
attended which conference, which paper got internal buzz, which target a senior
scientist championed a decade ago. The actual evidence basis is rarely explicit,
rarely scored consistently, and almost never stress-tested for sensitivity to the
assumptions baked into the ranking.

This repo makes the scoring contract explicit. Every weight, threshold, and
eligibility rule is checked in and versioned. Every ranking can be rerun from
the same inputs and produce the same outputs. Sensitivity analysis flags which
rankings are fragile — one missing evidence layer away from falling out of the
top tier.

## What It Does

The engine takes curated evidence tables for schizophrenia-relevant gene targets
and biological modules, scores them across five weighted evidence layers, and
produces ranked outputs with sensitivity analysis and warning overlays.

**Evidence layers** (each 20% weight for genes):

| Layer | What it captures |
|-------|-----------------|
| Common variant support | GWAS signal (PGC) |
| Rare variant support | Exome burden (SCHEMA) |
| Cell state support | Single-cell expression context (PsychENCODE) |
| Developmental regulatory support | Neurodevelopmental regulatory evidence |
| Tractability / compoundability | Can you actually drug this target? (ChEMBL) |

A gene must have at least one genetic layer AND one biological layer to be
eligible for ranking. The engine then runs 70+ sensitivity perturbations
(leave-one-layer-out, ±20% weight shifts) and reports which entities survive.

**Three additive output layers:**

- **v0** — the base weighted ranking. Stable reference. Reporting-only warnings
  never change scores.
- **v1** — decision vectors that decompose each target across 8 clinical domains
  (acute positive symptoms, negative symptoms, cognition, treatment resistance,
  etc.) using six scored heads including failure history and directionality
  confidence. Additive — does not change v0 outputs.
- **v2** — policy evaluation that blends v1 domain profiles under different
  strategic weightings and computes Pareto fronts. Also additive.

**Beyond scoring:**

- **Program memory** — a curated denominator of 59+ historical schizophrenia
  drug programs with outcomes, failure taxonomies, and directionality hypotheses.
  Effectively complete for approved programs, near-exhaustive for phase 2/3.
- **Benchmark system** — frozen historical snapshots that test whether the
  scoring system would have ranked successful programs highly, using
  honest pre-cutoff evidence only. Two tracks: ranking replay (Track A) and
  structural failure replay (Track B).
- **Rescue tasks** — a governed evaluation framework for testing whether models
  can identify targets worth "rescuing" from past clinical failures, starting
  with a glutamatergic neuron context.

## Current State

This is infrastructure, not a validated decision system. The v0 scores do not
justify advancing or killing any program. The benchmark archives are
fixture-scale. Benchmark breadth is still limited to the frozen schizophrenia
question and the current baseline subset. Calibration work, decision-threshold
setting, and broader operating-point evaluation remain future work.

The Track A historical replay gate is currently **infeasible** — the entire
honest schizophrenia late-stage replay window contains exactly 1 independent
positive event (the xanomeline-trospium / Cobenfy approval), which is too few
for meaningful statistical comparison. The stop-go comparison was executed on
2026-04-08 and the result is **HOLD**. Five checked-in cutoffs are evaluable on the principal `3y` horizon with one positive intervention object each. Replay
slices do not fall back to live source data.

See [docs/claim.md](docs/claim.md) for the full claim boundary.

## Quickstart

Requires [uv](https://github.com/astral-sh/uv).

```bash
# Run the example build (curated fixtures → ranked outputs)
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output

# Validate inputs without building
uv run scz-target-engine validate \
  --config config/v0.toml \
  --input-dir examples/v0/input

# Run the contract smoke path (rebuilds all example outputs, fails on drift)
./scripts/run_contract_smoke_path.sh

# Run tests
uv run --group dev pytest
```

**Refresh evidence from live sources:**

```bash
# Build a non-seed candidate registry from Open Targets + PGC
uv run scz-target-engine refresh-candidate-registry

# Refresh the example gene/module tables from live adapters
uv run scz-target-engine refresh-example-inputs
```

**Run the benchmark:**

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_small/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --materialized-at 2026-03-28

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_small/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_small/runner_outputs \
  --output-dir data/benchmark/generated/scz_small/public_payloads
```

## Roadmap

The repo is becoming an open schizophrenia digital-biology benchmark and
intervention observatory. The strategy has two lanes:

1. **Control plane** — program memory → historical replay → policy views →
   falsification-ready packets → external accountability
2. **Scientific core** — variant-to-context substrate → SCZ-Rescue-1 (flagship
   benchmark) → rescue models → assay/kill test plans

Both lanes converge on `intervention_object_id` as the shared key.

**Milestone sequencing:**

| # | Focus | Status |
|---|-------|--------|
| 0 | Contracts, compatibility, smoke path | Shipped |
| 1 | Program memory denominator | In progress |
| 2 | Historical replay with intervention-object features | Future |
| 3 | SCZ-Rescue-1 in glutamatergic context | Future |
| 4 | Variant-to-context substrate | Future |
| 5 | Policy and packet translation | Future |
| 6 | External credibility layer | Future |

Critical rule: milestones 5-6 don't start until milestone 4 proves real uplift.

See [docs/roadmap.md](docs/roadmap.md) for full strategy and
[docs/designs/deep-scz-validate-calibrate.md](docs/designs/deep-scz-validate-calibrate.md)
for the detailed working plan.

## Data Sources

| Source | What it provides |
|--------|-----------------|
| [Open Targets](https://www.opentargets.org/) | Disease-target association baseline |
| [PGC](https://pgc.unc.edu/) | Common variant support (schizophrenia GWAS) |
| [SCHEMA](https://schema.broadinstitute.org/) | Rare variant support (exome sequencing) |
| [PsychENCODE / BrainSCOPE](https://psychencode.org/) | Cell state and developmental regulatory context |
| [ChEMBL](https://www.ebi.ac.uk/chembl/) | Tractability and compoundability context |

Gene prep currently still requires a seed shortlist. The non-seed candidate
registry path exists but end-to-end scoring is not yet fully seed-independent.

## Repo Layout

```
config/v0.toml                   # Scoring weights and parameters
examples/v0/input/               # Curated evidence fixtures (gene + module)
examples/v0/output/              # Frozen reference outputs (contract surface)
data/curated/program_history/    # Historical program denominator
data/curated/rescue_tasks/       # Rescue task registry and contracts
data/benchmark/fixtures/         # Frozen benchmark inputs
data/benchmark/public_slices/    # Track A historical replay slices
src/scz_target_engine/           # Source code
schemas/artifact_schemas/        # Registered output schema definitions
docs/                            # Methodology, contracts, decisions
scripts/run_contract_smoke_path.sh  # CI-executed contract verifier
```

## Key Documentation

| Doc | What it covers |
|-----|---------------|
| [docs/claim.md](docs/claim.md) | What is and isn't shipped — the honest boundary |
| [docs/scoring_contract.md](docs/scoring_contract.md) | v0 methodology: weights, eligibility, stability |
| [docs/ontology.md](docs/ontology.md) | The 8 clinical domain definitions used by v1 |
| [docs/benchmarking.md](docs/benchmarking.md) | Benchmark protocol, workflow, artifact layout |
| [docs/program_history.md](docs/program_history.md) | Program memory schema and curation rules |
| [docs/rescue_tasks.md](docs/rescue_tasks.md) | Rescue task governance and contracts |
| [docs/roadmap.md](docs/roadmap.md) | Strategy and milestone sequencing |
| [docs/artifact_schemas.md](docs/artifact_schemas.md) | Registered output schemas and validation |

## Contract-Frozen Smoke Path

The smoke path rebuilds frozen example outputs into a temporary directory and
fails on drift from the checked-in `examples/v0/output/` contract surface.
CI runs this exact sequence:

```bash
./scripts/run_contract_smoke_path.sh

uv run scz-target-engine build --config config/v0.toml --input-dir examples/v0/input --output-dir examples/v0/output
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_small/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-file data/benchmark/generated/scz_small/snapshot_manifest.json --materialized-at 2026-03-28
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv --output-file data/benchmark/generated/scz_small/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-dir data/benchmark/generated/scz_small/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run python -m scz_target_engine.cli rescue compare baselines --output-dir .context/rescue-baseline-suite
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json
```

## Contract-Frozen Compatibility Surface

Future intervention-object-native work must project back through the checked-in
matrix in [docs/intervention_object_compatibility.md](docs/intervention_object_compatibility.md).
Projection multiplicity must be explicit, and silent legacy-consumer collisions
are forbidden during the dual-write period.

Current shipped consumer surfaces: `gene_target_ledgers`, `decision_vectors_v1`,
`domain_head_rankings_v1`, `policy_decision_vectors_v2`, `policy_pareto_fronts_v1`,
and `hypothesis_packets_v1`.

Release bundles freeze file membership, SHA256 digests, and expected_schema_version
through the registered `program_memory_release`, `benchmark_release`,
`rescue_release`, `variant_context_release`, `policy_release`, and
`hypothesis_release` manifest families. Cohort digests fail closed on drift.
See [docs/decisions/0002-release-manifest-contract.md](docs/decisions/0002-release-manifest-contract.md).

## Canonical Benchmark Workflow

The suite/task contract source of truth lives in
`data/curated/rescue_tasks/task_registry.csv`. Each `benchmark_task_id` maps to
a checked-in fixture set.

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_small/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --materialized-at 2026-03-28

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_small/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_small/runner_outputs \
  --output-dir data/benchmark/generated/scz_small/public_payloads
```

Key generated artifacts:

- `data/benchmark/generated/scz_small/snapshot_manifest.json` — `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv` — `benchmark_cohort_labels`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/` — `benchmark_model_run_manifest` files
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/` — `benchmark_metric_output_payload` files
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/` — `benchmark_confidence_interval_payload` files
- `data/benchmark/generated/scz_small/public_payloads/report_cards/` — derived public report cards
- `data/benchmark/generated/scz_small/public_payloads/leaderboards/` — derived leaderboard payloads

Backfill Track A public slices:

```bash
uv run scz-target-engine backfill-benchmark-public-slices \
  --output-dir data/benchmark/public_slices \
  --benchmark-task-id scz_translational_task

uv run scz-target-engine benchmark backfill public-slices \
  --output-dir data/benchmark/public_slices \
  --benchmark-task-id scz_translational_task
```

The checked-in slice catalog lives at `data/benchmark/public_slices/catalog.json`.
Write local replay outputs under
`data/benchmark/generated/public_slices/scz_translational_2024_09_25/` or another
checked-in slice id from the catalog.

## Artifact Schemas

Registered artifact families under `schemas/artifact_schemas/` are validated at
runtime through `scz_target_engine.artifacts`:

- `benchmark_snapshot_manifest`, `benchmark_cohort_labels`, `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`, `benchmark_confidence_interval_payload`
- `gene_target_ledgers`, `decision_vectors_v1`, `domain_head_rankings_v1`
- `policy_decision_vectors_v2`, `policy_pareto_fronts_v1`
- `rescue_task_contract`

Emitted snapshot and run manifests carry `benchmark_suite_id` and `benchmark_task_id`
as provenance fields. See [docs/artifact_schemas.md](docs/artifact_schemas.md).

## CLI Namespaces

Legacy flat commands remain supported. The namespaced routes are additive aliases:

- `engine validate`, `engine build`
- `sources opentargets`, `sources chembl`, `sources pgc scz2022`
- `sources schema`, `sources psychencode support`, `sources psychencode modules`
- `registry build`, `registry refresh`
- `prepare gene-table`, `prepare example-gene-table`, `prepare example-inputs`
- `benchmark snapshot`, `benchmark cohort`, `benchmark run`, `benchmark reporting`
- `hidden-eval task-package`, `hidden-eval simulate`

`config/v0.toml` remains the canonical path. `config/engine/v0.toml` is the
mirrored namespaced path.

## Design Principle

The output must give a researcher a transparent basis for comparing public
evidence, spotting fragile rankings, and deciding what warrants deeper
domain-specific review.
