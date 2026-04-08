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
fixture-scale. The Track A historical replay gate is currently **infeasible** —
the entire honest schizophrenia late-stage replay window contains exactly 1
independent positive event (the xanomeline-trospium / Cobenfy approval), which
is too few for meaningful statistical comparison.

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

## Design Principle

The output must give a researcher a transparent basis for comparing public
evidence, spotting fragile rankings, and deciding what warrants deeper
domain-specific review.
