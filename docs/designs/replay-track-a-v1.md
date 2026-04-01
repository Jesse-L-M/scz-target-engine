# replay-track-a-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/program-memory-denominator-v1.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-01

## Objective

Extend the current fixture-scale benchmark into real multi-snapshot historical
replay for intervention and program prioritization.

This spec covers Track A only:
historical prioritization.
It does not cover Track B failure-memory reasoning yet.

## Success Condition

- Primary success metric:
  average precision on the 3y horizon for a declared positive-outcome composite
  at the intervention-object level
- Secondary success metric:
  recall-at-K and negative-signal burden in the top decile are emitted for the
  same runs and slices
- Stop/go gate:
  if no new replay layer materially beats projected `v0_current` and `v1_current`
  on the principal slice with bootstrap CIs, do not proceed to later translation
  milestones

## Scope

- intervention-object replay cohorts and labels
- archived intervention-object feature bundles per snapshot
- multi-snapshot replay beyond `scz_small`
- baseline projection from current gene/module outputs onto intervention objects
- leaderboard and error-analysis generation for Track A
- honest public slices that preserve exclusion accounting

## Not in Scope

- Track B analog retrieval / "what must differ?" logic
- rescue benchmark training
- policy ranking changes
- packet contract changes
- atlas feature expansion beyond what replay needs

## Existing Surfaces To Reuse

- current benchmark snapshot / cohort / runner / reporting path:
  extend the existing fixture and registry-driven workflow
- current benchmark artifact schemas:
  keep `benchmark_snapshot_manifest`, `benchmark_cohort_labels`,
  `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and
  `benchmark_confidence_interval_payload`
- current source-archive and leakage rules:
  keep the strict no-fallback archive behavior
- intervention-object compatibility matrix from `contracts-and-compat-v2`:
  use explicit baseline projection instead of ad hoc mapping

## Inputs

- archived source descriptors and frozen source extracts
- current benchmark fixture and public slice machinery
- program-memory denominator and included-event outputs
- current `v0_current` and `v1_current` outputs projected through the compatibility
  layer
- operator inputs:
  `snapshot_request.json`, `source_archives.json`, cohort membership, and future
  outcome labels

## Outputs And Artifact Contracts

- New or changed artifact:
  `intervention_object_feature_bundle.parquet`
  with all pre-cutoff features used for one snapshot
- New or changed artifact:
  benchmark snapshot manifests that can declare `entity_type =
  intervention_object`
- New or changed artifact:
  benchmark cohort labels at intervention-object grain
- New or changed artifact:
  leaderboards and error-analysis outputs for Track A
- Backward-compatibility rule:
  current gene/module benchmark tasks and the `scz_small` fixture remain valid and
  runnable

### Proposed Positive Composite For Primary Metric

Count these labels as positive for the principal Track A metric:

- `future_schizophrenia_program_started`
- `future_schizophrenia_program_advanced`
- `future_schizophrenia_positive_signal`

Treat `future_schizophrenia_negative_signal` and
`no_qualifying_future_outcome` as non-positive for the primary ranking metric.

## Data Flow

```text
ARCHIVED SOURCES + PROGRAM MEMORY + CURRENT BASELINES
    -> SNAPSHOT REQUEST
    -> INTERVENTION-OBJECT FEATURE BUNDLE
    -> SNAPSHOT MANIFEST
    -> COHORT LABELS
    -> RUNNER EXECUTION
    -> METRICS + CIs
    -> LEADERBOARDS + ERROR ANALYSIS
```

- Archived source inputs freeze the evidence boundary.
- Program memory contributes pre-cutoff failure and lineage context only when that
  context itself was frozen before the snapshot.
- Current baseline projections are explicit artifacts, not implicit joins.
- The runner produces metrics and CIs on intervention-object cohorts.
- Reporting generates leaderboard and error-analysis outputs without rerunning
  scoring logic.

## Implementation Plan

1. Define intervention-object cohort grain and label semantics.
2. Add intervention-object feature-bundle materialization per snapshot.
3. Extend snapshot and cohort builders to accept intervention-object entity types.
4. Implement explicit baseline projections from `v0_current` and `v1_current`.
5. Run multi-snapshot replay on 10-15 slices and generate report cards plus error
   analysis.

## Acceptance Tests

- Unit:
  add tests for intervention-object feature-bundle validation and baseline
  projection determinism in benchmark test modules
- Integration:
  materialize one non-`scz_small` intervention-object replay slice end to end
- Regression:
  add a test that fails if a missing pre-cutoff archive is silently replaced by a
  live source pull
- E2E, if relevant:
  snapshot -> cohort -> run -> reporting for one public slice with only archived
  inputs and explicit source exclusions

## Failure Modes

- Failure mode:
  two intervention objects collapse because modality, population, or regimen is
  dropped from the key; feature-bundle and cohort builders must reject collisions
- Failure mode:
  a projected `v0_current` or `v1_current` baseline reuses post-cutoff state from
  current head; projection inputs must be frozen and versioned per snapshot
- Failure mode:
  a source archive is missing and the replay still proceeds from current data; the
  snapshot builder must emit explicit exclusion or reject the slice

## Rollout / Compatibility

- current gene/module benchmark tasks stay live
- new intervention-object replay runs are additive and versioned separately
- a breaking change is any replay implementation that repurposes current benchmark
  artifacts without a new entity-type or manifest-level distinction

## Open Questions

- Should intervention-object cohorts be benchmarked against one unified task id or
  a new suite/task row that lives alongside the current `scz_translational_task`?
- Should the principal metric stay average precision on the 3y horizon, or should
  it be a top-K enrichment metric if cohort sizes remain small?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/public_slices/scz_translational_2024_06_20/snapshot_request.json --archive-index-file data/benchmark/public_slices/scz_translational_2024_06_20/source_archives.json --output-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json --materialized-at 2026-03-30
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json --cohort-members-file data/benchmark/public_slices/scz_translational_2024_06_20/cohort_members.csv --future-outcomes-file data/benchmark/public_slices/scz_translational_2024_06_20/future_outcomes.csv --output-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv --archive-index-file data/benchmark/public_slices/scz_translational_2024_06_20/source_archives.json --output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run scz-target-engine build-benchmark-reporting --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv --runner-output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/runner_outputs --output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/public_payloads
```
