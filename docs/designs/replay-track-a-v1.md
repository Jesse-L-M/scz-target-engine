# replay-track-a-v1

Status: implemented
Owner branch: Jesse-L-M/track-a-replay-gap
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/program-memory-denominator-v1.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-08

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
  `benchmark_confidence_interval_payload`, while adding cohort-stage denominator
  artifacts that pin those labels back to the frozen snapshot
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
  with all pre-cutoff intervention-object features used for one snapshot.
  It is materialized beside the generated `benchmark_snapshot_manifest.json`.
- New or changed artifact:
  benchmark snapshot manifests that can declare `entity_type =
  intervention_object`
- New or changed artifact:
  `benchmark_cohort_members.csv`
  with the canonical cohort denominator consumed by runner and reporting.
- New or changed artifact:
  `benchmark_cohort_manifest.json`
  with SHA256-pinned links from the snapshot manifest plus raw cohort inputs to
  `benchmark_cohort_members.csv` and `benchmark_cohort_labels.csv`.
- New or changed artifact:
  benchmark cohort labels at intervention-object grain
- New or changed artifact:
  explicit `benchmark_intervention_object_baseline_projection` sidecars for
  `v0_current` and `v1_current` under `runner_outputs/baseline_projections/`
- New or changed artifact:
  leaderboards and error-analysis outputs for Track A under
  `public_payloads/leaderboards/` and `public_payloads/error_analysis/`
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

## Implementation Reality

- `scz_small` remains the canonical gene/module fixture path and still runs
  unchanged. The checked-in archive contents were restored to the minimal
  pre-Track-A two-gene/one-module regression surface so replay work does not
  silently widen the legacy fixture contract.
- Track A reuses the existing registry-backed
  `scz_translational_suite` / `scz_translational_task` row instead of creating a
  parallel benchmark task id.
- Checked-in public slices now switch to `entity_type = intervention_object`
  and derive admissible cohort rows plus future outcomes from the checked-in
  denominator and `program_history/v2/events.csv`.
- Replay no longer trusts the human-readable checked-in `program_universe_id`
  slug as the replay `entity_id`. It derives the replay key from
  `asset_lineage_id / target_class_lineage_id / modality / domain / population /
  regimen / stage_bucket`, and public-slice plus bundle materialization now fail
  closed if two rows still collapse to the same replay `entity_id`.
- Snapshot materialization writes
  `intervention_object_feature_bundle.parquet` beside the generated manifest
  using only archived benchmark inputs plus checked-in denominator/program-history
  tables.
- Before the runner consumes that sidecar bundle, it validates the bundle
  schema name/version, `as_of_date`, included/excluded source set, and exact
  intervention-object cohort alignment. The runner also rejects mixed-entity or
  stale cohort-label files whose `entity_types` do not exactly match the
  snapshot manifest. Runner and reporting no longer trust `cohort_labels.csv`
  on its own; they require the materialized `benchmark_cohort_members.csv` and
  `benchmark_cohort_manifest.json` artifacts emitted by `build-benchmark-cohort`.
- `v0_current` and `v1_current` are no longer implicit joins for
  intervention-object replay:
  the runner first materializes explicit projection sidecars from archived
  current gene/module outputs through the checked-in compatibility contract, then
  scores those sidecars.
- Reporting now emits one markdown error-analysis file per intervention-object run
  only when the principal `3y` intervention-object slice is actually evaluable.
- As of April 8, 2026, the checked-in `scz_translational_task` public-slice catalog
  ships honest replayable slices at `2024-06-15`, `2024-06-18`, `2024-06-20`,
  `2024-07-15`, `2024-11-11`, and `2025-01-16`. Cohort breadth rises from 5
  intervention objects on the earliest four slices to 6 on `2024-11-11` and 7 on
  `2025-01-16`, while each slice still inherits pinned local `program_universe.csv`
  and `events.csv` copies.
- None of those six slices are evaluable on the principal `3y` horizon yet because
  strict honest replay filtering still leaves zero post-cutoff positive
  intervention-object outcomes for every checked-in cutoff, so the shipped reporting
  flow continues to skip Track A error-analysis markdown for the public slices.

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

Implemented acceptance coverage in this PR:

- `tests/test_benchmark_snapshots.py`
- `tests/test_benchmark_labels.py`
- `tests/test_benchmark_runner.py`
- `tests/test_benchmark_leaderboard.py`
- `tests/test_benchmark_backfill.py`
- `tests/test_benchmark_protocol.py`
- `tests/test_docs_benchmark_workflow.py`
- `./scripts/run_contract_smoke_path.sh`

Hotfix coverage added on top of the shipped Track A path:

- duplicate replay `entity_id` rejection in public-slice and feature-bundle
  materialization
- runner rejection for stale, malformed, or mismatched
  `intervention_object_feature_bundle.parquet` sidecars
- runner and reporting rejection for mixed-entity cohort-label files
- principal-horizon evaluability guard for intervention-object error-analysis
  markdown
- explicit `scz_small` fixture-contract regression coverage

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
- public slices now exercise Track A intervention-object replay, while the
  frozen `scz_small` fixture remains the canonical regression path for the
  original gene/module workflow
- public-slice `cohort_members.csv` rows now carry the full replay-grain
  intervention-object ids that the bundle and runner validate, rather than the
  shorter checked-in denominator slugs
- missing pre-cutoff archives still exclude the source and are surfaced in the
  generated public-slice catalog and snapshot manifest; no live fallback is added

## Open Questions

- Should the principal metric stay average precision on the 3y horizon, or should
  it be a top-K enrichment metric if cohort sizes remain small?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/public_slices/<slice_id>/snapshot_request.json --archive-index-file data/benchmark/public_slices/<slice_id>/source_archives.json --output-file data/benchmark/generated/public_slices/<slice_id>/snapshot_manifest.json --materialized-at 2026-04-08
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/public_slices/<slice_id>/snapshot_manifest.json --cohort-members-file data/benchmark/public_slices/<slice_id>/cohort_members.csv --future-outcomes-file data/benchmark/public_slices/<slice_id>/future_outcomes.csv --output-file data/benchmark/generated/public_slices/<slice_id>/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/public_slices/<slice_id>/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/public_slices/<slice_id>/cohort_labels.csv --archive-index-file data/benchmark/public_slices/<slice_id>/source_archives.json --output-dir data/benchmark/generated/public_slices/<slice_id>/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run scz-target-engine build-benchmark-reporting --manifest-file data/benchmark/generated/public_slices/<slice_id>/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/public_slices/<slice_id>/cohort_labels.csv --runner-output-dir data/benchmark/generated/public_slices/<slice_id>/runner_outputs --output-dir data/benchmark/generated/public_slices/<slice_id>/public_payloads
```
