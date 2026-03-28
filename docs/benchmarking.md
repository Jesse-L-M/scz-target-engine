# Benchmark Protocol

This repo now freezes the benchmark contract before any full runner implementation lands.

`PR9A` is protocol-only. It defines:

- the benchmark question
- the date-cutoff and no-leakage rules
- the frozen baseline matrix
- the input and output artifact schemas that later benchmark PRs must honor

It does not implement:

- the benchmark runner
- historical source backfills
- synthetic historical snapshot generation
- wiring benchmark execution into current head internals

## Frozen Benchmark Question

The benchmark asks:

- given a schizophrenia evidence snapshot as of date `T`
- rank admissible gene and module entities using only evidence observable at or before `T`
- then compare those ranks against later translational outcomes recorded on a separate label channel

Entity types:

- `gene`
- `module`

Accepted translational outcome labels:

- `future_schizophrenia_program_started`
- `future_schizophrenia_program_advanced`
- `future_schizophrenia_positive_signal`
- `future_schizophrenia_negative_signal`
- `no_qualifying_future_outcome`

Evaluation horizons:

- `1y`
- `3y`
- `5y`

In-scope evidence for the ranking side:

- pre-cutoff schizophrenia genetics
- pre-cutoff schizophrenia transcriptomics and regulatory context
- pre-cutoff tractability and generic platform context
- pre-cutoff scoring-neutral failure and directionality ledgers only when those artifacts were themselves archived before the snapshot

Not valid as benchmark labels:

- post-cutoff source evidence refreshes
- post-cutoff genetics or transcriptomics publications used as if they were outcomes
- any current-head internal state that was not already frozen into a pre-cutoff artifact

The benchmark is therefore about ranking against later translational outcomes, not about replaying later evidence as if it were ground truth.

## Snapshot Semantics

Every benchmark snapshot is described by a `benchmark_snapshot_manifest` with:

- `as_of_date`: the last date allowed for ranking evidence
- `outcome_observation_closed_at`: the last date used to adjudicate future labels
- `benchmark_question_id`: the frozen benchmark question id, not an arbitrary per-run label
- `source_snapshots`: one explicit cutoff or exclusion entry for every frozen evidence source
- `leakage_controls`: an explicit strict no-leakage block
- `baseline_ids`: the frozen comparison set to evaluate

The manifest lives in code as `BenchmarkSnapshotManifest` in [src/scz_target_engine/benchmark_protocol.py](../src/scz_target_engine/benchmark_protocol.py).

### Date-Cutoff Rules

For every included evidence source:

- `allowed_data_through <= as_of_date`
- `evidence_frozen_at <= as_of_date`
- `source_name`, `cutoff_mode`, and per-source leakage policies must match the frozen source cutoff rules
- future-dated records are rejected
- missing cutoff metadata is not silently tolerated

`materialized_at` may be later than the benchmark `as_of_date` if the snapshot was reconstructed from a pre-cutoff archived release or extract. The anti-leakage requirement applies to when the evidence was frozen, not when a later PR regenerated the manifest from that frozen evidence.

If a source does not expose reliable row-level dates, the benchmark protocol does not guess.
Instead it uses a stricter rule:

- include the source only from a pre-cutoff archived release or pre-cutoff archived extract
- otherwise include an explicit `included = false` snapshot entry with an `exclusion_reason`

This is how the protocol represents the "no hindsight" rule without requiring this PR to generate historical backfills.

## No-Leakage Contract

The `LeakageControls` block is intentionally strict and rejects looser configurations.

It freezes these requirements:

- a snapshot manifest is required
- future evidence cannot enter the ranking inputs
- future outcome labels cannot be reused as model inputs
- pre-cutoff evidence freezing is required for included evidence artifacts
- undated sources default to `exclude_source`
- missing cutoff definitions default to `reject_snapshot`
- benchmark execution cannot depend on current head internals that are not already present in frozen artifacts

That last rule keeps this PR protocol-only. Later runner work may evaluate `v0` or `v1` outputs, but it must do so through declared artifacts rather than by coupling directly to mutable head internals.

## Source-Specific Cutoff Behavior

The current evidence stack is frozen as release-scoped for benchmarking:

- `PGC`
- `SCHEMA`
- `PsychENCODE`
- `Open Targets`
- `ChEMBL`

All five currently use release or archived-extract semantics rather than record-level timestamp semantics in the protocol. That means:

- historical benchmark slices require a release or archived extract frozen on or before the snapshot date
- if such an archive does not exist, the source is excluded
- this PR does not backfill or synthesize those historical archives

The code contract for these rules lives in `SOURCE_CUTOFF_RULES_V1`.

## Frozen Baseline Matrix

The benchmark comparison set is now frozen as:

1. `pgc_only`
2. `schema_only`
3. `opentargets_only`
4. `v0_current`
5. `v1_current`
6. `v1_pre_numeric_pr7_heads`
7. `v1_post_numeric_pr7_heads`
8. `chembl_only`
9. `random_with_coverage`

Notes:

- `v1_current` is the current additive `v1` output evaluated as shipped.
- `v1_pre_numeric_pr7_heads` and `v1_post_numeric_pr7_heads` are frozen protocol comparison labels so future benchmark runs remain comparable across the PR8.1 transition.
- `chembl_only` applies only where tractability context exists and is not a module baseline.
- `random_with_coverage` must match entity type, cohort size, and coverage masks.
- a snapshot may list a baseline only if that baseline applies to at least one entity type present in the snapshot manifest.

The code contract for the matrix lives in `FROZEN_BASELINE_MATRIX`.

## Artifact Schemas

The later benchmark runner must read and write these schema families exactly:

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`

Those schemas are frozen in code as `BENCHMARK_ARTIFACT_SCHEMAS_V1`.

At a minimum they define:

- snapshot identity and evidence boundary
- cohort membership and translational outcome labels
- run identity, inputs, and code version
- metric names, values, horizons, and cohort sizes
- confidence interval bounds, bootstrap counts, and resample units

## Implementation Boundary

This protocol is meant to be stable enough that later benchmark PRs can:

- materialize historical snapshot manifests
- build cohort label files
- execute frozen baselines
- publish metric and interval payloads

without changing the meaning of the benchmark itself.
