# Benchmark Protocol

This repo now freezes the benchmark contract, materializes real snapshot/cohort artifacts,
and executes the available-now benchmark matrix on top of those archived inputs.

`PR9A` defined:

- the benchmark question
- the date-cutoff and no-leakage rules
- the frozen baseline matrix
- the input and output artifact schemas that later benchmark PRs must honor

`PR9B` adds:

- `build-benchmark-snapshot`: real manifest materialization from archived source descriptors
- `build-benchmark-cohort`: real cohort / future-label materialization keyed to a snapshot
- a checked-in deterministic fixture flow under `data/benchmark/fixtures/scz_small/`

`PR9C` adds:

- `run-benchmark`: actual execution for the frozen `available_now` baselines
- `benchmark_model_run_manifest` emission for each executed baseline
- `benchmark_metric_output_payload` emission for ranking and top-k metrics
- `benchmark_confidence_interval_payload` emission using deterministic percentile bootstrap

It does not implement:

- historical source backfills
- synthetic historical snapshot generation
- protocol-only baseline execution without the required archived artifacts

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
The materializer lives in [src/scz_target_engine/benchmark_snapshots.py](../src/scz_target_engine/benchmark_snapshots.py).

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
`PR9B` now materializes that rule directly: every frozen source receives one `SourceSnapshot`
entry, and the inclusion state is explicit on the artifact itself through:

- `included = true` for an admitted archived source descriptor
- `included = false` plus a non-empty `exclusion_reason` when no valid pre-cutoff archive exists

There is no fallback path from a missing historical archive to current live source data.

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

That last rule keeps benchmark execution tied to declared artifacts rather than mutable
current-head internals.

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
The archive-descriptor loader and resolver live in
[src/scz_target_engine/benchmark_snapshots.py](../src/scz_target_engine/benchmark_snapshots.py).

## Snapshot Materialization Workflow

The snapshot builder expects two inputs:

- a snapshot request JSON with snapshot identity, cutoff dates, entity types, and baseline ids
- a source archive index JSON that lists archived source descriptors with archive paths and SHA256 digests

For each frozen source, the builder:

1. finds the latest descriptor whose `allowed_data_through` and `evidence_frozen_at` are both `<= as_of_date`
2. validates that the referenced archive file exists and matches the declared SHA256 digest
3. rejects the archive index if multiple eligible descriptors tie on the newest cutoff dates, because that makes source provenance ambiguous
4. emits an included `SourceSnapshot` when validation succeeds
5. emits an excluded `SourceSnapshot` with an explicit `exclusion_reason` when no valid pre-cutoff archive is available

Example command:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_small/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --materialized-at 2026-03-28
```

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
- `random_with_coverage` must randomize over the full admissible cohort for an entity type and be evaluated with the same full-cohort semantics as the primary baselines.
- a snapshot may list a baseline only if that baseline applies to at least one entity type present in the snapshot manifest.

The code contract for the matrix lives in `FROZEN_BASELINE_MATRIX`.

## Artifact Schemas

The benchmark runner reads and writes these schema families exactly:

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`

Those schemas are frozen in code as `BENCHMARK_ARTIFACT_SCHEMAS_V1`.

The cohort / label materializer now lives in
[src/scz_target_engine/benchmark_labels.py](../src/scz_target_engine/benchmark_labels.py).
It consumes:

- a built snapshot manifest
- a cohort-membership CSV that represents the ranking-side admissible cohort
- a future-outcomes CSV that represents label-side adjudication inputs

This separation keeps ranking inputs distinct from post-cutoff outcome labels.

Example command:

```bash
uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_small/cohort_labels.csv
```

For each `(entity, horizon, label_name)` triple, the materializer emits a deterministic row.
`no_qualifying_future_outcome` is computed by the builder rather than supplied in the raw future-outcomes input.
Future-outcome rows outside the valid label window are rejected rather than silently ignored:

- `outcome_date` must be strictly after `as_of_date`
- `outcome_date` must be `<= outcome_observation_closed_at`

At a minimum they define:

- snapshot identity and evidence boundary
- cohort membership and translational outcome labels
- run identity, inputs, and code version
- metric names, values, horizons, and cohort sizes
- confidence interval bounds, bootstrap counts, and resample units

## Current Runner Coverage

The current runner executes only baselines whose frozen protocol status is
`available_now` and which are explicitly listed in the snapshot manifest's
`baseline_ids`.

Implemented executable baselines:

1. `pgc_only`
2. `schema_only`
3. `opentargets_only`
4. `v0_current`
5. `v1_current`
6. `chembl_only`
7. `random_with_coverage`

Explicit protocol-only baselines that remain declared but non-executed:

1. `v1_pre_numeric_pr7_heads`
2. `v1_post_numeric_pr7_heads`

The runner consumes:

- a built `benchmark_snapshot_manifest`
- a built `benchmark_cohort_labels` artifact
- the archived source descriptor index used to resolve the actual archived source files

There is still no fallback from a missing archived source to current live data.

### Metric Bundle

Current metric payloads treat
`future_schizophrenia_program_started`,
`future_schizophrenia_program_advanced`, and
`future_schizophrenia_positive_signal`
as the positive retrieval target for each `(entity_type, horizon)` slice.
Primary metrics are computed on the full admissible cohort for that slice.
If a baseline cannot score an admissible entity, that entity remains in evaluation
as a deterministic bottom-tier row and the emitted `notes` keep coverage explicit
as `covered_entities=<covered>/<admissible>`.
`future_schizophrenia_negative_signal` and
`no_qualifying_future_outcome` remain explicit in the label artifact but are
treated as non-relevant for the current metric bundle.

The runner emits:

- `average_precision_any_positive_outcome`
- `mean_reciprocal_rank_any_positive_outcome`
- `precision_at_1_any_positive_outcome`
- `precision_at_3_any_positive_outcome`
- `precision_at_5_any_positive_outcome`
- `recall_at_1_any_positive_outcome`
- `recall_at_3_any_positive_outcome`
- `recall_at_5_any_positive_outcome`

The `v1_current` benchmark comparator resolves the current additive `v1` output
through the emitted domain-head contract by taking the mean available
`domain_head_score_v1` across domain profiles for each entity.

### Interval Method

The current interval payloads use percentile bootstrap confidence intervals with:

- resample unit `entity`
- explicit bootstrap iteration count on every payload
- explicit random seed on every payload
- deterministic test mode via a fixed seed and reduced iteration count

## Implementation Boundary

This protocol is meant to be stable enough that later benchmark PRs can:

- materialize historical snapshot manifests
- build cohort label files
- execute frozen baselines
- publish metric and interval payloads

without changing the meaning of the benchmark itself.

## Deterministic Fixture Flow

A small checked-in fixture path now lives under `data/benchmark/fixtures/scz_small/`.
It proves the full benchmark path end-to-end:

- build a snapshot manifest
- emit explicit source inclusions and exclusions
- build a cohort / label artifact
- execute the requested `available_now` baselines
- emit run manifests, metric payloads, and confidence interval payloads

Example command:

```bash
uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --deterministic-test-mode
```

The fixture intentionally includes:

- `PGC`, `Open Targets`, and `PsychENCODE`
- explicit exclusion for `SCHEMA` because no archived descriptor is provided
- explicit exclusion for `ChEMBL` because the only checked-in archive is after the snapshot cutoff

This keeps tests fast and deterministic while preserving the real artifact contracts that later runner work will consume.
