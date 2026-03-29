# Benchmark Protocol

The benchmark path shipped on `main` is now real, not protocol-only. `PR9A` froze the
benchmark question and artifact contracts. `PR9B` added snapshot and cohort builders.
`PR9C` added the runner plus emitted run manifests, metric payloads, and percentile-bootstrap
confidence interval payloads.

This is a fixture-scale release path. It is useful for reproducible end-to-end evaluation
and artifact validation. It is not yet a production-scale historical replay system.

## Current Release Boundary

- historical benchmark archives are fixture-scale and currently checked in only for `data/benchmark/fixtures/scz_small/`
- benchmark breadth is still limited to the frozen schizophrenia benchmark question, a small deterministic cohort, and the current `available_now` baseline subset
- protocol-only baselines remain declared for comparability but are not executed unless later archived artifacts make them runnable
- calibration work, decision-threshold setting, and broader operating-point evaluation remain future work
- current benchmark outputs are generated locally under `data/benchmark/generated/`; only the fixture inputs under `data/benchmark/fixtures/` are checked in

## Frozen Benchmark Question

The benchmark asks:

- given a schizophrenia evidence snapshot as of date `T`
- rank admissible gene and module entities using only evidence observable at or before `T`
- compare those ranks against later translational outcomes recorded on a separate label channel

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

The benchmark is about ranking against later translational outcomes, not about replaying
later evidence as if it were ground truth.

## Snapshot Semantics And Leakage Controls

Every benchmark snapshot is described by a `benchmark_snapshot_manifest` with:

- `as_of_date`: the last date allowed for ranking evidence
- `outcome_observation_closed_at`: the last date used to adjudicate future labels
- `benchmark_question_id`: the frozen benchmark question id
- `source_snapshots`: one explicit cutoff or exclusion entry for every frozen evidence source
- `leakage_controls`: an explicit strict no-leakage block
- `baseline_ids`: the frozen comparison set to evaluate

The manifest lives in code as `BenchmarkSnapshotManifest` in
[src/scz_target_engine/benchmark_protocol.py](../src/scz_target_engine/benchmark_protocol.py).
The materializer lives in
[src/scz_target_engine/benchmark_snapshots.py](../src/scz_target_engine/benchmark_snapshots.py).

The leakage contract stays strict:

- a snapshot manifest is required
- future evidence cannot enter the ranking inputs
- future outcome labels cannot be reused as model inputs
- pre-cutoff evidence freezing is required for included evidence artifacts
- undated sources default to `exclude_source`
- missing cutoff definitions default to `reject_snapshot`
- benchmark execution cannot depend on current head internals that are not already present in frozen artifacts

`materialized_at` may be later than the benchmark `as_of_date` if the snapshot was
reconstructed from a pre-cutoff archived release or extract. The anti-leakage check is
about when the evidence was frozen, not when a later PR regenerated the manifest.

## Source Cutoff Behavior

The current evidence stack is frozen as release-scoped for benchmarking:

- `PGC`
- `SCHEMA`
- `PsychENCODE`
- `Open Targets`
- `ChEMBL`

All five currently use release or archived-extract semantics rather than row-level
timestamp semantics. That means:

- historical benchmark slices require a release or archived extract frozen on or before the snapshot date
- if such an archive does not exist, the source is excluded
- the protocol does not backfill or synthesize those historical archives

If a source does not expose reliable row-level dates, the protocol does not guess.
It either admits a valid archived descriptor or emits `included = false` with a concrete
`exclusion_reason`. There is no fallback from a missing historical archive to current
live source data.

## Frozen Baseline Matrix

The comparison set is frozen as:

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

- `v1_current` is the current additive `v1` output evaluated as shipped
- `v1_pre_numeric_pr7_heads` and `v1_post_numeric_pr7_heads` stay frozen as protocol comparison labels across the PR8.1 transition
- `chembl_only` applies only where tractability context exists and is not a module baseline
- `random_with_coverage` randomizes across the full admissible cohort and is evaluated with the same full-cohort semantics as the main baselines
- a snapshot may list a baseline only if that baseline applies to at least one entity type present in the snapshot manifest

## Canonical End-To-End Workflow

The canonical benchmark workflow in this repo is the deterministic fixture under
`data/benchmark/fixtures/scz_small/`.

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
```

Supporting operator inputs:

- `snapshot_request.json`: snapshot identity, dates, entity types, and requested baseline ids
- `source_archives.json`: archived source descriptors with archive paths and SHA256 digests
- `cohort_members.csv`: admissible ranking cohort membership
- `future_outcomes.csv`: post-cutoff label adjudication input

Snapshot materialization behavior:

1. find the latest descriptor whose `allowed_data_through` and `evidence_frozen_at` are both `<= as_of_date`
2. validate that the referenced archive file exists and matches the declared SHA256 digest
3. reject ambiguous ties on the newest eligible cutoff dates
4. emit an included `SourceSnapshot` when validation succeeds
5. emit an excluded `SourceSnapshot` with an explicit `exclusion_reason` when no valid pre-cutoff archive is available

Cohort materialization behavior:

- rows are emitted for every `(entity, horizon, label_name)` triple
- `no_qualifying_future_outcome` is computed by the builder, not supplied in the raw future-outcomes file
- `outcome_date` must be strictly after `as_of_date`
- `outcome_date` must be `<= outcome_observation_closed_at`

## Artifact Families And Layout

The runner reads and writes these schema families exactly:

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`

Those schemas are frozen in code as `BENCHMARK_ARTIFACT_SCHEMAS_V1`.
The supporting request files `snapshot_request.json` and `source_archives.json` are
operator inputs, not part of `BENCHMARK_ARTIFACT_SCHEMAS_V1`.

Canonical generated locations:

- `data/benchmark/generated/scz_small/snapshot_manifest.json`: `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv`: `benchmark_cohort_labels`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/*.json`: `benchmark_model_run_manifest`
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_confidence_interval_payload`

What each generated artifact means:

- snapshot manifests freeze the evidence boundary, leakage controls, requested baselines, and per-source inclusion or exclusion accounting
- cohort label artifacts freeze admissible benchmark membership and future translational outcome labels
- run manifests record executed baseline, code version, parameterization, and input digests
- metric payloads record point estimates for one `(run_id, entity_type, horizon, metric_name)` slice
- confidence interval payloads record percentile-bootstrap intervals, bootstrap count, resample unit, and random seed for the same slice

## Current Runner Coverage

The runner executes only baselines whose frozen protocol status is `available_now`
and that are explicitly listed in the snapshot manifest's `baseline_ids`.

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

The checked-in deterministic fixture intentionally executes only:

- `pgc_only`
- `opentargets_only`
- `v0_current`
- `v1_current`
- `random_with_coverage`

That fixture includes archived `PGC`, `Open Targets`, and `PsychENCODE` inputs, while
`SCHEMA` and `ChEMBL` remain explicit exclusions at the `2024-06-30` cutoff.

## Metric Bundle And Interval Method

Current metric payloads treat
`future_schizophrenia_program_started`,
`future_schizophrenia_program_advanced`, and
`future_schizophrenia_positive_signal`
as the positive retrieval target for each `(entity_type, horizon)` slice.

Primary metrics are computed on the full admissible cohort for that slice. If a baseline
cannot score an admissible entity, that entity stays in evaluation as a deterministic
bottom-tier row and the emitted `notes` keep coverage explicit as
`covered_entities=<covered>/<admissible>`.

The runner emits:

- `average_precision_any_positive_outcome`
- `mean_reciprocal_rank_any_positive_outcome`
- `precision_at_1_any_positive_outcome`
- `precision_at_3_any_positive_outcome`
- `precision_at_5_any_positive_outcome`
- `recall_at_1_any_positive_outcome`
- `recall_at_3_any_positive_outcome`
- `recall_at_5_any_positive_outcome`

`future_schizophrenia_negative_signal` and `no_qualifying_future_outcome` remain explicit
in the label artifact but are treated as non-relevant for the current metric bundle.

The `v1_current` comparator resolves the current additive `v1` output by taking the mean
available `domain_head_score_v1` across domain profiles for each entity.

Current confidence interval payloads use percentile bootstrap with:

- resample unit `entity`, sampled with replacement and replayed in original rank order within each replicate
- explicit bootstrap iteration count on every payload
- explicit random seed on every payload
- deterministic test mode via a fixed seed and reduced iteration count

## Operator Flow

- Re-run the three canonical commands in order whenever the snapshot request, archive descriptors, future-outcome labels, code version, or benchmark parameters change.
- Re-running the snapshot or cohort command overwrites the manifest and label file at the same path.
- Re-running `run-benchmark` writes run-id keyed payload directories. Changing code version or benchmark parameters changes the run id. Identical inputs overwrite the same run-id files.
- If you want scratch outputs without touching the canonical generated path, run the same commands with `.context/...` output paths.

## What This Still Does Not Claim

- historical source backfills
- synthetic historical snapshot generation
- protocol-only baseline execution without the required archived artifacts
- calibration, threshold selection, or production deployment readiness

The goal is a stable benchmark contract that later PRs can widen without changing what
the benchmark means.
