# 0003 Track B Benchmark Task

Status: active
Date: 2026-04-05

## Context

Track B needs a real failure-memory benchmark path:
checked-in cases, frozen structural labels, executable baselines, metrics, and
case review outputs.

The repo already shipped one benchmark stack:
registry-backed snapshot, cohort, runner, reporting, and benchmark artifact
families.

Without an explicit decision, Track B could easily drift into one of two bad
shapes:

- a second ad hoc replay benchmark stack with different commands and output
  families
- mutation of the shipped public-slice catalog instead of a dedicated principal
  Track B fixture

Track B also has to keep the current anti-leakage posture:
no fallback from missing historical archives to live source data or repo-head
program-memory state.

## Decision

Track B ships as a second benchmark task row inside the existing benchmark
suite:

1. task id: `scz_failure_memory_track_b_task`
2. suite id: `scz_translational_suite`
3. question id: `scz_failure_memory_track_b_v1`
4. protocol id: `track_b_structural_replay_protocol_v1`
5. fixture path: `data/benchmark/fixtures/scz_failure_memory_2025_02_01/`

Track B reuses the existing benchmark command sequence and artifact families:

1. `build-benchmark-snapshot`
2. `build-benchmark-cohort`
3. `run-benchmark`
4. `build-benchmark-reporting`

Track B adds only additive checked-in inputs and derived sidecars:

1. `track_b_casebook.csv`
2. slice-local `program_universe.csv`, `events.csv`, `assets.csv`,
   `event_provenance.csv`, and `directionality_hypotheses.csv`
3. `runner_outputs/track_b_case_outputs/<run_id>.json`
4. `runner_outputs/track_b_confusion_summaries/<run_id>.json`
5. `public_payloads/error_analysis/.../<track_b_public_id>.md`
6. `public_payloads/error_analysis/.../<track_b_public_id>.json`

Those Track B sidecars are not new top-level benchmark schema families.

The principal checked-in Track B slice is the dedicated
`scz_failure_memory_2025_02_01` fixture, not the public-slice catalog.

Track B cohort artifacts are part of the same declared surface:

- `cohort_members.csv` uses the same six proposal ids as `track_b_casebook.csv`
- `build-benchmark-cohort` derives Track B replay-status labels from the casebook
  on horizon `structural_replay`
- runner and reporting fail closed if cohort ids, labels, or replay-status golds
  diverge from the casebook

The strict no-fallback archive rule remains unchanged.

## Consequences

### Good

- Track B stays inside the shipped benchmark workflow and artifact registry
- snapshot, cohort, runner, and reporting keep one explicit structural replay
  contract surface
- public Track B report cards, leaderboards, and case reviews are now derived
  from one validated reporting bundle, not a mix of unchecked manifest
  provenance and independently trusted sidecars
- the public-slice catalog remains focused on Track A intervention-object replay
- Track B case review outputs are reproducible from frozen runner sidecars

### Cost

- the benchmark registry now has multiple tasks under one suite, so task
  resolution has to consider explicit task/question/protocol ids plus entity
  types and baseline sets
- Track B-specific reporting needs additive sidecars because the core benchmark
  schema families remain task-agnostic
- Track B reporting now has a stricter fail-closed contract:
  the full expected baseline set must be present, interval provenance is bound
  to the run-manifest seed contract, public provenance is reconstructed from
  the validated cohort bundle plus pinned source artifacts, schema identity is
  enforced on every reporting-consumed runner artifact, public Track B outputs
  use stable public IDs derived from `snapshot_id`, `baseline_id`, and the
  validated public `run_parameterization`, redact `code_version` as
  `redacted_untrusted_runner_code_version`, redact self-attested runner
  timestamps and notes, keep runner `run_id` only as an internal
  bundle-consistency check, reject unexpected Track B parameterization keys,
  require explicit metric units, materialize the advertised
  `validated_track_b_runner_bundle/<track_b_public_id>/...` files, recompute
  and validate public `leaderboard_id`, pin public `source_snapshots`,
  `evaluation_input_artifacts`, and `derived_from_artifacts` `sha256` / `notes`
  against trusted files, rebuild expected Track B case outputs from the pinned
  casebook plus dataset instead of trusting self-consistent public bundles,
  require leaderboards to reference the complete expected baseline set, enforce
  relative-only public paths, and fail closed on duplicate input-artifact names

## Affected Specs

- `docs/designs/replay-track-b-v1.md`
- `docs/benchmarking.md`
- `data/benchmark/README.md`
