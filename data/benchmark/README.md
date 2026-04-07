# Benchmark Fixtures

This directory holds the checked-in inputs for the canonical deterministic benchmark
workflow shipped on `main`.

## Checked In

- `data/curated/rescue_tasks/task_registry.csv`: registry-backed suite/task contract source of truth
- `fixtures/scz_small/snapshot_request.json`: frozen snapshot request
- `fixtures/scz_small/source_archives.json`: archived source descriptor index with digests
- `fixtures/scz_small/archives/`: small fixture-scale archived source extracts
- `fixtures/scz_small/cohort_members.csv`: admissible ranking cohort
- `fixtures/scz_small/future_outcomes.csv`: post-cutoff label adjudication input
- `fixtures/scz_failure_memory_2025_02_01/`: checked-in Track B fixture with explicit structural replay question `scz_failure_memory_track_b_v1`, `snapshot_request.json`, `source_archives.json`, `track_b_casebook.csv`, pinned local `assets.csv`, `events.csv`, `event_provenance.csv`, `directionality_hypotheses.csv`, `program_universe.csv`, `cohort_members.csv`, and placeholder `future_outcomes.csv`
- `public_slices/catalog.json`: checked-in catalog of honest public historical slices derived from the registry-backed fixture task, including principal-horizon evaluability metadata

## Generated

The canonical local output path is `data/benchmark/generated/scz_small/`.
That directory is generated, not checked in. Public slice replays write to
`data/benchmark/generated/public_slices/<slice_id>/`.

- `data/benchmark/generated/scz_small/snapshot_manifest.json`: `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/benchmark_cohort_members.csv`: `benchmark_cohort_members`
- `data/benchmark/generated/scz_small/source_cohort_members.csv`: `benchmark_source_cohort_members`
- `data/benchmark/generated/scz_small/source_future_outcomes.csv`: `benchmark_source_future_outcomes`
- `data/benchmark/generated/scz_small/benchmark_cohort_manifest.json`: `benchmark_cohort_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv`: `benchmark_cohort_labels`
- `data/benchmark/generated/public_slices/<slice_id>/intervention_object_feature_bundle.parquet`: generated snapshot-side intervention-object replay bundle when the slice requests `entity_type = intervention_object`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/*.json`: `benchmark_model_run_manifest`
- `data/benchmark/generated/public_slices/<slice_id>/runner_outputs/baseline_projections/<baseline_id>__intervention_object.json`: explicit intervention-object projection sidecar for `v0_current` and `v1_current`
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_confidence_interval_payload`
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/metric_payloads/<run_id>/intervention_object/structural_replay/<metric>.json`: Track B `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/confidence_interval_payloads/<run_id>/intervention_object/structural_replay/<metric>.json`: Track B `benchmark_confidence_interval_payload`
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/track_b_case_outputs/<run_id>.json`: Track B per-case structural output sidecar
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/track_b_confusion_summaries/<run_id>.json`: Track B confusion summary sidecar
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/report_cards/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/<run_id>.json`: Track B public report card payload
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/leaderboards/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/intervention_object/structural_replay/<metric>.json`: Track B public leaderboard payload
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/error_analysis/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/<run_id>.md`: Track B markdown case review
- `data/benchmark/generated/scz_small/public_payloads/report_cards/scz_translational_suite/scz_translational_task/scz_fixture_2024_06_30/<run_id>.json`: public report card payload
- `data/benchmark/generated/scz_small/public_payloads/leaderboards/scz_translational_suite/scz_translational_task/scz_fixture_2024_06_30/<entity_type>/<horizon>/<metric>.json`: public leaderboard payload
- `data/benchmark/generated/public_slices/<slice_id>/public_payloads/error_analysis/scz_translational_suite/scz_translational_task/<snapshot_id>/<run_id>.md`: markdown error analysis for intervention-object replay runs when the principal intervention-object slice is evaluable

## Canonical Command Sequence

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

uv run scz-target-engine backfill-benchmark-public-slices \
  --output-dir data/benchmark/public_slices \
  --benchmark-task-id scz_translational_task

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_small/runner_outputs \
  --output-dir data/benchmark/generated/scz_small/public_payloads
```

This fixture flow proves the benchmark path end to end:

- it resolves the explicit `scz_translational_suite` / `scz_translational_task` contract from `data/curated/rescue_tasks/task_registry.csv`
- it writes a real `benchmark_snapshot_manifest`
- it emits explicit per-source inclusion or exclusion entries
- it materializes a canonical `benchmark_cohort_members` denominator, bundle-local source-copy artifacts, and a digest-pinned `benchmark_cohort_manifest`
- it materializes `benchmark_cohort_labels`
- it executes the requested `available_now` baselines only
- it keeps protocol-only baselines explicit and skipped
- it emits `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload`
- it derives public report cards and leaderboard payloads from those emitted artifacts without rerunning model inference
- Track B reporting additionally revalidates one complete owned reporting bundle:
  exact expected baseline set, canonical schema identity for every
  reporting-consumed runner artifact, exact Track B run-parameterization shape,
  run-id binding for the full published code version, pinned source-artifact
  provenance, manifest-only casebook/count provenance, pinned metric units,
  duplicate-artifact rejection, and deterministic interval-seed provenance
- when replaying intervention-object public slices, it also emits an explicit snapshot-side feature bundle, baseline projection sidecars, and markdown error-analysis outputs only for evaluable principal-horizon slices

Track B uses the same four commands with the checked-in failure-memory fixture:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json \
  --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --materialized-at 2026-04-05

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json \
  --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs \
  --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads
```

That Track B flow keeps the benchmark artifact families unchanged while adding:

- `track_b_casebook.csv` as a checked-in fixture input
- casebook-derived `benchmark_cohort_labels` on horizon `structural_replay`
- structural metric payloads on horizon `structural_replay`
- runner sidecars for per-case outputs and confusion summaries
- reporting-side markdown case reviews under `public_payloads/error_analysis/`
- fail-closed reporting validation for missing baselines, bundle swaps,
  tampered manifest input artifacts, tampered schema identity, forged
  same-prefix code provenance, extra Track B parameterization keys, tampered
  metric units, duplicate input-artifact names, tampered interval seeds, and
  tampered Track B casebook/count provenance

Public slices keep the same registry-driven task contract while changing only the
cutoff date, checked-in fixture path, and entity type. The catalog in
`data/benchmark/public_slices/catalog.json`
records which sources were honestly included or excluded at each cutoff. The
namespaced alias `uv run scz-target-engine benchmark backfill public-slices`
routes to the same builder and flags as the flat command above.

Current replay split:

- `fixtures/scz_small/` remains the canonical gene/module regression path, with the restored minimal pre-Track-A archive surface
- `fixtures/scz_failure_memory_2025_02_01/` is the checked-in Track B structural replay slice, pinned to the 2025-02-01 cutoff with a frozen `track_b_casebook.csv`
- that Track B fixture requires `track_b_casebook.csv`, `program_universe.csv`, `events.csv`, `assets.csv`, `event_provenance.csv`, and `directionality_hypotheses.csv` beside `source_archives.json`, and snapshot build validates that contract up front
- `cohort_members.csv` in the Track B fixture uses the same six proposal ids as the casebook, and `build-benchmark-cohort` fails closed if they diverge
- `public_slices/` now exercise the shipped Track A intervention-object replay path
- those intervention-object slices use only `v0_current`, `v1_current`, and `random_with_coverage`
- the Track B fixture uses only `track_b_exact_target`, `track_b_target_class`, `track_b_nearest_history`, and `track_b_structural_current`
- checked-in intervention-object `cohort_members.csv` ids use the full replay grain `asset_lineage_id / target_class_lineage_id / modality / domain / population / regimen / stage_bucket`
- generated cohort outputs now pin runner/reporting to `benchmark_cohort_members.csv` and `benchmark_cohort_manifest.json`, so downstream scoring does not trust ad hoc edits to `cohort_labels.csv`

Replay example beyond the original `scz_small` path, using the checked-in `scz_translational_2024_06_20` slice:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/public_slices/scz_translational_2024_06_20/snapshot_request.json \
  --archive-index-file data/benchmark/public_slices/scz_translational_2024_06_20/source_archives.json \
  --output-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json \
  --materialized-at 2026-04-02
```

This replay path does not fall back to live source data when a historical archive
is missing; the slice catalog keeps those sources as explicit exclusions.

As of April 2, 2026, `scz_translational_task` still ships honest public slices in the
checked-in catalog at `2024-06-15`, `2024-06-18`, and `2024-06-20`, but none are evaluable on the principal `3y` horizon because every one collapses to zero positive
intervention-object outcomes after strict replay filtering.

Current boundary:

- the historical archives are fixture-scale, not a production backfill catalog
- benchmark breadth is still limited to the frozen schizophrenia question plus a small deterministic cohort
- calibration work and operating-point claims remain future work
