# Public Benchmark Slices

This directory holds checked-in public historical benchmark slices derived from the
registry-backed `scz_translational_task`.

The shipped public slices are now the Track A intervention-object replay fixtures.
They keep the frozen benchmark question and leakage controls, but they materialize
`entity_type = intervention_object` and execute only `v0_current`,
`v1_current`, and `random_with_coverage`.

Current checked-in slices:

- `scz_translational_2024_06_15`
- `scz_translational_2024_06_18`
- `scz_translational_2024_06_20`
- `scz_translational_2024_07_15`
- `scz_translational_2024_11_11`
- `scz_translational_2025_01_16`
- As of April 8, 2026, those slices are honest and replayable, but none are evaluable on the principal `3y` horizon after strict replay filtering.

The catalog at `catalog.json` records the explicit source inclusions and exclusions
for each cutoff. The slice builder is:

```bash
uv run scz-target-engine backfill-benchmark-public-slices \
  --output-dir data/benchmark/public_slices \
  --benchmark-task-id scz_translational_task
```

Each slice directory contains only checked-in fixture inputs:

- `snapshot_request.json`
- `source_archives.json`
- `archives/`
- `program_universe.csv`
- `events.csv`
- `cohort_members.csv`
- `future_outcomes.csv`

Those `cohort_members.csv` and `future_outcomes.csv` inputs are intervention-object
tables derived from the checked-in denominator and program-history event ledger.
They are not copies of the canonical `scz_small` gene/module fixture rows.

Replay outputs stay local under `data/benchmark/generated/public_slices/<slice_id>/`.
The main generated additions for Track A are:

- `snapshot_manifest.json` plus `intervention_object_feature_bundle.parquet`
- `runner_outputs/baseline_projections/`
- `public_payloads/leaderboards/`
- `public_payloads/error_analysis/`

Replay example for the checked-in `scz_translational_2025_01_16` slice:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/public_slices/scz_translational_2025_01_16/snapshot_request.json \
  --archive-index-file data/benchmark/public_slices/scz_translational_2025_01_16/source_archives.json \
  --output-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/snapshot_manifest.json \
  --materialized-at 2026-04-08

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/snapshot_manifest.json \
  --cohort-members-file data/benchmark/public_slices/scz_translational_2025_01_16/cohort_members.csv \
  --future-outcomes-file data/benchmark/public_slices/scz_translational_2025_01_16/future_outcomes.csv \
  --output-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/cohort_labels.csv \
  --archive-index-file data/benchmark/public_slices/scz_translational_2025_01_16/source_archives.json \
  --output-dir data/benchmark/generated/public_slices/scz_translational_2025_01_16/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2025_01_16/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/public_slices/scz_translational_2025_01_16/runner_outputs \
  --output-dir data/benchmark/generated/public_slices/scz_translational_2025_01_16/public_payloads
```

The backfill path keeps missing historical archives as explicit exclusions and does
not fall back to live source data.
