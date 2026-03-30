# Public Benchmark Slices

This directory holds checked-in public historical benchmark slices derived from the
registry-backed `scz_translational_task`.

Current checked-in slices:

- `scz_translational_2024_06_15/`
- `scz_translational_2024_06_18/`
- `scz_translational_2024_06_20/`

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
- `cohort_members.csv`
- `future_outcomes.csv`

Replay outputs stay local under `data/benchmark/generated/public_slices/<slice_id>/`.
The backfill path keeps missing historical archives as explicit exclusions and does
not fall back to live source data.
