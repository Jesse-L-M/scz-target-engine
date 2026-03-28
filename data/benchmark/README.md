# Benchmark Fixtures

Checked-in benchmark fixture inputs live here for the runner-free snapshot and cohort
materialization flow introduced in `PR9B`.

The deterministic fixture path is:

1. Build a snapshot manifest from the frozen protocol plus archived source descriptors.
2. Build cohort labels from a separate cohort-membership file and future-outcomes file.

Fixture command sequence:

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
```

This flow intentionally stays runner-free:

- it writes a real `benchmark_snapshot_manifest`
- it emits explicit per-source inclusion or exclusion entries
- it materializes `benchmark_cohort_labels`
- it does not compute benchmark metrics or execute the baseline matrix

