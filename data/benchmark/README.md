# Benchmark Fixtures

Checked-in benchmark fixture inputs live here for the deterministic snapshot,
cohort, and runner flow introduced across `PR9B` and `PR9C`.

The deterministic fixture path is:

1. Build a snapshot manifest from the frozen protocol plus archived source descriptors.
2. Build cohort labels from a separate cohort-membership file and future-outcomes file.
3. Run the requested `available_now` baselines and emit metric plus interval payloads.

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

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --deterministic-test-mode
```

This flow now proves the benchmark path end to end:

- it writes a real `benchmark_snapshot_manifest`
- it emits explicit per-source inclusion or exclusion entries
- it materializes `benchmark_cohort_labels`
- it executes the requested `available_now` baselines only
- it keeps protocol-only baselines explicit and skipped
- it emits `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload`
