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

## Generated

The canonical local output path is `data/benchmark/generated/scz_small/`.
That directory is generated, not checked in.

- `data/benchmark/generated/scz_small/snapshot_manifest.json`: `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv`: `benchmark_cohort_labels`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/*.json`: `benchmark_model_run_manifest`
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_confidence_interval_payload`

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
```

This fixture flow proves the benchmark path end to end:

- it resolves the explicit `scz_translational_suite` / `scz_translational_task` contract from `data/curated/rescue_tasks/task_registry.csv`
- it writes a real `benchmark_snapshot_manifest`
- it emits explicit per-source inclusion or exclusion entries
- it materializes `benchmark_cohort_labels`
- it executes the requested `available_now` baselines only
- it keeps protocol-only baselines explicit and skipped
- it emits `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload`

Current boundary:

- the historical archives are fixture-scale, not a production backfill catalog
- benchmark breadth is still limited to the frozen schizophrenia question plus a small deterministic cohort
- calibration work and operating-point claims remain future work
