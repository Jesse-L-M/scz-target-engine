#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

uv run scz-target-engine build --config config/v0.toml --input-dir examples/v0/input --output-dir examples/v0/output
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_small/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-file data/benchmark/generated/scz_small/snapshot_manifest.json --materialized-at 2026-03-28
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv --output-file data/benchmark/generated/scz_small/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-dir data/benchmark/generated/scz_small/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run python -m scz_target_engine.cli rescue compare baselines --output-dir .context/rescue-baseline-suite
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json
