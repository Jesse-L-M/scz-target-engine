#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

FROZEN_EXAMPLE_OUTPUT_DIR="${ROOT_DIR}/examples/v0/output"
SMOKE_BUILD_OUTPUT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/scz-contract-smoke.XXXXXX")"

cleanup() {
  rm -rf "${SMOKE_BUILD_OUTPUT_DIR}"
}

trap cleanup EXIT

uv run scz-target-engine build --config config/v0.toml --input-dir examples/v0/input --output-dir "${SMOKE_BUILD_OUTPUT_DIR}"

python3 - "${SMOKE_BUILD_OUTPUT_DIR}" "${FROZEN_EXAMPLE_OUTPUT_DIR}" <<'PY'
from __future__ import annotations

import difflib
import sys
from pathlib import Path


def _files(path: Path) -> list[str]:
    return sorted(item.name for item in path.iterdir() if item.is_file())


generated_dir = Path(sys.argv[1])
frozen_dir = Path(sys.argv[2])

generated_files = _files(generated_dir)
frozen_files = _files(frozen_dir)

if generated_files != frozen_files:
    print("smoke build output file set drifted from examples/v0/output", file=sys.stderr)
    print(f"generated: {generated_files}", file=sys.stderr)
    print(f"frozen: {frozen_files}", file=sys.stderr)
    raise SystemExit(1)

for name in generated_files:
    generated_bytes = (generated_dir / name).read_bytes()
    frozen_bytes = (frozen_dir / name).read_bytes()
    if generated_bytes == frozen_bytes:
        continue
    print(f"smoke build output drifted for {name}", file=sys.stderr)
    generated_lines = generated_bytes.decode("utf-8", errors="replace").splitlines(
        keepends=True
    )
    frozen_lines = frozen_bytes.decode("utf-8", errors="replace").splitlines(
        keepends=True
    )
    diff = difflib.unified_diff(
        frozen_lines,
        generated_lines,
        fromfile=str(frozen_dir / name),
        tofile=str(generated_dir / name),
    )
    sys.stderr.writelines(diff)
    raise SystemExit(1)
PY

uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_small/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-file data/benchmark/generated/scz_small/snapshot_manifest.json --materialized-at 2026-03-28
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv --output-file data/benchmark/generated/scz_small/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-dir data/benchmark/generated/scz_small/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run python -m scz_target_engine.cli rescue compare baselines --output-dir .context/rescue-baseline-suite
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact "${SMOKE_BUILD_OUTPUT_DIR}/policy_decision_vectors_v2.json" --ledger-artifact "${SMOKE_BUILD_OUTPUT_DIR}/gene_target_ledgers.json" --output-file .context/hypothesis_packets_v1.json

git diff --exit-code -- examples/v0/output

example_output_status="$(git status --short --untracked-files=all -- examples/v0/output)"
if [[ -n "${example_output_status}" ]]; then
  printf '%s\n' "${example_output_status}" >&2
  exit 1
fi
