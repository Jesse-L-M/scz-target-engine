# Artifact Schemas

`PR-01` registers the artifact families the repo already emits today without changing
their meaning.

## Registered Families

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`
- `gene_target_ledgers`
- `decision_vectors_v1`
- `domain_head_rankings_v1`

The schema files live under `schemas/artifact_schemas/`.

## Runtime Validation

The runtime loader and validator surface lives under `scz_target_engine.artifacts`.

Use `load_artifact` when you want to infer the family from the file contents or path:

```python
from pathlib import Path

from scz_target_engine.artifacts import load_artifact

ledger_artifact = load_artifact(Path("examples/v0/output/gene_target_ledgers.json"))
assert ledger_artifact.artifact_name == "gene_target_ledgers"
```

Use the explicit `artifact_name` argument when you want to pin validation to a known
family:

```python
from pathlib import Path

from scz_target_engine.artifacts import load_artifact

manifest_artifact = load_artifact(
    Path("data/benchmark/generated/scz_small/snapshot_manifest.json"),
    artifact_name="benchmark_snapshot_manifest",
)
```

`load_artifact` validates structure and returns a `ValidatedArtifact`.
For benchmark artifacts, the payload is the existing typed model already used by the
benchmark code:

- `BenchmarkSnapshotManifest`
- `tuple[BenchmarkCohortLabel, ...]`
- `BenchmarkModelRunManifest`
- `BenchmarkMetricOutputPayload`
- `BenchmarkConfidenceIntervalPayload`

For current ledger and `v1` artifacts, validation stays additive and non-invasive:

- `gene_target_ledgers` validates the current top-level payload plus derived counts
  against the existing nested ledger shape
- `decision_vectors_v1` validates the emitted decision-head and domain-profile contract
  against the current `v1` definitions
- `domain_head_rankings_v1` validates the emitted flat ranking columns against the
  current `v1` head set

## Scope Boundary

- The registry covers current emitted artifact families only.
- The registry does not change build outputs or benchmark semantics.
- `snapshot_request.json` and `source_archives.json` remain operator inputs, not
  registered emitted artifact families.
