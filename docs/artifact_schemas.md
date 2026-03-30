# Artifact Schemas

The artifact registry covers the families the repo emits today plus contract artifacts
that future task work can consume without changing current build semantics.

## Registered Families

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`
- `rescue_dataset_card`
- `rescue_task_card`
- `rescue_freeze_manifest`
- `rescue_split_manifest`
- `rescue_raw_to_frozen_lineage`
- `rescue_task_contract`
- `gene_target_ledgers`
- `decision_vectors_v1`
- `policy_decision_vectors_v2`
- `domain_head_rankings_v1`
- `policy_pareto_fronts_v1`

The schema files live under `schemas/artifact_schemas/`. Rescue governance schemas
now live under `schemas/artifact_schemas/rescue/`.

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

For rescue task contracts and governance artifacts, validation now returns typed
rescue models:

- `RescueTaskContract`
- `RescueDatasetCard`
- `RescueTaskCard`
- `RescueFreezeManifest`
- `RescueSplitManifest`
- `RescueRawToFrozenLineage`

The registry-driven benchmark path keeps those same emitted families. The additive
contract provenance fields live on the manifest artifacts:

- `BenchmarkSnapshotManifest` may include optional `benchmark_suite_id` and `benchmark_task_id`
- `BenchmarkModelRunManifest` may include optional `benchmark_suite_id` and `benchmark_task_id`

For current ledger and `v1` artifacts, validation stays additive and non-invasive:

- `gene_target_ledgers` validates the current top-level payload plus derived counts
  against the existing nested ledger shape, even when the underlying checked-in
  program-memory source of truth is the normalized `program_history/v2` dataset
- `decision_vectors_v1` validates the emitted decision-head and domain-profile contract
  against the current `v1` definitions
- `policy_decision_vectors_v2` validates the checked-in policy definitions used for
  the build, the per-entity keyed and ordered policy views, and the explicit replay
  or uncertainty payload carried by each policy score
- `domain_head_rankings_v1` validates the emitted flat ranking columns against the
  current `v1` head set
- `policy_pareto_fronts_v1` validates the ordered policy dimensions and the grouped
  Pareto-front rows emitted for genes and modules
- `rescue_task_contract` validates registry-backed rescue task identity, artifact
  interface declarations, and the strict no-leakage rescue boundary
- `rescue_dataset_card` validates that a governed rescue dataset card resolves back
  to a declared rescue task artifact contract
- `rescue_task_card` validates that the checked-in governance bundle points at the
  declared rescue contract and strict leakage policy, and `load_artifact(...,
  artifact_name=\"rescue_task_card\")` now runs the full cross-file bundle validation
  instead of only checking the task-card file in isolation
- `rescue_freeze_manifest` validates the cutoff, current-head policy, upstream raw
  snapshots, and governed frozen outputs
- `rescue_split_manifest` validates deterministic split declarations against a frozen
  ranking-input dataset
- `rescue_raw_to_frozen_lineage` validates the raw-source to transformation-step to
  frozen-dataset chain that later rescue-data PRs must preserve

## Scope Boundary

- The registry covers current emitted artifact families plus explicit contract artifacts.
- Rescue task contracts and rescue governance records are registered as contract
  artifacts, not as emitted task data.
- The registry does not change build outputs or benchmark semantics.
- `snapshot_request.json` and `source_archives.json` remain operator inputs, not
  registered emitted artifact families.
