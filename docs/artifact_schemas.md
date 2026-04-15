# Artifact Schemas

The artifact registry covers the families the repo emits today plus contract artifacts
that future task work can consume without changing current build semantics.

## Registered Families

- `program_memory_release`
- `program_memory_v3_source_manifest`
- `program_memory_v3_study_index`
- `program_memory_v3_result_observations`
- `program_memory_v3_harm_observations`
- `program_memory_v3_contradiction_log`
- `program_memory_v3_claim_ledger`
- `program_memory_v3_caveats`
- `program_memory_v3_belief_updates`
- `program_memory_v3_program_card`
- `program_memory_v3_insight_packet`
- `benchmark_release`
- `rescue_release`
- `variant_context_release`
- `policy_release`
- `hypothesis_release`
- `benchmark_snapshot_manifest`
- `benchmark_cohort_members`
- `benchmark_source_cohort_members`
- `benchmark_source_future_outcomes`
- `benchmark_cohort_manifest`
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
- `hypothesis_packets_v1`
- `prospective_prediction_registration`
- `prospective_forecast_outcome_log`

The schema files live under `schemas/artifact_schemas/`. Rescue governance schemas
now live under `schemas/artifact_schemas/rescue/`.

## Release Manifest Entry Points

Milestone 0 freezes six top-level release-manifest families inside the existing
artifact registry:

- `program_memory_release`
- `benchmark_release`
- `rescue_release`
- `variant_context_release`
- `policy_release`
- `hypothesis_release`

Each of those artifacts is a top-level manifest JSON entrypoint for one release
bundle, not a directory name masquerading as a contract.

The shared release-manifest contract now pins:

- required files using paths relative to the manifest location
- a SHA256 digest for every required file
- `artifact_name` plus `expected_schema_version` whenever a required file is
  itself a registered artifact

Validation fails on:

- missing required files
- checksum drift
- nested registered artifact schema-version drift

The cross-cutting decision for that shared contract lives in
`docs/decisions/0002-release-manifest-contract.md`.

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

Release manifests use the same loader surface:

```python
from pathlib import Path

from scz_target_engine.artifacts import load_artifact

release_artifact = load_artifact(
    Path("dist/benchmark_release/benchmark_release_manifest.json"),
)
assert release_artifact.artifact_name == "benchmark_release"
```

`load_artifact` validates structure and returns a `ValidatedArtifact`.
For benchmark artifacts, the payload is the existing typed model already used by the
benchmark code:

- `BenchmarkSnapshotManifest`
- `tuple[CohortMember, ...]`
- `tuple[FutureOutcomeRecord, ...]`
- `BenchmarkCohortManifest`
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
- `ProspectivePredictionRegistration`
- `ProspectiveForecastOutcomeLog`
- `ReleaseManifest`

The registry-driven benchmark path keeps those same emitted families. The additive
contract provenance fields live on the manifest artifacts:

- `BenchmarkSnapshotManifest` may include optional `benchmark_suite_id` and `benchmark_task_id`
- `BenchmarkModelRunManifest` may include optional `benchmark_suite_id` and `benchmark_task_id`

The program-memory `v3` draft dossier path now also registers first-pass stable
artifact families for single-program review bundles and scoped synthesis packets:

- `program_memory_v3_source_manifest`
- `program_memory_v3_study_index`
- `program_memory_v3_result_observations`
- `program_memory_v3_harm_observations`
- `program_memory_v3_contradiction_log`
- `program_memory_v3_claim_ledger`
- `program_memory_v3_caveats`
- `program_memory_v3_belief_updates`
- `program_memory_v3_program_card`
- `program_memory_v3_insight_packet`

These are draft but explicit repo-wide contracts, not ad hoc workflow files. They
exist so `harvest-program`, `adjudicate-program`, and `build-insight-packet` can
emit self-validating machine-readable bundles from day one.

Within that `program_memory_v3` family, the current Gate 1 hardening contract now
expects:

- immutable source-capture metadata in `source_manifest.json`, including
  `capture_method`, `captured_at`, `raw_artifact_path`, `content_sha256`,
  `source_version`, and `content_type` for each source document; current KarXT
  fixtures now use source-faithful ClinicalTrials.gov JSON and PubMed XML
  captures, while unresolved ClinicalTrials.gov history context stays explicit as
  URL-seed records instead of pretending to be raw snapshots
- first-class structured confidence fields in `claims.csv` and
  `belief_updates.csv`
- first-class randomized, treated, and efficacy-analysis denominator fields plus
  comparator values in `result_observations.csv` and `harm_observations.csv`

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
- `hypothesis_packets_v1` validates policy-scoped gene hypothesis packets built from
  shipped `policy_decision_vectors_v2` and `gene_target_ledgers` artifacts, including
  explicit decision focus, first-class evidence anchors, explicit anchor-gap states,
  digest fields, explicit contradiction handling, explicit failure-escape logic, and
  per-packet traceability pointers that now dereference against the source artifacts
  rather than only checking string presence; packet policy signals must stay genuinely
  scored when `require_scored_policy_signal` is true, score pointers must resolve
  inside the same entity-scoped policy context, and the additive review-facing fields
  must stay derived from the same shipped policy and failure-memory payloads; empty
  packet artifacts are valid when no target meets the packet-generation criteria;
  packet materialization now validates the generated artifact before write/return so
  the build path cannot emit self-invalid packet outputs
- `prospective_prediction_registration` validates immutable forecast registrations
  anchored to a shipped `hypothesis_packets_v1` artifact, including exact packet-file
  checksum pinning, packet-pointer dereferencing, full frozen reviewed packet payload
  equality, and a separately hashed scoreable forecast payload that must preserve the
  packet decision options, a single highest-probability predicted outcome, explicit
  outcome-window dates, and rationale text
- `prospective_forecast_outcome_log` validates append-only realized-outcome logs
  against previously registered forecasts, including registration-file checksum pinning,
  observed outcomes constrained to the frozen forecast option set, and evidence-file
  checksum validation so later reconciliation can mark pending, resolved, or conflicted
  histories without silently mutating prior records
- `rescue_task_contract` validates registry-backed rescue task identity, artifact
  interface declarations, and the strict no-leakage rescue boundary
- `rescue_dataset_card` validates that a governed rescue dataset card resolves back
  to a declared rescue task artifact contract
- `rescue_task_card` validates that the checked-in governance bundle points at the
  declared rescue contract and strict leakage policy, and `load_artifact(...,
  artifact_name=\"rescue_task_card\")` now runs the full cross-file bundle validation
  instead of only checking the task-card file in isolation
- the rescue registry path now requires the registered `task_card_file` to pass that
  same normal `load_artifact(..., artifact_name=\"rescue_task_card\")` validation
  before it will return a registered rescue task contract
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
