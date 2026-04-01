# contracts-and-compat-v2

Status: implemented
Owner branch: Jesse-L-M/contract-freeze
Depends on: -
Blocked by: -
Supersedes: -
Last updated: 2026-04-01

## Objective

Freeze the compatibility and release-contract layer before the repo adds new
intervention-object, replay, or rescue surfaces.

This spec exists to prevent the next few milestones from quietly breaking the
current `v0`, `v1`, benchmark, rescue, and packet paths while the strategy
shifts from a target engine toward a digital-biology benchmark and intervention
observatory.

## Success Condition

- Primary success metric:
  the current smoke path runs end to end without any silent contract drift:
  candidate registry -> example build -> benchmark fixture -> rescue baseline
  suite -> hypothesis packet generation
- Secondary success metric:
  each planned release family has a registered manifest artifact in the existing
  artifact registry and validates through `scz_target_engine.artifacts`
- Stop/go gate:
  do not begin intervention-object replay or new rescue feature work until the
  compatibility matrix and release-manifest contract are frozen and tested

## Scope

- intervention-object compatibility matrix and dual-write rules
- additive release-family registration in the existing artifact registry
- smoke-path CI definition
- strategy / claim / roadmap doc synchronization for future workspaces
- explicit distribution path for frozen release bundles through GitHub Releases

## Not in Scope

- changing `v0` or `v1` scoring semantics
- adding new ranking or rescue models
- changing benchmark metrics
- package migration or namespace rewrites
- building new UI surfaces

## Existing Surfaces To Reuse

- `docs/artifact_schemas.md` and `scz_target_engine.artifacts`:
  use the existing typed registry instead of inventing a parallel
  `release_contracts/` stack
- `examples/v0/output/` and the current build CLI:
  use the shipped example build as the smoke-path anchor
- `docs/benchmarking.md` fixture workflow:
  extend the current benchmark fixture path instead of defining a new one
- `docs/rescue_tasks.md` and `docs/hidden_eval.md`:
  reuse the existing rescue baseline suite and hidden-eval package boundary
- `docs/hypothesis_packets.md`:
  keep packet generation downstream of existing artifact contracts

## Inputs

- Strategy docs:
  `docs/roadmap.md` and `docs/designs/deep-scz-validate-calibrate.md`
- Current artifact families:
  `gene_target_ledgers`, `decision_vectors_v1`,
  `policy_decision_vectors_v2`, `policy_pareto_fronts_v1`,
  `benchmark_*`, and rescue governance artifacts
- Current smoke-path commands:
  example build, benchmark fixture, rescue baseline suite, and packet generator
- Current runtime validation layer:
  `scz_target_engine.artifacts.load_artifact(...)`

## Outputs And Artifact Contracts

- New or changed artifact:
  registered release-manifest families for:
  `program_memory_release`, `benchmark_release`, `rescue_release`,
  `variant_context_release`, `policy_release`, and `hypothesis_release`
- New or changed artifact:
  one manifest JSON is the registry entrypoint for each release family, not the
  directory itself
- New or changed artifact:
  `docs/intervention_object_compatibility.md` or equivalent compatibility doc
  that maps:
  `intervention_object_id` -> gene/module/policy/packet consumers
- New or changed artifact:
  `scripts/run_contract_smoke_path.sh` and `.github/workflows/ci.yml` pin the
  same smoke-path command set
- Backward-compatibility rule:
  existing artifact names, current CLI commands, and current generated example
  outputs stay valid during the dual-write period

## Data Flow

```text
ROADMAP + SHIPPED ARTIFACTS
    -> COMPATIBILITY MATRIX
    -> RELEASE MANIFEST SCHEMAS
    -> SMOKE PATH COMMAND SET
    -> REGISTRY VALIDATION
    -> CI + GITHUB RELEASE DOCUMENTATION
```

- The roadmap defines the target state.
- The current shipped artifacts define the compatibility boundary.
- The compatibility matrix freezes how new intervention objects project back to
  current consumers.
- The release-manifest schemas freeze what a public release bundle must contain.
- The smoke path proves those contracts still compose in one fresh run.

## Implementation Reality

- The compatibility matrix is now checked in at
  `docs/intervention_object_compatibility.md`.
- The six top-level release families are registered in
  `schemas/artifact_schemas/` as:
  `program_memory_release`, `benchmark_release`, `rescue_release`,
  `variant_context_release`, `policy_release`, and `hypothesis_release`.
- `scz_target_engine.artifacts` now validates those manifest entrypoints against
  required files, SHA256 digests, and nested registered artifact schema
  versions.
- The shared smoke path lives at `scripts/run_contract_smoke_path.sh`, rebuilds
  the frozen example outputs into a temporary directory, and fails on drift from
  `examples/v0/output/`.
- CI now executes that same script from `.github/workflows/ci.yml` and also
  asserts that `examples/v0/output/` stays clean afterward.
- The shared release-manifest choice is logged in
  `docs/decisions/0002-release-manifest-contract.md`.

## Implementation Plan

1. Write the intervention-object compatibility matrix and dual-write policy.
2. Register the release-manifest families in the existing artifact registry.
3. Add helpers or validators that verify required files, digests, and schema
   versions from the top-level release manifest.
4. Pin the smoke-path command sequence in docs and CI.
5. Update README / claim / roadmap pointers so future agents know where strategy,
   shipped behavior, and build-specs live.

## Acceptance Tests

- Unit:
  add artifact-registry validation tests for each new release-manifest family in
  `tests/test_artifacts.py`
- Integration:
  run the smoke path from a clean worktree, confirm the example build is replayed
  into a temporary directory, and fail on any drift from the frozen example
  outputs under `examples/v0/output/`
- Regression:
  add a test that fails if a release bundle validates without a required file or
  checksum entry
- E2E, if relevant:
  one CI job runs the documented smoke path and fails if
  `examples/v0/output/` drifts

Required local commands:

```bash
uv run --group dev pytest
uv run --group dev pytest tests/test_artifacts.py
./scripts/run_contract_smoke_path.sh
```

## Failure Modes

- Failure mode:
  two distinct intervention objects collapse onto one legacy consumer row and
  silently corrupt replay or packet logic; the compatibility matrix must make
  multiplicity explicit and tests must reject ambiguous projections
- Failure mode:
  a release bundle looks valid because files exist, but one file is stale or from
  another run; the manifest must pin checksums and schema versions
- Failure mode:
  the docs claim a smoke path that CI does not run; CI must execute the same
  command set listed in the docs
- Failure mode:
  smoke verification rewrites checked-in fixtures and hides contract drift; the
  smoke path must compare a temporary build against the frozen example outputs

## Rollout / Compatibility

- `v0`, `v1`, benchmark fixture, rescue baseline suite, and hypothesis packet
  flows remain supported unchanged
- intervention-object outputs may be additive, but current consumers keep their
  existing artifact surfaces during the dual-write period
- a breaking change is any PR that renames, drops, or semantically repurposes an
  existing artifact or CLI path without a compatibility bridge

## Open Questions

- The compatibility matrix is checked in as prose for Milestone 0.
  A machine-readable mapping artifact is deferred until replay or rescue work
  needs to materialize intervention-object-native dual-write bundles.
- Release-manifest validation now verifies exact required file paths, SHA256
  digests, and nested registered artifact schema versions.
  Minimum semantic content counts stay deferred to the owning milestone.

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`
- `docs/decisions/0002-release-manifest-contract.md`

## Commands

```bash
./scripts/run_contract_smoke_path.sh
uv run --group dev pytest
uv run --group dev pytest tests/test_artifacts.py
uv run scz-target-engine build --config config/v0.toml --input-dir examples/v0/input --output-dir examples/v0/output
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_small/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-file data/benchmark/generated/scz_small/snapshot_manifest.json --materialized-at 2026-03-28
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv --output-file data/benchmark/generated/scz_small/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-dir data/benchmark/generated/scz_small/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run python -m scz_target_engine.cli rescue compare baselines --output-dir .context/rescue-baseline-suite
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json
```
