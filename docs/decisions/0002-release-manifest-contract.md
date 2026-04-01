# 0002 Release Manifest Contract

Status: active
Date: 2026-04-01

## Context

Milestone 0 freezes compatibility before replay, rescue, atlas, policy, and
credibility work expand the artifact surface.

Those later milestones need one shared release-bundle contract so they do not
invent separate validation stacks, drift on file naming, or quietly weaken the
current artifact registry guarantees.

## Decision

The repo will use one top-level release-manifest contract across six registered
artifact families:

1. `program_memory_release`
2. `benchmark_release`
3. `rescue_release`
4. `variant_context_release`
5. `policy_release`
6. `hypothesis_release`

Each release-manifest artifact must:

1. be the registry entrypoint for its bundle
2. list required files using paths relative to the manifest location
3. pin a SHA256 digest for every required file
4. pin `artifact_name` plus `expected_schema_version` whenever the file is
   itself a registered artifact
5. fail validation on missing files, checksum drift, or nested schema-version
   drift

## Consequences

### Good

- later milestones inherit one additive release contract instead of six custom
  validators
- bundle validation stays inside `scz_target_engine.artifacts`
- release manifests become the place where dual-write compatibility bundles can
  declare exact file membership without renaming current consumer artifacts

### Cost

- future release builders have to emit checksums for every required file
- opaque files such as parquet, markdown, or csv sidecars still need explicit
  manifest entries even when they are not registered artifacts

## Affected Specs

- `docs/designs/contracts-and-compat-v2.md`
- `docs/designs/program-memory-denominator-v1.md`
- `docs/designs/replay-track-a-v1.md`
- `docs/designs/replay-track-b-v1.md`
- `docs/designs/scz-rescue-1-v1.md`
- `docs/designs/variant-to-context-v1.md`
- `docs/designs/policy-and-packets-v1.md`
- `docs/designs/external-credibility-v1.md`
