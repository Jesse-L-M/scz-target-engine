# 0004 Program-Memory Denominator Boundary

Status: active
Date: 2026-04-08

## Context

`program_universe.csv` now represents the real checked-in denominator boundary for
schizophrenia molecular programs rather than a small state-machine fixture.

Replay, backfill, and later control-plane work need one shared rule for how this
boundary is inherited. Without that rule, later slice builders can silently drop
unresolved rows, double-count alias rows, or promote follow-on/supporting studies into
new cohort members that were never part of the canonical denominator.

## Decision

Replay and later denominator-consuming work must inherit the checked-in
`program_universe.csv` boundary, or an explicit snapshot-local copy of it, rather than
rebuilding schizophrenia program universes ad hoc.

That means:

1. `included` rows are the only denominator rows that can claim checked-in event-backed
   replay history.
2. `duplicate`, `excluded`, and `out_of_scope` rows must stay explicit in manifests and
   gap outputs, but they do not create separate replay cohort members.
3. `unresolved` rows must remain visible with their exact reason codes whenever they
   cannot yet be turned into evaluable replay history; they may not be silently
   dropped.
4. Follow-on or postapproval supporting studies do not become new denominator identities
   unless they create a genuinely new program-opportunity grain under the existing
   `asset_lineage_id / target_class_lineage_id / modality / domain / population /
   regimen / stage_bucket` contract.

## Consequences

### Good

- Track A and Track B inherit one explicit schizophrenia program boundary.
- Replay backfill can distinguish "known but unresolved" from "not in denominator."
- Alias handling and lineage logic stay consistent across program memory and replay.

### Cost

- replay/backfill builders must carry denominator-state metadata forward instead of
  emitting only included-event slices
- unresolved late-stage rows will remain visible to reviewers until direct-source event
  landing closes them

## Affected Specs

- `docs/designs/program-memory-denominator-v1.md`
- `docs/designs/replay-track-a-v1.md`
- `docs/designs/replay-track-b-v1.md`
- `docs/designs/deep-scz-validate-calibrate.md`
