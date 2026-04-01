# 0001 Planning Contract

Status: active
Date: 2026-04-01

## Context

The repo now has a broad product direction:
an open schizophrenia digital-biology benchmark plus intervention observatory.

That direction spans multiple workstreams:
contracts, program memory, replay, rescue, atlas, policy, packets, and external
credibility.

Without a planning contract, future workspaces can easily drift into:

- duplicate roadmap docs
- two active plans for the same milestone
- code shipping without the spec being updated
- conflicting assumptions across replay, rescue, and policy work

## Decision

The repo will use this planning contract:

1. Strategy source of truth:
   `docs/roadmap.md` plus `docs/designs/deep-scz-validate-calibrate.md`
2. Build-specs live under `docs/designs/`
3. Each workstream gets at most one active spec
4. If direction changes, update the active spec or mark it superseded
5. Every implementation PR updates the matching spec in the same PR
6. Cross-cutting choices are logged in `docs/decisions/`
7. New build-specs should start from `docs/templates/milestone-spec-template.md`

## Consequences

### Good

- future agents have one place to start
- milestone ownership is clearer
- superseded plans do not quietly stay live
- cross-workstream rules become easier to reuse

### Cost

- every real implementation PR now has to touch docs too
- changing direction requires explicit supersession instead of quiet drift

## Affected Specs

- `docs/roadmap.md`
- `docs/designs/deep-scz-validate-calibrate.md`
- all future milestone specs under `docs/designs/`
