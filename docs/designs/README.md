# Design Specs

This directory is for build-spec documents, not broad strategy docs.

Use [docs/roadmap.md](../roadmap.md) for the short strategy view and
[deep-scz-validate-calibrate.md](deep-scz-validate-calibrate.md) for the detailed
working roadmap.

## What Belongs Here

Each file in this directory should describe one milestone, workstream, or bounded
implementation lane.

Good examples:

- `contracts-and-compat-v2.md`
- `program-memory-denominator-v1.md`
- `replay-track-a-v1.md`
- `replay-track-b-v1.md`
- `scz-rescue-1-v1.md`

Bad examples:

- another umbrella vision doc
- a duplicate roadmap
- a vague brainstorm with no stop/go gate

## Required Metadata

Every active build-spec should start with these fields:

```text
Status: draft | active | implemented | superseded
Owner branch: <branch-name or TBD>
Depends on: <specs or ->
Blocked by: <specs or ->
Supersedes: <specs or ->
Last updated: YYYY-MM-DD
```

## Repo Planning Contract

- One strategy source of truth:
  `docs/roadmap.md` plus `docs/designs/deep-scz-validate-calibrate.md`
- One active build-spec per workstream
- If direction changes, update the existing spec or mark it superseded
- Do not keep two active specs for the same milestone
- Every implementation PR must update the matching spec in the same PR
- Cross-cutting decisions go in `docs/decisions/`

## Required Sections

Use the template at `docs/templates/milestone-spec-template.md`.

At minimum, every build-spec must include:

- Objective
- Scope
- Not in scope
- Inputs and artifact contracts
- Data flow diagram
- Acceptance tests
- Failure modes
- Stop/go gate

## Current Planned Specs

These are the next build-specs the roadmap expects:

1. `contracts-and-compat-v2.md`
2. `program-memory-denominator-v1.md`
3. `replay-track-a-v1.md`
4. `replay-track-b-v1.md`
5. `scz-rescue-1-v1.md`
6. `variant-to-context-v1.md`
7. `policy-and-packets-v1.md`
8. `external-credibility-v1.md`
