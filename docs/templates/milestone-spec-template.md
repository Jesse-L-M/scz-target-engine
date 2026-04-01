# <Milestone / Workstream Name>

Status: draft
Owner branch: TBD
Depends on: -
Blocked by: -
Supersedes: -
Last updated: YYYY-MM-DD

## Objective

What this work does, why it matters, and what changes for the user or developer.

## Success Condition

- Primary success metric:
- Secondary success metric:
- Stop/go gate:

## Scope

- In scope item 1
- In scope item 2
- In scope item 3

## Not in Scope

- Explicitly excluded item 1
- Explicitly excluded item 2

## Existing Surfaces To Reuse

- Existing code / artifact / workflow:
  how this spec extends it instead of rebuilding it

## Inputs

- Upstream datasets:
- Existing artifacts:
- Runtime commands:
- External dependencies:

## Outputs And Artifact Contracts

- New or changed artifact:
  format, location, and validation rule
- Backward-compatibility rule:

## Data Flow

```text
INPUTS -> NORMALIZATION -> VALIDATION -> CORE BUILD STEP -> OUTPUT ARTIFACTS -> QA / RELEASE
```

Describe each stage in plain English.

## Implementation Plan

1. Step 1
2. Step 2
3. Step 3

## Acceptance Tests

- Unit:
  exact file or module to test
- Integration:
  exact command or workflow to run
- Regression:
  specific prior failure mode this spec must pin down
- E2E, if relevant:
  full flow that must work

## Failure Modes

- Failure mode:
  what breaks, whether the user sees it, and how the system should fail
- Failure mode:
  what breaks, whether the user sees it, and how the system should fail

## Rollout / Compatibility

- What existing consumers stay stable
- What dual-write or migration period is required
- What would be a breaking change

## Open Questions

- Concrete unresolved question 1
- Concrete unresolved question 2

## Decision Log Links

- Related decision:
  `docs/decisions/0001-planning-contract.md`

## Commands

```bash
# Put the canonical commands for this spec here.
```
