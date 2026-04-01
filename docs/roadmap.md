# Roadmap

This is the short source-of-truth pointer for the repo strategy.

For the full working roadmap, read
`docs/designs/deep-scz-validate-calibrate.md`.
For shipped behavior and claim boundaries, read `docs/claim.md`.
For build-spec rules and the milestone-spec template, read
`docs/designs/README.md`.

## North Star

The repo is becoming an open schizophrenia digital-biology benchmark and
intervention observatory.

That means two things at once:

- an auditable control plane for intervention decisions:
  program memory, failure analogs, historical replay, policy views, and
  falsification-ready packets
- one flagship schizophrenia rescue benchmark that is benchmarked, packaged, and
  falsifiable in the open

## Two-Speed Architecture

```text
OPEN CONTROL PLANE
==================
program memory -> replay -> policy views -> packets -> external accountability
       |             |            |             |
       +-------------+------------+-------------+
                     intervention_object_id

SCIENTIFIC CORE
===============
variant-to-context substrate -> SCZ-Rescue-1 -> rescue models -> assay / kill test plans
            |                         |
            +-------------------------+
                 benchmarked translation path
```

## Current Sequencing

1. Freeze contracts, compatibility, smoke path, and distribution docs.
2. Build program-memory denominator and failure-memory coverage.
3. Prove real uplift on historical replay.
4. Only then ship `SCZ-Rescue-1` as the flagship scientific product.
5. Expand the atlas into a variant-to-context feature store for rescue and replay.
6. Translate replay and rescue wins into policy and packet surfaces.
7. Externalize one real credibility track.

## Milestones

- Milestone 0: contracts, compatibility, smoke path, and release path
- Milestone 1: Program Memory 2.0 with explicit denominator accounting
- Milestone 2: real historical replay with intervention-object feature bundles
- Milestone 3: `SCZ-Rescue-1` in one defined glutamatergic-neuron context
- Milestone 4: variant-to-context substrate for rescue and replay
- Milestone 5: policy and packet translation
- Milestone 6: external credibility layer

## PR Sequence

1. `contracts-and-compat-v2`
2. `program-memory-denominator-v1`
3. `replay-track-a-v1`
4. `replay-track-b-v1`
5. `SCZ-Rescue-1-v1`
6. `variant-to-context-v1`
7. `policy-and-packets-v1`
8. `external-credibility-v1`

Critical sequencing rule: do not start PRs 5-8 until PR4 proves there is real
uplift worth translating.

## Execution Contract

- Strategy docs:
  `docs/roadmap.md` plus `docs/designs/deep-scz-validate-calibrate.md`
- Build-specs:
  `docs/designs/*.md`
- Cross-cutting decisions:
  `docs/decisions/*.md`
- New build-specs should start from:
  `docs/templates/milestone-spec-template.md`

Do not create a second active roadmap or a second active spec for the same
workstream.
