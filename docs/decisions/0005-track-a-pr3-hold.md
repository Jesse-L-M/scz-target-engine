# 0005 Track A PR3 Hold

Status: active
Date: 2026-04-08

## Context

PR2 denominator recovery and PR3 replay-surface recovery are now merged, so the
real Track A stop/go comparison can finally be run on honest public slices.

The governing gate from `docs/designs/deep-scz-validate-calibrate.md` is:
at least one new layer must materially beat `v0_current` and `v1_current` on a
predeclared principal slice with bootstrap confidence intervals and explicit
failure analysis.

The predeclared decision setup for this run was:

- principal slice: `scz_translational_2024_09_25`
- principal horizon: `3y`
- principal metric: `average_precision_any_positive_outcome`
- control baselines: `v0_current`, `v1_current`
- sanity baseline: `random_with_coverage`

On Track A `intervention_object` slices today, the only legal available-now
baselines that can actually run from archived artifacts are `v0_current`,
`v1_current`, and `random_with_coverage`. Gene-only baselines and protocol-only
labels do not apply to intervention-object replay, and Track B baselines are a
separate task.

The five evaluable public slices were rerun on April 8, 2026 under copied
decision-run manifests in `.context/track_a_pr3_decision/` with full 1,000-sample
bootstrap intervals.

## Decision

Track A PR3 is `HOLD`.

Principal-slice result, `scz_translational_2024_09_25`, `3y`
`average_precision_any_positive_outcome`:

| baseline_id | metric | 95% CI | admissible | positive | covered |
|---|---:|---:|---:|---:|---:|
| `random_with_coverage` | 0.500000 | [0.000000, 1.000000] | 8 | 1 | 8 |
| `v0_current` | 0.125000 | [0.000000, 0.275794] | 8 | 1 | 4 |
| `v1_current` | 0.125000 | [0.000000, 0.275794] | 8 | 1 | 5 |

Principal-slice error review:

- `v0_current` and `v1_current` both miss the lone principal positive,
  `xanomeline + trospium | acute positive symptoms | phase_3_or_registration`
- neither control has a true positive in the top five on the principal slice
- `random_with_coverage` is a sanity baseline, not a milestone-clearing replay
  layer, and its principal-slice interval spans the full `[0, 1]` range

Secondary evaluable slices:

- `scz_translational_2024_06_15`: `random_with_coverage` 1.000000 [0.000000, 1.000000], `v0_current` 0.125000 [0.000000, 0.275794], `v1_current` 0.125000 [0.000000, 0.275794]
- `scz_translational_2024_06_18`: `random_with_coverage` 0.166667 [0.000000, 0.416667], `v0_current` 0.125000 [0.000000, 0.275794], `v1_current` 0.125000 [0.000000, 0.275794]
- `scz_translational_2024_06_20`: `random_with_coverage` 0.200000 [0.000000, 0.500000], `v0_current` 0.125000 [0.000000, 0.275794], `v1_current` 0.125000 [0.000000, 0.275794]
- `scz_translational_2024_07_15`: `random_with_coverage` 0.333333 [0.000000, 1.000000], `v0_current` 0.125000 [0.000000, 0.275794], `v1_current` 0.125000 [0.000000, 0.275794]

This run does not show a genuine challenger that materially beats both control
baselines on the predeclared principal slice. In practice, there is no
milestone-clearing Track A challenger implemented today beyond the controls plus
the sanity random baseline.

## Consequences

### Good

- the Track A gate is now decided from an honest replay run rather than from
  protocol intent
- the repo can claim real intervention-object replay infrastructure, explicit
  archived baseline projection, bootstrap intervals, and failure analysis
- the decision boundary is now explicit: Track A is not a `GO`

### Cost

- later translation milestones cannot cite Track A as having cleared the replay
  uplift gate
- the current Track A benchmark surface has no implemented available-now
  challenger layer that can beat both controls on the principal slice
- random baseline wins on tiny cohorts do not rescue the milestone because they
  are noisy sanity checks, not defensible uplift

Exact next blocker:

- implement at least one new available-now intervention-object replay
  layer/baseline that can run from archived artifacts on the same five slices,
  then rerun the same predeclared principal gate

## Affected Specs

- `docs/designs/replay-track-a-v1.md`
- `README.md`
- `docs/claim.md`
