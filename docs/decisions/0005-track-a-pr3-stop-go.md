# 0005 Track A PR3 Stop/Go Decision

Status: active
Date: 2026-04-08
Decision: **HOLD**

## Context

PR2 (denominator recovery) and PR3 (replay-surface recovery) are merged. The
Track A replay path now ships ten honest replayable public slices, five of which
are principal-`3y` evaluable with one positive intervention object each. The
hard gate from `deep-scz-validate-calibrate.md` remains:

> "at least one new layer materially beats `v0_current` and `v1_current` on a
> predeclared principal slice with bootstrap CIs and explicit failure analysis."

This decision record documents the rerun executed on 2026-04-08 after enabling
available-now intervention-object challengers through the checked-in projection
contract and widening the honest replay archive surface where archived
pre-cutoff source evidence existed.

## Predeclared Setup

| Parameter | Value |
|---|---|
| Principal slice | `scz_translational_2024_09_25` |
| Principal horizon | `3y` |
| Principal metric | `average_precision_any_positive_outcome` |
| Control baselines | `v0_current`, `v1_current` |
| Available-now challengers | `pgc_only`, `schema_only`, `opentargets_only`, `chembl_only` |
| Sanity baseline | `random_with_coverage` |
| Code version | `b7e14fb401e007f3084756f3da747599e6f38033` |
| Bootstrap iterations | 100 (`deterministic_test_mode=true`) |
| Bootstrap confidence | 0.95 |
| Resample unit | entity |

## Challenger Enablement

The Track A benchmark contract now admits projected intervention-object
execution for these available-now challenger baselines:

- `pgc_only`
- `schema_only`
- `opentargets_only`
- `chembl_only`

Implementation remained inside the frozen compatibility path:

- `pgc_only`, `schema_only`, `opentargets_only`, and `chembl_only` now declare
  `entity_types = (gene, intervention_object)` in the frozen baseline matrix.
- The runner reuses the same intervention-object projection contract already
  used for `v0_current` and `v1_current`; no ad hoc scorer or hand-authored
  projection was introduced.

Honest replay archive widening was required and was limited to the source
families that had archived pre-cutoff support for the Track A denominator:

- `SCHEMA`: added a replay extract from the public gene-results release,
  including `CHRM1`, `CHRM4`, `DRD1`, `DRD2`, `GRM2`, `GRM3`, `HTR1A`,
  `HTR2A`, `SLC6A9`, and `TAAR1`
- `Open Targets`: widened the checked-in 24.03 replay extract to the same target
  set except `TAAR1`, which had no schizophrenia direct-association row in the
  archived 24.03 release
- `PGC`, `PsychENCODE`, and `ChEMBL`: kept their existing replay roles

The widened archive surface activates challengers on public slices dated
2024-06-20 and later. The earlier 2024-06-15 and 2024-06-18 slices remain
honest but challenger-sparse because those replay archives were not yet
available at those cutoffs.

## Principal Slice Result (`scz_translational_2024_09_25`)

| Baseline | AP (3y) | 95% CI | Covered | Admissible | Positives |
|---|---|---|---|---|---|
| `pgc_only` | 0.1250 | [0.000, 0.238] | 4/8 | 8 | 1 |
| `schema_only` | 0.5000 | [0.000, 1.000] | 8/8 | 8 | 1 |
| `opentargets_only` | 0.3333 | [0.000, 1.000] | 8/8 | 8 | 1 |
| `v0_current` | 0.1250 | [0.000, 0.276] | 5/8 | 8 | 1 |
| `v1_current` | 0.1667 | [0.000, 0.325] | 8/8 | 8 | 1 |
| `chembl_only` | 0.1250 | [0.000, 0.238] | 0/8 | 8 | 1 |
| `random_with_coverage` | 0.5000 | [0.000, 1.000] | 8/8 | 8 | 1 |

## Principal Failure Analysis

The single positive entity remains:

- `xanomeline + trospium | acute positive symptoms | phase_3_or_registration`
  via `CHRM1` / `CHRM4`

What changed:

- `schema_only` now covers the positive via `CHRM1` and `CHRM4` and ranks it
  `2/8` (AP = 0.500). It still ranks `emraclidine` first because the replay
  SCHEMA signal is even more concentrated on `CHRM4`.
- `opentargets_only` now covers the positive via `CHRM1` and `CHRM4` and ranks
  it `3/8` (AP = 0.333). It still over-ranks `pimavanserin` and `Lu AF35700`
  because archived Open Targets schizophrenia scores remain larger for `HTR2A`
  and `DRD1`/`DRD2`.
- `v1_current` now covers the positive via `CHRM1` and `CHRM4` and ranks it
  `6/8` (AP = 0.1667), but its additive output still prefers glutamatergic,
  glycinergic, dopaminergic, and serotonergic assets above the muscarinic
  approval.
- `v0_current` still does not cover the positive. The muscarinic genes are now
  present in the archive surface, but the current v0 composite still does not
  make them eligible enough to survive into the projected ranked set.
- `pgc_only` and `chembl_only` do not cover the positive on the principal slice.

Why the gate still fails:

- The best challenger point estimate (`schema_only`, AP = 0.500) does beat
  `v0_current` and `v1_current` on the principal slice.
- The bootstrap interval for that challenger is still `[0.0, 1.0]`, and
  `opentargets_only` is also `[0.0, 1.0]`.
- With only 1 positive out of 8 admissible entities, the replay remains too thin
  to support a defensible claim of a material bootstrap-backed win.

## Secondary Evaluable Slices

| Slice | Schema AP / Covered | Open Targets AP / Covered | v0 AP / Covered | v1 AP / Covered |
|---|---|---|---|---|
| `2024-06-15` | 0.125 / 0 | 0.125 / 0 | 0.125 / 0 | 0.125 / 4 |
| `2024-06-18` | 0.125 / 0 | 0.125 / 0 | 0.125 / 4 | 0.125 / 5 |
| `2024-06-20` | 0.500 / 8 | 0.333 / 8 | 0.125 / 5 | 0.167 / 8 |
| `2024-07-15` | 0.500 / 8 | 0.333 / 8 | 0.125 / 5 | 0.167 / 8 |

The four secondary summaries show the same boundary:

- before 2024-06-20, the replay surface does not yet honestly include the
  widened `SCHEMA` and `Open Targets` challenger archives
- from 2024-06-20 onward, `schema_only` and `opentargets_only` both achieve
  non-zero honest challenger lift, but the cohort still has only one positive
  and the intervals stay too wide to clear the gate

## Decision

**HOLD.** Track A still does not pass the PR3 gate.

### Primary reason

Available-now challengers now exist and run honestly at
`intervention_object` grain, but none materially beats both `v0_current` and
`v1_current` on the principal slice with bootstrap support. The point-estimate
lift is real; the evidence is still too noisy.

### One-Sentence Decision Reason

`schema_only` and `opentargets_only` now cover the muscarinic approval and beat
the control point estimates on `scz_translational_2024_09_25`, but with only 1
positive in the cohort their 95% bootstrap intervals remain too wide to justify
`GO`.

## Consequences

### Good

- Track A now has genuine available-now challengers at
  `intervention_object` grain.
- The principal positive is now honestly covered by challengers through archived
  `CHRM1` / `CHRM4` source evidence.
- The benchmark stack remains fully replayable end to end on the five evaluable
  slices with explicit source provenance.

### Cost

- The milestone gate still cannot advance because the replay surface has only
  one positive per evaluable slice.
- The current replay shows directionally useful challenger lift, but not a
  robust enough estimate to claim calibration success.

## Next Blocker

To turn this into a `GO`, the replay surface needs more evaluable positive
signal, not merely more legal baselines. The next unblocker is to expand the
honest evaluable cohort or otherwise predeclare a richer principal surface that
can yield narrower bootstrap intervals.

## Affected Specs

- `docs/designs/replay-track-a-v1.md`
- `docs/designs/deep-scz-validate-calibrate.md`
- `docs/benchmarking.md`
- `docs/claim.md`
- `README.md`
