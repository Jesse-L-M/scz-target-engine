# 0005 Track A PR3 Stop/Go Decision

Status: active
Date: 2026-04-08
Decision: **HOLD**

## Context

PR2 (denominator recovery) and PR3 (replay-surface recovery) are merged. The
Track A replay path now ships ten honest replayable public slices, five of which
are principal-`3y` evaluable with one positive intervention object each. The
hard gate from `deep-scz-validate-calibrate.md` requires:

> "at least one new layer materially beats `v0_current` and `v1_current` on a
> predeclared principal slice with bootstrap CIs and explicit failure analysis."

This decision record documents the PR3 stop/go run executed on 2026-04-08.

## Predeclared Setup

| Parameter | Value |
|---|---|
| Principal slice | `scz_translational_2024_09_25` |
| Principal horizon | `3y` |
| Principal metric | `average_precision_any_positive_outcome` |
| Control baselines | `v0_current`, `v1_current` |
| Sanity baseline | `random_with_coverage` |
| Code version | `fc1ae4df147d962d420e06696b593d396b89957f` |
| Bootstrap iterations | 100 (deterministic test mode) |
| Bootstrap confidence | 0.95 |
| Resample unit | entity |

## Challenger Discovery

No challenger baselines qualify for intervention-object entity type on Track A:

- `pgc_only`, `schema_only`, `opentargets_only`, `chembl_only`: entity_types = `(gene,)` only
- `v1_pre_numeric_pr7_heads`, `v1_post_numeric_pr7_heads`: status = `protocol_only`, entity_types = `(gene, module)` only
- Track B baselines: different protocol (`track_b_structural_replay_protocol_v1`)

The only baselines that can run on `intervention_object` with `available_now`
status are the three already in the public-slice contract: `v0_current`,
`v1_current`, `random_with_coverage`.

**The challenger set is empty. The gate cannot be passed.**

## Principal Slice Result (scz_translational_2024_09_25)

| Baseline | AP (3y) | 95% CI | Covered | Admissible | Positives |
|---|---|---|---|---|---|
| `random_with_coverage` | 0.5000 | [0.000, 1.000] | 8/8 | 8 | 1 |
| `v0_current` | 0.1250 | [0.000, 0.276] | 4/8 | 8 | 1 |
| `v1_current` | 0.1250 | [0.000, 0.276] | 5/8 | 8 | 1 |

### Error Analysis

The single positive entity is **xanomeline + trospium | acute positive symptoms |
phase_3_or_registration** (CHRM1 / CHRM4).

- `v0_current`: positive entity has rank=None, score=None (not in covered set).
  Top-ranked entities are bitopertin (SLC6A9), iclepertin (SLC6A9), pomaglumetad
  methionil (GRM2/GRM3), Lu AF35700 (DRD1/DRD2) — all false positives.
- `v1_current`: positive entity has rank=None, score=None (not in covered set).
  Top-ranked entities are pomaglumetad methionil (GRM2/GRM3), pimavanserin
  (HTR2A), bitopertin (SLC6A9), iclepertin (SLC6A9), Lu AF35700 (DRD1/DRD2) —
  all false positives.

Both controls place the positive at rank 8/8 (AP = 1/8 = 0.125) because it
falls outside their projection coverage. The CIs span [0.0, 0.276], confirming
the result is uninformative noise on a cohort with only 1 positive out of 8.

## Secondary Evaluable Slices

| Slice | v0 AP | v0 Covered | v1 AP | v1 Covered | random AP | random Covered |
|---|---|---|---|---|---|---|
| `2024-06-15` | 0.125 | 0/8 | 0.125 | 4/8 | 1.000 | 8/8 |
| `2024-06-18` | 0.125 | 4/8 | 0.125 | 5/8 | 0.167 | 8/8 |
| `2024-06-20` | 0.125 | 4/8 | 0.125 | 5/8 | 0.200 | 8/8 |
| `2024-07-15` | 0.125 | 4/8 | 0.125 | 5/8 | 0.333 | 8/8 |
| `2024-09-25` (principal) | 0.125 | 4/8 | 0.125 | 5/8 | 0.500 | 8/8 |

All secondary slices show the same pattern: v0 and v1 score identically
(AP = 0.125), CIs span [0.0, 0.276], each slice has exactly 1 positive out of
8 admissible entities. The random baseline varies between 0.167 and 1.0 due to
seed-dependent rank assignment with only 1 positive, with CIs spanning [0.0, 1.0].

## Decision

**HOLD.** Track A does not pass the PR3 gate.

### Primary reason

No challenger baselines exist that support the `intervention_object` entity type
with `available_now` status. The single-source ablation baselines (`pgc_only`,
`schema_only`, `opentargets_only`, `chembl_only`) only support `gene` entity
type. The v1 PR7 variants are protocol-only. Without a challenger, the gate
condition — "at least one new layer materially beats `v0_current` and
`v1_current`" — cannot be satisfied.

### Secondary observations

Even if a challenger existed, the evaluation surface is too thin to produce
meaningful signal:

1. Each evaluable slice has only 1 positive out of 8 entities.
2. The single positive (xanomeline + trospium, CHRM1/CHRM4) is not covered by
   either v0 or v1 projections on the principal slice.
3. Bootstrap CIs span most of the [0, 1] range for all baselines.
4. v0 and v1 are indistinguishable (identical AP on all 5 slices).

## Next Blocker

To unblock Track A, at least one of:

1. **Implement an intervention-object-native challenger baseline** that goes
   beyond projecting current gene/module outputs. This would require adding a
   new baseline definition with `entity_types = (intervention_object,)` and
   `status = available_now`, registering it in the task registry, and
   implementing execution logic in the benchmark runner.
2. **Expand the evaluable cohort** so that evaluation surfaces have more than
   1 positive entity per slice. The current denominator and outcome set produce
   extremely noisy estimates.
3. **Enable single-source ablations for intervention objects** by extending
   `pgc_only`, `schema_only`, `opentargets_only`, and `chembl_only` to support
   the `intervention_object` entity type through the projection compatibility
   layer. This would allow testing whether individual source contributions
   provide uplift, though per the judgment rules, single-source ablations alone
   do not count as milestone-clearing new layers.

## Consequences

### Good

- The benchmark infrastructure is verified end-to-end: snapshot, cohort, runner,
  reporting, leaderboard, and error analysis all produce valid outputs on all 5
  evaluable slices.
- The gap is clearly identified: no intervention-object-native challenger
  baseline exists yet.
- The result is documented honestly rather than deferred or wordsmithed.

### Cost

- Track A cannot advance to later translation milestones until the next blocker
  is resolved.
- The replay investment in PR2 and PR3 produced working infrastructure but no
  scientific signal yet.

## Affected Specs

- `docs/designs/replay-track-a-v1.md`
- `docs/designs/deep-scz-validate-calibrate.md`
- `docs/claim.md`
- `README.md`
