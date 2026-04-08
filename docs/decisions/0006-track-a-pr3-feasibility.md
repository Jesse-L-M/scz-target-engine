# 0006 Track A PR3 Feasibility Audit

Status: active
Date: 2026-04-08
Decision: **INFEASIBLE — the current honest archive universe cannot support a credible PR3 stop/go gate**

## Context

Decision 0005 ended in HOLD because the principal replay surface has only 1 positive
intervention object per evaluable slice, producing bootstrap intervals too wide for a
defensible material win. This follow-up audit determines whether the HOLD is a temporary
condition (fixable by reconfiguring the replay surface) or a fundamental blocker (the
archive universe simply does not contain enough positive signal).

## Audit Method

1. Enumerate all positive intervention-object events in the checked-in
   `program_history/v2/events.csv` that fall within the replay window.
2. Enumerate all evaluable slices and identify which positive event each observes.
3. Collapse repeated views of the same underlying event across cutoff dates.
4. Quantify the effective independent positive sample size.
5. Compute the bootstrap power profile for the observed sample.
6. Search exhaustively for honest alternative surfaces that stay faithful to the
   benchmark question.

## Findings

### Distinct Positive Entities

| # | Entity | Event | Date |
|---|--------|-------|------|
| 1 | xanomeline-trospium \| acute positive symptoms \| phase_3_or_registration | cobenfy-xanomeline-trospium-approval-us-2024 | 2024-09-26 |

**Total: 1 distinct positive intervention-object entity.**

### Distinct Positive Event Lineages

| # | Event ID | Type | Positive Labels |
|---|----------|------|-----------------|
| 1 | cobenfy-xanomeline-trospium-approval-us-2024 | regulatory_approval | future_schizophrenia_positive_signal, future_schizophrenia_program_advanced |

**Total: 1 distinct positive event lineage.**

All other events in the replay window are failures:

- pomaglumetad-methionil phase 3 (2014-03-19): inferior to aripiprazole
- bitopertin phase 3 (2017-07-01): did not meet primary endpoint
- pimavanserin phase 2 (2019-11-25): met primary but not key secondary
- lu-af35700 phase 3 (2022-10-01): did not demonstrate superiority
- ulotaront phase 3 (2023-07-31): did not meet primary endpoint
- pimavanserin phase 3 (2024-03-11): did not meet primary endpoint
- emraclidine phase 2 (2024-11-11): did not meet primary endpoint
- iclepertin phase 3 (2025-01-16): did not meet primary or key secondary endpoints

### Slice–Event Overlap

| Slice | Positive Event | Independent Signal |
|-------|---------------|--------------------|
| scz_translational_2024_06_15 | cobenfy approval | same event as all other evaluable slices |
| scz_translational_2024_06_18 | cobenfy approval | same event |
| scz_translational_2024_06_20 | cobenfy approval | same event |
| scz_translational_2024_07_15 | cobenfy approval | same event |
| scz_translational_2024_09_25 | cobenfy approval | same event |
| scz_translational_2024_09_26 | none | — |
| scz_translational_2024_11_10 | none | — |
| scz_translational_2024_11_11 | none | — |
| scz_translational_2025_01_15 | none | — |
| scz_translational_2025_01_16 | none | — |

**All 5 evaluable slices observe the same single positive event. Zero independent
positive signal is added by combining them.**

### Power Analysis

With N = 8 admissible entities and 1 positive:

- P(no positive drawn in a bootstrap resample) = (7/8)^8 = 0.344
- 34.4% of bootstrap resamples yield AP = 0 regardless of ranker quality
- The 2.5th percentile of any challenger's bootstrap AP distribution is always 0
- The 97.5th percentile approaches 1.0 for challengers that sometimes rank the
  positive first
- Result: **95% CI always spans [0.0, ~1.0]** for every baseline
- The gate requires bootstrap-backed material win; this is mathematically
  impossible with 1 positive in 8 entities

Minimum independent positives needed for a non-degenerate CI:

- 3 positives: P(none drawn) = (5/8)^8 = 0.023 (just below 2.5th percentile)
- 4+ positives: needed in practice for meaningful baseline separation

### Honest Alternative Surface Search

| Alternative | Considered | Viable | Reason |
|-------------|-----------|--------|--------|
| Different principal slice from catalog | Yes | No | All 5 evaluable slices have the same 1 positive; best coverage already on 2024-09-25 |
| Combined surface across multiple slices | Yes | No | All slices share the same positive event lineage; combining adds 0 independent signal |
| Broader cohort from existing denominator | Yes | No | All additional included programs are historical approvals or have no events in replay window |
| Different positive aggregation | Yes | No | Positive composite already includes started/advanced/positive_signal; no other entity qualifies |
| Gene-level principal surface | Yes | No | CHRM1/CHRM4 both derive from the same approval event; this is event reuse not independent signal |

**No honest alternative principal surface exists within the checked-in archive
universe.**

## Decision

**INFEASIBLE.** The current PR3 gate cannot be passed with the honest schizophrenia
archive universe. This is not a configuration or machinery problem — it is a fact about
the world's schizophrenia late-stage pipeline having produced exactly one novel positive
translational event (the Cobenfy approval) in the relevant time window.

### Primary Blocker

Too few independent positive events. The honest archive universe contains 1 positive
event lineage, the Cobenfy approval on 2024-09-26. The AP + bootstrap gate requires
minimum 3 independent positives for non-degenerate confidence intervals, and practically
4–5 for meaningful baseline separation.

### What Is Not The Blocker

- Archive sparsity: source coverage is adequate (SCHEMA, Open Targets, PGC,
  PsychENCODE, ChEMBL all available on later slices)
- Challenger availability: `schema_only` and `opentargets_only` exist and run honestly
- Benchmark machinery: the stack is fully functional end to end
- Scoring logic: point estimates show real lift (`schema_only` AP = 0.500 vs
  `v1_current` AP = 0.167)

### One-Sentence Decision Reason

The honest schizophrenia replay archive has exactly one independent positive event, and
no honest reconfiguration of slices, cohort, or aggregation can produce the minimum 3
independent positives needed for the AP + bootstrap gate to yield non-degenerate
confidence intervals.

## Consequences

### Good

- The PR3 pipeline machinery is validated: slices, challengers, bootstrap, and
  error analysis all work correctly
- The directional signal is real: `schema_only` ranks the muscarinic approval 2/8
  and `opentargets_only` ranks it 3/8, both above the controls
- The audit is honest: the repo now says plainly that the gate is infeasible rather
  than continuing to produce HOLD results that can never resolve

### Cost

- Track A cannot advance to later translation milestones through the predeclared
  AP + bootstrap gate
- The next real-world positive schizophrenia pipeline event has no predictable
  timeline

## Narrowest Next Decision

The project owner must choose one of:

1. **Wait for real-world events.** Additional schizophrenia late-stage pipeline readouts
   (positive or negative) would expand the replay cohort's effective positive count.
   Timeline is unpredictable and depends on external clinical trial schedules.

2. **Accept directional evidence.** Treat the Track A point estimates as informative
   qualitative signal without requiring bootstrap-backed statistical significance.
   This weakens the gate contract but honestly reflects the available evidence.

3. **Redirect the gate.** Move the stop/go question to Track B, a combined Track A+B
   surface, or a different benchmark formulation where the available evidence can
   support a defensible statistical comparison.

4. **Close Track A PR3.** Declare the gate infeasible and stop Track A advancement work
   until the archive universe changes. Redirect effort to other workstreams.

## Affected Specs

- `docs/designs/replay-track-a-v1.md`
- `docs/designs/deep-scz-validate-calibrate.md`
- `docs/benchmarking.md`
- `docs/claim.md`
- `README.md`
