# Failure Taxonomy

This taxonomy defines the first-pass vocabulary for why a clinical program might fail. It is a maintained artifact for curation and later reasoning. It is not yet wired into scoring.

## Use Rules

- Choose one best-fit label per row for now.
- Use `not_applicable_nonfailure` for approvals or positive signals where failure attribution does not apply.
- Use `unresolved` when the miss is real but the reason is not yet defensible.
- Do not treat these labels as interchangeable. In particular, `molecule_failure`, `target_class_failure`, and `probable_target_invalidity` make progressively stronger claims.

## Labels

### `not_applicable_nonfailure`

- Meaning: The event is an approval, positive readout, or other nonfailure milestone.
- Use when: Failure attribution would be misleading because the recorded event is not a miss.
- Do not use when: The event failed but you are merely unsure why. Use `unresolved` instead.

### `unresolved`

- Meaning: The program missed, but the current evidence is not strong enough to defend a sharper failure explanation.
- Use when: A landmark miss should be recorded now, while stronger adjudication is deferred to fuller historical curation.
- Do not use when: There is direct evidence for a more specific label such as clear exposure failure or repeated class-level baggage.

### `molecule_failure`

- Meaning: Best current read is that the specific asset failed for molecule-level reasons while the broader target or class remains plausibly live.
- Use when: Chemistry, PK/PD profile, tolerability, formulation, or another asset-specific issue is the most defensible explanation.
- Do not use when: Multiple assets with aligned mechanism are failing in the same way, or when the data actually point to wrong population, wrong endpoint, or poor exposure.

### `target_class_failure`

- Meaning: The mechanistic class appears burdened across more than one asset, suggesting class baggage rather than a one-off molecule problem.
- Use when: Multiple aligned assets in the same class have failed in relevant settings and there is no strong counterexample that keeps the class clean.
- Do not use when: The evidence comes from a single molecule miss or when a nearby mechanistic cousin has already succeeded.

### `endpoint_mismatch`

- Meaning: The biology might be real, but the chosen primary endpoint, measurement construct, or time horizon was poorly aligned to the claimed benefit.
- Use when: The program shows movement in a nearby signal while the formal primary endpoint misses, or when the endpoint is too far from the mechanism's plausible window of effect.
- Do not use when: The more likely issue is who was enrolled, inadequate exposure, or straightforward absence of efficacy.

### `population_mismatch`

- Meaning: The enrolled population was the wrong illness stage, symptom subset, or biomarker subgroup for the mechanism being tested.
- Use when: A mechanism plausibly needs a narrower or earlier subgroup than the one actually enrolled.
- Do not use when: The evidence mainly points to assay, endpoint, exposure, or class-level problems.

### `dosing_or_exposure_issue`

- Meaning: The most defensible explanation is inadequate target engagement or an inability to sustain the needed exposure window safely.
- Use when: PK, occupancy, titration, or tolerability constraints strongly suggest the tested doses were not the right ones.
- Do not use when: The source only says the trial missed without evidence on exposure adequacy.

### `heterogeneity_or_subgroup_dilution`

- Meaning: Broad enrollment, noisy trial conduct, or unusually large placebo response likely diluted a signal that may exist in a subset.
- Use when: The program narrative is dominated by subgroup dilution, site heterogeneity, or abnormal placebo behavior.
- Do not use when: There is no concrete reason to think dilution, rather than lack of efficacy, drove the outcome.

### `probable_target_invalidity`

- Meaning: Best current read is that modulating the target in humans is unlikely to produce meaningful benefit in the intended domain.
- Use when: Adequate tests repeatedly fail despite credible engagement and there is little remaining room to blame molecule design, population, endpoint, or dose.
- Do not use when: The evidence still supports a live alternative explanation or when only one asset has failed.

## Interpretation Guardrails

- `molecule_failure` says the asset failed. It does not say the mechanism is dead.
- `target_class_failure` says class baggage exists. It does not necessarily say the biological target is invalid under every modality.
- `probable_target_invalidity` is the strongest label here and should be rare.
- This starter PR intentionally allows conservative `unresolved` rows so the repo can preserve history without pretending the adjudication problem is already solved.
