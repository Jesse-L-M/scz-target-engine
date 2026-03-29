# Ontology

This document defines the implementation-ready disease-domain and illness-stage vocabulary
used by the repo's curated data and emitted `v1` domain/stage profiles. Shared `v0`
scores still do not rank these buckets separately, but current `v1` outputs already use
the canonical slugs here instead of inventing new labels ad hoc.

## Usage Rules

- Use the canonical slug exactly as written when populating curated data.
- `domain` in curated data should hold the single primary ontology bucket for the event being recorded.
- Trial enrollment details belong in the `population` field, not in ad hoc variants of the ontology labels.
- `functioning_durable_recovery_relevance` is a cross-cutting outcome lens. It is not a synonym for negative symptoms or cognition.
- `clozapine_resistant_schizophrenia` is a strict subset of `treatment_resistant_schizophrenia`.
- `chr_transition_prevention` is pre-threshold. Do not use it for relapse prevention in established schizophrenia.
- If a program genuinely spans multiple buckets, choose the bucket closest to the primary endpoint and explain the overlap in `notes`.

## Canonical Buckets

| Slug | Label | Axis |
| --- | --- | --- |
| `acute_positive_symptoms` | Acute positive symptoms | outcome_domain |
| `relapse_prevention` | Relapse prevention | outcome_domain |
| `treatment_resistant_schizophrenia` | Treatment-resistant schizophrenia | population_or_stage |
| `clozapine_resistant_schizophrenia` | Clozapine-resistant schizophrenia | population_or_stage |
| `negative_symptoms` | Negative symptoms | outcome_domain |
| `cognition` | Cognition | outcome_domain |
| `chr_transition_prevention` | CHR / transition prevention | population_or_stage |
| `functioning_durable_recovery_relevance` | Functioning / durable recovery relevance | cross_cutting_outcome |

## Bucket Definitions

### `acute_positive_symptoms`

- Label: Acute positive symptoms
- Definition: Short-horizon improvement in active psychosis, including hallucinations, delusions, conceptual disorganization, suspiciousness, and agitation during an acute episode or exacerbation.
- Boundaries: Use when the primary readout is acute symptom reduction over days to roughly 6-8 weeks. Do not use for maintenance, relapse prevention, stable residual symptoms, cognition-first studies, or broad recovery claims.
- Belongs here:
  - Monotherapy or adjunct trials in acutely ill adults using PANSS total, PANSS positive, or BPRS-style acute efficacy endpoints
  - Registrational schizophrenia programs aimed at reducing active psychotic symptoms
- Does not belong here:
  - Maintenance studies with time-to-relapse endpoints
  - Predominant negative-symptom programs in clinically stable patients
  - CIAS programs centered on MCCB or similar neurocognitive batteries

### `relapse_prevention`

- Label: Relapse prevention
- Definition: Reduction in recurrence risk after stabilization, remission, or hospital discharge in established schizophrenia.
- Boundaries: Use when the central question is maintenance durability, recurrence timing, or prevention of symptom return. Do not use for acute response studies, even if they include short safety follow-up.
- Belongs here:
  - Maintenance or withdrawal designs with time-to-relapse as the key endpoint
  - Longitudinal continuation studies asking whether benefit is sustained after stabilization
- Does not belong here:
  - Acute inpatient PANSS studies
  - First-episode or CHR prevention studies before threshold psychosis

### `treatment_resistant_schizophrenia`

- Label: Treatment-resistant schizophrenia
- Definition: Established schizophrenia with inadequate clinical control despite adequate exposure to standard antipsychotic treatment, usually after at least two prior antipsychotic trials.
- Boundaries: This is a population/stage bucket, not a symptom-domain bucket. Use it when resistance to standard antipsychotics is part of the core enrollment logic or labeled use. Do not use it as shorthand for severe illness alone.
- Belongs here:
  - Clozapine-labeled TRS use
  - Trials requiring documented inadequate response to standard antipsychotics before enrollment
- Does not belong here:
  - Acute exacerbation studies in broad all-comer schizophrenia populations
  - Patients who are merely difficult to treat but do not meet resistance criteria
  - Patients already known to have failed an adequate clozapine trial

### `clozapine_resistant_schizophrenia`

- Label: Clozapine-resistant schizophrenia
- Definition: Persistent, clinically meaningful illness burden despite an adequate clozapine trial with appropriate dose, duration, adherence, and, when available, exposure confirmation.
- Boundaries: Use only for the narrower post-clozapine subgroup. Do not collapse ordinary TRS into this bucket.
- Belongs here:
  - Adjunctive programs that explicitly require inadequate response on clozapine
  - Studies designed for residual symptoms after verified clozapine exposure
- Does not belong here:
  - Clozapine-naive TRS populations
  - Broad refractory cohorts where clozapine exposure is mixed or undocumented

### `negative_symptoms`

- Label: Negative symptoms
- Definition: Deficits such as avolition, anhedonia, alogia, blunted affect, and asociality, ideally measured in populations where these symptoms are primary rather than secondary to positive symptoms, depression, sedation, or extrapyramidal burden.
- Boundaries: Use when the program explicitly targets primary or predominant negative symptoms. Do not use for generic quality-of-life claims or social-function endpoints without a negative-symptom hypothesis.
- Belongs here:
  - Predominant negative-symptom trials using NSA-16, BNSS, or analogous scales
  - Programs designed to improve motivation, affective flattening, or social withdrawal independent of acute psychosis control
- Does not belong here:
  - Acute PANSS-total studies where negative items are secondary
  - Functioning-only programs with no direct negative-symptom endpoint
  - Sedation-reduction or depression-treatment programs masquerading as negative-symptom readouts

### `cognition`

- Label: Cognition
- Definition: Neurocognitive performance domains such as attention, working memory, executive function, learning, memory, and processing speed in schizophrenia.
- Boundaries: Use when formal cognitive batteries or cognition-focused endpoints are primary. Do not use for subjective concentration complaints or for real-world functioning as a proxy.
- Belongs here:
  - CIAS programs using MCCB or comparable cognitive composites
  - Studies explicitly designed around learning, memory, or executive-function improvement
- Does not belong here:
  - General functioning studies without direct cognitive endpoints
  - Acute symptom trials with exploratory cognition subscales only

### `chr_transition_prevention`

- Label: CHR / transition prevention
- Definition: Prevention of transition from a clinical high-risk or prodromal state into threshold psychotic illness.
- Boundaries: This bucket is pre-threshold and pre-established-schizophrenia. Do not use it for relapse prevention after diagnosis or for first-episode stabilization studies once full psychosis is already present.
- Belongs here:
  - Programs enrolling CHR, UHR, or prodromal populations to reduce conversion to psychosis
  - Longitudinal studies where transition rate is the central outcome
- Does not belong here:
  - Established schizophrenia maintenance studies
  - Early-course schizophrenia programs after first psychotic break

### `functioning_durable_recovery_relevance`

- Label: Functioning / durable recovery relevance
- Definition: Real-world functional capacity, social or occupational participation, sustained recovery, and longitudinal outcome relevance that matters beyond a short symptom-scale win.
- Boundaries: Treat this as a cross-cutting outcome lens. Use it when functioning or durable recovery is itself the main claim, not when it is merely an exploratory secondary endpoint in an acute efficacy study.
- Belongs here:
  - Programs built around social functioning, occupational recovery, or sustained community functioning
  - Longitudinal interventions where durable recovery is the principal clinical objective
- Does not belong here:
  - Acute symptom studies with incidental quality-of-life follow-up
  - Pure cognition studies unless the primary claim is functional translation rather than test performance

## Overlap Rules

- A row can belong to `negative_symptoms` or `cognition` while the enrolled population is also `treatment_resistant_schizophrenia`; keep the ontology bucket in `domain` and the enrollment detail in `population`.
- `functioning_durable_recovery_relevance` should only be the chosen bucket when functioning or durable recovery is the main reason the program matters.
- `relapse_prevention` and `acute_positive_symptoms` are different questions even when the same molecule is used in both settings. Record separate rows for separate events.
