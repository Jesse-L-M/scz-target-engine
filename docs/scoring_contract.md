# Scoring Contract, V0

This is the method contract the code is expected to honor.

`v0` is a transparent public-evidence prioritization heuristic, not a validated target decision system.

## Entity Model

Two separate entity types:

- `gene`: keyed by `entity_id`, intended to map to Ensembl IDs in real builds
- `module`: keyed by `{source}:{module_id}` in real builds

These entities are never blended into one leaderboard.

## Gene Layers

Each curated layer score must already be normalized into `[0, 1]`.

- `common_variant_support`, weight `0.20`
- `rare_variant_support`, weight `0.20`
- `cell_state_support`, weight `0.20`
- `developmental_regulatory_support`, weight `0.20`
- `tractability_compoundability`, weight `0.20`

Eligibility rule:

- at least one genetic layer present:
  - `common_variant_support` or `rare_variant_support`
- at least one biological layer present:
  - `cell_state_support` or `developmental_regulatory_support`

Composite:

- weighted mean over available layers

## Module Layers

- `member_gene_genetic_enrichment`, weight `0.35`
- `cell_state_specificity`, weight `0.35`
- `developmental_regulatory_relevance`, weight `0.30`

Eligibility rule:

- `member_gene_genetic_enrichment` must be present
- at least one biological layer present:
  - `cell_state_specificity` or `developmental_regulatory_relevance`

Composite:

- weighted mean over available layers

## Warning Overlay

Warnings do not change numeric rank in `v0`.

They exist to stop polished target cards from hiding:

- prior schizophrenia trial failures
- target-class baggage
- evidence gaps
- direction-of-effect ambiguity

Warning inputs can come from two places:

- manual warning CSV rows
- automatic reporting warnings synthesized from obvious evidence gaps, such as missing required layer groups and missing source-backed coverage flags when those source-presence fields are present in the input table

These overlays remain reporting-only in `v0`; they do not alter numeric score, rank, or the heuristic-stability threshold rule.

`PR7` also emits structural failure-history and directionality ledger fields for later consumption.
Those fields remain scoring-neutral in `v0`.

## Baselines

- Naive baseline: `common_variant_support` only
- Generic platform baseline: `generic_platform_baseline`

The engine should demonstrate what changes relative to both.

## Stability

Sensitivity runs:

- leave-one-layer-out for each scored layer
- `+/- 20%` perturbation for each layer weight, with renormalization

Heuristic-stability rule:

- entity appears in the reference top `N`
- entity survives in at least `70%` of sensitivity runs

This rule is used to label rankings as more or less robust inside `v0`; it is not a validated go/no-go threshold.

Pass condition:

- median top-`N` overlap across perturbation runs is at least `0.70`
- no leave-one-layer-out run ejects more than half of the reference top 10

## V0 Exclusions

- no TRS-specific scoring
- no symptom-domain scoring
- no illness-stage or relapse-prevention scoring heads
- no numeric warning penalties
- no numeric failure-history or directionality penalties
- no raw-source ingest pipeline
- no fully seed-independent end-to-end scoring claim

## V1 Decision Vector Overlay

`v1` is additive. It does not mutate `v0` weights, eligibility, ranks, stability rules, or existing output files.

The build emits:

- `decision_vectors_v1.json`: nested per-entity decision vectors plus domain/stage head outputs
- `domain_head_rankings_v1.csv`: flat per-domain/per-stage ranks with side-by-side `heuristic_score_v0` comparison columns

Each entity in `decision_vectors_v1.json` now exposes:

- named head fields such as `human_support_score` and `biology_context_score`
- matching `*_status` fields for those heads
- a `decision_vector` object keyed by head name for the richer per-head payload
- a `domain_profiles` object keyed by ontology slug for per-domain/per-stage inspection, including `projected_head_scores` for the head values actually consumed by that profile

### Decision Heads

- `human_support_score`
  - Gene semantics: mean of `common_variant_support` and `rare_variant_support`
  - Module semantics: `member_gene_genetic_enrichment`
- `biology_context_score`
  - Gene semantics: mean of `cell_state_support` and `developmental_regulatory_support`
  - Module semantics: mean of `cell_state_specificity` and `developmental_regulatory_relevance`
- `intervention_readiness_score`
  - Gene semantics: weighted blend of `tractability_compoundability` and `generic_platform_baseline`
  - Module semantics: not currently applicable; modules are not direct intervention objects in this build
- `failure_burden_score`
  - Gene semantics: higher means lower known failure burden in the PR7 target ledger
  - Score `1.0` when `structural_failure_history.failure_event_count == 0`
  - Otherwise score `1.0 - count_penalty - strongest_scope_penalty - strongest_evidence_penalty`, clamped into `[0, 1]`
  - Count penalty: `0.15 * failure_event_count`, capped at `0.45`
  - Strongest scope penalty: `target 0.35`, `target_class 0.30`, `unresolved 0.25`, `population 0.20`, `endpoint 0.15`, `molecule 0.10`
  - Strongest evidence penalty: `strong 0.10`, `moderate 0.05`, `provisional 0.0`
  - Domain profiles and `domain_head_rankings_v1.csv` project this head through the profile ontology bucket, so an acute-only failure event stays neutral for unrelated domains such as `negative_symptoms` or `treatment_resistant_schizophrenia`
  - Module semantics: not currently applicable because the merged PR7 ledger substrate is target-level only
- `directionality_confidence`
  - Gene semantics: confidence that directionality is curated, concrete, and not heavily contradicted in the ledger
  - Score `0.25` when `directionality_hypothesis.status != curated`
  - Otherwise start from the curated confidence map: `high 0.90`, `medium 0.75`, `low 0.60`
  - Subtract `0.10` for `desired_perturbation_direction == undetermined`
  - Subtract `0.10` for `modality_hypothesis == undetermined`
  - Subtract `0.10` when contradiction conditions are present
  - Subtract `0.05` when falsification conditions are present
  - Subtract `0.05 * open_risk_count`, capped at `0.10`
  - Module semantics: not currently applicable because the merged PR7 ledger substrate is target-level only
- `subgroup_resolution_score`
  - Gene semantics: additive clarity score over structured domain, population, and cell-state resolution, penalized by explicit heterogeneity signals
  - Add `0.25` for exactly one `clinical_domain`, or `0.15` when multiple domains are present
  - Add `0.25` for exactly one `clinical_population`, or `0.15` when multiple populations are present
  - Add `0.10` when `mono_or_adjunct_contexts` is non-empty
  - Add `0.15` when `psychencode_deg_top_cell_types` is non-empty
  - Add `0.15` when `psychencode_grn_top_cell_types` is non-empty
  - Subtract `0.15` when `failure_scopes` includes `population`
  - Subtract `0.15` when `failure_taxonomies` includes `heterogeneity_or_subgroup_dilution`
  - Domain profiles and `domain_head_rankings_v1.csv` only consume the clinical-domain, population, regimen, and heterogeneity parts of this head when the ledger event domain matches the profile ontology bucket; PsychENCODE cell-state context remains global
  - Module semantics: not currently applicable because the merged PR7 ledger substrate is target-level only

### Domain / Stage Head Profiles

Canonical ontology buckets from `docs/ontology.md` now map to explicit `v1` profiles:

- `acute_positive_symptoms`
- `relapse_prevention`
- `treatment_resistant_schizophrenia`
- `clozapine_resistant_schizophrenia`
- `negative_symptoms`
- `cognition`
- `chr_transition_prevention`
- `functioning_durable_recovery_relevance`

Each profile is a documented weighted blend over the `v1` decision heads. Gene-target rows now consume all six heads numerically when the PR7 ledger is present, but `failure_burden_score` and `subgroup_resolution_score` are projected through the profile's ontology bucket before they affect that profile. Module rows still compute over the available subset because the merged PR7 substrate is target-level only.

### Legacy Comparison Contract

- `heuristic_score_v0` in `v1` artifacts is the unchanged `v0` `composite_score`
- `heuristic_rank_v0` is the unchanged `v0` rank
- `heuristic_stable_v0` is the unchanged `v0` stability label

Existing `v0` numeric outputs remain unchanged. `v1` exists beside them as a comparison and inspection layer.
