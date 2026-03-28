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
- a `domain_profiles` object keyed by ontology slug for per-domain/per-stage inspection

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
  - PR7-backed structural substrate is now available, but PR8 keeps the numeric head explicit and unscored
- `directionality_confidence`
  - PR7-backed directionality substrate is now available, but PR8 keeps the numeric head explicit and unscored
- `subgroup_resolution_score`
  - PR7-backed subgroup and heterogeneity substrate is now available structurally, but PR8 leaves the numeric head explicit and unscored

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

Each profile is a documented weighted blend over the `v1` decision heads. Scores are computed over the available head subset, and each output row carries a coverage fraction plus explicit PR7-substrate status fields where numeric semantics remain intentionally deferred.

### Legacy Comparison Contract

- `heuristic_score_v0` in `v1` artifacts is the unchanged `v0` `composite_score`
- `heuristic_rank_v0` is the unchanged `v0` rank
- `heuristic_stable_v0` is the unchanged `v0` stability label

Existing `v0` numeric outputs remain unchanged. `v1` exists beside them as a comparison and inspection layer.
