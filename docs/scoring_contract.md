# Scoring Contract, V0

This is the method contract the code is expected to honor.

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

## Baselines

- Naive baseline: `common_variant_support` only
- Generic platform baseline: `generic_platform_baseline`

The engine should demonstrate what changes relative to both.

## Stability

Sensitivity runs:

- leave-one-layer-out for each scored layer
- `+/- 20%` perturbation for each layer weight, with renormalization

Decision-grade rule:

- entity appears in the reference top `N`
- entity survives in at least `70%` of sensitivity runs

Pass condition:

- median top-`N` overlap across perturbation runs is at least `0.70`
- no leave-one-layer-out run ejects more than half of the reference top 10

## V0 Exclusions

- no TRS-specific scoring
- no symptom-domain scoring
- no numeric warning penalties
- no raw-source ingest pipeline
