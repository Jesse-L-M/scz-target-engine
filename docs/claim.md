# Claim Boundaries

## What This Repo Currently Is

- `v0` is a public-evidence prioritization scaffold for schizophrenia-oriented targets and biological modules.
- `v0` turns curated evidence tables into explicit weighted rankings, sensitivity checks, and warning-rich reports.
- `heuristic_stable` means an entity was rank-eligible and survived at least the configured share of `v0` sensitivity runs.
- `v1` is an additive multi-head decision-vector layer that exposes separate domain/stage profiles without changing the underlying `v0` numbers.

## What This Repo Currently Is Not

- `v0` is not a validated target decision system.
- `v0` is not a justified `advance / do not advance` authority for programs or assets.
- `v1` is not a validated clinical advancement authority, and its PR7-backed failure, directionality, and subgroup heads remain explicit and unscored even though the structural substrate is now landed.
- `v0` now has a non-seed candidate-registry ingest path, but the checked-in example scoring workflow still relies on curated shortlists and source-specific harmonization.

## What `v0` Outputs Mean

- Composite scores are transparent weighted summaries over the evidence layers that are present.
- Eligibility means the current scoring contract has the minimum required evidence groups for ranking.
- `heuristic_stable` means the entity cleared the current `v0` sensitivity-survival rule; it does not validate the biology.
- Warning overlays highlight evidence gaps and prior concerns without changing numeric score, rank, or stability status.
- Structural target ledgers expose source primitives, failure history, and directionality hypotheses without changing numeric score, rank, or stability status.
- `v1` decision vectors expose component-head tradeoffs and separate ontology-specific profiles, but they still leave PR7-backed failure, directionality, and subgroup semantics explicitly unscored in this branch.

## What `v0` Outputs Do Not Justify Claiming

- target validation
- disease-mechanism certainty
- symptom-domain-specific efficacy expectations
- relapse-prevention or transition-prevention efficacy expectations
- clinical advancement or termination decisions
- a fully seed-independent end-to-end scoring claim
