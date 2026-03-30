# Claim Boundaries

## What This Repo Currently Is

- `v0` is a public-evidence prioritization scaffold for schizophrenia-oriented targets and biological modules.
- `v0` turns curated evidence tables into explicit weighted rankings, sensitivity checks, and warning-rich reports.
- `heuristic_stable` means an entity was rank-eligible and survived at least the configured share of `v0` sensitivity runs.
- `v1` is an additive multi-head decision-vector layer that exposes separate domain/stage profiles without changing the underlying `v0` numbers.
- the benchmark path is now runnable end to end from frozen snapshot, cohort, and runner artifacts

## What This Repo Currently Is Not

- `v0` is not a validated target decision system.
- `v0` is not a justified `advance / do not advance` authority for programs or assets.
- `v1` is not a validated clinical advancement authority, even though it now assigns explicit numeric failure, directionality, and subgroup heads from the PR7 target-ledger substrate.
- `v0` now has a non-seed candidate-registry ingest path, but the checked-in example scoring workflow still relies on curated shortlists and source-specific harmonization.
- atlas raw-source staging now exists for selected adapter-backed pulls, but it is still not raw consortium-dump ingestion and it does not replace the current scoring inputs
- the benchmark path is not a production-scale historical replay system
- the benchmark path is not a calibration, threshold-selection, or deployment-readiness claim

## What `v0` Outputs Mean

- Composite scores are transparent weighted summaries over the evidence layers that are present.
- Eligibility means the current scoring contract has the minimum required evidence groups for ranking.
- `heuristic_stable` means the entity cleared the current `v0` sensitivity-survival rule; it does not validate the biology.
- Warning overlays highlight evidence gaps and prior concerns without changing numeric score, rank, or stability status.
- Structural target ledgers expose source primitives, failure history, and directionality hypotheses without changing numeric score, rank, or stability status.
- `v1` decision vectors expose component-head tradeoffs and separate ontology-specific profiles, including numeric PR7-backed failure, directionality, and subgroup heads for gene targets.

## Benchmark Boundary

- snapshot manifests freeze the pre-cutoff evidence boundary and explicit per-source inclusion or exclusion decisions
- cohort label artifacts keep admissible ranking membership separate from post-cutoff future outcomes
- run manifests, metric payloads, and confidence interval payloads record what was executed without changing scoring semantics
- protocol-only baselines remain declared for comparability but are not executed unless later archived artifacts make them runnable

## Current Release Limitations

- historical benchmark archives are fixture-scale and currently checked in only for `data/benchmark/fixtures/scz_small/`
- benchmark breadth is still limited to the frozen schizophrenia benchmark question, a small deterministic cohort, and the current `available_now` baseline subset
- calibration work, decision-threshold setting, and broader operating-point evaluation remain future work

## What `v0` Outputs Do Not Justify Claiming

- target validation
- disease-mechanism certainty
- symptom-domain-specific efficacy expectations
- relapse-prevention or transition-prevention efficacy expectations
- clinical advancement or termination decisions
- a fully seed-independent end-to-end scoring claim
