# Claim Boundaries

This file is the shipped-behavior boundary.

Do not read roadmap goals here as shipped facts. For the current strategy and
sequencing, read `docs/roadmap.md`. For the detailed working plan, read
`docs/designs/deep-scz-validate-calibrate.md`.

## What This Repo Currently Is

- `v0` is a public-evidence prioritization scaffold for schizophrenia-oriented targets and biological modules.
- `v0` turns curated evidence tables into explicit weighted rankings, sensitivity checks, and warning-rich reports.
- `heuristic_stable` means an entity was rank-eligible and survived at least the configured share of `v0` sensitivity runs.
- `v1` is an additive multi-head decision-vector layer that exposes separate domain/stage profiles without changing the underlying `v0` numbers.
- the checked-in program-memory release now includes a real schizophrenia molecular-
  program denominator with effectively complete approved coverage, near-exhaustive
  phase 2/3 coverage, and explicit `included` / `unresolved` / `duplicate` /
  `excluded` / `out_of_scope` accounting
- the benchmark path is now runnable end to end from frozen snapshot, cohort, and runner artifacts
- the Milestone 0 compatibility surface is now frozen through
  `docs/intervention_object_compatibility.md`, six registered release-manifest
  families, and the shared smoke path at `scripts/run_contract_smoke_path.sh`

## What This Repo Currently Is Not

- `v0` is not a validated target decision system.
- `v0` is not a justified `advance / do not advance` authority for programs or assets.
- `v1` is not a validated clinical advancement authority, even though it now assigns explicit numeric failure, directionality, and subgroup heads from the PR7 target-ledger substrate.
- `v0` now has a non-seed candidate-registry ingest path, but the checked-in example scoring workflow still relies on curated shortlists and source-specific harmonization.
- atlas raw-source staging now exists for selected adapter-backed pulls, but it is still not raw consortium-dump ingestion and it does not replace the current scoring inputs
- the benchmark path is not a production-scale historical replay system
- the benchmark path now backfills ten checked-in Track A public slices with
  slice-local `program_universe.csv` and `events.csv`, and five of those cutoffs
  are now principal-`3y` evaluable. The PR3 stop/go decision run was executed on
  2026-04-08 and the result is **HOLD**: projected available-now challengers now
  exist at `intervention_object` grain, but no challenger has a
  bootstrap-backed material win on the principal slice. `schema_only` improves
  the principal AP to 0.500 and `opentargets_only` improves it to 0.333, yet the
  intervals remain too wide to justify `GO`. See
  `docs/decisions/0005-track-a-pr3-stop-go.md`
- a feasibility audit on 2026-04-08 determined the Track A PR3 gate is **infeasible**
  with the current honest archive universe: the entire schizophrenia late-stage replay
  window contains exactly 1 independent positive event (the Cobenfy approval), so the
  AP + bootstrap gate can never produce non-degenerate confidence intervals regardless
  of challenger quality. No honest alternative surface exists. See
  `docs/decisions/0006-track-a-pr3-feasibility.md`
- the benchmark path is not a calibration, threshold-selection, or deployment-readiness claim
- `intervention_object_id` is not yet the shipped replacement key for the current
  gene, module, policy, or packet artifacts during the dual-write period

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

- historical benchmark archives are fixture-scale and currently checked in for `data/benchmark/fixtures/scz_small/` plus the dedicated Track A replay archive surface at `data/benchmark/fixtures/scz_track_a_historical_replay/`
- replay public slices now cover ten honest Track A cutoffs with pinned local
  denominator inputs; five checked-in slices are principal-`3y` evaluable with one
  positive intervention-object each, while the later five still have zero positives
- on the principal `2024-09-25` slice, `schema_only`, `opentargets_only`,
  `v1_current`, and `random_with_coverage` cover `8/8` admissible intervention
  objects, `v0_current` covers `5/8`, `pgc_only` covers `4/8`, and `chembl_only`
  covers `0/8`. The best challenger point estimate now comes from `schema_only`
  at AP = 0.500, but its 95% CI still spans `[0.0, 1.0]`
- benchmark breadth is still limited to the frozen schizophrenia benchmark question, a small deterministic cohort, and the current `available_now` baseline subset
- calibration work, decision-threshold setting, and broader operating-point evaluation remain future work

## Contract-Frozen Surface

- Current shipped compatibility consumers remain `gene_target_ledgers`,
  `decision_vectors_v1`, `domain_head_rankings_v1`,
  `policy_decision_vectors_v2`, `policy_pareto_fronts_v1`, and
  `hypothesis_packets_v1`.
- Future intervention-object-native work must dual-write back through the checked-in
  compatibility rules in `docs/intervention_object_compatibility.md`.
- Projection multiplicity must be explicit, and silent legacy-consumer collisions
  are forbidden during the dual-write period.
- Release bundles now freeze file membership, SHA256 digests, and nested schema
  versions through the registered `program_memory_release`,
  `benchmark_release`, `rescue_release`, `variant_context_release`,
  `policy_release`, and `hypothesis_release` manifest families.
- The pinned smoke path lives at `scripts/run_contract_smoke_path.sh` and is the
  same command set executed in `.github/workflows/ci.yml`.
- That smoke path now rebuilds the frozen example outputs in a temporary
  directory and fails on drift instead of silently rewriting
  `examples/v0/output/`.

## What `v0` Outputs Do Not Justify Claiming

- target validation
- disease-mechanism certainty
- symptom-domain-specific efficacy expectations
- relapse-prevention or transition-prevention efficacy expectations
- clinical advancement or termination decisions
- a fully seed-independent end-to-end scoring claim
