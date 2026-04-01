# SCZ Digital Biology Observatory -- Roadmap

Generated: 2026-04-01
Branch: Jesse-L-M/contract-freeze

This is the detailed working roadmap.

The short pointer doc for future readers lives at `docs/roadmap.md`.
Earlier roadmap drafts are superseded in git history.

## North Star

Build the open schizophrenia digital-biology benchmark and intervention observatory.

The repo is not just a better literature-weighted ranker. It is the public control
plane for schizophrenia intervention decisions plus one flagship schizophrenia rescue /
virtual-cell benchmark that can be tested, falsified, and improved in the open.

That means the repo must do two jobs at once:

1. Keep an auditable intervention control plane:
   program memory, failure analogs, historical replay, policy views, and falsification-
   ready packets.
2. Benchmark one genuinely hard, schizophrenia-specific predictive biology task:
   a rescue benchmark in a defined cellular context with explicit baselines and
   hidden-eval-ready packaging.

## Two-Speed Architecture

The architecture is now explicitly two-speed.

```text
OPEN CONTROL PLANE
==================
program memory -> replay -> policy views -> packets -> external accountability
       |             |            |             |
       +-------------+------------+-------------+
                     intervention_object_id

SCIENTIFIC CORE
===============
variant-to-context substrate -> SCZ-Rescue-1 -> rescue models -> assay / kill test plans
            |                         |
            +-------------------------+
                 benchmarked translation path
```

The control plane is where the repo becomes legible, auditable, and publishable.
The scientific core is where the repo stops being "interesting infrastructure" and
starts becoming field-shaping.

## Why the Reframe

The previous rewrite correctly moved the repo beyond gene ranking and toward
intervention objects, historical replay, and failure memory. That part stays.

What changes is what becomes central:

1. **Generic target ranking is not the bleeding edge.** Frontier groups win by solving
   narrow, measurable biology bottlenecks, not by building prettier literature
   synthesizers.
2. **The repo's best open wedge is still the control plane.** Program memory, replay,
   failure-scope reasoning, rescue governance, and falsification packets remain real
   public-good infrastructure.
3. **The flagship scientific product should be rescue / virtual-cell work.** In
   schizophrenia, the high-value bottleneck is not chemistry-first design. It is
   disease-state specification and intervention prediction in the right cell context
   and developmental window.
4. **The atlas is not a display layer.** It becomes the feature store for rescue and
   replay, not an end in itself.
5. **Use frontier molecular predictors, do not rebuild them.** Variant-effect,
   regulatory-effect, and structure predictors are inputs to this system, not the
   system itself.

## Intervention Object Ontology

The primary decision unit remains:

```text
intervention_object_id = (
    asset,
    mechanism,
    target_set,
    direction,
    modality,
    population,
    stage,
    endpoint_domain,
    regimen
)
```

But this promotion is now compatibility-first:

- intervention objects are the source of truth for new replay, rescue, policy, and
  packet work
- genes, modules, and current policy entities remain materialized projections until
  the replay benchmark proves the new substrate
- Milestone 0 must ship the compatibility contract and dual-write rules before later
  milestones touch current artifact consumers

## Success Criterion (6-month gate)

This roadmap now has two coupled gates.

### Gate A -- Control Plane

Can the repo beat strong baselines on real, leakage-controlled historical
schizophrenia replay?

If no, simplify and ship the repo as public memory + benchmark infrastructure.

### Gate B -- Scientific Core

Can `SCZ-Rescue-1` beat simple rescue baselines on one sharply defined, public,
schizophrenia-relevant cell context and produce translation-ready hypothesis packets?

If no, stop pretending the rescue layer is predictive and keep it benchmark-first.

The project becomes field-defining only if both gates stay honest.

## Strategic Principles

- Build the control plane and the scientific core in parallel, but do not let UI or
  policy sprawl outrun benchmark truth.
- Benchmark first, publish second.
- Use existing frontier molecular predictors downstream of schizophrenia biology
  shortlisting, not as the core product.
- Keep explicit failure memory and "what must differ now?" reasoning as the main
  public differentiator.
- Do not expand beyond schizophrenia until the control plane and rescue benchmark are
  credible.

## Milestones

### Milestone 0 -- Freeze Contracts, Compatibility, and Smoke Path

Goal: make the repo legible, reproducible, and compatible before adding new model
surfaces.

Modules:
- docs/*
- src/scz_target_engine/artifacts/*
- src/scz_target_engine/core/*
- src/scz_target_engine/workflows/*
- tests/*

Ship:
- README / claim-doc rewrite around "digital-biology benchmark and intervention
  observatory"
- intervention-object compatibility matrix with dual-write rules back to current
  gene/module/policy consumers
- release artifact registration in the existing artifact registry, not a parallel
  `release_contracts/` layer
- smoke path: candidate registry -> example build -> benchmark fixture -> rescue
  baseline suite -> hypothesis packet generation
- GitHub Actions CI for smoke path + existing test surface
- documented GitHub Releases distribution path for frozen release artifacts
- concrete checked-in contract anchors:
  `docs/intervention_object_compatibility.md`,
  `scripts/run_contract_smoke_path.sh`, and `.github/workflows/ci.yml`

Hard gate: a fresh user can reproduce the core stack from documented commands and no
current gene/module consumer silently breaks.

Stop condition: do not spend a sprint on package migration. Compatibility plumbing is
not product progress.

### Milestone 1 -- Program Memory 2.0 (control-plane moat)

Goal: build the strongest public failure-memory substrate first.

Modules:
- src/scz_target_engine/program_memory/*
- src/scz_target_engine/sources/*

Ship:
- event-level corpus for approved schizophrenia molecular programs
- effectively exhaustive phase 2/3 coverage
- high-coverage phase 1 / repurposing layer
- explicit lineage IDs for assets and target classes
- analog index and failure-scope adjudication
- disagreement tracking and source archiving
- ClinicalTrials.gov adapter feeding program memory, not redefining the ontology
- explicit denominator artifact:
  `program_universe.csv` with `included`, `excluded`, `unresolved`, and confidence
  states

Hard gate: for the core molecular scope, every top-ranked intervention object maps to
prior analogs and the coverage release tells a user exactly what is included,
excluded, unresolved, or weakly sourced.

Coverage thresholds:
- approved programs: 100%
- phase 2/3 programs: >95% with unresolved rows explicitly listed
- phase 1/repurposing: high coverage with tracked denominator and confidence-weighted
  inclusion rules

Required event fields:
`event_id`, `asset_id`, `asset_lineage_id`, `target_class_lineage_id`, `date`,
`sponsor`, `asset`, `target`, `target_class`, `mechanism`, `modality`, `population`,
`domain`, `regimen`, `phase`, `endpoint_context`, `event_type`,
`primary_outcome_result`, `failure_reason_taxonomy`, `failure_scope`, `source_tier`,
`source_url`, `confidence`, `notes`.

### Milestone 2 -- Historical Replay (control-plane proving ground)

Goal: convert the benchmark from fixture-scale honesty into real historical replay.

Modules:
- src/scz_target_engine/benchmark/*
- src/scz_target_engine/hidden_eval/*
- src/scz_target_engine/challenge/prospective_registry.py

Ship:
- real multi-snapshot replay, not just `scz_small`
- explicit archived intervention-object feature bundles per snapshot
- Track A: historical intervention / program prioritization
- Track B: failure-memory reasoning:
  analog retrieval, failure-scope classification, and "what must differ?" evaluation
- frozen baseline registry:
  `v0_current`, `v1_current`, genetics-only, platform-only, program-memory-only, and
  failure-memory-aware variants
- automatic leaderboard and error-analysis generation

Hard gate: at least one new layer materially beats `v0_current` and `v1_current` on a
predeclared principal slice with bootstrap CIs and explicit failure analysis.

Concrete thresholds:
- at least 10-15 historical slices
- 1y / 3y / 5y horizons
- predeclared principal metric per track
- one nontrivial win that survives CI and subgroup / error review

Mandatory stop: if no genuine uplift appears here, stop after PR4 and ship the repo as
public memory + benchmark infrastructure rather than pushing on with prettier product
surfaces.

### Milestone 3 -- SCZ-Rescue-1 (flagship scientific product)

Goal: make one sharply defined schizophrenia rescue benchmark the scientific center of
gravity.

Modules:
- src/scz_target_engine/rescue/*
- src/scz_target_engine/hidden_eval/*
- src/scz_target_engine/atlas/*
- src/scz_target_engine/hypothesis_lab/*

Ship:
- `SCZ-Rescue-1`: one public schizophrenia rescue benchmark in a defined
  glutamatergic-neuron context
- explicit disease-state definition, intervention universe, and target treated-state
  objective
- frozen public package for submitters and hidden-eval-ready operator bundle
- baseline suite:
  nearest-neighbor, simple graph/context, program-memory-aware, and current shipped
  rescue baselines
- model-admission rule: no non-baseline rescue model lands unless it beats the best
  shipped baseline on the declared principal metric
- translation bridge from top rescue predictions into hypothesis packets with first
  assay and kill criteria

Hard gate: at least one non-baseline model beats the best shipped baseline on the
principal rescue metric without hidden regression on the rest of the benchmark.

Critical rule: do not claim "virtual schizophrenia." One context first. One task
first. One hard benchmark first.

### Milestone 4 -- Variant-to-Context Substrate (feature store, not display layer)

Goal: make the atlas the feature store for rescue and replay.

Modules:
- src/scz_target_engine/atlas/*
- src/scz_target_engine/sources/*

Ship:
- SCHEMA and PsychENCODE into the atlas substrate
- developmental timing and cell-context resolution
- developmental xQTL / regulatory context joins where the data is public and queryable
- explicit joins from variant evidence to genes, cells, developmental windows, and
  intervention objects
- explicit program-memory and rescue joins into tensor queries
- convergence hubs and mechanistic axes only where they are benchmark-relevant

Hard gate: every ranked intervention object and every admitted rescue model can show an
inspectable chain from variant / gene evidence to cell context / developmental timing
to program-memory and rescue evidence.

Do NOT build: graph database, atlas UI, or a raw-dump ingestion marathon.

### Milestone 5 -- Policy and Packet Translation

Goal: turn replay and rescue wins into auditable decisions.

Modules:
- src/scz_target_engine/policy/*
- src/scz_target_engine/hypothesis_lab/*
- src/scz_target_engine/observatory/*

Ship:
- three initial policies only:
  repurposing / off-patent, novel-mechanism, and adjunctive TRS
- explicit "why this?" and "why not this?" bottleneck decomposition for top candidates
- packet outputs extended with first assay, first decisive falsification test, explicit
  kill criteria, biomarker suggestions, and subgroup suggestions
- blinded expert packet comparison with a predeclared rubric, reviewer count, and
  success threshold

Hard gate: structured packets beat plain rankings in blinded review under a declared
protocol, not an ad hoc taste test.

Do NOT implement broad policy proliferation before replay and rescue are real.

### Milestone 6 -- External Credibility Layer

Goal: cross the line from strong internal infrastructure to outside accountability.

Modules:
- src/scz_target_engine/hidden_eval/*
- src/scz_target_engine/rescue/*
- src/scz_target_engine/challenge/prospective_registry.py

Ship:
- one genuinely live external credibility track:
  distribution-separated hidden-eval, external blinded expert review, or partner-lab
  assay path
- public submitter package boundary for the flagship rescue benchmark
- admissions reports and release notes for new rescue models and new packet protocols
- optional prospective registry only after retrospective replay and rescue performance
  are already credible

Hard gate: one external credibility track is live and can be shown without hand-waving.

## Release Artifact Contracts

All new feature work must target one of these registered release families.

These top-level release manifest families are now registered in
`scz_target_engine.artifacts` and validate required files, SHA256 digests, and
nested registered artifact schema versions from the manifest entrypoint.

### 1. Program-Memory Release

```text
program_memory_release/
  program_events.parquet
  program_asset_lineages.parquet
  program_universe.csv
  failure_adjudications.parquet
  analog_index.parquet
  coverage_manifest.json
  coverage_summary.csv
  coverage_gaps.csv
  coverage_denominator_summary.csv
  coverage_denominator_gaps.csv
  source_archive_index.json
  release_manifest.json
```

### 2. Benchmark Release

```text
benchmark_release/
  benchmark_snapshot_manifest.json
  intervention_object_feature_bundle.parquet
  benchmark_cohort_labels.csv
  baseline_registry.yaml
  metric_payloads/*.json
  confidence_interval_payloads/*.json
  report_cards/*.json
  leaderboards/*.json
  error_analysis/*.md
  benchmark_release_manifest.json
```

### 3. Rescue Release

```text
rescue_release/
  rescue_task_card.json
  rescue_dataset_cards/*.json
  rescue_feature_bundle.parquet
  rescue_baseline_reports/*.json
  hidden_eval_public_package/*
  admissions_report.json
  rescue_release_manifest.json
```

### 4. Variant-Context Release

```text
variant_context_release/
  evidence_tensor.parquet
  entity_alignments.csv
  variant_effect_priors.parquet
  provenance_bundles.csv
  context_feature_manifest.json
  atlas_release_manifest.json
```

### 5. Policy Release

```text
policy_release/
  decision_vectors_v2.parquet
  pareto_fronts.parquet
  policy_rankings/*.parquet
  uncertainty_payloads/*.json
  policy_manifest.json
```

### 6. Hypothesis-Lab Release

```text
hypothesis_release/
  hypothesis_packets/*.json
  hypothesis_packets/*.md
  blinded_review_packets/*.json
  review_response_templates/*.json
  failure_escape_analyses/*.md
  falsification_plans/*.json
  assay_cards/*.md
```

## Stop / Continue / Defer

### Continue and make central

- `program_memory/*` -- the control-plane moat
- `benchmark/*` -- the proving ground
- `rescue/*` -- the flagship scientific product once replay gates are real
- `atlas/*` -- as a feature store for rescue and replay
- `tests/*` -- product credibility work, not aftercare

### Continue, but narrowly

- `policy/*` -- keep the semantics, not the sprawl
- `hypothesis_lab/*` -- only after replay / rescue produce real decision pressure
- `hidden_eval/*` -- keep the boundary explicit and distribution-separated
- `observatory/*` -- thin artifact browser only until benchmark truth is strong

### Defer

- `agents/*` -- structured, cited, benchmarked objects first
- `challenge/prospective_registry.py` as public centerpiece -- only after replay and
  rescue are strong enough that forecasting is not theater
- chemistry-first or de novo molecule generation as a core product
- generic "LLM for schizophrenia papers" directions

### Stop

- package migration as a milestone
- automated refresh as credibility substitute
- broad policy proliferation before rescue exists
- new rescue models unless they clear the shipped admission bar
- expanding beyond schizophrenia before this north star is complete

## PR Sequence

1. **contracts-and-compat-v2** -- freeze artifact families in the existing registry,
   add compatibility matrix, wire smoke path CI, and rewrite README / claim docs.
2. **program-memory-denominator-v1** -- add `program_universe.csv`, coverage manifest,
   lineage tightening, source archiving, and CT.gov ingestion.
3. **replay-track-a-v1** -- real multi-snapshot intervention-object replay with frozen
   feature bundles and stronger baselines.
4. **replay-track-b-v1** -- failure-memory reasoning benchmark:
   analog retrieval, failure-scope classification, and "what must differ?" evaluation.
5. **SCZ-Rescue-1-v1** -- ship the flagship public glutamatergic rescue benchmark,
   baseline suite, hidden-eval package, and admission rule.
6. **variant-to-context-v1** -- expand the atlas into a rescue / replay feature store
   with developmental and regulatory joins.
7. **policy-and-packets-v1** -- extend packet and policy surfaces only after replay and
   rescue generate real decision pressure.
8. **external-credibility-v1** -- externalize one real hidden / blinded / partner
   evaluation path.

Non-negotiable sequencing rule: do not start PRs 5-8 until PR4 proves there is real
uplift worth translating.

## What Makes This Head-and-Shoulders Stand Out

It stands out if an outside scientist can inspect one schizophrenia intervention idea
and get:

- the evidence vector
- the nearest failure analogs
- what must differ now for the idea not to be redundant
- the rescue context in which the prediction is meant to work
- the first assay, first kill test, and explicit failure conditions

It stands out if the repo keeps a public ledger of historical failures and later
reconciles forecasts without rewriting history.

It stands out if one public schizophrenia rescue benchmark becomes the place where new
models are compared rather than merely described.

It stands out if it proves, publicly, that at least one added layer materially beats
strong baselines. Until then, this is promising infrastructure. After that, it becomes
reference infrastructure.

## Relationship to Previous Plan

This version fully subsumes the previous two drafts.

What stays:
- intervention objects remain the correct ontology
- program memory remains the public moat
- historical replay remains the proving ground
- failure memory and falsification packets remain the differentiator
- agents and broad UI work remain downstream

What changes:
- the repo is now explicitly framed as a digital-biology benchmark plus intervention
  observatory
- `SCZ-Rescue-1` becomes the flagship scientific product
- the atlas is reframed as a variant-to-context feature store
- release contracts are folded into the existing artifact registry, not a parallel
  release-contract layer
- compatibility and dual-write rules become Milestone 0 work instead of an implicit
  future cleanup
- PR4 becomes a mandatory stop / continue gate before later product-surface work
