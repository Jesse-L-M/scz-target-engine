# SCZ Intervention Observatory -- Roadmap

Supersedes: deep-scz-validate-calibrate.md (2026-03-31, CEO+Eng review plan)
Generated: 2026-04-01
Branch: Jesse-L-M/intervention-observatory

## North Star

Build the public schizophrenia intervention reference system that can, at a frozen
date, rank and critique intervention objects better than strong baselines, explain
why, point to the nearest historical analog failures, and emit the first decisive
falsification experiment.

The repo stops being "a target engine" and becomes a benchmarked schizophrenia
intervention observatory with failure memory, policy-based prioritization, and
falsification-ready hypothesis packets.

## Why the Reframe

The previous plan (13 items across 3 phases, ~8 hours CC) was a valid buildout of
existing surfaces. But it kept gene/module ranking as the primary ontology and treated
validation as a feature to add. The deeper analysis revealed:

1. **Target ranking alone is commoditized.** Open Targets already does disease-agnostic
   target prioritization. Recent SCZ papers already combine GWAS + locus-based methods
   + druggability lookups. A repo that stops there is publishable but not field-defining.

2. **The repo's actual moat is already beyond gene ranking.** Program memory, failure-scope
   reasoning, leakage-controlled replay, rescue tasks, policy views, and falsification
   packets are all surfaces that don't exist in any comparable open tool.

3. **The primary ontology should be the intervention object.** Translational decisions are
   about (target x direction x modality x population x stage x endpoint x regimen), not
   about gene lists. The repo's best surfaces already think this way.

4. **Success should be falsifiable.** Not "did we ship 13 items" but "can the repo beat
   strong baselines on historical SCZ intervention replay?"

## Intervention Object Ontology

Promote immediately. The decision unit becomes:

```
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

Genes and modules become evidence nodes inside this decision object, not the final
decision object.

## Success Criterion (6-month gate)

Can the repo beat simple baselines on a real, leakage-controlled historical
schizophrenia program replay?

Not "can it emit more artifacts." Not "can it refresh automatically." Not "can it
produce a nicer dashboard." If the answer is no, simplify. If yes, double down.

## Milestones

### Milestone 0 -- Freeze Contracts and Smoke Path

Goal: make the repo legible and reproducible before adding more moving parts.

Modules: docs/*, src/scz_target_engine/core/*, workflows/*, artifacts/*, tests/*

Ship:
- Release-facing README rewrite: "intervention observatory," not just "target engine"
- release_contracts/ directory for the 5 artifact families (see below)
- Smoke-test path: candidate registry, example build, benchmark fixture, rescue
  baseline suite, hypothesis packet generation
- GitHub Actions CI for smoke path + existing test surface

Hard gate: a fresh user can reproduce the core stack from documented commands and
frozen artifacts.

Stop condition: do not spend a sprint on package migration. The architecture doc
says namespaced subpackages are narrow wrappers over legacy flat modules. That is
compatibility plumbing, not product progress.

### Milestone 1 -- Program Memory 2.0 (the primary moat)

Goal: build the strongest non-commoditized public-good layer first.

Modules: src/scz_target_engine/program_memory/*, sources/*

Ship:
- Exhaustive event-level corpus for all approved SCZ molecular programs
- Effectively exhaustive phase 2/3 coverage
- High-coverage phase 1 / repurposing layer
- Explicit lineage IDs for assets and target classes
- Analog index and failure-scope adjudication
- Disagreement tracking and source archiving
- ClinicalTrials.gov source adapter (ingestion into program memory, not the ontology)

Hard gate: for the core molecular scope, every top-ranked intervention object can be
mapped to prior analogs, and coverage reports tell a user what is included, excluded,
unresolved, and weakly sourced.

Coverage thresholds:
- Approved programs: 100%
- Phase 2/3 programs: >95% with unresolved rows explicitly listed
- Phase 1/repurposing: high coverage with tracked denominator and confidence-weighted
  inclusion rule

Required event fields:
event_id, asset_id, asset_lineage_id, target_class_lineage_id, date, sponsor, asset,
target, target_class, mechanism, modality, population, domain, regimen, phase,
endpoint_context, event_type, primary_outcome_result, failure_reason_taxonomy,
failure_scope, source_tier, source_url, confidence, notes.

### Milestone 2 -- Historical Replay (the proving ground)

Goal: convert the benchmark from fixture-scale honesty into real historical replay.

Modules: src/scz_target_engine/benchmark/*, hidden_eval/*, challenge/prospective_registry.py

Ship:
- Real multi-snapshot replay (not just scz_small)
- Track A: historical intervention/program prioritization
- Track B: failure-memory reasoning (analog retrieval, failure-scope classification,
  "what must differ?" evaluation)
- Baseline registry: v0_current, v1_current, genetics-only, platform-only,
  program-memory-only, failure-memory-aware variants
- Automatic leaderboard and error-analysis generation

Hard gate: at least one new layer materially beats v0_current and v1_current on a
predeclared principal slice with bootstrap CIs and explicit failure analysis.

Concrete thresholds:
- At least 10-15 historical slices
- 1y / 3y / 5y horizons
- Predeclared principal metric per track
- One nontrivial model/policy win that survives CI and subgroup/error review

If no genuine uplift: pivot toward "public memory + benchmark resource" and stop
pretending to be a better selector.

### Milestone 3 -- "Why This?" and "Why Not This?" (the product surface)

Goal: turn rankings into auditable decisions.

Modules: src/scz_target_engine/policy/*, hypothesis_lab/*, observatory/*

Ship:
- Three initial policies only: repurposing/off-patent, novel-mechanism, adjunctive TRS
- Explicit "why not this?" bottleneck decomposition for every top candidate (sparse
  history, contradictory directionality, no subgroup fit, analog failure burden, weak
  tractability, missing developmental evidence, lack of rescue support)
- Packet outputs extended: first assay, first decisive falsification test, explicit
  kill criteria, biomarker/subgroup suggestions
- Blinded expert packet comparison

Hard gate: structured packets beat plain rankings in blinded review.

Do NOT implement all seven scenario policies first. Benchmark three hard, add the
rest only once the replay framework can actually judge them.

### Milestone 4 -- Atlas Expansion (only after replay tells you what to expand)

Goal: turn the atlas into a real evidence substrate, not a decorative abstraction.

Modules: src/scz_target_engine/atlas/*, sources/*

Ship:
- SCHEMA and PsychENCODE into the atlas substrate
- Developmental timing / cell-context resolution
- Explicit program-memory joins into tensor queries
- Convergence hubs and mechanistic axes only where queryable and benchmark-relevant

Hard gate: every ranked intervention object can show an inspectable evidence slice
spanning common variant support, rare variant support, transcriptomic/regulatory
evidence, context/timing evidence, and program-memory evidence.

Do NOT build: graph database, atlas UI, full raw-dump ingestion sprint.

### Milestone 5 -- Rescue Credibility and External Accountability

Goal: keep rescue benchmark-first and cross the line into outside accountability.

Modules: src/scz_target_engine/rescue/*, hidden_eval/*, challenge/prospective_registry.py

Ship:
- Four-task rescue spine fully documented as dataset/task cards + admissions reports
- At least one non-baseline model that clears the task-specific admission bar
- Either true distribution-separated hidden-eval, external blinded expert review,
  or live prospective registry

Hard gate: one rescue model beats the best shipped baseline on at least one declared
principal rescue task without hidden regression on the others, and one external
credibility track is genuinely live.

Critical: the shipped hidden-eval is only real if submitters get an exported public
task package rather than a repo checkout. The repo itself still contains the held-out
evaluation CSVs.

## Release Artifact Contracts

Five artifact families. All new feature work must target one of these.

### 1. Program-Memory Release

```
program_memory_release/
  program_events.parquet
  program_asset_lineages.parquet
  failure_adjudications.parquet
  analog_index.parquet
  coverage_audit.json
  coverage_summary.csv
  coverage_gaps.csv
  source_archive_index.json
  release_manifest.json
```

### 2. Benchmark Release

```
benchmark_release/
  benchmark_snapshot_manifest.json
  benchmark_cohort_labels.csv
  baseline_registry.yaml
  metric_payloads/*.json
  confidence_interval_payloads/*.json
  report_cards/*.json
  leaderboards/*.json
  error_analysis/*.md
  benchmark_release_manifest.json
```

### 3. Policy Release

```
policy_release/
  decision_vectors_v2.parquet
  pareto_fronts.parquet
  policy_rankings/*.parquet
  uncertainty_payloads/*.json
  policy_manifest.json
```

### 4. Hypothesis-Lab Release

```
hypothesis_release/
  hypothesis_packets/*.json
  hypothesis_packets/*.md
  blinded_review_packets/*.json
  review_response_templates/*.json
  failure_escape_analyses/*.md
  falsification_plans/*.json
  assay_cards/*.md
```

### 5. Atlas Release

```
atlas_release/
  evidence_tensor.parquet
  entity_alignments.csv
  provenance_bundles.csv
  mechanistic_axes.yaml
  convergence_hubs.parquet
  atlas_release_manifest.json
```

## Stop / Continue / Defer

### Continue and make central

- `program_memory/*` -- the moat. Make exhaustive before making anything prettier.
- `benchmark/*` -- the proving ground. Scale before expanding speculative modeling.
- `hypothesis_lab/{packets,expert_packets}` -- user-facing layer with best chance of
  immediate field usefulness.
- `rescue/{registry,governance,baselines,tasks,models}` -- continue, keep the rule
  that models enter only by beating hard baselines.
- `sources/*` -- continue and add trial-registry ingestion.
- `tests/*` -- continue aggressively. CI is now part of product credibility.

### Continue, but narrowly

- `policy/*` -- keep the semantics, not the sprawl. Benchmark three policies first.
- `atlas/*` -- continue only insofar as it supports replay, rescue, and packets.
- `hidden_eval/*` -- continue as infra, keep claim boundary explicit.

### Defer

- `agents/*` -- hard defer. Agentic surface comes last. Agents may only operate over
  structured, cited, benchmarked objects. Easiest place to look more impressive than
  the repo actually is.
- `observatory/*` beyond thin artifact browser -- soft defer. Rich UI is not core
  credibility work.
- `challenge/prospective_registry.py` as public centerpiece -- soft defer. Keep it,
  test it, don't market forecast scorecards until retrospective replay is strong.

### Stop

- Package migration as a milestone. Compatibility plumbing, not product progress.
- Automated refresh as credibility substitute. Raw-source staging is additive, scoring
  semantics are still stabilizing.
- New rescue models unless they clear the shipped baseline-admission rule.
- Expanding beyond schizophrenia before this north star is complete.

## PR Sequence

1. **contracts-and-smoke-v1** -- Freeze release artifacts, add CI smoke path, rewrite
   README/claim docs around intervention observatory.
2. **program-memory-corpus-v1** -- CT.gov adapter, event schema tightening, asset and
   class lineage IDs, coverage denominator, exhaustive approved + phase 2/3 pass.
3. **benchmark-track-a-v1** -- Real multi-snapshot historical replay with intervention
   objects and stronger baselines.
4. **benchmark-track-b-v1** -- Failure-memory reasoning benchmark: analog retrieval,
   failure-scope classification, "what must differ?" evaluation.
5. **packets-utility-v1** -- Add assay/falsification/kill-criteria fields and run the
   first blinded usefulness study.
6. **atlas-expansion-v1** -- Add SCHEMA, PsychENCODE, developmental context, and
   program-memory joins to the tensor.
7. **rescue-admission-v1** -- Admit only models that beat baselines; externalize one
   real hidden/blinded/partner evaluation path.

## What Makes This Head-and-Shoulders Stand Out

It stands out if an outside scientist can pick a candidate and the repo returns a
decision packet showing: evidence vector, uncertainty, nearest failure analogs, what
must differ for the idea not to be redundant, subgroup/timing assumptions, first
assay, and first kill test.

It stands out if it keeps a public ledger of forecasts and later reconciles them
without rewriting history.

It stands out if failure memory is first-class. Most systems tell you why a target
looks interesting. Almost none tell you, in structured form, why similar attempts
already failed, how close the analogy really is, and what evidence would justify
trying again.

It stands out if it proves, publicly, that at least one added layer materially beats
strong baselines. Until that test is passed, this is promising infrastructure. After
that, it becomes reference infrastructure.

## Relationship to Previous Plan

The previous plan (deep-scz-validate-calibrate.md, 2026-03-31) is fully subsumed:

- Phase 1 items (#10 CI, #11 retry, #12 checkpointing) -> Milestone 0
- Phase 2 items (#1 backtesting, #2 CIs, #13 CT.gov) -> Milestones 1-2
- Phase 3 items (#5 attribution, #6 "why not?", #4 scorecards, #7 freshness) -> Milestone 3
- Deferred items (#3 refresh, #9 manifest) -> remain deferred (Stop list)
- Deprioritized items (#8 CLI split) -> remain deprioritized (Stop list)

Key changes from previous plan:
- Intervention object promoted to primary ontology (was gene/module)
- Success criterion is now falsifiable (beat baselines on replay, not "ship 13 items")
- Program memory promoted to first workstream (was Phase 2)
- Automated refresh demoted more aggressively (was deferred, now Stop)
- Agents and observatory deferred explicitly (were not addressed)
- Hard gates added at every milestone (previous plan had phases but no kill criteria)
