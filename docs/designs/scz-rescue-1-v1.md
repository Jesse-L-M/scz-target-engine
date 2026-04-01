# scz-rescue-1-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/replay-track-a-v1.md
Blocked by: evidence of real uplift after replay-track-b-v1
Supersedes: -
Last updated: 2026-04-01

## Objective

Turn the existing glutamatergic convergence rescue lane into the flagship public
schizophrenia rescue benchmark:
`SCZ-Rescue-1`.

This spec is intentionally narrow.
One cell context first. One governed rescue task first. One benchmark-first public
package first.

## Success Condition

- Primary success metric:
  average precision on the held-out `test` split for the flagship rescue task
- Secondary success metric:
  the hidden-eval public package round-trips from task package to simulated
  scorecard without leaking labels or metric details
- Stop/go gate:
  no non-baseline model is admitted unless it beats the best shipped baseline on
  the principal metric without regressions on the rest of the benchmark

## Scope

- build `SCZ-Rescue-1` on the existing
  `glutamatergic_convergence_rescue_task` governance bundle by default
- keep the rescue task gene-level in v1, but require a translation bridge from top
  rescue hits into intervention-object packet candidates
- freeze the public submitter package and operator-side hidden evaluator
- define the baseline suite and model-admission rule
- add a rescue feature bundle that can support non-baseline rescue models without
  reopening raw rescue or atlas sources

## Not in Scope

- multi-context or multi-cell-state rescue benchmarking
- de novo molecule generation
- broad virtual-cell claims
- replacing the hidden-eval distribution boundary with repo checkout access
- broad policy or UI work

## Existing Surfaces To Reuse

- `glutamatergic_convergence_rescue_task` contract, task card, dataset cards,
  freeze manifest, split manifest, and lineage artifacts:
  treat the shipped governed lane as the seed surface
- current `rescue run glutamatergic-convergence` path:
  keep the frozen-input execution model
- current rescue baseline pack:
  use the shipped aggregate-only baseline suite as the minimum comparison floor
- current hidden-eval packager and simulator:
  keep the public package / operator-only evaluator split
- current hypothesis-packet contract:
  translate top rescue outputs into packet-ready candidates without weakening the
  existing packet guardrails

## Inputs

- governed glutamatergic ranking and evaluation CSVs
- frozen split manifest and freeze manifest
- current rescue baseline outputs
- hidden-eval public package and simulator contract
- program-memory failure context and packet-ready ledger artifacts where available
- additive rescue feature bundle derived only from frozen, pre-cutoff inputs

## Outputs And Artifact Contracts

- New or changed artifact:
  `rescue_feature_bundle.parquet`
  derived from the governed ranking-input surface only
- New or changed artifact:
  `admissions_report.json`
  documenting baseline comparisons and any admitted non-baseline rescue model
- New or changed artifact:
  public hidden-eval task package for `SCZ-Rescue-1`
- New or changed artifact:
  packet-bridge output mapping top rescue hits into packet candidate inputs with
  traceable pointers
- Backward-compatibility rule:
  the existing `glutamatergic_convergence_rescue_task` id and hidden-eval CLI path
  stay valid unless a concrete contract gap forces a new task id

### Default Task Identity Rule

`SCZ-Rescue-1` should extend the shipped
`glutamatergic_convergence_rescue_task` by default.

Do not mint a second parallel task id unless one of these becomes impossible under
the current contract:

- freezing the new feature bundle
- expressing the evaluation split policy
- packaging a label-safe public submission surface

## Data Flow

```text
GOVERNED GLUTAMATERGIC RANKING INPUTS
    + SPLIT MANIFEST
    + RESCUE FEATURE BUNDLE
    -> BASELINE / MODEL PREDICTIONS
    -> OFFLINE EVALUATION
    -> ADMISSIONS REPORT
    -> HIDDEN-EVAL PUBLIC PACKAGE
    -> TOP HITS -> PACKET CANDIDATES
```

- The governed frozen CSVs remain the only supported ranking-input surface.
- The rescue feature bundle is additive and must be derivable from those frozen
  inputs only.
- Baselines and candidate models are compared on frozen splits.
- Hidden eval remains distribution-separated.
- The translation layer turns top rescue outputs into packet candidates, not into
  automatic advancement claims.

## Implementation Plan

1. Freeze the `SCZ-Rescue-1` benchmark definition on top of the shipped
   glutamatergic lane.
2. Add a rescue feature-bundle artifact that preserves the frozen-input boundary.
3. Extend the baseline suite with any additional simple graph/context baselines
   that still respect the frozen boundary.
4. Implement the admission report and model-admission rule.
5. Add a packet-bridge path for top rescue hits.
6. Verify the public package / simulated hidden-eval round trip remains label-safe.

## Acceptance Tests

- Unit:
  add tests for rescue feature-bundle validation, admission-report generation, and
  packet-bridge traceability
- Integration:
  run `rescue run glutamatergic-convergence`, package a submission from the
  predictions file, and simulate hidden eval end to end
- Regression:
  add a test that fails if `public_scorecard.json` includes split summaries,
  metrics, or top-ranked symbols for the shipped small task
- E2E, if relevant:
  governed task load -> rescue run -> hidden-eval package -> simulated operator
  scorecard -> packet candidate materialization

## Failure Modes

- Failure mode:
  a model consumes post-cutoff or operator-only information through the rescue
  feature bundle; the bundle must be derivable only from governed pre-cutoff
  ranking inputs
- Failure mode:
  public package export leaks hidden labels because stale files survive in the
  output directory; the packager must keep rejecting non-empty output dirs
- Failure mode:
  the packet bridge produces vague rescue-to-hypothesis stubs; packet candidates
  must carry traceable rescue provenance and obey current packet guardrails

## Rollout / Compatibility

- current rescue CLI commands remain supported
- current hidden-eval packaging remains the submitter-facing boundary
- any admitted model must be additive to the shipped baseline suite and explicitly
  declared in the admissions report
- a breaking change is any implementation that requires raw rescue inputs,
  reopens atlas fixture sources at runtime, or turns repo checkout into the
  submitter API

## Open Questions

- Should the primary output of `SCZ-Rescue-1` remain a gene ranking in v1, with
  intervention-object translation downstream, or should later versions promote
  intervention objects into the rescue task itself?
- What is the smallest non-baseline graph/context model worth admitting as the
  first serious challenger to the shipped baseline pack?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run python -m scz_target_engine.cli rescue run glutamatergic-convergence --output-dir .context/glutamatergic-convergence-run
uv run scz-target-engine hidden-eval-task-package --task-id glutamatergic_convergence_rescue_task --output-dir .context/glutamatergic_hidden_eval_task
uv run scz-target-engine hidden-eval-pack-submission --task-package-dir .context/glutamatergic_hidden_eval_task --predictions-file .context/glutamatergic-convergence-run/ranked_predictions.csv --submitter-id internal-baseline --submission-id glutamatergic-rescue-baseline-v1 --scorer-id convergence_state_baseline_v1 --output-file .context/glutamatergic_rescue_baseline_submission.tar.gz
uv run scz-target-engine hidden-eval-simulate --task-package-dir .context/glutamatergic_hidden_eval_task --submission-file .context/glutamatergic_rescue_baseline_submission.tar.gz --output-dir .context/glutamatergic_hidden_eval_run
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json
```
