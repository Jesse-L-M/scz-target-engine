# replay-track-b-v1

Status: active
Owner branch: Jesse-L-M/replay-track-b
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/program-memory-denominator-v1.md, docs/designs/replay-track-a-v1.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-05

## Objective

Turn the current program-memory replay explanation layer into a benchmarked
failure-memory reasoning track.

This spec covers Track B only:

- analog retrieval
- failure-scope classification
- "what must differ?" reasoning

The point is not to produce nicer prose. The point is to test whether the repo can
retrieve the right historical analogs, classify the right failure scope, and make
the right conditional distinction between "already failed" and "worth trying if X
is truly different."

## Success Condition

- Primary success metric:
  macro-average of:
  top-3 analog recall, failure-scope macro-F1, and "what must differ?" checklist
  F1 on a predeclared case set
- Secondary success metric:
  exact-match rate for `replay_supported | replay_not_supported |
  replay_inconclusive | insufficient_history`
- Stop/go gate:
  do not treat failure memory as a core product differentiator unless this track
  materially beats simple retrieval baselines on the principal slice with bootstrap
  CIs and explicit error review

## Scope

- benchmark case set for historical failure-memory reasoning
- gold analog sets and gold failure-scope labels
- explicit "what must differ?" checklist targets per case
- baseline suite for analog retrieval and structural reasoning
- reporting and casebook outputs for error analysis

## Not in Scope

- intervention/program prioritization metrics from Track A
- rescue model training
- packet schema changes
- atlas feature expansion
- open-ended LLM essay grading

## Existing Surfaces To Reuse

- `assess_counterfactual_replay_risk(...)` and the current replay API:
  keep the structural, inspectable explanation posture instead of replacing it
  with free text
- checked-in `assets.csv`, `events.csv`, and `event_provenance.csv`:
  use the current v2 program-memory tables as the substrate for analogs
- current coverage-audit outputs:
  reuse explicit sparse-history and curation-gap signals when labeling cases
- Track A snapshot and archive machinery:
  reuse the same cutoff and leakage rules instead of defining a parallel replay path
- current benchmark runner and reporting pattern:
  extend the existing benchmark/report-card flow with Track B case review outputs

## Resolved Decisions In V1

- Track B ships as a second task row, `scz_failure_memory_track_b_task`, inside
  the existing `scz_translational_suite`
- Track B keeps the existing benchmark artifact families and command sequence;
  v1 adds Track B-specific sidecars rather than a parallel benchmark stack
- The checked-in principal Track B fixture is
  `data/benchmark/fixtures/scz_failure_memory_2025_02_01/`
- The Track B fixture pins `track_b_casebook.csv`, `program_universe.csv`, and
  `events.csv` beside `source_archives.json` so replay stays cutoff-local and
  does not read repo-head history tables implicitly

## Inputs

- checked-in program-memory v2 tables
- denominator and coverage-state outputs from `program-memory-denominator-v1`
- historical snapshot requests and archive indices from Track A
- curated benchmark case set with:
  intervention proposal, gold analog event ids, gold failure scope, gold replay
  status, and gold "what must differ?" checklist items
- baseline retrieval and classification implementations

## Outputs And Artifact Contracts

- New or changed artifact:
  `track_b_casebook.csv`
  with one benchmark case per row and frozen gold labels
- New or changed artifact:
  runner sidecars under benchmark generated outputs:
  `runner_outputs/track_b_case_outputs/<run_id>.json` and
  `runner_outputs/track_b_confusion_summaries/<run_id>.json`
- New or changed artifact:
  derived case-review payloads under benchmark reporting outputs:
  per-run JSON or Markdown summaries of misses, confusions, and analog disagreements
- New or changed artifact:
  benchmark metric payloads and CI payloads for:
  `analog_recall_at_3`, `failure_scope_macro_f1`,
  `what_must_differ_checklist_f1`, and `replay_status_exact_match`
- Backward-compatibility rule:
  use the current benchmark artifact families and derived reporting outputs rather
  than introducing a second benchmark artifact stack in v1

### Proposed Gold Label Surface Per Case

Each Track B case should freeze:

- `case_id`
- `snapshot_id`
- `proposal_payload`
- `gold_analog_event_ids_json`
- `gold_failure_scope`
- `gold_replay_status`
- `gold_required_differences_json`
- `coverage_state_at_cutoff`
- `notes`

Allowed `gold_failure_scope` values should stay aligned with the current structural
ledger semantics:

- `target_class`
- `molecule`
- `endpoint`
- `population`
- `target`
- `unresolved`
- `nonfailure`

## Data Flow

```text
PROGRAM MEMORY V2 + COVERAGE AUDIT + SNAPSHOT CUTS
    -> TRACK B CASEBOOK
    -> BASELINE / MODEL REASONING RUNS
    -> ANALOG / FAILURE-SCOPE / DIFFERENCE OUTPUTS
    -> METRICS + CIs
    -> ERROR CASEBOOK + CASE REVIEWS
```

- Program-memory tables provide the analog substrate.
- Coverage state at the historical cutoff keeps sparse-history vs unresolved-history
  distinctions explicit.
- Each run emits structural reasoning outputs, not just scores.
- Reporting turns misses into case reviews so the repo learns where its historical
  memory is weak or overconfident.

## Implementation Plan

1. Define the Track B casebook schema and curate an initial historical case set.
2. Add baseline implementations:
   exact-target retrieval, target-class retrieval, and naive nearest-history rules.
3. Materialize structural reasoning outputs for each run:
   retrieved analogs, predicted failure scope, predicted replay status, and predicted
   required differences.
4. Score those outputs against the frozen casebook.
5. Emit per-run error analysis and a cross-run confusion summary.

## Acceptance Tests

- Unit:
  add tests for casebook validation, failure-scope label validation, and scoring of
  analog recall / checklist F1
- Integration:
  run one Track B slice end to end from frozen snapshot to case-review output
- Regression:
  add a test that fails if `unresolved_failure_scope` cases are silently coerced
  into a stronger gold or predicted failure label
- E2E, if relevant:
  load program-memory substrate -> run structural replay reasoning -> emit metrics,
  CI payloads, and case reviews for one historical slice

## Failure Modes

- Failure mode:
  an analog retrieval method looks good only because the casebook includes future
  evidence artifacts; Track B must inherit Track A snapshot leakage rules
- Failure mode:
  unresolved history is collapsed into confident class failure; scoring and case
  review outputs must preserve the `unresolved` label
- Failure mode:
  "what must differ?" becomes vague natural-language drift; the benchmark must score
  against a frozen checklist or tagged reason set, not a subjective paragraph

## Rollout / Compatibility

- additive to the current benchmark stack
- does not change `v0`, `v1`, or current packet semantics
- a breaking change is any Track B implementation that stops exposing structural
  analog and failure-scope outputs and hides everything behind a scalar score

## Open Questions

- Should Track B live under the current benchmark suite as a second task row, or as
  a second track within one expanded suite/task definition?
- Should the principal metric weight the three sub-metrics equally, or should
  failure-scope F1 carry more weight than analog recall?

Current v1 answers:

- Track B now lives as a second task row in the current suite
- v1 emits the four structural metrics separately and leaves the final stop/go
  weighting decision to the post-PR4 review

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`
- `docs/decisions/0003-track-b-benchmark-task.md`

## Commands

```bash
uv run scz-target-engine program-memory coverage-audit --dataset-dir data/curated/program_history/v2 --output-dir .context/program_memory/coverage --focus-target CHRM4
uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json --materialized-at 2026-04-05
uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/future_outcomes.csv --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv
uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs --config config/v0.toml --deterministic-test-mode
uv run scz-target-engine build-benchmark-reporting --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv --runner-output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads
```
