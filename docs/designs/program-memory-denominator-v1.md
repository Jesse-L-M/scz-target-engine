# program-memory-denominator-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/contracts-and-compat-v2.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-01

## Objective

Turn program memory from a curation-scale substrate into a measurable coverage
system with an explicit denominator.

The key change is that coverage will no longer be implied by a growing event table.
It will be measured by a checked-in `program_universe.csv` that says, row by row,
what is included, excluded, unresolved, duplicate, or out of scope.

## Success Condition

- Primary success metric:
  every in-scope schizophrenia molecular program in the denominator has an
  explicit coverage state and reason
- Secondary success metric:
  the coverage release emits deterministic summary and gap artifacts without
  mutating existing ledgers or scoring outputs
- Stop/go gate:
  do not claim approved-program completeness or phase 2/3 coverage percentages
  until the denominator artifact and unresolved-row accounting are shipped

## Scope

- define the row grain and schema for `program_universe.csv`
- define allowed coverage states and reasons
- add lineage IDs for assets and target classes where missing
- add ClinicalTrials.gov ingestion for source discovery and evidence support
- emit coverage manifest, summary, and gaps artifacts from the denominator plus
  adjudicated `program_history/v2` tables
- keep the current v2 event substrate as the authoritative included-event table

## Not in Scope

- changing `v0` or `v1` numeric scoring
- replacing human adjudication with full automation
- expanding beyond schizophrenia
- rescue model training
- historical replay metrics

## Existing Surfaces To Reuse

- `data/curated/program_history/v2/`:
  keep `assets.csv`, `events.csv`, `event_provenance.csv`, and
  `directionality_hypotheses.csv` as the included source of truth
- harvest / adjudication workflow:
  use existing suggestion, review, and proposal machinery instead of inventing a
  new curation lane
- `program-memory coverage-audit` path:
  extend the existing audit surface to emit denominator-driven accounting
- replay explanation layer:
  preserve the current analog / failure-scope API semantics

## Inputs

- checked-in v2 program-memory tables
- current legacy compatibility views under `data/curated/program_history/`
- direct sources and curated provenance URLs
- ClinicalTrials.gov records used as trial-registry support or gap-discovery input
- ontology and failure-taxonomy docs

## Outputs And Artifact Contracts

- New or changed artifact:
  `program_universe.csv`
  with one row per program opportunity, not one row per event
- New or changed artifact:
  `coverage_manifest.json`
  with row counts, inclusion rules, timestamp, and source cut rules
- New or changed artifact:
  `coverage_summary.csv`
  aggregated by phase, modality, status, and coverage state
- New or changed artifact:
  `coverage_gaps.csv`
  only unresolved, excluded, duplicate, or weak-source rows needing attention
- New or changed artifact:
  additive lineage fields in included-event outputs where missing
- Backward-compatibility rule:
  `program_history/v2` remains the authoritative included-event substrate and
  legacy compatibility views remain materializable

### Proposed `program_universe.csv` Row Grain

One row equals one schizophrenia molecular program opportunity:

```text
asset / target_class / modality / population / regimen / stage_opportunity
```

This is intentionally program-level, not event-level.
Multiple event rows may map to one denominator row.

### Proposed Minimum Columns

- `program_universe_id`
- `asset_name`
- `asset_lineage_id`
- `target_class`
- `target_class_lineage_id`
- `mechanism`
- `modality`
- `population`
- `regimen`
- `stage_bucket`
- `coverage_state`
- `coverage_reason`
- `source_candidate_url`
- `supporting_event_count`
- `confidence`
- `notes`

Allowed `coverage_state` values:

- `included`
- `unresolved`
- `excluded`
- `duplicate`
- `out_of_scope`

## Data Flow

```text
DIRECT SOURCES + CT.GOV + EXISTING V2 TABLES
    -> PROGRAM CANDIDATE DISCOVERY
    -> PROGRAM_UNIVERSE ROWS
    -> CURATOR REVIEW / ADJUDICATION
    -> INCLUDED V2 EVENTS + LINEAGE IDS
    -> COVERAGE MANIFEST / SUMMARY / GAPS
    -> PROGRAM-MEMORY RELEASE
```

- Direct sources and CT.gov expose candidate programs.
- Candidate programs become denominator rows first.
- Curators decide whether a row is included, unresolved, excluded, duplicate, or
  out of scope.
- Included rows must map to one or more checked-in `events.csv` rows.
- Coverage artifacts are emitted from the denominator plus the included-event
  tables.

## Implementation Plan

1. Define the denominator row grain, columns, and state machine.
2. Materialize an initial `program_universe.csv` from the current checked-in
   program-memory surface plus CT.gov discovery candidates.
3. Extend the coverage-audit path to emit manifest, summary, and gaps outputs.
4. Add lineage tightening and alias resolution for assets and target classes.
5. Integrate CT.gov as a source adapter feeding discovery and provenance support,
   not as a direct substitute for adjudicated program rows.

## Acceptance Tests

- Unit:
  add schema and state-validation tests for `program_universe.csv` and the
  coverage manifest in `tests/test_program_memory_coverage.py`
- Integration:
  run the program-memory coverage-audit command against the checked-in v2 tables
  and verify deterministic summary and gap outputs
- Regression:
  add a test that fails if a denominator row can disappear from summary counts
  without being marked `duplicate` or `out_of_scope`
- E2E, if relevant:
  harvest candidate -> adjudicate proposal -> land included row -> rerun coverage
  audit and observe the row move from `unresolved` to `included`

## Failure Modes

- Failure mode:
  a CT.gov trial and a curated direct source create two denominator rows for the
  same underlying program; alias resolution and duplicate states must catch this
- Failure mode:
  a weak registry source is treated as sufficient proof of a failure mechanism;
  the row must remain `unresolved` or low-confidence rather than fabricating a
  stronger claim
- Failure mode:
  included events and denominator rows drift apart; the coverage audit must fail
  if an `included` denominator row has no mapped checked-in event

## Rollout / Compatibility

- additive only:
  current `program_history/v2` tables stay authoritative for included events
- legacy `programs.csv` compatibility views remain consumer-facing during this
  phase
- a breaking change is any PR that replaces the event tables with denominator rows
  or changes existing event semantics without a compatibility projection

## Open Questions

- Should a fixed-combination asset get one denominator row or multiple rows when
  mechanism and regimen both matter for later replay?
- Should weak-source but likely-real programs count against completeness
  thresholds, or only against unresolved counts?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run scz-target-engine program-memory harvest --input-file .context/program_memory/raw_harvest.json --output-file .context/program_memory/harvest.json --harvest-id example-curation --harvester llm-assist --created-at 2026-03-30 --review-file .context/program_memory/review_queue.csv
uv run scz-target-engine program-memory adjudicate --harvest-file .context/program_memory/harvest.json --decisions-file .context/program_memory/decisions.json --output-dir .context/program_memory/adjudicated --adjudication-id example-curation-review --reviewer curator@example.com --reviewed-at 2026-03-30
uv run scz-target-engine program-memory coverage-audit --output-dir .context/program_memory_coverage_audit
```
