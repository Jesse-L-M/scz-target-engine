# program-memory-denominator-v1

Status: implemented
Owner branch: Jesse-L-M/real-scz-denom
Depends on: docs/designs/contracts-and-compat-v2.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-08

## Objective

Make schizophrenia program-memory coverage explicit instead of implied, and move the
checked-in denominator from a fixture-scale example to the real approved plus near-
exhaustive phase 2/3 molecular-program boundary that replay will later inherit.

The checked-in event tables under `data/curated/program_history/v2/` remain the
authoritative substrate for included schizophrenia program history, but coverage now
flows through a checked-in denominator artifact:
`data/curated/program_history/v2/program_universe.csv`.

## Success Condition

- Every tracked schizophrenia molecular program opportunity in the denominator has an
  explicit `coverage_state` and `coverage_reason`.
- Included denominator rows map to one or more checked-in `events.csv` rows.
- Approved schizophrenia molecular programs are effectively complete and explicitly
  represented in the denominator.
- Phase 2/3 schizophrenia molecular programs are near-exhaustive, with unresolved,
  duplicate, excluded, and out-of-scope rows listed explicitly rather than omitted.
- The coverage audit emits deterministic manifest, summary, and gap artifacts from
  the denominator plus the included-event substrate.

## Scope

- materialize `program_universe.csv` at program-opportunity grain
- expand the checked-in denominator to the real schizophrenia molecular-program
  release instead of a state-machine example set
- define and validate allowed `included`, `unresolved`, `excluded`, `duplicate`, and
  `out_of_scope` states plus reasons
- tighten asset and target-class lineage IDs plus alias handling
- integrate ClinicalTrials.gov as discovery and provenance support for denominator
  rows
- use direct regulatory, peer-reviewed, and company sources where available so
  included and unresolved rows carry reviewable provenance
- keep `program_history/v2` authoritative for included events while denominator
  accounting stays additive

## Not In Scope

- changing `v0` or `v1` scoring semantics
- replacing human adjudication with full automation
- expanding beyond schizophrenia
- rescue-model training or replay benchmarking

## Inputs

- checked-in `assets.csv`, `events.csv`, `event_provenance.csv`, and
  `directionality_hypotheses.csv`
- checked-in `program_universe.csv`
- direct-source provenance URLs already used by included event rows
- ClinicalTrials.gov study references used as discovery or provenance support for
  denominator rows

## Implemented Artifact Contracts

### `program_universe.csv`

One row equals one program opportunity keyed by:

```text
asset_lineage_id / target_class_lineage_id / modality / domain / population / regimen / stage_bucket
```

`domain` is part of the implemented grain so acute, cognition, negative-symptom, and
TRS opportunities do not silently collapse into one row for the same asset lineage.

The checked-in columns are:

- `program_universe_id`
- `asset_id`
- `asset_name`
- `asset_lineage_id`
- `asset_aliases_json`
- `target`
- `target_symbols_json`
- `target_class`
- `target_class_lineage_id`
- `target_class_aliases_json`
- `mechanism`
- `modality`
- `domain`
- `population`
- `regimen`
- `stage_bucket`
- `coverage_state`
- `coverage_reason`
- `coverage_confidence`
- `mapped_event_ids_json`
- `duplicate_of_program_universe_id`
- `discovery_source_type`
- `discovery_source_id`
- `source_candidate_url`
- `notes`

Allowed `coverage_state` values:

- `included`
- `unresolved`
- `excluded`
- `duplicate`
- `out_of_scope`

Allowed `coverage_reason` values by state:

- `included`:
  `checked_in_event_history`
- `unresolved`:
  `ctgov_candidate_pending_adjudication`,
  `needs_direct_source_recovery`,
  `needs_alias_resolution`
- `excluded`:
  `follow_on_supporting_study`,
  `insufficient_interventional_signal`
- `duplicate`:
  `asset_alias_duplicate`,
  `registry_alias_duplicate`
- `out_of_scope`:
  `non_schizophrenia_indication`,
  `non_molecular_intervention`

Validation rules:

- included rows must map to checked-in `event_id` values
- included rows must preserve the checked-in asset and target identity surface:
  `asset_id`, `asset_name`, `target`, `target_symbols_json`, `target_class`, and
  the existing lineage/grain fields must still match the mapped checked-in events
- duplicate rows must point to one canonical `duplicate_of_program_universe_id`
- non-duplicate rows may not silently collide on the implemented program-opportunity
  grain
- `discovery_source_type` fails closed to the implemented source vocabulary instead of
  bypassing registry validation on typos
- ClinicalTrials.gov-backed rows must use canonical `NCT...` identifiers plus canonical
  `https://clinicaltrials.gov/study/NCT...` URLs

### Coverage Audit Outputs

`uv run scz-target-engine program-memory coverage-audit` now writes:

- `coverage_manifest.json`:
  deterministic denominator metadata, state counts, reason counts, source-cut rules,
  allowed vocabularies, absolute-path `dataset_dir` compatibility surface, and the core in-scope program
  count excluding duplicates, excluded follow-ons, and out-of-scope rows
- `coverage_summary.csv`:
  legacy scope summaries aggregated by `target`, `target_class`, `domain`, and
  `failure_scope`
- `coverage_gaps.csv`:
  legacy scope gaps used by curation tooling and focused review
- `coverage_denominator_summary.csv`:
  denominator summaries aggregated by `stage_bucket`, `modality`, `domain`,
  `coverage_state`, and `coverage_reason`
- `coverage_denominator_gaps.csv`:
  only non-included denominator rows with lineage fields, provenance support, and
  duplicate targets

Additive compatibility outputs are still emitted:

- `coverage_audit.json`
- `coverage_scope_summary.csv`
- `coverage_scope_gaps.csv`
- `coverage_evidence.csv`
- optional `coverage_focus.json`

## Data Flow

```text
DIRECT SOURCES + CT.GOV + CHECKED-IN V2 EVENTS
    -> PROGRAM_UNIVERSE ROWS
    -> HUMAN ADJUDICATION / LANDING INTO V2 EVENTS
    -> COVERAGE MANIFEST / SUMMARY / GAPS
```

ClinicalTrials.gov can justify why a denominator row exists, but it does not replace
the adjudicated included-event tables.

## Implementation Reality

- the checked-in denominator is no longer a 14-row fixture; it now contains 59 rows:
  31 `included`, 15 `unresolved`, 11 `duplicate`, 1 `excluded`, and 1
  `out_of_scope`
- the checked-in release is effectively complete for approved schizophrenia molecular
  programs and near-exhaustive for phase 2/3 schizophrenia molecular programs, with
  unresolved rows kept explicit instead of silently omitted
- every `included` denominator row maps to checked-in event history; the current
  release carries 32 mapped event rows across the 31 included denominator rows
- unresolved rows are partitioned explicitly by reason: 6
  `ctgov_candidate_pending_adjudication` rows and 9 `needs_direct_source_recovery`
  rows
- direct-source provenance for the expanded denominator now spans regulatory approval
  records, peer-reviewed primary results, company press releases, and
  ClinicalTrials.gov candidate studies
- `assets.csv` now carries `asset_lineage_id`, `asset_aliases_json`,
  `target_class_lineage_id`, and `target_class_aliases_json`
- `coverage_evidence.csv` now carries lineage IDs for included-event evidence rows
- alias rows such as `KarXT`, `SEP-363856`, `BI 425809`, `MIN-101`, `RP5063`,
  `LY2140023`, `RO4917838`, and `ACP-103` are explicit denominator duplicates instead
  of silently doubling coverage
- the checked-in denominator now points unresolved late-stage candidates such as
  brilaroxazine, roluperidone, evenamide, NBI-1117568, LB-102, encenicline, and
  valbenazine at explicit ClinicalTrials.gov or direct-source provenance surfaces
- approved anchors now include first-generation, second-generation, and newer
  schizophrenia approvals such as chlorpromazine, haloperidol, clozapine,
  risperidone, olanzapine, quetiapine, aripiprazole, lumateperone, Lybalvi, and
  Cobenfy through checked-in event history plus regulatory provenance
- denominator rows are explicit under
  `coverage_denominator_summary.csv` / `coverage_denominator_gaps.csv`, while the
  legacy `coverage_summary.csv` / `coverage_gaps.csv` scope surfaces remain
  materializable for additive compatibility
- adjudication proposal slices now carry a checked-in dataset-contract marker so
  renamed or reloaded `proposed_v2` directories keep their optional-denominator
  behavior without basename heuristics
- legacy pre-contract 4-file v2 proposal slices still load as scope-only datasets by
  schema fallback, while denominator-aware checked-in-style v2 surfaces fail closed if
  `program_universe.csv` is missing
- the current checked-in denominator release keeps the full state machine in-repo
  without pretending unresolved late-stage rows are already adjudicated event history
- machine-readable coverage-audit outputs keep the pre-hotfix absolute-path
  `dataset_dir` contract instead of silently switching existing fields to repo-relative

## Acceptance Tests

```bash
uv run --group dev pytest
uv run --group dev pytest tests/test_program_memory_coverage.py tests/test_program_memory_adjudication.py tests/test_program_memory_harvest.py tests/test_curation_assistant.py
uv run scz-target-engine program-memory coverage-audit --output-dir .context/program_memory_coverage_audit
./scripts/run_contract_smoke_path.sh
```

## Failure Modes

- a canonical row and an alias row share the same program-opportunity grain without
  the alias being marked `duplicate`
- an included denominator row loses its checked-in event mapping
- a ClinicalTrials.gov-backed candidate is treated as included without adjudicated
  checked-in events
- a follow-on or non-schizophrenia study quietly inflates completeness claims

## Rollout Notes

- additive only: `program_history/v2` stays authoritative for included events
- compatibility views under `data/curated/program_history/` remain materializable
- the PR1 frozen smoke path must stay green
- preserve machine-readable `dataset_dir` compatibility so downstream replay work does
  not build on a silently changed path surface
- replay must inherit the checked-in denominator boundary and explicit non-included
  row accounting instead of rebuilding proposal universes ad hoc from later slice
  generation logic

## Commands

```bash
uv run scz-target-engine program-memory harvest --input-file .context/program_memory/raw_harvest.json --output-file .context/program_memory/harvest.json --harvest-id example-curation --harvester llm-assist --created-at 2026-03-30 --review-file .context/program_memory/review_queue.csv
uv run scz-target-engine program-memory adjudicate --harvest-file .context/program_memory/harvest.json --decisions-file .context/program_memory/decisions.json --output-dir .context/program_memory/adjudicated --adjudication-id example-curation-review --reviewer curator@example.com --reviewed-at 2026-03-30
uv run scz-target-engine program-memory coverage-audit --output-dir .context/program_memory_coverage_audit
uv run --group dev pytest tests/test_program_memory_coverage.py tests/test_program_memory_adjudication.py tests/test_program_memory_harvest.py tests/test_curation_assistant.py
```
