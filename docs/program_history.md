# Program History

`program_history` is the curated substrate for landmark schizophrenia program events that later ranking, warning overlays, and domain-specific reasoning can consume. This dataset is intentionally checked in before any scoring integration.

`PR7` now consumes this substrate into scoring-neutral target ledgers. `PR8.1` then uses
those ledger fields inside numeric `v1` gene heads while leaving shared `v0` numeric
outputs unchanged.

## Current Boundary

- This directory records externally documented program events.
- It does not itself change shared `v0` numeric ranking.
- It now feeds target-ledger structure and downstream `v1` gene heads.
- It still does not adjudicate a full event ledger for every schizophrenia program.
- Coverage is now measured explicitly through a checked-in denominator instead of being
  implied by the event table alone.

## v2 Source Of Truth

`data/curated/program_history/v2/` is now the normalized source of truth for
checked-in program memory.

The normalized tables are:

- `assets.csv`: stable asset-level identity via `asset_id`, `molecule`, `target`,
  `target_symbols_json`, `target_class`, `mechanism`, `modality`,
  `asset_lineage_id`, `asset_aliases_json`, `target_class_lineage_id`, and
  `target_class_aliases_json`
- `events.csv`: dated program events via `event_id`, `asset_id`, sponsor and clinical
  context fields, normalized outcome fields, curator confidence, and `sort_order`
- `event_provenance.csv`: event-level provenance keyed by `event_id`
- `directionality_hypotheses.csv`: target-level directionality records keyed by
  `hypothesis_id`, with `supporting_event_ids_json` instead of legacy
  `supporting_program_ids_json`
- `program_universe.csv`: explicit denominator rows at program-opportunity grain with
  coverage states, reasons, alias handling, and discovery provenance support

The legacy `programs.csv` and `directionality_hypotheses.csv` files remain checked in
as compatibility views for current ledger consumers. `event_id` in v2 maps directly to
the legacy `program_id`, so existing rows remain traceable back to the checked-in
curation and its provenance. The denominator is additive; it does not replace the
legacy compatibility view.

## Row Granularity

One row equals one dated, externally backed program event.

Examples:

- an FDA approval
- a phase 2 top-line readout
- a phase 3 top-line miss
- a peer-reviewed publication of primary results

If a single asset has both a positive phase 2 signal and a later phase 3 miss, record separate rows rather than collapsing them.

In v2, one legacy compatibility row corresponds to:

- one `assets.csv` row
- one `events.csv` row
- one `event_provenance.csv` row

## Denominator Row Granularity

`program_universe.csv` is intentionally not event-granular.

One row equals one program opportunity keyed by:

```text
asset_lineage_id / target_class_lineage_id / modality / domain / population / regimen / stage_bucket
```

Multiple event rows can map to one denominator row when they belong to the same
opportunity path. The checked-in `pimavanserin` negative-symptom row is the current
example: one denominator row maps to both the earlier phase 2 signal and the later
pivotal miss.

## Legacy Compatibility View

`data/curated/program_history/programs.csv` remains the current consumer-facing view and
still uses the following columns:

| Column | Meaning |
| --- | --- |
| `program_id` | Stable event identifier in kebab-case. Prefer `{molecule}-{program}-{event}-{year}`. |
| `sponsor` | Sponsor or sponsor collaboration at the time of the event. |
| `molecule` | Asset name or fixed-combination name. |
| `target` | Principal molecular target or target set. Use ` / ` when multiple targets are intrinsic to the mechanism. |
| `target_class` | Broad mechanistic grouping used for later class-level reasoning. Keep this at the family level that should cluster related assets together. Put finer pharmacology in `mechanism`. |
| `mechanism` | Short human-readable mechanism description. |
| `modality` | Normalized modality label such as `small_molecule` or `small_molecule_combination`. |
| `population` | Human-readable enrollment or labeled-use population. |
| `domain` | Canonical ontology slug from [ontology.md](ontology.md). |
| `mono_or_adjunct` | `monotherapy`, `adjunct`, or another explicit regimen label if truly needed. |
| `phase` | Program stage at the time of the event. Current seed rows use `approved`, `phase_2`, and `phase_3`. |
| `event_type` | Event kind such as `regulatory_approval` or `topline_readout`. |
| `date` | Event date in `YYYY-MM-DD`. |
| `primary_outcome_result` | Short normalized summary of the main outcome. |
| `failure_reason_taxonomy` | One best-fit label from [failure_taxonomy.md](../data/curated/program_history/failure_taxonomy.md). Use a nonfailure label when the event is positive. |
| `source_tier` | Provenance tier that signals how direct the source is. |
| `source_url` | Direct URL to the primary source for the event. |
| `confidence` | Curator confidence in the full row, especially any failure-taxonomy assignment. |
| `notes` | Short explanation for curation choices that would otherwise be ambiguous. |

## Source Tier Meanings

- `regulatory`: FDA or equivalent regulatory label, approval notice, or review artifact.
- `peer_reviewed_primary_results`: primary efficacy or safety results in a peer-reviewed paper.
- `company_press_release`: official company press release or investor/newsroom disclosure.
- `trial_registry`: ClinicalTrials.gov or equivalent registry entry used when no better source is available.
- `secondary_summary_last_resort`: use only temporarily when a direct source cannot yet be recovered.

Prefer the highest-directness tier available for the specific event being recorded.

## Confidence Meanings

- `high`: event facts and the row classification are directly supported with little interpretation.
- `medium`: event facts are clear, but domain mapping or failure-taxonomy assignment involves bounded interpretation.
- `low`: event is likely real enough to keep, but the row needs follow-up because classification is weak or the source is imperfect.

Confidence applies to the full curated row, not just to whether the event happened.

## Curation Standards

- Use the ontology slugs exactly as defined in [ontology.md](ontology.md).
- Use the failure-taxonomy labels exactly as defined in [failure_taxonomy.md](../data/curated/program_history/failure_taxonomy.md).
- Prefer regulatory, peer-reviewed, or official company sources over commentary.
- Normalize `target_class` at one abstraction level. If two assets should be compared as part of the same later class-baggage question, they should share the same `target_class` value.
- Keep `notes` short and factual. Use them to justify taxonomy calls, not to smuggle in ranking policy.
- Do not silently change a definition. If a new row needs a new ontology bucket or failure label, update the corresponding doc in the same change.
- Do not overstate mechanism invalidity. If a miss is real but the reason is unclear, use `unresolved` or a higher-uncertainty confidence instead of forcing a stronger claim.
- Keep source URLs in the CSV itself so later consumers do not have to reconstruct provenance from commit history.

## How To Add Rows

1. Confirm the event has a direct source URL worth preserving.
2. Choose the primary ontology bucket from [ontology.md](ontology.md).
3. Choose one best-fit failure-taxonomy label from [failure_taxonomy.md](../data/curated/program_history/failure_taxonomy.md).
4. Set `confidence` based on the whole row, not just the source.
5. Add or update the normalized v2 rows under `data/curated/program_history/v2/`.
6. Keep the legacy compatibility views materializable without semantic drift.
7. Add a concise `notes` entry whenever the taxonomy assignment is interpretive.

That workflow keeps the checked-in data stable enough for later code without pretending the repo already has a complete historical adjudication layer.

## Assisted Harvesting And Adjudication

The repository now supports an additive assisted-curation workflow on top of the v2
schema:

- `src/scz_target_engine/program_memory/extract.py` normalizes machine-generated
  candidate events or directionality hypotheses into structured suggestions that reuse
  the v2 entity shapes.
- `src/scz_target_engine/program_memory/harvest.py` stores those suggestions inside a
  durable harvest bundle with explicit source documents and a review queue.
- `src/scz_target_engine/program_memory/adjudication.py` records explicit human
  `accept`, `edit`, or `reject` decisions and materializes only the adjudicated rows
  into a separate `proposed_v2/` directory.

This workflow is intentionally non-authoritative:

- machine suggestions are not loaded by ledger or build paths
- missing adjudication decisions leave suggestions pending
- adjudication outputs write proposal tables outside the checked-in source-of-truth
- proposal tables carry a small dataset-contract marker so curation tooling preserves
  optional denominator behavior after reload or rename
- a curator still has to review and manually land any accepted or edited rows

### Provenance Model

- Every suggestion points to a harvested `source_document_id`.
- Event suggestions carry proposed `asset`, `event`, and `provenance` rows.
- Directionality suggestions carry a proposed `directionality_hypothesis` row.
- Adjudication records preserve who reviewed the suggestion, when they reviewed it,
  and whether they accepted, rejected, or edited the machine proposal.

### Example Curation Path

One focused smoke path uses an `emraclidine-event-suggestion` harvested from an
AbbVie press release.

1. Create a harvest bundle from raw source and suggestion JSON:

```bash
uv run scz-target-engine program-memory harvest \
  --input-file .context/program_memory/raw_harvest.json \
  --output-file .context/program_memory/harvest.json \
  --harvest-id example-curation \
  --harvester llm-assist \
  --created-at 2026-03-30 \
  --review-file .context/program_memory/review_queue.csv
```

2. Record an explicit curator edit decision:

```bash
uv run scz-target-engine program-memory adjudicate \
  --harvest-file .context/program_memory/harvest.json \
  --decisions-file .context/program_memory/decisions.json \
  --output-dir .context/program_memory/adjudicated \
  --adjudication-id example-curation-review \
  --reviewer curator@example.com \
  --reviewed-at 2026-03-30
```

In the tested example, the curator keeps the event but edits the proposed row from
machine `confidence=medium` to curator `confidence=low`. The resulting
`.context/program_memory/adjudicated/proposed_v2/events.csv` contains the edited row,
while the checked-in `data/curated/program_history/v2/` tables remain unchanged.

### What Remains Manual

- recovering better direct sources when a suggestion comes from a weak source tier
- deciding whether a machine-proposed taxonomy or confidence call is defensible
- merging adjudicated proposal rows into the checked-in v2 tables
- updating compatibility views or docs when a landed row changes the checked-in
  curation surface

## Migration Posture

- v2 normalization is additive and compatibility-first.
- Current ledger consumers still read the legacy row shape through a thin compatibility
  projection.
- No shared `v0` scoring semantics, `v1` decision-vector semantics, benchmark semantics,
  or ledger artifact semantics change as part of this normalization.
- The checked-in substrate remains curation-scale rather than a claim of exhaustive
  historical adjudication.

## Analogs And Replay Logic

`PR12` adds an API-only replay-explanation layer on top of the normalized v2
program-memory tables.

That layer:

- retrieves analogs from checked-in `assets.csv`, `events.csv`, and
  `event_provenance.csv` rather than reparsing ad hoc CSV rows
- returns explicit match reasons such as exact target, shared target class, shared
  domain, and contextual gaps rather than only an aggregate similarity score
- carries checked-in record references via `asset_id`, `event_id`, and direct source
  provenance so every explanation stays traceable to the repository substrate
- keeps uncertainty explicit through flags such as `unresolved_failure_scope`,
  `composite_mechanism_analog`, `mixed_history`, and `sparse_history`
- emits replay judgments as inspectable statuses such as `replay_supported`,
  `replay_not_supported`, `replay_inconclusive`, or `insufficient_history`

The replay API is intentionally structural and explainable. It does not change shared
`v0` ranking, current `v1` semantics, or the emitted ledger artifact contract.

## Example Explanation Path

Concrete checked-in example: a `CHRM4` acute-schizophrenia monotherapy proposal.

```python
from pathlib import Path

from scz_target_engine.program_memory import (
    InterventionProposal,
    assess_counterfactual_replay_risk,
)

assessment = assess_counterfactual_replay_risk(
    Path("data/curated/program_history/v2"),
    InterventionProposal(
        target_symbol="CHRM4",
        domain="acute_positive_symptoms",
        mono_or_adjunct="monotherapy",
    ),
)
```

Current checked-in interpretation:

- `emraclidine-empower-acute-scz-topline-2024` is a direct analog and an important
  caution signal, but it stays `unresolved` rather than being silently promoted into a
  defended class-failure claim
- `cobenfy-xanomeline-trospium-approval-us-2024` is a checked-in nonfailure anchor in
  the same muscarinic neighborhood, which is why the repository currently explains this
  as `replay_not_supported` rather than as a settled replay of prior failure
- the resulting counterfactual remains falsifiable because future aligned selective
  CHRM4 failures could still move the interpretation toward replay

## Coverage Audit And Gap Reports

`src/scz_target_engine/program_memory/coverage.py` now runs two additive audits over
the checked-in v2 substrate:

- denominator accounting driven by `program_universe.csv`
- scope summaries driven by the checked-in included-event and directionality tables

It writes these machine-readable artifact surfaces:

- `coverage_manifest.json`: deterministic denominator metadata, state counts, reason
  counts, and source-cut rules
- `coverage_summary.csv` and `coverage_gaps.csv`: legacy scope-level summaries and
  gaps kept stable for curation tooling and other existing consumers
- `coverage_denominator_summary.csv`: denominator summaries aggregated by
  `stage_bucket`, `modality`, `domain`, `coverage_state`, and `coverage_reason`
- `coverage_denominator_gaps.csv`: only non-included denominator rows that still need
  action or explicit explanation
- `coverage_audit.json`: combined JSON payload containing both denominator outputs and
  the preserved scope-level audit
- `coverage_scope_summary.csv`, `coverage_scope_gaps.csv`, and `coverage_evidence.csv`:
  the earlier target / target-class / domain / failure-scope surfaces, kept for
  curation tooling and focused review
- machine-readable `dataset_dir` fields in coverage-audit JSON, manifest, and CLI
  stdout remain absolute paths for compatibility with downstream consumers

For dataset loading:

- denominator-aware checked-in-style v2 datasets require `program_universe.csv`
- proposal slices can opt out with `program_memory_dataset_contract.json`
- legacy pre-contract 4-file v2 proposal slices still load as scope-only by schema
  fallback for backward compatibility

Denominator states distinguish:

- `included`: mapped to checked-in `event_id` rows
- `unresolved`: tracked in the denominator but not yet adjudicated into checked-in
  event history
- `excluded`: explicit candidate that should not count as a core program-opportunity
  row
- `duplicate`: explicit alias or registry duplicate of a canonical denominator row
- `out_of_scope`: explicit non-schizophrenia or non-molecular row discovered during
  denominator support work

Denominator validation also fails closed on discovery provenance:

- included rows must preserve checked-in asset and target display fields, not just
  lineage/grain identity
- `discovery_source_type` must stay inside the implemented source vocabulary
- ClinicalTrials.gov-backed rows must use canonical `NCT...` identifiers and canonical
  `https://clinicaltrials.gov/study/NCT...` URLs

Example focused audit path for the current `CHRM4` slice:

```bash
uv run scz-target-engine program-memory coverage-audit \
  --dataset-dir data/curated/program_history/v2 \
  --output-dir .context/program_memory/coverage \
  --focus-target CHRM4
```

That focused output should currently show:

- `CHRM4` target coverage as `partial`, not `strong`
- a provenance-backed nonfailure anchor through
  `cobenfy-xanomeline-trospium-approval-us-2024`
- an explicit `unresolved_failure_scope` gap from
  `emraclidine-empower-acute-scz-topline-2024`
- no silent promotion of the selective CHRM4 miss into a stronger failure claim
- a separate denominator manifest where included rows, unresolved candidates, explicit
  duplicates, and out-of-scope CT.gov discoveries are counted independently

## PR7 Structural Consumption

The target-ledger output still consumes the legacy-compatible `programs.csv` view to
populate:

- `failure_scope`: normalized structural scope such as `target_class`, `molecule`, `endpoint`, `population`, `target`, `unresolved`, or `nonfailure`
- `what_failed`: the object currently judged to have failed at that scope
- `where`: domain, population, phase, and regimen context for the event
- `evidence_strength`: a scoring-neutral strength label derived from curator confidence

See [ledger_contract.md](ledger_contract.md) for the emitted JSON artifact shape.
