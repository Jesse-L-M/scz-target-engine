# Program History

`program_history` is the curated substrate for landmark schizophrenia program events that later ranking, warning overlays, and domain-specific reasoning can consume. This dataset is intentionally checked in before any scoring integration.

## Current Boundary

- This directory records externally documented program events.
- It does not yet change numeric ranking.
- It does not yet adjudicate a full historical ledger for every target class.
- It does not yet implement domain heads.

## Row Granularity

One row equals one dated, externally backed program event.

Examples:

- an FDA approval
- a phase 2 top-line readout
- a phase 3 top-line miss
- a peer-reviewed publication of primary results

If a single asset has both a positive phase 2 signal and a later phase 3 miss, record separate rows rather than collapsing them.

## Schema

`data/curated/program_history/programs.csv` currently uses the following columns:

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
5. Add a concise `notes` entry whenever the taxonomy assignment is interpretive.

That workflow keeps the checked-in data stable enough for later code without pretending the repo already has a complete historical adjudication layer.
