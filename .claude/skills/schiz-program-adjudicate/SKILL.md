---
name: schiz-program-adjudicate
description: |
  Draft workflow for adjudicating one harvested schizophrenia or directly adjacent
  intervention program into accepted claims, caveats, contradictions, and belief
  updates. Use when a harvest bundle already exists and you need the exact
  `program_memory_v3` adjudicated dossier rather than another raw extraction pass.
---

# Schiz Program Adjudicate

Read this first:

- `docs/designs/program-memory-v3-resolver.md`
- `docs/designs/program-memory-v3.md`
- `docs/program_history.md`
- `.context/program_memory/reviews/{program_id}/harvest/`

This skill starts after harvest.

The job is to decide what is accepted, disputed, weak, or unresolved, then convert
that into a reusable program dossier and belief updates.

## Inputs

- `HARVEST_DIR` required
- `ADJUDICATION_ID` required
- `REVIEWER` required
- `OUTPUT_DIR=.context/program_memory/reviews/{program_id}/adjudicated/`
- `WRITE_POLICY=review_only`

Read the harvest bundle from `HARVEST_DIR` with these minimum files:

- `source_manifest.json`
- `study_index.csv`
- `result_observations.csv`
- `harm_observations.csv`
- `contradictions.csv`

## Output Directory

Write adjudicated artifacts under:

`.context/program_memory/reviews/{program_id}/adjudicated/`

Required files:

- `claims.csv`
- `caveats.csv`
- `belief_updates.csv`
- `contradictions.csv`
- `program_card.json`

Bootstrap the bundle first:

```bash
uv run scz-target-engine program-memory adjudicate-program \
  --harvest-dir "$HARVEST_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --adjudication-id "$ADJUDICATION_ID" \
  --reviewer "$REVIEWER"
```

## Workflow

1. Review source hierarchy.
   Regulatory and primary-results sources usually outrank press releases, but do not
   override contradictions silently.
2. Treat `claims.csv` as the single adjudicated claim ledger.
   Use `adjudication_status` inside the ledger instead of splitting accepted and
   rejected claims into separate files.
3. Score evidence quality separately.
   Keep `extraction_confidence`, `source_reliability`, `risk_of_bias`,
   `reporting_integrity_risk`, `transportability_confidence`, and
   `interpretation_confidence` separate wherever the current columns permit, and use
   `notes` plus `caveats.csv` until dedicated fields land.
4. Resolve or preserve contradictions.
   If a conflict cannot be resolved defensibly, keep it explicit in
   `contradictions.csv` and surface it in `caveats.csv` or `program_card.json`.
5. Write belief updates at the right scope.
   Separate `molecule`, `mechanism`, `target`, `population`, `endpoint`, and
   `design_lesson` claims.
6. Generate the compact program card from accepted structures, not from freeform
   narrative memory.

## Guardrails

- Do not over-promote weak evidence into strong mechanism claims.
- Do not erase unresolved contradictions just to make the dossier clean.
- Do not treat subgroup findings as definitive unless the evidence is strong and
  pre-specified.
- Do not generalize from one population slice to all schizophrenia populations.
- Do not write outside `.context/program_memory/reviews/` from this skill.

## Done Condition

The adjudication is done when a reviewer can inspect one folder and answer:

- what happened in the program
- how trustworthy the evidence is
- what caveats remain
- what belief should update, and at what scope
- what still blocks landing into an authoritative corpus
