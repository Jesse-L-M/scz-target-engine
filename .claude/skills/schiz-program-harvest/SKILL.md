---
name: schiz-program-harvest
description: |
  Draft workflow for harvesting one schizophrenia or directly adjacent intervention
  program into a structured review bundle. Use when adding or reviewing a specific
  program and you need a review-only harvest dossier with exact `program_memory_v3`
  artifacts, source manifests, structured observations, and contradiction logging
  before adjudication.
---

# Schiz Program Harvest

Read this first:

- `docs/designs/program-memory-v3-resolver.md`
- `docs/designs/program-memory-v3.md`
- `docs/program_history.md`
- `docs/claim.md`

This is a parameterized procedure for one program at a time.

The job is not "summarize the program." The job is to populate the exact registered
harvest bundle without jumping early to interpretation.

## Inputs

- `PROGRAM_ID` required
- `PROGRAM_LABEL` optional
- `CORPUS_TIER` optional
- `SOURCE_URLS[]` optional seed set
- `OUTPUT_DIR=.context/program_memory/reviews/{PROGRAM_ID}/harvest/`
- `WRITE_POLICY=review_only`

## Output Directory

Write draft review artifacts under:

`.context/program_memory/reviews/{program_id}/harvest/`

Required files:

- `source_manifest.json`
- `study_index.csv`
- `result_observations.csv`
- `harm_observations.csv`
- `contradictions.csv`

Bootstrap the bundle first:

```bash
uv run scz-target-engine program-memory harvest-program \
  --program-id "$PROGRAM_ID" \
  --output-dir "$OUTPUT_DIR"
```

## Workflow

1. Resolve identity first.
   Map aliases, sponsor codes, brand names, molecule names, and combination names.
   If identity is unresolved, fail closed and log it in `contradictions.csv` or
   `source_manifest.json.unresolved_questions`.
2. Capture sources.
   Prefer registry current record, registry history, primary-results paper,
   regulator documents, and sponsor materials in that order.
3. Populate `source_manifest.json`.
   Preserve source tier, locator, and extraction state. Use
   `unresolved_questions` for anything still blocked.
4. Populate `study_index.csv`, `result_observations.csv`, and
   `harm_observations.csv`.
   Keep rows source-aligned and use `notes` fields for qualifiers that do not yet
   have first-class columns.
5. Populate `contradictions.csv`.
   Registry-paper, paper-press, and source-version mismatches must stay explicit.
6. Stop before interpretation.
   Do not write mechanism conclusions, belief updates, or adjudicated claims here.

## Guardrails

- Do not create sidecar dossier files outside the current registered contract.
- Do not silently choose one source when two sources conflict.
- Do not infer efficacy from a safety extension.
- Do not infer target failure when exposure adequacy is unclear.
- Do not write outside `.context/program_memory/reviews/` from this skill.

## Done Condition

The harvest is done when a reviewer can inspect one folder and answer:

- what sources exist
- what was extracted at study, result, and harm level
- where the contradictions are
- what still needs adjudication
