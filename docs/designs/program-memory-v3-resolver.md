# program-memory-v3-resolver

Status: draft
Owner branch: Jesse-L-M/schiz-trial-audit
Depends on: docs/designs/program-memory-v3.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-14

## Purpose

This document is the routing table for `program_memory v3`.

It keeps the harness thin by making three things explicit:

- which task types exist
- which context should load first for each task
- which deterministic artifact contract each task is allowed to read or write

The rule is directional:

- skills own judgment, caveating, and procedure
- deterministic code owns artifact creation, schema validation, and packet assembly
- review work stays read-only outside `.context/program_memory/reviews/` by default
- real pilot program identifiers should resolve through the checked-in alias catalog
  or an explicit pilot registry before any bundle is written

## Task Types

### 1. Harvest program

Use when the job is to capture one program into a review bundle without making
authoritative interpretation calls.

Load first:

- `docs/designs/program-memory-v3.md`
- `.claude/skills/schiz-program-harvest/SKILL.md`
- the target program's current harvest directory if it already exists

Parameters:

- `PROGRAM_ID` required
- `PROGRAM_LABEL` optional
- `CORPUS_TIER` optional
- `SOURCE_URLS[]` optional seed set; required when `SEED_MODE=true` for an unknown
  non-pilot program
- `SEED_MODE=false` by default; when true, allow an explicit seed bootstrap for an
  unregistered program instead of failing closed
- `OUTPUT_DIR=.context/program_memory/reviews/{PROGRAM_ID}/harvest/`
- `WRITE_POLICY=review_only`

Deterministic bootstrap:

```bash
uv run scz-target-engine program-memory harvest-program \
  --program-id "$PROGRAM_ID" \
  --output-dir "$OUTPUT_DIR"
```

Writable artifacts:

- `source_manifest.json`
- `study_index.csv`
- `result_observations.csv`
- `harm_observations.csv`
- `contradictions.csv`

Do not create sidecar dossier files outside this contract. Record open questions in
`source_manifest.json` under `unresolved_questions`. Use row `notes` fields only for
qualifiers that are not already carried by the first-class capture, confidence, or
denominator/comparator columns.

### 2. Adjudicate program

Use when a harvest bundle exists and the job is to convert harvested evidence into a
trusted dossier with explicit caveats and belief updates.

Load first:

- `docs/designs/program-memory-v3.md`
- `.claude/skills/schiz-program-adjudicate/SKILL.md`
- `.context/program_memory/reviews/{PROGRAM_ID}/harvest/`

Parameters:

- `HARVEST_DIR` required
- `ADJUDICATION_ID` required
- `REVIEWER` required
- `OUTPUT_DIR=.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/`
- `WRITE_POLICY=review_only`

Deterministic bootstrap:

```bash
uv run scz-target-engine program-memory adjudicate-program \
  --harvest-dir "$HARVEST_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --adjudication-id "$ADJUDICATION_ID" \
  --reviewer "$REVIEWER"
```

Writable artifacts:

- `claims.csv`
- `caveats.csv`
- `belief_updates.csv`
- `contradictions.csv`
- `program_card.json`

`claims.csv` is the single adjudicated claim ledger. Do not split accepted and
rejected claims into separate files; use `adjudication_status` inside the ledger.

### 3. Build insight packet

Use when the job is to package already-adjudicated evidence for a scoped synthesis
question.

Load first:

- `docs/designs/program-memory-v3.md`
- `.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/program_card.json`
- `.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/claims.csv`
- `.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/caveats.csv`
- `.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/belief_updates.csv`
- `.context/program_memory/reviews/{PROGRAM_ID}/adjudicated/contradictions.csv`

Parameters:

- `PROGRAM_DIR` required
- `PACKET_ID` required
- `PACKET_QUESTION` optional but preferred
- `SCOPE_SUMMARY` optional but preferred
- `OUTPUT_FILE` required
- `WRITE_POLICY=review_only`

Deterministic bootstrap:

```bash
uv run scz-target-engine program-memory build-insight-packet \
  --program-dir "$PROGRAM_DIR" \
  --output-file "$OUTPUT_FILE" \
  --packet-id "$PACKET_ID"
```

Writable artifact:

- `insight_packet.json`

## Context Hygiene

- Never load the whole corpus when working on one program.
- Never read raw source dumps unless the current artifact rows are insufficient.
- Prefer the smallest adjudicated artifact set that can answer the task.
- If a contradiction is already explicit in the bundle, reason from it; do not
  re-litigate the same source conflict from scratch unless the bundle is wrong.

## Current Boundary

Implemented now:

- harvest bootstrap bundle
- adjudication bootstrap bundle
- scoped insight packet bootstrap
- immutable source-capture fields in `source_manifest.json`
- first-class structured confidence fields in `claims.csv` and `belief_updates.csv`
- first-class denominator and comparator fields in `result_observations.csv` and
  `harm_observations.csv`
- fail-closed default behavior for unknown programs unless explicit `seed_mode` is
  requested
- registry-backed validation for the emitted artifact families

Deferred to the next contract revision:

- `exposure_evidence`
- claim-link graphs
- source-history diffs
- dedicated diarization briefs
- compatibility projections into shipped `v2` consumers
