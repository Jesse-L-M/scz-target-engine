# program-memory-v3

Status: draft
Owner branch: Jesse-L-M/schiz-trial-audit
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/program-memory-denominator-v1.md
Blocked by: -
Supersedes: -
Last updated: 2026-04-13

## Objective

Turn program memory from a denominator-plus-events layer into a living schizophrenia
intervention evidence system that can:

- ingest and normalize the public history of schizophrenia and directly adjacent
  intervention programs
- preserve high-resolution facts without flattening away caveats, contradictions, or
  weak-source warnings
- materialize adjudicated program dossiers plus a synthesis-ready feature store
- generate scoped insight packets that agents can reason over without reading the
  whole corpus raw

This work matters because the repo's moat is not "more trial records." It is the
ability to retain negative knowledge, distinguish molecule failure from mechanism
failure, and surface subgroup or design-level lessons from decades of public program
history without silently laundering weak evidence into strong claims.

## Success Condition

- Primary success metric:
  a pilot cohort of 20-30 high-value schizophrenia programs can be represented as
  deterministic per-program dossiers with explicit provenance, contradiction logging,
  and adjudicated belief updates.
- Secondary success metric:
  scoped insight packets can be built from the pilot corpus for questions about
  mechanism validity, subgroup structure, trial-design failure, and analog retrieval
  without requiring raw PDFs in the prompt context.
- Stop/go gate:
  do not scale beyond the pilot until at least 90% of pilot programs can be
  harvested into structured dossiers without unresolved schema escapes, and at least
  80% of adjudicated pilot dossiers survive expert spot-checks for factual fidelity
  and caveat handling.

## Scope

- define the `v3` ontology for program, study, arm, endpoint, result, harm,
  exposure, source document, claim, contradiction, caveat, and belief-update units
- create a tiered in-scope corpus boundary:
  `A` schizophrenia drug/biologic programs,
  `B` direct schizophrenia clinical-domain adjacencies such as CIAS, negative
  symptoms, adolescent schizophrenia, relapse prevention, and TRS,
  `C` same-asset or same-mechanism adjacent psychosis programs used for analog
  context,
  `D` explicitly tagged mechanistic context programs that are not schizophrenia but
  may inform failure interpretation
- specify the raw-source capture, harvest, adjudication, feature-store, and
  insight-packet artifacts
- define how weak or contradictory evidence is stored so it remains usable without
  being overtrusted
- draft human-plus-skill workflows for program harvest and adjudication
- run a pilot on a bounded set of landmark approvals, late-stage failures, and
  current novel-mechanism active programs

## Not in Scope

- replacing the current `data/curated/program_history/v2/` substrate as the shipped
  source of truth for existing ledger consumers
- claiming full individual participant data coverage or requiring IPD before `v3`
  can ship
- full psychiatry expansion beyond schizophrenia and tightly justified adjacent
  context programs
- end-to-end causal inference or "discover the biology of schizophrenia" claims
- direct changes to `v0`, `v1`, or benchmark scoring semantics before `v3` proves
  itself as an additive substrate

## Existing Surfaces To Reuse

- `data/curated/program_history/v2/`:
  keeps the current authoritative denominator and checked-in event history while `v3`
  matures additively
- `src/scz_target_engine/program_memory/*`:
  reuse the current harvest and adjudication patterns instead of rebuilding a parallel
  review pipeline from scratch
- `docs/program_history.md` and `docs/claim.md`:
  preserve the current claim boundary and explicit non-exhaustiveness language until
  `v3` lands
- benchmark and replay artifacts:
  later consumers should read from materialized `v3` compatibility projections, not
  from raw `v3` tables directly

## Inputs

- Upstream datasets:
  ClinicalTrials.gov API v2, ClinicalTrials.gov archive/history surfaces, AACT bulk
  snapshots, PubMed-indexed primary results papers, regulatory labels and review
  packages, company press releases and investor decks, conference abstracts when no
  better source exists
- Existing artifacts:
  `assets.csv`, `events.csv`, `event_provenance.csv`, `program_universe.csv`,
  `directionality_hypotheses.csv`
- Runtime commands:
  future `program-memory` CLI commands for discover, capture, harvest, adjudicate,
  materialize, and packet-build steps
- External methodology anchors:
  PRISMA 2020 for acquisition and update discipline, CONSORT-Outcomes 2022 and
  CONSORT Harms 2022 for extraction completeness, RoB 2 for randomized-trial bias,
  ROB-MEN for missing-evidence risk in network-style synthesis, and HTE / PATH-style
  guidance for subgroup inference

## Outputs And Artifact Contracts

- New raw-source root:
  `data/raw/program_memory/v3/{program_id}/{materialized_at}/`
  This stores immutable raw source captures plus SHA256 digests and source metadata.
- New staged harvest root:
  `.context/program_memory/reviews/{program_id}/harvest/`
  This stores non-authoritative skill outputs during review.
- New adjudication root:
  `.context/program_memory/reviews/{program_id}/adjudicated/`
  This stores accepted claims, contradictions, caveats, and draft program cards
  before any landed update.
- New authoritative processed root:
  `data/processed/program_memory/v3/`
  This stores the synthesis-ready feature store and packet-ready artifacts.
- Backward-compatibility rule:
  `v2` remains the only shipped source-of-truth substrate until explicit compatibility
  projections from `v3` are added and contract-tested.

## Resolver And Harness Boundary

Resolver rules live in:

- `docs/designs/program-memory-v3-resolver.md`

The operating model is:

- skills own judgment, caveating, and review procedure
- the CLI stays thin and only bootstraps or materializes deterministic bundles
- registered artifacts are the canonical dossier contract
- review work writes under `.context/program_memory/reviews/` only, unless an
  explicit landing step is added later

This keeps the harness thin, the skill layer reusable, and the deterministic layer
reliable.

### Required `v3` entities

- `programs`
  canonical opportunity-level row; one row per adjudicated program lineage and use
  case
- `studies`
  one row per study record or publication-level trial object
- `arms`
  one row per comparable arm or arm-group
- `endpoints`
  one row per endpoint definition and timepoint
- `result_observations`
  one row per arm/endpoint/timepoint result
- `harm_observations`
  one row per adverse-event or harm summary observation
- `exposure_evidence`
  one row per dose / PK / PD / occupancy / adherence fact
- `source_documents`
  one row per raw artifact with immutable digest and source tier
- `claims`
  one row per extracted or adjudicated claim, typed as `observed_fact`,
  `source_assertion`, `candidate_interpretation`, or `accepted_interpretation`
- `claim_links`
  typed support and conflict edges from claims to sources, studies, results, and
  belief updates
- `contradictions`
  explicit conflicts such as registry-paper mismatch, endpoint switching, sample-size
  mismatch, inconsistent harm totals, and unresolved alias collision
- `caveats`
  explicit caveats such as weak source tier, incomplete harms reporting, probable
  underexposure, underpowered subgroup analysis, and non-transportable population
- `belief_updates`
  adjudicated reusable conclusions at `molecule`, `mechanism`, `target`,
  `population`, `endpoint`, or `design_lesson` scope
- `program_cards`
  compact synthesis-facing summaries generated from adjudicated structures rather than
  freeform prose

### Required confidence separation

Every accepted `claim` or `belief_update` must separate:

- `extraction_confidence`
- `source_reliability`
- `risk_of_bias`
- `reporting_integrity_risk`
- `transportability_confidence`
- `interpretation_confidence`

No single blended score is allowed to erase which part is weak.

### Canonical dossier contract

The registered `v3` artifacts are the canonical contract.

Harvest bundle:

- `source_manifest.json`
- `study_index.csv`
- `result_observations.csv`
- `harm_observations.csv`
- `contradictions.csv`

Adjudicated bundle:

- `claims.csv`
- `caveats.csv`
- `belief_updates.csv`
- `contradictions.csv`
- `program_card.json`

Scoped synthesis bundle:

- `insight_packet.json`

Current rules:

- `source_manifest.json` carries `unresolved_questions`; there is no separate
  `open_questions.md` contract yet
- `claims.csv` is the single adjudicated claim ledger; accepted and rejected claims
  are distinguished by status, not by separate files
- insight packets must be built from adjudicated artifacts, not from raw documents or
  freeform summaries

Deferred next-contract artifacts:

- `exposure_evidence`
- claim-link graphs
- dedicated source-history diff artifacts
- dedicated diarization briefs
- compatibility projections for replay / policy / packet consumers

## Data Flow

```text
DISCOVERY + SOURCE CAPTURE
    -> RAW SOURCE MANIFESTS
    -> STRUCTURED FACT EXTRACTION
    -> CONTRADICTION DETECTION
    -> HUMAN / SKILL ADJUDICATION
    -> PROGRAM DOSSIERS + BELIEF UPDATES
    -> SYNTHESIS FEATURE STORE
    -> SCOPED INSIGHT PACKETS
    -> COMPATIBILITY PROJECTIONS FOR REPLAY / POLICY / PACKETS
```

Plain-English flow:

1. discover candidate programs and source artifacts, but do not trust any one source
   as authoritative by default
2. capture raw source documents with immutable digests
3. bootstrap the canonical dossier bundle with deterministic commands
4. extract typed facts into the registered harvest artifacts instead of ad hoc files
5. compare sources and create explicit contradiction rows instead of silently picking
   winners
6. adjudicate what is accepted, disputed, weak, or unresolved
7. materialize a compact program card and feature rows from adjudicated structures
8. build scoped insight packets for a specific synthesis question so agents never need
   the full corpus in context

## Implementation Plan

1. Freeze the `v3` ontology, tiered corpus boundary, canonical artifact contract, and
   resolver rules.
2. Extend raw-source capture for ClinicalTrials.gov current records, archive history,
   AACT, PubMed-linked papers, regulatory docs, and sponsor artifacts.
3. Build deterministic harvest outputs for one program at a time, with raw facts kept
   separate from candidate interpretations.
4. Add contradiction detection for registry vs paper vs press vs regulator mismatches.
5. Add adjudication outputs that accept, reject, or edit candidate claims and assign
   caveats plus confidence fields.
6. Materialize synthesis-facing `program_cards`, `belief_updates`, and feature tables
   under `data/processed/program_memory/v3/`.
7. Add `insight_packet` builders for scoped questions such as mechanism history,
   subgroup hypothesis generation, and replay analog retrieval.
8. Run a pilot on 20-30 programs before broad scaling.

Milestone 0 sequencing:

1. Use the deterministic CLI to bootstrap each program bundle.
2. Run the skill against 3-5 archetype programs first.
3. Capture reviewer corrections.
4. Update the skill and resolver, not just the individual dossier.
5. Only then widen to the larger pilot cohort.

## Acceptance Tests

- Unit:
  schema tests for `source_documents`, `claims`, `contradictions`,
  `belief_updates`, and `program_cards`
- Integration:
  deterministic harvest of one known approval, one known phase 3 failure, and one
  current active program from raw capture to adjudicated dossier
- Regression:
  a registry-paper discrepancy must fail into an explicit contradiction row rather
  than silently overwriting one source with another
- E2E:
  build one scoped insight packet from pilot dossiers and verify that every returned
  insight cites supporting and opposing program IDs plus caveats

## Failure Modes

- Failure mode:
  alias collisions silently split one asset lineage into multiple fake programs;
  the system must fail closed and emit an unresolved identity contradiction
- Failure mode:
  a press release is treated as equivalent to a regulator review or primary results
  paper; the system must preserve source-tier differences explicitly
- Failure mode:
  endpoint changes between registry history and publication are missed; the system
  must flag reporting-integrity risk instead of trusting the latest record blindly
- Failure mode:
  program cards become lossy marketing summaries; the system must generate them from
  accepted claim structures, not manual prose
- Failure mode:
  insight generation reads the whole corpus raw and misses high-value patterns due to
  context bloat; the system must build scoped packets from the feature store instead
  of prompting over raw artifacts
- Failure mode:
  skills, docs, and emitted artifacts drift apart; the registered artifact families
  plus resolver rules must remain the source of truth for dossier shape

## Rollout / Compatibility

- Existing `v2` consumers stay stable until `v3` projections are explicitly added and
  tested.
- `v3` starts as additive under `data/raw/program_memory/v3/`,
  `.context/program_memory/reviews/`, and `data/processed/program_memory/v3/`.
- A dual-write period is required before replay, policy, or packet consumers can rely
  on `v3`.
- It is a breaking change if `v2` semantics drift before a compatibility projection is
  checked in and documented.

## Open Questions

- What exact threshold moves a `C` or `D` adjacent program from useful analog context
  into in-scope decision evidence?
- Do we store the processed feature store primarily as CSV for auditability, Parquet
  for scale, or both?
- Which slices deserve mandatory regulator-document recovery before adjudication can
  be considered complete?

## Decision Log Links

- Related decision:
  `docs/decisions/0001-planning-contract.md`
- Related decision:
  `docs/decisions/0004-program-memory-denominator-boundary.md`

## Commands

```bash
# Proposed future workflow, not yet implemented
uv run scz-target-engine program-memory discover --scope schizophrenia --output-dir .context/program_memory/discovery
uv run scz-target-engine program-memory capture-sources --program-id <program_id> --output-dir data/raw/program_memory/v3/<program_id>/<materialized_at>
uv run scz-target-engine program-memory harvest-program --program-id <program_id> --output-dir .context/program_memory/reviews/<program_id>/harvest
uv run scz-target-engine program-memory adjudicate-program --harvest-dir .context/program_memory/reviews/<program_id>/harvest --output-dir .context/program_memory/reviews/<program_id>/adjudicated
uv run scz-target-engine program-memory materialize-v3 --input-dir .context/program_memory/reviews --output-dir data/processed/program_memory/v3
uv run scz-target-engine program-memory build-insight-packet --question-file .context/program_memory/questions/<question>.json --output-file .context/program_memory/packets/<packet>.json
```
