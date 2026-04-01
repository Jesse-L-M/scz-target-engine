# policy-and-packets-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/replay-track-a-v1.md, docs/designs/replay-track-b-v1.md, docs/designs/scz-rescue-1-v1.md, docs/designs/variant-to-context-v1.md
Blocked by: measurable replay uplift and one admitted rescue benchmark winner
Supersedes: -
Last updated: 2026-04-01

## Objective

Turn replay and rescue wins into auditable decision artifacts.

This spec exists to keep the policy layer narrow and evidence-bound.
It should translate proven replay and rescue signal into policy views and packet
outputs, not invent new product surface area ahead of benchmark truth.

## Success Condition

- Primary success metric:
  structured packets beat plain ranking outputs in blinded expert review on a
  predeclared rubric and threshold
- Secondary success metric:
  each top policy recommendation includes explicit "why this?" and "why not this?"
  bottleneck decomposition tied to replay, rescue, and failure-memory evidence
- Stop/go gate:
  do not expand beyond three initial policies or broaden public packet claims until
  blinded review shows the structured packet format is actually more useful

## Scope

- three initial policy views only:
  repurposing / off-patent, novel-mechanism, and adjunctive TRS
- explicit decision decomposition for top intervention objects
- packet extensions for first assay, first decisive falsification test, kill
  criteria, biomarker suggestions, and subgroup suggestions
- blinded expert comparison between structured packets and plain rank outputs
- additive policy release artifacts and packet outputs

## Not in Scope

- broad scenario-policy proliferation
- changing replay or rescue benchmark metrics
- admitting new rescue models
- UI-heavy observatory work
- generic narrative report generation

## Existing Surfaces To Reuse

- current `policy_decision_vectors_v2.json` and `policy_pareto_fronts_v1.json`:
  extend the shipped additive policy surfaces rather than replacing them
- current hypothesis-packet builder and packet contract:
  keep packet generation downstream of existing artifact boundaries
- current blinded expert review rubric:
  use the shipped review posture instead of ad hoc taste testing
- replay and rescue release artifacts:
  packet claims must point back to frozen replay/rescue evidence, not live head state

## Inputs

- replay Track A and Track B releases
- `SCZ-Rescue-1` admissions report and top prediction outputs
- variant-to-context feature manifests used by admitted replay/rescue consumers
- current policy vector artifacts and target ledgers
- current hypothesis-packet templates and expert-review rubric

## Outputs And Artifact Contracts

- New or changed artifact:
  `policy_rankings/<policy_id>.parquet`
  with top intervention objects and policy-specific decomposition fields
- New or changed artifact:
  `policy_explanations/<policy_id>.json`
  with "why this?", "why not this?", main bottlenecks, and cited evidence pointers
- New or changed artifact:
  `hypothesis_packets/*.json` and `hypothesis_packets/*.md`
  extended with first assay, first kill test, biomarker suggestions, subgroup
  suggestions, and explicit kill criteria
- New or changed artifact:
  `blinded_review_packets/*.json`
  freezing the expert-facing comparison payloads
- New or changed artifact:
  `packet_review_summary.json`
  with rubric scores, reviewer counts, and pass/fail decision
- Backward-compatibility rule:
  current policy vector outputs and packet builder entrypoints remain valid while the
  richer packet fields are added

## Data Flow

```text
REPLAY RELEASES + RESCUE RELEASES + CONTEXT MANIFESTS
    -> POLICY FILTERS / BOTTLENECK DECOMPOSITION
    -> POLICY RANKINGS + EXPLANATIONS
    -> HYPOTHESIS PACKETS
    -> BLINDED REVIEW PACKETS
    -> REVIEW SCORES + PASS / FAIL
```

- Replay contributes historical prioritization and failure-memory evidence.
- Rescue contributes context-specific signal and top translation candidates.
- Policy views filter and order intervention objects for one concrete decision frame.
- Packet generation turns those choices into falsification-ready artifacts.
- Blinded review decides whether the richer packet surface is actually useful.

## Implementation Plan

1. Freeze the three initial policy definitions and required decomposition fields.
2. Extend policy outputs so each top recommendation carries replay, rescue, context,
   and failure-memory pointers.
3. Extend packet generation with assay, kill-test, biomarker, and subgroup fields.
4. Build blinded review packet variants that compare structured packets against plain
   ranking-style outputs.
5. Score the blinded review results against a predeclared success threshold.

## Acceptance Tests

- Unit:
  add tests for policy explanation payload validation and packet field completeness
- Integration:
  generate one full policy ranking and packet set from frozen replay/rescue artifacts
- Regression:
  add a test that fails if packet generation falls back to uncited narrative text
  without replay/rescue provenance pointers
- E2E, if relevant:
  replay/rescue artifacts -> policy ranking -> packet generation -> blinded review
  packet export -> review summary

## Failure Modes

- Failure mode:
  policy rankings drift into hand-wavy preference scoring disconnected from replay
  and rescue evidence; each top recommendation must carry traceable evidence pointers
- Failure mode:
  packet fields become longer prose instead of sharper falsification logic; required
  structured fields must stay mandatory
- Failure mode:
  blinded review compares different underlying candidates instead of different packet
  presentations; the comparison set must keep candidate identity fixed

## Rollout / Compatibility

- policy and packet changes are additive to current outputs
- three initial policies only in v1
- a breaking change is any implementation that hides current additive policy outputs
  behind a new opaque packet-only surface

## Open Questions

- Should blinded review success require mean rubric improvement, pass-rate
  improvement, or both?
- Should packet fields for biomarkers and subgroup suggestions stay optional when the
  upstream replay/rescue evidence is sparse, or must they always emit an explicit
  "insufficient evidence" state?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json
uv run scz-target-engine build-benchmark-reporting --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv --runner-output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/runner_outputs --output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/public_payloads
```
