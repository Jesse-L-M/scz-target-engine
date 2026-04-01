# external-credibility-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/scz-rescue-1-v1.md, docs/designs/policy-and-packets-v1.md
Blocked by: one internally credible replay/rescue and packet stack worth exposing
Supersedes: -
Last updated: 2026-04-01

## Objective

Cross the line from strong internal infrastructure to outside accountability.

This spec is about one real credibility track, not a bundle of half-real signals.
The goal is to make the repo answerable to an external evaluator without weakening
the benchmark or leaking hidden state.

## Success Condition

- Primary success metric:
  one external credibility track runs end to end with frozen artifacts and a public
  receipt of what was evaluated
- Secondary success metric:
  the repo can show release notes and admissions reports for the exposed replay,
  rescue, and packet surfaces without mixing shipped claims with roadmap claims
- Stop/go gate:
  do not market a prospective registry, external challenge, or partner assay loop
  unless one external path is actually live and reproducible

## Scope

- choose and operationalize one credibility track:
  distribution-separated hidden eval, external blinded expert review, or partner-lab
  assay path
- publish frozen public package boundaries and release notes
- emit admissions and evaluation summaries that external users can inspect
- define operator responsibilities and artifact boundaries for the chosen path

## Not in Scope

- multiple simultaneous credibility programs in v1
- broad community platform work
- public leaderboard theater without controlled artifact boundaries
- replacing frozen release bundles with live repo access
- expanding beyond schizophrenia

## Existing Surfaces To Reuse

- current hidden-eval packaging and simulator:
  reuse the operator / submitter split if hidden eval is the chosen first path
- current blinded expert review rubric and packet generator:
  reuse the packet comparison surface if blinded review is the chosen first path
- rescue admissions reports and release-manifest families:
  keep external evaluation tied to frozen releases instead of mutable repo state
- challenge registry code:
  keep prospective registry as a later option, not the default first credibility path

## Inputs

- frozen replay, rescue, policy, and packet release artifacts
- admissions reports for any exposed rescue models
- public package manifests and release notes
- expert-review rubric or operator-side scoring bundle, depending on the chosen path

## Outputs And Artifact Contracts

- New or changed artifact:
  `external_eval_manifest.json`
  declaring the chosen credibility track, artifact versions, operator boundary, and
  release ids
- New or changed artifact:
  `external_eval_receipt.json`
  with date, evaluator class, evaluated release ids, and public-safe result summary
- New or changed artifact:
  `release_notes.md`
  distinguishing shipped claims, benchmark boundaries, and what was externally tested
- New or changed artifact:
  optional `submission_package.tar.gz` or blinded review bundle, depending on track
- Backward-compatibility rule:
  internal hidden-eval simulation and current packet-generation paths remain valid
  even if an externalized path is added

## Data Flow

```text
FROZEN RELEASES + ADMISSIONS REPORTS
    -> CHOSEN EXTERNAL CREDIBILITY TRACK
    -> EXTERNAL EVALUATION OR REVIEW
    -> RECEIPT + RELEASE NOTES + PUBLIC-SAFE SUMMARY
```

- Frozen releases are the only acceptable inputs.
- The chosen track runs under an explicit operator or reviewer boundary.
- The public output is a receipt and bounded summary, not raw hidden labels or live
  mutable state.

## Implementation Plan

1. Pick one first credibility track and freeze its operator boundary.
2. Define the external evaluation manifest and receipt schema.
3. Package the required frozen artifacts for the chosen path.
4. Run one external evaluation cycle and record the public-safe receipt.
5. Publish release notes that separate shipped facts from future roadmap claims.

## Acceptance Tests

- Unit:
  add validation tests for `external_eval_manifest.json` and
  `external_eval_receipt.json`
- Integration:
  package one frozen release bundle for the selected path and verify it can be
  consumed without repo checkout access
- Regression:
  add a test that fails if public-safe outputs include hidden labels, unreleased
  evaluator details, or mutable working-directory paths
- E2E, if relevant:
  frozen release -> external package -> operator/reviewer run -> public receipt

## Failure Modes

- Failure mode:
  the "external" track is just an internal simulation with no new boundary; the
  manifest must identify the evaluator class and the public receipt must show what
  was actually externalized
- Failure mode:
  public summaries leak hidden labels or operator-only metadata; outputs must remain
  receipt-style and schema-validated
- Failure mode:
  release notes blur roadmap and shipped claims; notes must explicitly separate what
  was evaluated from what is merely planned

## Rollout / Compatibility

- start with one external credibility track only
- keep internal simulation and review tooling for iteration before the external path
- a breaking change is any implementation that makes repo checkout or mutable local
  state part of the external submitter contract

## Open Questions

- Which credibility track should go first if hidden-eval packaging is operational
  sooner but blinded review may be easier to recruit?
- Should external receipts include absolute score values, percentile-only summaries,
  or pass/fail plus bounded narrative?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run scz-target-engine hidden-eval-task-package --task-id glutamatergic_convergence_rescue_task --output-dir .context/glutamatergic_hidden_eval_task
uv run scz-target-engine hidden-eval-pack-submission --task-package-dir .context/glutamatergic_hidden_eval_task --predictions-file .context/glutamatergic-convergence-run/ranked_predictions.csv --submitter-id external-eval-dry-run --submission-id scz-rescue-1-external-dry-run --scorer-id convergence_state_baseline_v1 --output-file .context/scz_rescue_1_external_submission.tar.gz
uv run scz-target-engine hidden-eval-simulate --task-package-dir .context/glutamatergic_hidden_eval_task --submission-file .context/scz_rescue_1_external_submission.tar.gz --output-dir .context/scz_rescue_1_external_eval
```
