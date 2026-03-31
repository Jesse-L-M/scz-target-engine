# Prospective Registry

`PR-60` adds a credibility lane for freezing future-facing forecasts against the
review-ready `hypothesis_packets_v1` contract from `PR-53`.

The goal is not product UX. The goal is to make it possible to:

- register a forecast from a shipped packet without mutating the packet artifact
- preserve enough provenance to trace the forecast back to the exact shipped artifact
- log realized outcomes later without overwriting the original forecast
- reconcile pending, resolved, and conflicted histories into score-ready records

## Frozen Registration Contract

Each forecast registration is a standalone
`prospective_prediction_registration` artifact.

Registration materialization is immutable by rule:

- if `output_file` already exists, the write fails
- if a sibling registration artifact in the target registrations directory already
  uses the same `registration_id`, the write fails

The writer does not silently rename files or mutate ids.

The registration freezes two distinct payloads:

- `frozen_packet_payload`
  This is the exact reviewed packet copied from the referenced
  `hypothesis_packets_v1` artifact. Validation dereferences the packet pointer and
  requires byte-level file checksum agreement plus exact packet-payload equality.
- `frozen_forecast_payload`
  This is the explicit scoreable forecast payload. It is the only block intended to
  drive later scoring.

`frozen_forecast_payload` currently carries:

- `forecast_type = reviewed_packet_disposition`
- `scoring_target = multiclass_single_label`
- `outcome_options`
  Must exactly match `decision_focus.decision_options` from the frozen packet.
- `option_probabilities`
  Must cover every outcome option, sum to `1.0`, and expose a single highest-probability option.
- `predicted_outcome`
  Must equal that unique highest-probability option.
- `outcome_window`
  Explicit open/close dates for later reconciliation.
- `rationale`
  Reviewable human-readable reasons for the frozen forecast.

Both frozen payloads carry canonical sha256 hashes inside the registration artifact
so the repo can later detect silent drift.

## Outcome Logging

Realized outcomes are logged separately in append-only
`prospective_forecast_outcome_log` artifacts.

Each outcome record must reference:

- the exact registration artifact path
- the registration artifact sha256
- the registration id
- one observed outcome from the registered option set
- a checksum-pinned evidence file path plus optional pointer

That means later outcome logging does not edit the original forecast file.

## Reconciliation Rules

Reconciliation is intentionally conservative:

- zero outcome records for a registration => `pending`
- exactly one outcome record inside the inclusive `opens_on` / `closes_on` window => `resolved`
- exactly one outcome record outside that inclusive window => `out_of_window`
- more than one outcome record => `conflicted`

The loader does not silently choose between competing outcome records.
Conflicted histories are excluded from `build_prospective_scoring_records(...)`
until the repo has an explicit resolution artifact or manual cleanup path.
Out-of-window histories are also excluded from scoring. They stay visible in
reconciliation output, but they do not become scoreable records just because
there is only one observed outcome row.

## CLI Path

Register one frozen forecast from a shipped packet artifact:

```bash
uv run scz-target-engine register-prospective-prediction \
  --hypothesis-artifact examples/v0/output/hypothesis_packets_v1.json \
  --packet-id ENSG00000180720__acute_translation_guardrails_v1 \
  --output-file data/prospective_registry/registrations/forecast_chrm4_acute_translation_guardrails_2026_03_31.json \
  --registered-at 2026-03-31T00:00:00Z \
  --registered-by repo_checked_in_example \
  --predicted-outcome advance \
  --option-probability advance=0.58 \
  --option-probability hold=0.32 \
  --option-probability kill=0.10 \
  --outcome-window-closes-on 2027-12-31 \
  --rationale "The packet carries a scoreable available policy signal and explicit nonfailure offsetting evidence." \
  --rationale "Contradiction and replay uncertainty remain live, so the forecast still assigns substantial hold and kill mass." \
  --registration-id forecast_chrm4_acute_translation_guardrails_2026_03_31
```

The checked-in example registration produced from that path lives at:

- `data/prospective_registry/registrations/forecast_chrm4_acute_translation_guardrails_2026_03_31.json`

No checked-in outcome log is shipped yet. Realized outcomes should only be added
once the repository has actual follow-up evidence to log.
