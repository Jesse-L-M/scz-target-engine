# Prospective Registry Data

This directory is the append-only home for credibility artifacts derived from
reviewed hypothesis packets.

- `registrations/`
  Frozen `prospective_prediction_registration` artifacts. These pin the exact
  reviewed packet payload plus the separately hashed scoreable forecast payload.
  Existing files are immutable, and duplicate `registration_id` values in the
  same registrations directory are rejected before write.
- `outcomes/`
  Later `prospective_forecast_outcome_log` artifacts. No checked-in outcome logs
  ship with this PR because the repository does not yet have real follow-up
  outcomes to record.

The checked-in example registration is:

- `registrations/forecast_chrm4_acute_translation_guardrails_2026_03_31.json`
