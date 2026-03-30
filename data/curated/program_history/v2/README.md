# Program Memory v2

This directory is the normalized source of truth for checked-in schizophrenia program
memory.

Tables:

- `assets.csv`: stable asset and mechanism records
- `events.csv`: dated program events keyed by `event_id`
- `event_provenance.csv`: source tier and URL records keyed by `event_id`
- `directionality_hypotheses.csv`: target-level directionality hypotheses keyed by
  `hypothesis_id`

The legacy `../programs.csv` and `../directionality_hypotheses.csv` files remain
checked in as compatibility views for current ledger consumers.
