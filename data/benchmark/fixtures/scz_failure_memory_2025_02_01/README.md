# Track B Fixture

This fixture is the checked-in Track B failure-memory slice for the post-PR4
stop/go review.

The fixture request now declares the explicit structural replay question
`scz_failure_memory_track_b_v1`.

It keeps the existing benchmark command sequence:

- `build-benchmark-snapshot`
- `build-benchmark-cohort`
- `run-benchmark`
- `build-benchmark-reporting`

Track B-specific checked-in inputs:

- `track_b_casebook.csv`: frozen gold analog ids, failure-scope labels, replay
  status labels, required-differences checklist targets, and coverage state at
  cutoff
- `program_universe.csv` and `events.csv`: pinned program-memory denominator and
  event ledger as of the fixture slice
- `assets.csv`, `event_provenance.csv`, and `directionality_hypotheses.csv`:
  pinned local program-memory substrate so Track B replay does not depend on
  repo-head dataset files at execution time
- `cohort_members.csv`: the same six proposal ids and labels scored by the
  casebook
- `future_outcomes.csv`: empty compatibility placeholder retained so Track B can
  stay inside the benchmark artifact families while deriving cohort labels from
  the casebook replay-status golds

Snapshot build validates the full Track B sibling-file contract up front. Cohort
materialization fails closed if `cohort_members.csv` diverges from the casebook
ids or labels.

The casebook is intentionally small and structural. It is meant to expose:

- exact-target and target-class wins
- mixed-history cases
- unresolved failure-scope cases
- sparse-history denominator rows that must stay explicit instead of being
  coerced into stronger replay labels
