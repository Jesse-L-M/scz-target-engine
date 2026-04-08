## scz_track_a_historical_replay

This fixture surface is checked in only to support default Track A public-slice
replay and catalog backfill.

- `scz_small/` remains the canonical minimal gene/module regression fixture.
- `scz_track_a_historical_replay/` carries the dedicated Track A historical
  archive surface used by public-slice replay: preserved `PGC`,
  `PsychENCODE`, and `ChEMBL` fixtures plus widened `SCHEMA` and
  `Open Targets` replay extracts where honest pre-cutoff support exists for the
  checked-in Track A denominator.
- The default public-slice backfill path reads archive descriptors from this
  directory while continuing to derive replay `cohort_members.csv` and
  `future_outcomes.csv` from the checked-in program-history denominator.
