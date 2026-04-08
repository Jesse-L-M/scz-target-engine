## scz_track_a_historical_replay

This fixture surface is checked in only to support default Track A public-slice
replay and catalog backfill.

- `scz_small/` remains the canonical minimal gene/module regression fixture.
- `scz_track_a_historical_replay/` carries the widened historical `PGC` and
  `PsychENCODE` archive extracts needed to create honest non-zero overlap between
  archived source support and the shipped Track A intervention-object slices.
- The default public-slice backfill path reads archive descriptors from this
  directory while continuing to derive replay `cohort_members.csv` and
  `future_outcomes.csv` from the checked-in program-history denominator.
