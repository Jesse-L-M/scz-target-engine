# Raw Data

Place raw or manually exported source files here when raw-source ingestion is added.

Rules:

- raw downloaded archives, spreadsheets, and manual exports belong here
- do not put normalized source tables here
- do not put the candidate registry here
- do not put benchmark snapshot, cohort, or runner artifacts here

The current repo does not yet ship raw consortium-dump parsers. The non-seed ingest
path starts from processed source tables, not raw snapshots.

The checked-in historical archives used by the benchmark path are fixture-scale and live
under `data/benchmark/fixtures/scz_small/archives/`. They are benchmark fixtures, not a
general raw-data landing zone and not a production historical backfill catalog.

This directory is gitignored except for this file.

Exception:

- governed rescue historical snapshots that are explicitly frozen, carded, and
  referenced by rescue freeze manifests may be checked in under `data/raw/rescue/`
