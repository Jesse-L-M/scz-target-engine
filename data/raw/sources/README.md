# Raw Source Staging

This directory holds staged raw-source artifacts for atlas-facing ingest work.

Current contract:

- path layout: `data/raw/sources/{source}/{dataset}/{materialized_at}/`
- each staged source run writes a `manifest.json`
- manifests record the source contract, request parameters, staged artifact digests,
  and downstream processed artifact paths

Current scope:

- adapter-backed `Open Targets` request/response capture
- adapter-backed `PGC scz2022` release metadata and workbook capture

Not current scope:

- raw consortium-dump ingestion
- direct scoring from files in this directory

The current scoring path still runs from processed source tables and curated evidence
tables elsewhere in the repo.
