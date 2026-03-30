# Atlas Curated Fixture

This directory holds the deterministic atlas tensor fixture and its source manifests.

Files:

- `example_ingest_manifest.json`: checked-in atlas ingest-style manifest consumed by the taxonomy and tensor builders
- `example_sources/`: small processed source tables plus metadata sidecars
- `example_raw/`: staged-artifact manifests and placeholder raw artifacts referenced by the ingest manifest

The fixture is intentionally narrow:

- it is not a live-source snapshot or a scoring input
- it preserves provenance handles back to staged artifact identity
- it exercises observed evidence, source-level missingness, field-level missingness, cross-source ID conflict, and structural uncertainty

Use it when you need a deterministic atlas tensor build path without calling live APIs.
