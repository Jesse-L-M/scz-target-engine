# Atlas Tensor Contract

The atlas tensor is an additive substrate on top of the current atlas ingest foundation.
It does not rewrite scoring, replace the current processed source tables, or assert that
source-row alignment has already been fully solved.

## Input Boundary

The tensor builders consume an atlas ingest-style manifest that points at:

- processed source tables
- processed source metadata sidecars
- staged raw-source manifests

The builder currently materializes tensor slices for the atlas-supported source set:

- `Open Targets` schizophrenia baseline pulls
- `PGC scz2022` prioritized-gene pulls

## Emitted Surfaces

Taxonomy builder:

- `context_dimensions.csv`
- `context_members.csv`
- `feature_taxonomy.csv`
- `taxonomy_manifest.json`

Tensor builder:

- `provenance_bundles.csv`
- `entity_alignments.csv`
- `evidence_tensor.csv`
- nested taxonomy output
- `tensor_manifest.json`

## Channel Contract

Every tensor slice lands in one explicit channel:

- `observed`: a source emitted a concrete value for a feature
- `missingness`: a source or source field was absent
- `conflict`: atlas detected a structural conflict instead of flattening it away
- `uncertainty`: atlas emitted a separate uncertainty row instead of encoding uncertainty only implicitly

## Provenance

Observed and missingness rows carry:

- `provenance_bundle_id`
- `source_row_index`
- source-side entity identifiers and labels

`provenance_bundles.csv` links that bundle ID back to:

- the processed source table
- the processed metadata sidecar
- the staged raw manifest
- the staged raw artifact identities captured in that manifest

## Alignment And Conflict

The tensor groups source rows conservatively by normalized label into `entity_alignments.csv`.
That alignment table is deliberately lower-confidence than the candidate registry:

- `id_consistent`: one non-empty source ID across the alignment
- `label_only`: no source ID was available
- `id_conflict`: multiple non-empty source IDs appeared across the alignment

`id_conflict` rows stay explicit in the tensor. The builder does not choose a winner.

## Uncertainty

Current uncertainty rows are structural, not mechanistic. They capture:

- single-source alignments
- label-only alignments
- cross-source ID conflicts
- missingness-derived uncertainty

This means the tensor is ready for later provenance-aware mechanistic work, but it is not
yet a calibrated uncertainty model.

## Non-Goals

- no scoring rewrite
- no replacement of current source adapter outputs used by scoring
- no convergence-hub or mechanistic-axis work
- no claim that the checked-in fixture is a live-source backfill
