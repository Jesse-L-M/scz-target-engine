# Atlas Mechanistic Structure

This layer sits on top of the atlas evidence tensor.
It does not replace the tensor, change v0 ranks, or back-propagate new scalar scores into
existing benchmark or rescue semantics.

## What A Mechanistic Axis Means Here

A mechanistic axis is an interpretable evidence family assembled from tensor features that
already exist in the factorized atlas substrate.

Current axes:

- `mechanistic_axis:disease-association`
  - `opentargets.generic_platform_baseline`
  - `opentargets.datatype.genetic_association`
  - `pgc.common_variant_support`
- `mechanistic_axis:clinical-translation`
  - `opentargets.datatype.clinical`
- `mechanistic_axis:variant-to-gene`
  - `pgc.prioritised`
  - `pgc.priority_index_snp_count`
  - `pgc.priority_index_snps`
  - `pgc.criterion.*`

The axis output is not a score. Each row keeps:

- which expected tensor features were observed
- which expected tensor features were missing
- whether the alignment carried a structural conflict
- the maximum structural uncertainty level seen on the axis
- direct tensor slice links for observed, missingness, conflict, and uncertainty rows

## What A Convergence Hub Means Here

A convergence hub is a gene-alignment surface with one or more supported or partially
supported mechanistic axes.

Current hub rows stay explicit about:

- source coverage: `none`, `single_source`, or `cross_source`
- axis coverage: `none`, `single_axis`, or `multi_axis`
- missingness state: `none`, `source_absent`, `field_blank`, or `mixed`
- conflict state: currently `none` or `alignment_id_conflict`
- uncertainty max level: `none`, `low`, `medium`, or `high`

This keeps "hub" meaning additive and auditable. A hub is not a rank, not a rescue score,
and not a claim that the mechanism is settled.

## Provenance And Uncertainty

Axis and hub summaries link back to tensor slices through dedicated evidence-link tables.
Those link rows carry:

- `tensor_slice_id`
- `tensor_channel`
- source, dataset, entity, and source-row coordinates when present
- `resolved_provenance_bundle_ids_json`
- `resolved_source_row_indices_json`

For atlas-native structural conflict rows, provenance is recovered from the tensor row's
embedded `row_refs` payload so downstream work can still trace the disagreement back to the
underlying evidence rows.

Uncertainty is not folded away. Summaries keep:

- missingness-derived uncertainty
- alignment-conflict uncertainty
- the original tensor uncertainty reasons

## Artifacts

Mechanistic-axis builder:

- `mechanistic_axes.csv`
- `mechanistic_axis_evidence_links.csv`
- `mechanistic_axes_manifest.json`

Convergence-hub builder:

- `convergence_hubs.csv`
- `hub_axis_members.csv`
- `hub_evidence_links.csv`
- `convergence_manifest.json`

## Example Path

```bash
uv run scz-target-engine atlas build tensor \
  --ingest-manifest-file data/curated/atlas/example_ingest_manifest.json \
  --output-dir .context/example-atlas/tensor

uv run scz-target-engine atlas build mechanistic-axes \
  --tensor-manifest-file .context/example-atlas/tensor/tensor_manifest.json \
  --output-dir .context/example-atlas/mechanistic_axes

uv run scz-target-engine atlas build convergence-hubs \
  --tensor-manifest-file .context/example-atlas/tensor/tensor_manifest.json \
  --output-dir .context/example-atlas/convergence
```
