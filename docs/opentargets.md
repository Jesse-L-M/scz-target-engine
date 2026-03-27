# Open Targets Preprocessor

This repo now includes a real upstream preprocessor for the `Open Targets` generic-platform baseline.

## Why Start Here

`Open Targets` is the cleanest first real source because:

- it is already the explicit generic-platform comparator in the scoring contract
- schizophrenia can be queried directly by disease ID
- the official GraphQL API exposes disease association rows with target IDs, labels, overall score, and datatype scores

That means we can replace one synthetic layer with a real upstream pull without pretending we have finished the harder biology layers.

## Current Contract

The fetcher:

- resolves a disease from the official search endpoint if you pass `--disease-query`
- fetches metadata from the official API
- paginates through `associatedTargets`
- writes a flat CSV with:
  - `entity_id`
  - `entity_label`
  - `approved_name`
  - `generic_platform_baseline`
  - `opentargets_disease_id`
  - `opentargets_disease_name`
  - `opentargets_api_version`
  - `opentargets_data_version`
  - one JSON column for datatype scores
  - one flattened column per observed datatype score
- writes a JSON sidecar with fetch metadata

## Example

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/opentargets/schizophrenia_baseline.csv
```

## Notes

- V0 uses `Open Targets` as a baseline comparator, not as the project's source of truth.
- Schizophrenia currently resolves to `MONDO_0005090` through the live API.
- The fetcher intentionally does not try to materialize raw parquet releases yet. The GraphQL disease association path is enough for the baseline layer.
