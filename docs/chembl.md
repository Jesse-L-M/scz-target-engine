# ChEMBL Preprocessor

This repo includes a shortlist-oriented `ChEMBL` fetcher for tractability and compoundability context.

## Why This Is The Right Second Source

`ChEMBL` gives us something the current engine actually needs:

- a real activity footprint
- a real mechanism footprint
- evidence of clinical maturity through `max_phase`
- a target type that helps separate single proteins from messier target objects

That is enough for a transparent v0 tractability heuristic.

## Current Contract

The fetcher:

- reads an input CSV with at least `entity_id` and `entity_label`
- treats `entity_label` as the gene symbol query
- searches `ChEMBL` targets by symbol
- only accepts exact human gene-symbol matches by default
- fetches:
  - target metadata
  - activity count
  - mechanism count and max phase
- writes a flat CSV with:
  - raw `ChEMBL` match columns
  - `tractability_compoundability`
  - `chembl_match_confidence`
  - `chembl_match_status`

## Heuristic

The current heuristic is simple and inspectable, not magical:

- `40%` max mechanism phase
- `35%` activity count signal
- `15%` mechanism count signal
- `10%` target type signal

This is a v0 heuristic, not a claim that ChEMBL alone can define tractability.

## Example

```bash
uv run scz-target-engine fetch-chembl \
  --input-file examples/v0/input/gene_evidence.csv \
  --output-file data/processed/chembl/example_tractability.csv \
  --limit 10
```

## Notes

- This fetcher is intentionally shortlist-oriented. It is not meant to hammer the API for thousands of genes in one pass.
- Matching is strict by default. If `ChEMBL` does not expose an exact human `GENE_SYMBOL` synonym match, the row is emitted as unmatched.
