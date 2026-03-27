# SCHEMA Rare-Variant Preprocessor

This repo includes a real `SCHEMA` schizophrenia rare-variant fetcher for shortlist genes.

## Why This Is The Right V0 Move

`SCHEMA` is the cleanest public rare-variant layer for schizophrenia in the current stack.

For `v0`, the honest move is not to invent a new burden model. It is to query the official SCHEMA results browser for the genes we care about right now, preserve the published gene-level burden fields, and derive a transparent normalized `rare_variant_support`.

## Official Source

- SCHEMA results browser
- public gene endpoint under `/api/gene/<query>`
- public bulk gene-results download `SCHEMA_gene_results.tsv.bgz`

## Current Contract

The fetcher:

- reads a shortlist gene table
- queries the official SCHEMA gene endpoint by Ensembl ID first, then gene symbol
- uses the official SCHEMA search endpoint to resolve symbols and skips ambiguous alias hits
- applies a checked-in curated alias override layer before search when a symbol is known to be ambiguous
- falls back to the official bulk `SCHEMA_gene_results.tsv.bgz` download if an override points to a real gene ID but the per-gene API is broken
- preserves the published `meta` gene-level burden fields
- computes `rare_variant_support` from:
  - SCHEMA `P meta`
  - SCHEMA `Q meta`
  - positive burden-direction evidence in the published odds ratios

## Rare-Variant Support Heuristic

This score is explicitly a `SCHEMA rare-variant support` score, not a replacement for the consortium statistics.

It combines:

- a significance component weighted toward `Q meta`
- an effect-direction component based on positive `OR (PTV)`, `OR (Class I)`, and `OR (Class II)`

If the published odds ratios do not show positive enrichment, the normalized score collapses toward zero even if an uncorrected `P meta` exists.

The override layer lives in `config/schema_alias_overrides.csv` so the hard cases stay explicit and reviewable.

## Example

```bash
uv run scz-target-engine fetch-schema \
  --input-file examples/v0/input/gene_evidence.csv \
  --output-file data/processed/schema/example_rare_variant.csv
```
