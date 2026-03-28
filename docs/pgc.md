# PGC Schizophrenia Preprocessor

This repo includes a real `PGC` schizophrenia importer based on the official `scz2022` public release.

## Why This Is The Right V0 Move

The public `scz2022` release gives us two different things:

- raw summary statistics in `PGCsumstatsVCFv1.0` format
- an official prioritized-gene workbook

For `v0`, the prioritized-gene workbook is the honest path to a gene-level `common_variant_support` column. It uses PGC's own locus-to-gene prioritization outputs instead of pretending that nearest-gene mapping is good enough.

## Official Source

- PGC researcher downloads page, `scz2022`
- figshare article `19426775`
- workbook file `scz2022-Extended-Data-Table1.xlsx`

## Current Contract

The fetcher:

- resolves the official figshare article through the public API
- downloads the official workbook
- parses the `Extended.Data.Table.1` and `ST12 all criteria` sheets
- aggregates rows by Ensembl ID and gene symbol
- computes `common_variant_support` from common-variant-linked prioritization criteria only

## Primitive Prepared Fields

When this output is merged into the prepared gene table, the `PGC` primitive block keeps:

- `gene_biotype`
- `pgc_scz2022_prioritised`
- `pgc_scz2022_priority_index_snp_count`
- `pgc_scz2022_priority_index_snps_json`
- every emitted `pgc_scz2022_*` prioritization criterion column

## Common-Variant Support Heuristic

This score is explicitly a `PGC scz2022 prioritized-gene support` score, not a whole-genome gene statistic.

Included criteria:

- `FINEMAP.priority.gene`
- `SMR.priority.gene`
- `FINEMAPk3.5`
- `nonsynPP0.10`
- `UTRPP0.10`
- `k3.5singleGene`
- `SMRpsych`
- `SMRfetal`
- `SMRblood`
- `SMRmap`
- `SMRsingleGene`
- `HI.C.SMR`
- `sig.adultFUSION`
- `sig.fetalFUSION`
- `sig.EpiXcan.gene.filtered`
- `sig.EpiXcan.trans.filtered`

The score is the mean of those binary criteria after aggregation by gene.

Not included:

- rare-variant criteria
- neurodevelopmental overlap criteria
- gene-set membership criteria
- testability-only columns

## Example

```bash
uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/pgc/scz2022_prioritized_genes.csv
```
