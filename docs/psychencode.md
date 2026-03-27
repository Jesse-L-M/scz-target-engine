# PsychENCODE / BrainSCOPE Fetch Contract

`fetch-psychencode` is a shortlist-oriented importer for the public `BrainSCOPE` resource family within `PsychENCODE`.

It currently fills two engine columns:

- `cell_state_support`
- `developmental_regulatory_support`

The first is a direct schizophrenia cell-state proxy from differential expression. The second is currently the regulatory half of the engine layer, derived from adult cell-type gene regulatory networks. `v0` does not yet add a separate developmental atlas on top of that.

## Upstream Sources

- schizophrenia DEG combined table:
  - `https://brainscope.gersteinlab.org/data/DEG-combined/Schizophrenia_DEGcombined.csv`
- cell-type gene regulatory network bundle:
  - `https://brainscope.gersteinlab.org/GRNs.zip`
- official BrainSCOPE resource pages:
  - `https://brainscope.gersteinlab.org/key_resource_files.html`
  - `https://brainscope.gersteinlab.org/output-reg-celltype.html`

## Command

```bash
uv run scz-target-engine fetch-psychencode \
  --input-file examples/v0/input/gene_evidence.csv \
  --output-file data/processed/psychencode/example_support.csv
```

## Output Shape

One row per shortlist gene with any BrainSCOPE DEG or GRN support.

Genes with no exact BrainSCOPE match are omitted from the CSV and recorded in the
sidecar metadata JSON instead.

Key fields:

- `entity_id`
- `entity_label`
- `cell_state_support`
- `developmental_regulatory_support`
- `psychencode_match_status`
- metadata-only unmatched coverage fields:
  - `matching_rule`
  - `unique_input_gene_count`
  - `matched_gene_count`
  - `unmatched_gene_count`
  - `unmatched_gene_labels`
  - `unmatched_genes`
- DEG summary fields:
  - `psychencode_deg_strength_signal`
  - `psychencode_deg_breadth_signal`
  - `psychencode_deg_row_count`
  - `psychencode_deg_cell_type_count`
  - `psychencode_deg_top_cell_types_json`
- GRN summary fields:
  - `psychencode_grn_strength_signal`
  - `psychencode_grn_breadth_signal`
  - `psychencode_grn_tf_diversity_signal`
  - `psychencode_grn_edge_count`
  - `psychencode_grn_cell_type_count`
  - `psychencode_grn_top_cell_types_json`
  - `psychencode_grn_top_tfs_json`

## Normalization

`cell_state_support` combines:

- the best per-cell-type DEG score
- the mean of the top three per-cell-type DEG scores
- the breadth of nontrivial DEG signal across cell types

Each cell-type DEG score uses:

- significance from `padj` when available, otherwise `pvalue`
- absolute `log2FoldChange`

`developmental_regulatory_support` currently combines:

- strongest GRN cell-type score
- GRN breadth across cell types
- TF diversity
- total edge-count signal

This is intentionally a transparent regulatory proxy, not a claim that adult GRN density alone captures developmental biology.

## Matching Rule

`fetch-psychencode` only performs exact uppercase symbol matching against the official
BrainSCOPE schizophrenia DEG `gene` column and GRN `TG` column.

It does not:

- do substring or family-name matching
- remap one gene to another official gene symbol
- expand HGNC aliases unless a future curated exception is backed by both an official
  BrainSCOPE symbol and an official nomenclature source that establish a one-to-one
  identity

If an input gene is absent from both BrainSCOPE resources, the importer leaves it out
of the support CSV and records an `absent_from_deg_and_grn` entry in the metadata JSON.

## Coverage Note: C4A and CHRM4

For the current public BrainSCOPE schizophrenia resources:

- `C4A` has no exact row in `Schizophrenia_DEGcombined.csv`
- `C4A` has no exact `TG` target in `GRNs.zip`
- `CHRM4` has no exact row in `Schizophrenia_DEGcombined.csv`
- `CHRM4` has no exact `TG` target in `GRNs.zip`

Nearby symbols do exist upstream, for example `CHRM1`, `CHRM2`, `CHRM3`, and `CHRM5`,
but those are different official genes and are not valid substitutes for `CHRM4`.
Likewise, HGNC records historical aliases for `C4A`, but they do not justify remapping
it to distinct official genes such as `C4B`.
