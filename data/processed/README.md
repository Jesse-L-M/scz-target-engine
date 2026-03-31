# Processed Data

Generated normalized source tables, candidate registries, and other processed prep
artifacts live here during local work.

Rules:

- source-normalized fetch outputs belong under source-specific subdirectories here
- the non-seed candidate registry artifact belongs under `data/processed/full_universe_ingest/registry/`
- seed-driven example fixture prep stays under `data/processed/example_gene_workflow/`
- example module fixture prep stays under `data/processed/example_module_workflow/`
- benchmark snapshot, cohort, and runner artifacts do not belong here, they belong under `data/benchmark/generated/`

`refresh-candidate-registry` writes processed full-universe source pulls under
`data/processed/full_universe_ingest/` and publishes
`data/processed/full_universe_ingest/registry/candidate_gene_registry.csv`.

`refresh-example-gene-table` writes the example gene workflow snapshots under
`data/processed/example_gene_workflow/`.

`refresh-example-module-table` writes the example module workflow snapshots under
`data/processed/example_module_workflow/`.

`refresh-example-inputs` refreshes the checked-in example fixtures from those processed
workflows and publishes `examples/v0/input/gene_evidence.csv` plus
`examples/v0/input/module_evidence.csv`.

This directory is gitignored except for this file.

Exception:

- governed rescue frozen datasets referenced by checked-in rescue dataset cards
  may be checked in under `data/processed/rescue/`
