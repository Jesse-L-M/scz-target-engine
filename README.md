# Schizophrenia Target Engine

A transparent schizophrenia target-decision scaffold that now ships three concrete
layers on `main`: a stable `v0` evidence-ranking reference path, additive `v1`
decision-vector outputs, and a fixture-scale benchmark path built from frozen
snapshot, cohort, and runner artifacts.

The core challenge in target selection is separating reproducible biological signal
from accumulated intuition. This repo makes the scoring contract explicit, keeps the
shared `v0` numeric outputs stable, adds inspectable `v1` heads without changing those
`v0` scores, and emits benchmark artifacts that can be rerun end to end from checked-in
fixtures.

## What Ships On Main

- A `v0` reference build for schizophrenia gene and module ranking, with sensitivity analysis, markdown/CSV reports, and warning overlays that remain reporting-only
- Additive `v1` decision vectors plus per-domain/per-stage ranking artifacts
- PR7 scoring-neutral target ledgers with failure history, directionality hypotheses, and source primitives
- PR9A/PR9B/PR9C benchmark protocol, snapshot manifests, cohort labels, runner manifests, metric payloads, and confidence interval payloads
- A non-seed candidate registry built from `Open Targets` baseline pulls plus optional `PGC` support
- A checked-in example scoring fixture path under `examples/v0/`, where `v0` still exists as the reference workflow and `v1` outputs are emitted alongside it when you rerun the build
- Live fetchers for `Open Targets`, `ChEMBL`, `PGC`, `SCHEMA`, and `PsychENCODE / BrainSCOPE`

Raw consortium-dump ingestion is still not implemented. The current scoring path
operates from curated tables with normalized layer scores in `[0, 1]`.
Atlas now also has an additive raw-source staging foundation for adapter-backed
`Open Targets` and `PGC` pulls. It writes provenance-bearing request/download
captures under `data/raw/sources/` and can rebuild a candidate registry through
`atlas ingest candidate-registry`. Atlas also now has additive taxonomy/tensor
builders that materialize provenance-bearing evidence slices, missingness,
conflict, and structural uncertainty from an ingest manifest, but that
foundation still does not implement consortium-dump parsing.

## Claim Boundary

- `v0` is infrastructure, not a validated target decision system.
- `v1` is an additive multi-head output layer, not a validated clinical advancement authority.
- Warning overlays and structural ledgers do not change shared `v0` score, rank, or `heuristic_stable`.
- `v1` domain/stage scores now combine human-support, biology-context, and intervention-readiness heads with numeric PR7-backed failure, directionality, and subgroup heads for gene targets.
- Benchmark execution now runs the frozen `available_now` baseline set against archived snapshot/cohort artifacts, while protocol-only baselines remain explicit and skipped.
- Benchmark metric payloads and percentile-bootstrap confidence interval payloads are emitted without changing current `v0` or `v1` scoring semantics.
- `v0` now has a non-seed ingest path and a full-universe module-prep path, but gene prep and end-to-end scoring are not yet fully seed-independent.
- Atlas raw-source staging now captures upstream request/download artifacts for selected adapter-backed pulls, but it does not replace the current processed source outputs used by scoring.
- Config naming note: `stability.heuristic_stability_threshold` is the preferred key. The legacy `stability.decision_grade_threshold` alias is still accepted temporarily for compatibility.

## Current Limitations

- Historical benchmark archives are fixture-scale. The checked-in archive set under `data/benchmark/fixtures/scz_small/` is a small deterministic test path, not a production backfill catalog.
- Benchmark breadth is still limited to the frozen schizophrenia question, a small deterministic cohort, and the current `available_now` baseline subset.
- Benchmark outputs are diagnostic artifacts. They are not proof of calibration, threshold quality, or deployment readiness.
- Calibration work, decision-threshold setting, and broader operating-point evaluation remain future work.
- Raw-source ingestion from consortium dumps is still future work.

See [docs/claim.md](docs/claim.md) for the current claim boundary,
[docs/ontology.md](docs/ontology.md) for the domain/stage vocabulary emitted by `v1`,
and [docs/program_history.md](docs/program_history.md) for the curated program-history
substrate. See [docs/ledger_contract.md](docs/ledger_contract.md) for the target-ledger
output contract, [docs/benchmarking.md](docs/benchmarking.md) for the canonical
benchmark workflow, and [docs/artifact_schemas.md](docs/artifact_schemas.md) for the
registered artifact families and runtime validation surface. See
[docs/rescue_tasks.md](docs/rescue_tasks.md) for the dedicated rescue registry and
contract surface. See
[docs/atlas_source_ingest.md](docs/atlas_source_ingest.md) for the staged raw-source
contract and atlas ingest boundary, and [docs/atlas_tensor.md](docs/atlas_tensor.md)
for the taxonomy/tensor contract layered on top of that ingest foundation.

## Quickstart

Build the non-seed candidate registry:

```bash
uv run scz-target-engine refresh-candidate-registry
```

That writes `data/processed/full_universe_ingest/registry/candidate_gene_registry.csv`.

Refresh the example gene and module tables from the live source adapters:

```bash
uv run scz-target-engine refresh-example-inputs
```

`refresh-example-inputs` still publishes the checked-in example fixtures; the module side
now rebuilds from the non-seed candidate registry while the gene side remains seed-driven.

Then run the example build:

```bash
uv run scz-target-engine build \
  --config config/v0.toml \
  --input-dir examples/v0/input \
  --output-dir examples/v0/output
```

`examples/v0/input/gene_evidence.csv` and `examples/v0/input/module_evidence.csv` are generated fixture snapshots from that refresh flow.
The build now also emits `gene_target_ledgers.json`, a structured per-target artifact that stays scoring-neutral in `v0`.

The build now also emits:

- `decision_vectors_v1.json`: nested per-entity `v1` decision vectors with named head fields, a keyed `decision_vector` object, and domain/stage scores
- `domain_head_rankings_v1.csv`: per-domain/per-stage `v1` ranking rows with side-by-side `heuristic_score_v0` comparison fields

The checked-in `examples/v0/output/` fixtures still capture the legacy shared `v0`
example outputs. Re-run the build if you want current `gene_target_ledgers.json`,
`decision_vectors_v1.json`, and `domain_head_rankings_v1.csv`.

Build the registry manually from processed full-universe-capable sources:

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/full_universe_ingest/opentargets/schizophrenia_baseline.csv

uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/full_universe_ingest/pgc/scz2022_prioritized_genes.csv

uv run scz-target-engine build-candidate-registry \
  --opentargets-file data/processed/full_universe_ingest/opentargets/schizophrenia_baseline.csv \
  --pgc-file data/processed/full_universe_ingest/pgc/scz2022_prioritized_genes.csv \
  --output-file data/processed/full_universe_ingest/registry/candidate_gene_registry.csv
```

Fetch a real `Open Targets` schizophrenia baseline table:

```bash
uv run scz-target-engine fetch-opentargets \
  --disease-query schizophrenia \
  --output-file data/processed/opentargets/schizophrenia_baseline.csv
```

Fetch `ChEMBL` tractability context for a shortlist:

```bash
uv run scz-target-engine fetch-chembl \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/chembl/example_tractability.csv
```

Fetch `PGC` schizophrenia common-variant gene support:

```bash
uv run scz-target-engine fetch-pgc-scz2022 \
  --output-file data/processed/pgc/scz2022_prioritized_genes.csv
```

Fetch `SCHEMA` schizophrenia rare-variant gene support for a shortlist:

```bash
uv run scz-target-engine fetch-schema \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv
```

Fetch `PsychENCODE / BrainSCOPE` schizophrenia DEG and GRN support for a shortlist:

```bash
uv run scz-target-engine fetch-psychencode \
  --input-file examples/v0/input/gene_seed.csv \
  --output-file data/processed/example_gene_workflow/psychencode/example_support.csv
```

Fetch `PsychENCODE / BrainSCOPE` source-backed module evidence from the candidate registry
or another provenance-bearing candidate input:

```bash
uv run scz-target-engine fetch-psychencode-modules \
  --input-file data/processed/full_universe_ingest/registry/candidate_gene_registry.csv \
  --output-file data/processed/example_module_workflow/psychencode/example_module_evidence.csv
```

Prepare an engine-ready gene table from joined source outputs:

```bash
uv run scz-target-engine prepare-gene-table \
  --seed-file examples/v0/input/gene_seed.csv \
  --pgc-file data/processed/pgc/scz2022_prioritized_genes.csv \
  --schema-file data/processed/example_gene_workflow/schema/example_rare_variant_support.csv \
  --psychencode-file data/processed/example_gene_workflow/psychencode/example_support.csv \
  --opentargets-file data/processed/opentargets/schizophrenia_baseline.csv \
  --chembl-file data/processed/example_gene_workflow/chembl/example_tractability.csv \
  --output-file data/processed/example_gene_workflow/curated/example_gene_evidence.csv
```

Validate only:

```bash
uv run scz-target-engine validate \
  --config config/v0.toml \
  --input-dir examples/v0/input
```

Run tests:

```bash
uv run --group dev pytest
```

## CLI Namespaces

Legacy flat commands remain supported. The new namespaced routes are additive aliases
that call the same handlers with the same flags:

- `engine validate`, `engine build`
- `sources opentargets`, `sources chembl`, `sources pgc scz2022`
- `sources schema`, `sources psychencode support`, `sources psychencode modules`
- `registry build`, `registry refresh`
- `atlas sources opentargets`, `atlas sources pgc scz2022`, `atlas ingest candidate-registry`
- `atlas build taxonomy`, `atlas build tensor`
- `prepare gene-table`, `prepare example-gene-table`, `prepare example-module-table`, `prepare example-inputs`
- `benchmark snapshot`, `benchmark cohort`, `benchmark run`

Migration posture:

- Keep existing scripts on the legacy flat commands when stability matters.
- Prefer the namespaced aliases for new automation and future module expansion.
- `config/v0.toml` remains the canonical compatibility path.
- `config/engine/v0.toml` is a mirrored namespaced path for the engine namespace.

Example legacy and namespaced equivalents:

```bash
uv run scz-target-engine validate \
  --config config/v0.toml \
  --input-dir examples/v0/input

uv run scz-target-engine engine validate \
  --config config/engine/v0.toml \
  --input-dir examples/v0/input
```

Atlas raw-source staging example:

```bash
uv run scz-target-engine atlas ingest candidate-registry \
  --output-file .context/atlas/candidate_gene_registry.csv \
  --work-dir .context/atlas/work \
  --raw-dir .context/atlas/raw \
  --materialized-at 2026-03-30
```

That atlas path stages raw adapter captures under `.context/atlas/raw/`, rebuilds the
same candidate-registry contract from processed source outputs, and keeps the existing
`registry refresh` workflow unchanged.

Deterministic atlas tensor example:

```bash
uv run scz-target-engine atlas build tensor \
  --ingest-manifest-file data/curated/atlas/example_ingest_manifest.json \
  --output-dir .context/atlas/example_tensor
```

That tensor path consumes the checked-in fixture manifest under `data/curated/atlas/`
and emits taxonomy, provenance, alignment, and evidence-tensor artifacts without
calling live APIs.

## Canonical Benchmark Workflow

The canonical end-to-end benchmark path in this repo is the checked-in deterministic
fixture under `data/benchmark/fixtures/scz_small/` plus generated outputs under
`data/benchmark/generated/scz_small/`.

The suite/task contract source of truth lives in
`data/curated/rescue_tasks/task_registry.csv`. The current registry-backed task is
`scz_translational_task` in suite `scz_translational_suite`, and it maps directly to
the checked-in `scz_small` fixture inputs. The emitted snapshot and run manifests
carry `benchmark_suite_id` and `benchmark_task_id` as optional provenance fields.
Rescue tasks now use the separate
`data/curated/rescue_tasks/rescue_task_registry.csv` index plus validated
`rescue_task_contract` JSON files, so the shipped benchmark registry remains benchmark
only.

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_small/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --materialized-at 2026-03-28

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_small/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode
```

Artifact layout:

- `data/benchmark/fixtures/scz_small/`: checked-in fixture request, archive index, archived source extracts, cohort membership, and future outcomes
- `data/curated/rescue_tasks/task_registry.csv`: registry-backed suite/task contract for the current schizophrenia benchmark
- `data/curated/rescue_tasks/rescue_task_registry.csv`: dedicated registry for rescue task identity and contract lookup
- `data/curated/rescue_tasks/contracts/*.json`: validated rescue task contract artifacts
- `data/benchmark/generated/scz_small/snapshot_manifest.json`: generated `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv`: generated `benchmark_cohort_labels`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/*.json`: generated `benchmark_model_run_manifest` files, one per executed baseline
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: generated `benchmark_metric_output_payload` files
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: generated `benchmark_confidence_interval_payload` files

Operator notes:

- Re-run the three commands in order whenever the snapshot request, archive descriptors, future-outcome labels, code version, or benchmark parameters change.
- Re-running the snapshot or cohort commands overwrites the manifest and label files at the same paths.
- Re-running `run-benchmark` writes run-id keyed payload directories. Changing code version or parameters changes the run id. Identical inputs overwrite the same run-id files.
- The checked-in fixture intentionally stays small: it includes archived `PGC`, `Open Targets`, and `PsychENCODE` inputs, while `SCHEMA` and `ChEMBL` remain explicit exclusions at the `2024-06-30` cutoff.
- Everything under `data/benchmark/generated/` is locally generated. The repo checks in the fixture inputs, not the generated benchmark outputs.

## Artifact Schemas

Current benchmark, ledger, and `v1` artifact families are registered under
`schemas/artifact_schemas/`.

The runtime loader and validator surface lives under `scz_target_engine.artifacts`.
It can load and validate:

- `benchmark_snapshot_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`
- `rescue_task_contract`
- `gene_target_ledgers`
- `decision_vectors_v1`
- `domain_head_rankings_v1`

See [docs/artifact_schemas.md](docs/artifact_schemas.md) for details and example usage.

## Repo Layout

- [config/v0.toml](config/v0.toml): scoring and build config
- [config/README.md](config/README.md): config tree scaffolding and migration posture
- [docs/artifact_schemas.md](docs/artifact_schemas.md): registered output schemas plus validation usage
- [docs/claim.md](docs/claim.md): current capability and claim boundary for `v0`
- [docs/ontology.md](docs/ontology.md): implementation-ready domain/stage vocabulary consumed by the additive `v1` head layer
- [docs/program_history.md](docs/program_history.md): curated landmark program-history schema and curation rules
- [docs/scoring_contract.md](docs/scoring_contract.md): methodological contract for `v0`
- [docs/benchmarking.md](docs/benchmarking.md): frozen benchmark question, canonical workflow, artifact layout, and current runner boundary
- [docs/rescue_tasks.md](docs/rescue_tasks.md): dedicated rescue registry shape, contract surface, and leakage boundary
- [data/benchmark](data/benchmark): checked-in benchmark fixtures plus the canonical generated benchmark output path under `data/benchmark/generated/`
- [docs/ledger_contract.md](docs/ledger_contract.md): structured failure and directionality ledger contract
- [schemas/artifact_schemas](schemas/artifact_schemas): registered schema files for current emitted artifact families
- [docs/source_manifest.md](docs/source_manifest.md): source roles and intended upstream inputs
- [docs/opentargets.md](docs/opentargets.md): Open Targets fetch contract
- [docs/chembl.md](docs/chembl.md): ChEMBL fetch contract
- [docs/pgc.md](docs/pgc.md): PGC scz2022 fetch contract
- [docs/schema.md](docs/schema.md): SCHEMA fetch contract
- [docs/psychencode.md](docs/psychencode.md): PsychENCODE / BrainSCOPE fetch contract
- [docs/prep.md](docs/prep.md): source join and curation contract
- [data/curated/program_history](data/curated/program_history): landmark program-history substrate with source URLs and failure-taxonomy labels
- [examples/v0/input](examples/v0/input): seed shortlist plus curated gene and module snapshots
- [src/scz_target_engine](src/scz_target_engine): scoring engine

## Input Tables

### Gene Evidence

Required columns:

- `entity_id`
- `entity_label`
- `common_variant_support`
- `rare_variant_support`
- `cell_state_support`
- `developmental_regulatory_support`
- `tractability_compoundability`
- `generic_platform_baseline`

Optional free-text columns are preserved in reports.

Prepared gene tables also carry:

- rolled-up `v0` layer inputs that remain numerically stable
- identity and provenance metadata such as `primary_gene_id`, `seed_entity_id`,
  `source_entity_ids_json`, `match_confidence`, and `match_provenance_json`
- primitive source-field groups for `PGC`, `SCHEMA`, `PsychENCODE`, `Open Targets`, and `ChEMBL`

The legacy `canonical_entity_id` column is kept temporarily as a deprecated alias to
`primary_gene_id`. See [docs/prep.md](docs/prep.md) for the prepared-table contract.

The non-seed candidate registry uses the same provenance fields, keeps `seed_entity_id` blank,
and records which full-universe-capable sources currently back each candidate row.

### Module Evidence

Required columns:

- `entity_id`
- `entity_label`
- `member_gene_genetic_enrichment`
- `cell_state_specificity`
- `developmental_regulatory_relevance`

Prepared module tables also carry admissibility and provenance context such as
`psychencode_module_genetically_supported_gene_count`,
`psychencode_module_member_source_breakdown_json`, and
`psychencode_module_admissibility_json`.

### Warning Overlays

Required columns:

- `entity_type`
- `entity_id`
- `severity`
- `warning_kind`
- `warning_text`

## Design Principle

The output must give a researcher a transparent basis for comparing public evidence, spotting fragile rankings, and deciding what warrants deeper domain-specific review.
