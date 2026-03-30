# Rescue Task Contracts

`PR-40` adds a dedicated rescue-task registry and a validated contract surface without
changing the shipped benchmark registry path.

## Registry Split

- `data/curated/rescue_tasks/task_registry.csv` remains the benchmark suite/task source
  of truth for the existing `scz_translational_task`
- `data/curated/rescue_tasks/rescue_task_registry.csv` is the new rescue-task registry
- rescue task contracts live as JSON artifacts under
  `data/curated/rescue_tasks/contracts/`

This split is intentional. Rescue tasks and benchmark tasks now both have registry
identity, but rescue tasks do not silently inherit the benchmark registry or benchmark
question contract.

## Rescue Registry Shape

Each row in `rescue_task_registry.csv` carries:

- `suite_id`, `suite_label`
- `task_id`, `task_label`
- `task_type`
- `disease`
- `entity_type`
- `contract_scope`
- `contract_file`
- `registry_status`
- `notes`

The registry row is only the index. The contract file is the source of truth for the
task identity and future artifact expectations.

## Rescue Contract Shape

Each `rescue_task_contract` JSON artifact carries:

- stable suite/task identity and labels
- `contract_version`
- disease and entity type
- `contract_scope`, which defaults to `rescue_only`
- `artifact_contracts`, an explicit list of future task inputs and outputs
- `leakage_boundary`, a strict policy block that already forbids post-cutoff ranking
  inputs and evaluation-label reuse

Current artifact channels are:

- `ranking_input`: consumed, must stay `pre_cutoff`
- `evaluation_target`: consumed, must stay `post_cutoff`
- `task_output`: emitted, must stay `derived`
- `task_metadata`: optional supporting metadata that must not depend on post-cutoff
  adjudications

## Leakage Boundary Before PR-40A

The rescue leakage policy is explicit now even though freeze manifests and governance
records are not shipped yet:

- explicit cutoff is required
- explicit artifact contracts are required
- post-cutoff artifacts are forbidden in ranking inputs
- evaluation labels are forbidden in ranking inputs
- `freeze_manifest_required` is pinned to `false`
- `freeze_manifest_policy` is pinned to `deferred_until_pr40a`
- unfrozen current-head state is forbidden

`PR-40A` can extend this contract with governance, cards, split manifests, and freeze
manifests without having to redefine task identity.
