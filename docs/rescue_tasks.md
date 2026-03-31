# Rescue Task Contracts And Governance

`PR-40` adds a dedicated rescue-task registry and a validated contract surface without
changing the shipped benchmark registry path. `PR-40A` turns that registry-backed
contract into a governed data lane with schema-validated task cards, dataset cards,
freeze manifests, split manifests, and raw-to-frozen lineage.

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
- `task_card_file`
- `registry_status`
- `notes`

The registry row is only the index. The contract file is the source of truth for the
task identity and future artifact expectations, and the task card file is the
governance entry point that must validate before the normal rescue registry path will
return that task contract.

## Rescue Contract Shape

Each `rescue_task_contract` JSON artifact carries:

- stable suite/task identity and labels
- `contract_version`
- disease and entity type
- `contract_scope`, which defaults to `rescue_only`
- `artifact_contracts`, an explicit list of future task inputs and outputs
- `leakage_boundary`, a strict policy block that already forbids post-cutoff ranking
  inputs and evaluation-label reuse while requiring governance artifacts before
  rescue data work can land

Current artifact channels are:

- `ranking_input`: consumed, must stay `pre_cutoff`
- `evaluation_target`: consumed, must stay `post_cutoff`
- `task_output`: emitted, must stay `derived`
- `task_metadata`: optional supporting metadata that must not depend on post-cutoff
  adjudications

## Governed Data Lane

The checked-in example family lives under:

- `data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/task_card.json`
- `data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/dataset_cards/`
- `data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/freeze_manifests/`
- `data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/split_manifests/`
- `data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/lineage/`

The task card is the top-level governance entry point. It binds one rescue task
contract to the checked-in dataset cards, freeze manifest, split manifest, and
raw-to-frozen lineage artifacts that later rescue-data PRs must materialize.

## Active Interneuron Family

`PR-40C` now ships one active rescue family for downstream interneuron work:

- `data/curated/rescue_tasks/contracts/interneuron_gene_rescue_task.json`
- `data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/task_card.json`
- `data/raw/rescue/interneuron_synapse/interneuron_synapse_candidate_snapshot_2023_12_31.csv`
- `data/raw/rescue/interneuron_arbor/interneuron_arbor_candidate_snapshot_2023_12_31.csv`
- `data/raw/rescue/interneuron_followup/interneuron_followup_adjudications_2026_03_31.csv`
- `data/processed/rescue/interneuron_gene_rescue_task/frozen/interneuron_synapse_ranking_inputs_2023_12_31.csv`
- `data/processed/rescue/interneuron_gene_rescue_task/frozen/interneuron_arbor_ranking_inputs_2023_12_31.csv`
- `data/processed/rescue/interneuron_gene_rescue_task/frozen/interneuron_followup_labels_2026_03_31.csv`

The active family keeps two ranking-input datasets (`synapse`, `arbor`) separate
under one task contract and binds both to a shared held-out post-cutoff follow-up
label table.

The checked-in example validation and load path is:

```bash
python3 scripts/rescue/load_interneuron_bundle.py
```

That script validates the full governance bundle from the task card and then opens
each checked-in frozen CSV referenced by the dataset cards.

## Leakage Boundary After PR-40A

The rescue leakage policy is explicit and now requires schema-validated governance
artifacts:

- explicit cutoff is required
- explicit artifact contracts are required
- post-cutoff artifacts are forbidden in ranking inputs
- evaluation labels are forbidden in ranking inputs
- `freeze_manifest_required` is pinned to `true`
- `freeze_manifest_policy` is pinned to `schema_validated_rescue_governance_v1`
- dataset cards are required
- a task card is required
- split manifests are required
- raw-to-frozen lineage is required
- unfrozen current-head state is forbidden

## What Later Rescue-Data PRs Must Emit

`PR-40B`, `PR-40C`, and `PR-40D` should treat the example family as the contract to
copy, not as loose documentation:

- update the existing task card from `example` to `active` or add a new task-family task card
- emit at least one `rescue_dataset_card` for each governed ranking input and
  evaluation target dataset
- emit a `rescue_freeze_manifest` that documents the cutoff, upstream raw snapshots,
  and frozen dataset outputs
- emit a `rescue_split_manifest` before any downstream training or evaluation code
  consumes the governed ranking inputs
- emit a `rescue_raw_to_frozen_lineage` artifact that traces raw snapshots through
  named transformation steps into every frozen dataset listed in the freeze manifest

The intended validation path is:

```python
from pathlib import Path

from scz_target_engine.rescue import validate_rescue_governance_bundle

bundle = validate_rescue_governance_bundle(
    Path(
        "data/curated/rescue_tasks/governance/"
        "example_scz_gene_rescue_task/task_card.json"
    )
)
assert bundle.task_card.task_id == "example_scz_gene_rescue_task"
```

The normal artifact path now enforces the same bundle checks. A
`load_artifact(..., artifact_name="rescue_task_card")` call fails if the referenced
dataset cards, freeze manifests, split manifests, or lineage artifacts are missing or
broken.

## Active NPC Freeze

`PR-40B` adds the first active rescue-data bundle:

- contract:
  `data/curated/rescue_tasks/contracts/scz_npc_signature_reversal_rescue_task.json`
- governance:
  `data/curated/rescue_tasks/governance/scz_npc_signature_reversal_rescue_task/`
- raw staged inputs:
  `data/raw/rescue/npc_signature_reversal/`
- frozen outputs:
  `data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/`

This task freezes a schizophrenia NPC gene universe at `2020-12-31` using only
pre-cutoff sources in the ranking inputs:

- 2017 hiPSC-derived NPC differential expression
- 2018 SZ_iPSC_NPC perturbation signatures

The evaluation labels come from a post-cutoff 2022 rescue paper and stay in a
separate governed dataset. The checked-in ranking CSV already includes deterministic
`split_name` assignments so downstream task code can load one pre-cutoff artifact and
avoid any raw extraction path.

The preferred load path for downstream rescue work is now:

```python
from scz_target_engine.rescue import load_frozen_rescue_task_bundle

bundle = load_frozen_rescue_task_bundle(
    rescue_task_id="scz_npc_signature_reversal_rescue_task"
)
assert bundle.ranking_input.path.name == (
    "scz_npc_signature_reversal_ranking_inputs_2020_12_31.csv"
)
assert bundle.evaluation_target.path.name == (
    "scz_npc_signature_reversal_evaluation_labels_2022_02_23.csv"
)
```

## Active Glutamatergic Convergence Lane

`PR-40D` adds a convergence-hub-grounded rescue family under:

- `data/curated/rescue_tasks/glutamatergic_convergence/`
- `data/raw/rescue/glutamatergic_convergence/`

The ranking-input freeze is anchored to the shipped convergence-hub framing generated
from the dedicated atlas fixture:

- `data/curated/atlas/glutamatergic_convergence_fixture/example_ingest_manifest.json`

The checked-in raw rescue chain is portable and self-contained in git. Its manifests
resolve without developer-local absolute paths or `.context` dependencies, and
regeneration from the checked-in fixture preserves that portable-path behavior.

Downstream rescue implementations must stay on the frozen CSV surface and must not
re-open either the raw rescue snapshots or the atlas fixture inputs.

```python
from scz_target_engine.rescue import load_glutamatergic_convergence_rescue_bundle

bundle = load_glutamatergic_convergence_rescue_bundle()
assert bundle.governance_bundle.task_card.task_id == (
    "glutamatergic_convergence_rescue_task"
)
assert len(bundle.ranking_input_rows) == 4
```
