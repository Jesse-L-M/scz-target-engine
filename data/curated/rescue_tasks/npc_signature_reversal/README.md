# NPC Signature-Reversal Rescue Task

This task family packages the schizophrenia NPC signature-reversal rescue lane into
checked-in frozen artifacts so downstream code does not touch raw source logic.

Key paths:
- `data/raw/rescue/npc_signature_reversal/`
- `data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/`
- `data/curated/rescue_tasks/contracts/scz_npc_signature_reversal_rescue_task.json`
- `data/curated/rescue_tasks/governance/scz_npc_signature_reversal_rescue_task/`

Example load path:

```python
from scz_target_engine.rescue import load_frozen_rescue_task_bundle

bundle = load_frozen_rescue_task_bundle(
    rescue_task_id="scz_npc_signature_reversal_rescue_task"
)
assert bundle.ranking_input.card.dataset_id == (
    "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
)
assert bundle.evaluation_target.card.dataset_id == (
    "scz_npc_signature_reversal_evaluation_labels_2022_02_23"
)
```

The ranking dataset is entirely pre-cutoff. The evaluation dataset carries sparse
post-cutoff positives plus unlabeled background rows only.
