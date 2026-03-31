from __future__ import annotations

from dataclasses import dataclass

from scz_target_engine.rescue.models.base import (
    RescueModelDefinition,
    RescueModelInput,
)


NPC_SIGNATURE_REVERSAL_TASK_ID = "scz_npc_signature_reversal_rescue_task"
NPC_ABS_LOG_FC_PRIORITY_MODEL_ID = "npc_abs_log_fc_priority_v1"


def _absolute_npc_log_fc(row: dict[str, str]) -> float:
    value = row.get("npc_log_fc")
    if value is None or not value.strip():
        raise ValueError("ranking row is missing npc_log_fc")
    return abs(float(value))


@dataclass(frozen=True)
class NpcAbsLogFcPriorityModelPlugin:
    definition: RescueModelDefinition = RescueModelDefinition(
        model_id=NPC_ABS_LOG_FC_PRIORITY_MODEL_ID,
        task_id=NPC_SIGNATURE_REVERSAL_TASK_ID,
        label="NPC absolute log fold-change priority v1",
        description=(
            "Ranks genes only by absolute frozen NPC disease log fold-change "
            "magnitude. It remains the only shipped NPC rescue model because it "
            "beats the task's simple baselines on the principal test split."
        ),
        leakage_rule=(
            "Consumes only the frozen pre-cutoff NPC ranking input columns and "
            "never receives held-out rescue labels."
        ),
        input_fields=("npc_log_fc",),
        admission_metric_names=(
            "average_precision",
            "mean_reciprocal_rank",
            "first_positive_rank",
        ),
        principal_split="test",
    )

    def rank_entities(
        self,
        model_input: RescueModelInput,
    ) -> tuple[str, ...]:
        model_input.require_columns(self.definition.input_fields)
        return tuple(
            row[model_input.entity_id_field]
            for row in sorted(
                model_input.ranking_rows,
                key=lambda row: (
                    -_absolute_npc_log_fc(row),
                    row[model_input.entity_id_field],
                ),
            )
        )


NPC_SIGNATURE_REVERSAL_MODEL_PLUGINS = (
    NpcAbsLogFcPriorityModelPlugin(),
)


__all__ = [
    "NPC_ABS_LOG_FC_PRIORITY_MODEL_ID",
    "NPC_SIGNATURE_REVERSAL_MODEL_PLUGINS",
    "NPC_SIGNATURE_REVERSAL_TASK_ID",
    "NpcAbsLogFcPriorityModelPlugin",
]
