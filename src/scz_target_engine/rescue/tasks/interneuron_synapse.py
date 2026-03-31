from __future__ import annotations

from pathlib import Path

from scz_target_engine.rescue.tasks.interneuron_shared import (
    load_interneuron_axis_task_data,
    materialize_interneuron_axis_rescue_runs,
)


INTERNEURON_SYNAPSE_AXIS_ID = "interneuron_synapse"


def load_interneuron_synapse_task_data():
    return load_interneuron_axis_task_data(INTERNEURON_SYNAPSE_AXIS_ID)


def materialize_interneuron_synapse_rescue_runs(
    output_dir: Path,
    *,
    baseline_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    return materialize_interneuron_axis_rescue_runs(
        INTERNEURON_SYNAPSE_AXIS_ID,
        output_dir=output_dir,
        baseline_ids=baseline_ids,
    )


__all__ = [
    "INTERNEURON_SYNAPSE_AXIS_ID",
    "load_interneuron_synapse_task_data",
    "materialize_interneuron_synapse_rescue_runs",
]
