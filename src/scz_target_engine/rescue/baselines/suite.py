from __future__ import annotations

from pathlib import Path

from scz_target_engine.io import write_json
from scz_target_engine.rescue.baselines.reporting import (
    RescueComparisonRow,
    comparison_rows_to_dicts,
    write_rescue_comparison_rows,
)
from scz_target_engine.rescue.tasks import (
    materialize_glutamatergic_convergence_baseline_pack,
    materialize_interneuron_rescue_baseline_pack,
    materialize_npc_signature_reversal_baseline_pack,
)


def _task_comparison_rows(
    task_result: dict[str, object],
) -> tuple[RescueComparisonRow, ...]:
    raw_rows = task_result.get("comparison_rows")
    if not isinstance(raw_rows, tuple):
        raise ValueError("task_result must expose tuple comparison_rows")
    if not all(isinstance(row, RescueComparisonRow) for row in raw_rows):
        raise ValueError("comparison_rows must contain RescueComparisonRow entries")
    return raw_rows


def materialize_rescue_baseline_suite(
    output_dir: Path,
) -> dict[str, object]:
    resolved_output_dir = output_dir.resolve()
    glutamatergic_result = materialize_glutamatergic_convergence_baseline_pack(
        output_dir=resolved_output_dir / "glutamatergic_convergence_rescue_task"
    )
    interneuron_result = materialize_interneuron_rescue_baseline_pack(
        output_dir=resolved_output_dir / "interneuron_gene_rescue_task"
    )
    npc_result = materialize_npc_signature_reversal_baseline_pack(
        output_dir=resolved_output_dir / "scz_npc_signature_reversal_rescue_task"
    )

    comparison_rows = (
        _task_comparison_rows(glutamatergic_result)
        + _task_comparison_rows(interneuron_result)
        + _task_comparison_rows(npc_result)
    )
    suite_rows_file = resolved_output_dir / "suite_comparison_rows.csv"
    suite_summary_file = resolved_output_dir / "suite_summary.json"
    write_rescue_comparison_rows(suite_rows_file, comparison_rows)
    serialized_rows, fieldnames = comparison_rows_to_dicts(comparison_rows)
    summary_payload = {
        "task_run_count": 3,
        "comparison_row_count": len(comparison_rows),
        "comparison_fieldnames": fieldnames,
        "task_runs": [
            {
                key: value
                for key, value in glutamatergic_result.items()
                if key != "comparison_rows"
            },
            {
                key: value
                for key, value in interneuron_result.items()
                if key != "comparison_rows"
            },
            {
                key: value
                for key, value in npc_result.items()
                if key != "comparison_rows"
            },
        ],
        "comparison_rows": serialized_rows,
        "suite_rows_file": str(suite_rows_file),
    }
    write_json(suite_summary_file, summary_payload)
    return {
        "output_dir": str(resolved_output_dir),
        "comparison_rows_file": str(suite_rows_file),
        "summary_file": str(suite_summary_file),
        "task_runs": summary_payload["task_runs"],
    }


__all__ = ["materialize_rescue_baseline_suite"]
