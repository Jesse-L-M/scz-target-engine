from pathlib import Path

from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.rescue.baselines import materialize_rescue_baseline_suite


def test_rescue_baseline_suite_materializes_all_shipped_tasks(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "rescue_suite"
    result = materialize_rescue_baseline_suite(output_dir=output_dir)

    comparison_rows = read_csv_rows(Path(result["comparison_rows_file"]))
    summary_payload = read_json(Path(result["summary_file"]))

    assert summary_payload["task_run_count"] == 3
    assert summary_payload["comparison_row_count"] == 54
    assert len(comparison_rows) == 54
    assert {row["task_id"] for row in comparison_rows} == {
        "glutamatergic_convergence_rescue_task",
        "interneuron_gene_rescue_task",
        "scz_npc_signature_reversal_rescue_task",
    }
    assert (
        output_dir
        / "glutamatergic_convergence_rescue_task"
        / "baseline_comparison_summary.json"
    ).exists()
    assert (
        output_dir
        / "interneuron_gene_rescue_task"
        / "lane_comparison_summary.json"
    ).exists()
    assert (
        output_dir
        / "scz_npc_signature_reversal_rescue_task"
        / "baseline_comparison_summary.json"
    ).exists()
