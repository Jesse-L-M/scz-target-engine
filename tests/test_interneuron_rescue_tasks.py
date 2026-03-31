from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path
import subprocess

from scz_target_engine.rescue import (
    DEFAULT_INTERNEURON_BASELINE_IDS,
    VALID_INTERNEURON_AXIS_IDS,
    build_interneuron_axis_predictions,
    evaluate_interneuron_axis_predictions,
    load_frozen_rescue_governance_bundle,
    load_interneuron_axis_task_data,
    materialize_interneuron_rescue_lane,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
INTERNEURON_TASK_CARD_PATH = (
    REPO_ROOT
    / "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/task_card.json"
)


def _assert_emitted_text_has_no_dereferenceable_pointers(
    text: str,
    *,
    task_data,
    output_root: Path,
) -> None:
    ranking_input_path = str(task_data.ranking_input.path)
    evaluation_dataset_id = task_data.evaluation_target.card.dataset_id
    evaluation_dataset_path = str(task_data.evaluation_target.path)
    evaluation_dataset_sha256 = sha256(task_data.evaluation_target.path.read_bytes()).hexdigest()
    freeze_manifest_id = (
        task_data.frozen_bundle.governance.freeze_manifests[0].freeze_manifest_id
    )
    split_manifest_id = task_data.split_manifest.split_manifest_id
    forbidden_tokens = {
        '"task_card_path"',
        '"output_dir"',
        '"prediction_file"',
        '"freeze_manifest_id"',
        '"split_manifest_id"',
        '"evaluation_dataset_id"',
        '"input_artifacts"',
        str(INTERNEURON_TASK_CARD_PATH),
        str(output_root.resolve()),
        ranking_input_path,
        evaluation_dataset_id,
        evaluation_dataset_path,
        evaluation_dataset_sha256,
        freeze_manifest_id,
        split_manifest_id,
    }
    for token in forbidden_tokens:
        assert token not in text


def test_load_frozen_interneuron_governance_bundle_reads_all_governed_datasets() -> None:
    bundle = load_frozen_rescue_governance_bundle(
        task_card_path=INTERNEURON_TASK_CARD_PATH
    )

    assert bundle.governance.task_card.task_id == "interneuron_gene_rescue_task"
    assert {dataset.card.dataset_id for dataset in bundle.datasets} == {
        "interneuron_synapse_ranking_inputs_2023_12_31",
        "interneuron_arbor_ranking_inputs_2023_12_31",
        "interneuron_followup_labels_2026_03_31",
    }
    assert {
        dataset.card.dataset_id: len(dataset.rows)
        for dataset in bundle.datasets
    } == {
        "interneuron_synapse_ranking_inputs_2023_12_31": 5,
        "interneuron_arbor_ranking_inputs_2023_12_31": 4,
        "interneuron_followup_labels_2026_03_31": 8,
    }


def test_synapse_predictions_do_not_change_when_evaluation_labels_change() -> None:
    task_data = load_interneuron_axis_task_data("interneuron_synapse")
    original_predictions = build_interneuron_axis_predictions(
        task_data,
        baseline_id="recent_publication_first",
    )

    mutated_evaluation_target = replace(
        task_data.evaluation_target,
        rows=tuple(
            {
                **row,
                "followup_support_label": (
                    "1"
                    if row["followup_support_label"] == "0"
                    else "0"
                ),
            }
            for row in task_data.evaluation_target.rows
        ),
    )
    mutated_task_data = replace(
        task_data,
        evaluation_target=mutated_evaluation_target,
    )

    mutated_predictions = build_interneuron_axis_predictions(
        mutated_task_data,
        baseline_id="recent_publication_first",
    )

    assert mutated_predictions == original_predictions


def test_load_interneuron_axis_task_data_materializes_manifest_splits() -> None:
    synapse_task = load_interneuron_axis_task_data("interneuron_synapse")
    arbor_task = load_interneuron_axis_task_data("interneuron_arbor")

    assert synapse_task.split_counts == {
        "train": 3,
        "validation": 1,
        "test": 1,
    }
    assert arbor_task.split_counts == {
        "train": 2,
        "validation": 1,
        "test": 1,
    }
    assert "data/processed/rescue" in str(synapse_task.ranking_input.path)
    assert "data/raw/rescue" not in str(synapse_task.ranking_input.path)
    assert "followup_support_label" not in synapse_task.ranking_input.columns


def test_offline_evaluation_remains_available_in_memory_only() -> None:
    task_data = load_interneuron_axis_task_data("interneuron_synapse")
    prediction_rows = build_interneuron_axis_predictions(
        task_data,
        baseline_id="frozen_priority_rank",
    )

    evaluation_summaries = evaluate_interneuron_axis_predictions(
        task_data,
        prediction_rows=prediction_rows,
    )

    assert set(evaluation_summaries) == {"validation", "test", "heldout"}
    assert evaluation_summaries["heldout"]["positive_count"] == 1
    assert "metric_values" in evaluation_summaries["heldout"]


def test_materialize_interneuron_rescue_lane_writes_predictions_and_summaries(
    tmp_path: Path,
) -> None:
    result = materialize_interneuron_rescue_lane(tmp_path)

    assert result["task_id"] == "interneuron_gene_rescue_task"
    assert set(result["axis_ids"]) == set(VALID_INTERNEURON_AXIS_IDS)
    assert set(result["baseline_ids"]) == set(DEFAULT_INTERNEURON_BASELINE_IDS)

    for axis_summary in result["axis_runs"]:
        axis_output_dir = tmp_path / axis_summary["axis_id"]
        assert axis_output_dir.exists()
        assert set(axis_summary["baseline_ids"]) == set(DEFAULT_INTERNEURON_BASELINE_IDS)
        for run_summary in axis_summary["runs"]:
            task_data = load_interneuron_axis_task_data(run_summary["axis_id"])
            prediction_file = (
                axis_output_dir / run_summary["baseline_id"] / "predictions.csv"
            )
            summary_file = axis_output_dir / run_summary["baseline_id"] / "summary.json"
            assert prediction_file.exists()
            assert summary_file.exists()

            summary_text = summary_file.read_text(encoding="utf-8")
            written_summary = json.loads(summary_file.read_text(encoding="utf-8"))
            assert written_summary["leakage_boundary_policy_id"] == (
                "strict_rescue_task_boundary_v1"
            )
            assert written_summary["split_counts"]["test"] == 1
            assert written_summary["offline_evaluation"]["executed"] is True
            assert "evaluation_summaries" not in written_summary
            assert "evaluation_dataset_id" not in written_summary
            assert "input_artifacts" not in written_summary
            assert "freeze_manifest_id" not in written_summary
            assert "split_manifest_id" not in written_summary
            assert "prediction_file" not in written_summary
            assert written_summary["ranking_input_artifact"]["dataset_id"] == (
                task_data.ranking_input.card.dataset_id
            )
            assert written_summary["ranking_input_artifact"]["governed_sha256"]
            assert "path" not in written_summary["ranking_input_artifact"]
            assert "positive_gene_ids" not in summary_text
            assert "ranked_gene_ids" not in summary_text
            assert "metric_values" not in summary_text
            _assert_emitted_text_has_no_dereferenceable_pointers(
                summary_text,
                task_data=task_data,
                output_root=tmp_path,
            )

        axis_summary_text = (axis_output_dir / "axis_summary.json").read_text(
            encoding="utf-8"
        )
        _assert_emitted_text_has_no_dereferenceable_pointers(
            axis_summary_text,
            task_data=load_interneuron_axis_task_data(axis_summary["axis_id"]),
            output_root=tmp_path,
        )

    lane_summary_text = (tmp_path / "lane_summary.json").read_text(encoding="utf-8")
    assert "evaluation_summaries" not in lane_summary_text
    assert "evaluation_dataset_id" not in lane_summary_text
    assert "input_artifacts" not in lane_summary_text
    assert "positive_gene_ids" not in lane_summary_text
    assert "metric_values" not in lane_summary_text
    assert "task_card_path" not in result
    assert "output_dir" not in result
    assert all("output_dir" not in axis_summary for axis_summary in result["axis_runs"])
    for axis_id in VALID_INTERNEURON_AXIS_IDS:
        _assert_emitted_text_has_no_dereferenceable_pointers(
            lane_summary_text,
            task_data=load_interneuron_axis_task_data(axis_id),
            output_root=tmp_path,
        )


def test_interneuron_run_script_executes_synapse_axis(tmp_path: Path) -> None:
    output_dir = tmp_path / "script_run"
    result = subprocess.run(
        [
            "python3",
            "scripts/rescue/run_interneuron_rescue_lane.py",
            "--output-dir",
            str(output_dir),
            "--axis",
            "interneuron_synapse",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["axis_ids"] == ["interneuron_synapse"]
    assert set(payload["baseline_ids"]) == set(DEFAULT_INTERNEURON_BASELINE_IDS)
    assert (output_dir / "lane_summary.json").exists()
    assert "evaluation_summaries" not in result.stdout
    assert "evaluation_dataset_id" not in result.stdout
    assert "input_artifacts" not in result.stdout
    _assert_emitted_text_has_no_dereferenceable_pointers(
        result.stdout,
        task_data=load_interneuron_axis_task_data("interneuron_synapse"),
        output_root=output_dir,
    )
