import json
import tarfile
from pathlib import Path

from scz_target_engine.hidden_eval import (
    materialize_hidden_eval_simulation,
    materialize_hidden_eval_submission_archive,
    materialize_rescue_hidden_eval_task_package,
)
from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.rescue.tasks import (
    materialize_glutamatergic_convergence_rescue_evaluation,
)


GLUTAMATERGIC_TASK_ID = "glutamatergic_convergence_rescue_task"
GLUTAMATERGIC_RANKING_INPUTS_PATH = Path(
    "data/curated/rescue_tasks/glutamatergic_convergence/frozen/"
    "glutamatergic_convergence_ranking_inputs_2025_01_15.csv"
)
GLUTAMATERGIC_EVALUATION_LABELS_PATH = Path(
    "data/curated/rescue_tasks/glutamatergic_convergence/frozen/"
    "glutamatergic_convergence_evaluation_labels_2025_06_30.csv"
)
EXAMPLE_SUBMISSION_PATH = Path(
    "examples/benchmark_submissions/glutamatergic_convergence_hidden_eval/"
    "ranked_predictions.csv"
)


def test_hidden_eval_task_package_copies_real_governed_ranking_input(
    tmp_path: Path,
) -> None:
    result = materialize_rescue_hidden_eval_task_package(
        task_id=GLUTAMATERGIC_TASK_ID,
        output_dir=tmp_path / "hidden_eval_task",
    )

    manifest = read_json(Path(result["task_manifest_file"]))
    ranking_rows = read_csv_rows(Path(result["ranking_input_file"]))
    expected_ranking_rows = read_csv_rows(GLUTAMATERGIC_RANKING_INPUTS_PATH)

    assert ranking_rows == expected_ranking_rows
    assert manifest["public_artifacts"]["ranking_dataset_id"] == (
        "glutamatergic_convergence_ranking_inputs_2025_01_15"
    )
    assert manifest["submission_contract"]["required_columns"] == [
        "task_id",
        "gene_id",
        "rank",
    ]
    assert "evaluation_target" in " ".join(
        manifest["protocol_boundaries"]["hidden_evaluator_only"]
    )
    manifest_text = json.dumps(manifest, sort_keys=True)
    assert "evaluation_target_file" not in manifest_text
    assert GLUTAMATERGIC_EVALUATION_LABELS_PATH.name not in manifest_text


def test_hidden_eval_submission_packager_accepts_shipped_rescue_predictions(
    tmp_path: Path,
) -> None:
    package_result = materialize_rescue_hidden_eval_task_package(
        task_id=GLUTAMATERGIC_TASK_ID,
        output_dir=tmp_path / "hidden_eval_task",
    )
    run_result = materialize_glutamatergic_convergence_rescue_evaluation(
        output_dir=tmp_path / "glutamatergic_run"
    )
    archive_result = materialize_hidden_eval_submission_archive(
        task_package_dir=Path(package_result["output_dir"]),
        predictions_file=Path(run_result["predictions_file"]),
        output_file=tmp_path / "submission.tar.gz",
        submitter_id="partner-demo",
        submission_id="demo-baseline-v1",
        scorer_id="shipped-glutamatergic-baseline",
    )

    assert Path(archive_result["output_file"]).exists()
    with tarfile.open(Path(archive_result["output_file"]), "r:gz") as archive:
        submission_manifest = json.loads(
            archive.extractfile("submission_manifest.json").read().decode("utf-8")
        )
        assert submission_manifest["prediction_row_count"] == 4
        assert "baseline_id" in submission_manifest["submitted_columns"]
        assert "rescue_score" in submission_manifest["submitted_columns"]


def test_hidden_eval_simulation_scores_example_submission_against_real_bundle(
    tmp_path: Path,
) -> None:
    package_result = materialize_rescue_hidden_eval_task_package(
        task_id=GLUTAMATERGIC_TASK_ID,
        output_dir=tmp_path / "hidden_eval_task",
    )
    archive_result = materialize_hidden_eval_submission_archive(
        task_package_dir=Path(package_result["output_dir"]),
        predictions_file=EXAMPLE_SUBMISSION_PATH,
        output_file=tmp_path / "example-submission.tar.gz",
        submitter_id="partner-demo",
        submission_id="example-submission-v1",
        scorer_id="glutamatergic-example-baseline",
    )
    simulation_result = materialize_hidden_eval_simulation(
        task_package_dir=Path(package_result["output_dir"]),
        submission_file=Path(archive_result["output_file"]),
        output_dir=tmp_path / "hidden_eval_run",
    )

    public_scorecard = read_json(Path(simulation_result["public_scorecard_file"]))
    internal_rows = read_csv_rows(Path(simulation_result["internal_evaluation_rows_file"]))
    simulation_manifest = read_json(Path(simulation_result["simulation_manifest_file"]))

    assert public_scorecard["metric_values"] == {
        "average_precision_any_positive_outcome": 1.0,
        "mean_reciprocal_rank_any_positive_outcome": 1.0,
        "precision_at_1_any_positive_outcome": 1.0,
        "precision_at_3_any_positive_outcome": 0.666667,
        "precision_at_5_any_positive_outcome": 0.5,
        "recall_at_1_any_positive_outcome": 0.5,
        "recall_at_3_any_positive_outcome": 1.0,
        "recall_at_5_any_positive_outcome": 1.0,
    }
    assert {
        row["gene_symbol"]: row["evaluation_label"] for row in internal_rows
    } == {
        "GRIN2A": "1",
        "GRM3": "1",
        "GRIA1": "0",
        "GRM5": "0",
    }
    assert "evaluation_label" not in json.dumps(public_scorecard, sort_keys=True)
    assert Path(simulation_manifest["evaluation_target_file"]).name == (
        GLUTAMATERGIC_EVALUATION_LABELS_PATH.name
    )
