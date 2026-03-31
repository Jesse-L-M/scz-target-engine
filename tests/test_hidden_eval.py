import hashlib
import json
import tarfile
from pathlib import Path

import pytest

from scz_target_engine.hidden_eval import (
    materialize_hidden_eval_simulation,
    materialize_hidden_eval_submission_archive,
    materialize_rescue_hidden_eval_task_package,
)
from scz_target_engine.hidden_eval.rescue import _build_operator_prediction_rows
from scz_target_engine.io import (
    read_csv_rows,
    read_csv_table,
    read_json,
    write_csv,
    write_json,
)
from scz_target_engine.rescue.tasks import (
    load_glutamatergic_convergence_rescue_task_bundle,
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
PUBLIC_SCORECARD_NOTES = (
    "Metrics, split summaries, and rank-conditioned display fields are withheld "
    "from the public scorecard because the shipped rescue task is small enough "
    "that those values would reveal held-out labels."
)


def _tamper_task_package_ranking_input(
    task_package_dir: Path,
    *,
    gene_id: str,
    replacement_symbol: str,
) -> None:
    ranking_input_path = task_package_dir / "ranking_input.csv"
    fieldnames, ranking_rows = read_csv_table(ranking_input_path)
    for row in ranking_rows:
        if row["gene_id"] == gene_id:
            row["gene_symbol"] = replacement_symbol
            break
    else:
        raise AssertionError(f"missing gene_id in ranking_input.csv: {gene_id}")

    write_csv(ranking_input_path, ranking_rows, fieldnames=fieldnames)

    manifest_path = task_package_dir / "task_manifest.json"
    manifest = read_json(manifest_path)
    assert isinstance(manifest, dict)
    public_artifacts = manifest["public_artifacts"]
    assert isinstance(public_artifacts, dict)
    public_artifacts["ranking_dataset_sha256"] = hashlib.sha256(
        ranking_input_path.read_bytes()
    ).hexdigest()
    write_json(manifest_path, manifest)


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


def test_hidden_eval_task_package_rejects_non_empty_output_dir(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "hidden_eval_task"
    output_dir.mkdir()
    stale_internal_rows = output_dir / "internal_evaluation_rows.csv"
    stale_internal_rows.write_text("gene_id\nstale\n", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=(
            "hidden-eval task package output_dir must be empty before "
            "materialization"
        ),
    ):
        materialize_rescue_hidden_eval_task_package(
            task_id=GLUTAMATERGIC_TASK_ID,
            output_dir=output_dir,
        )

    assert stale_internal_rows.exists()


def test_build_operator_prediction_rows_uses_operator_bundle_truth_for_display_fields() -> None:
    bundle = load_glutamatergic_convergence_rescue_task_bundle()
    governed_row = bundle.ranking_input.rows[0]

    operator_rows = _build_operator_prediction_rows(
        bundle=bundle,
        normalized_rows=[
            {
                "task_id": GLUTAMATERGIC_TASK_ID,
                "gene_id": governed_row["gene_id"],
                "rank": "1",
                "baseline_id": "submitter-provided-baseline",
                "gene_symbol": "TAMPERED_SYMBOL",
                "split_name": "tampered-split",
                "score": "0.99",
            }
        ],
        scorer_id="operator-baseline",
    )

    assert operator_rows == [
        {
            "task_id": GLUTAMATERGIC_TASK_ID,
            "gene_id": governed_row["gene_id"],
            "rank": "1",
            "score": "0.99",
            "baseline_id": "operator-baseline",
            "gene_symbol": governed_row["gene_symbol"],
            "split_name": governed_row["split_name"],
        }
    ]


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

    assert public_scorecard == {
        "schema_name": "hidden_eval_public_scorecard",
        "schema_version": "v1",
        "protocol_id": "rescue_hidden_eval_v1",
        "generated_at": public_scorecard["generated_at"],
        "task_id": GLUTAMATERGIC_TASK_ID,
        "package_id": simulation_result["package_id"],
        "submission_id": "example-submission-v1",
        "scorer_id": "glutamatergic-example-baseline",
        "submission_status": "accepted_and_scored",
        "candidate_count": 4,
        "public_report_tier": "receipt_only",
        "hidden_metrics_withheld": True,
        "notes": PUBLIC_SCORECARD_NOTES,
    }
    assert {
        row["gene_symbol"]: row["evaluation_label"] for row in internal_rows
    } == {
        "GRIN2A": "1",
        "GRM3": "1",
        "GRIA1": "0",
        "GRM5": "0",
    }
    public_scorecard_text = json.dumps(public_scorecard, sort_keys=True)
    assert "evaluation_label" not in public_scorecard_text
    assert "metric_values" not in public_scorecard_text
    assert "top_ranked_gene_symbols" not in public_scorecard_text
    assert Path(simulation_manifest["evaluation_target_file"]).name == (
        GLUTAMATERGIC_EVALUATION_LABELS_PATH.name
    )
    assert "metric_values" not in simulation_result
    assert simulation_result["submission_status"] == "accepted_and_scored"


def test_hidden_eval_simulation_rejects_tampered_public_task_package(
    tmp_path: Path,
) -> None:
    package_result = materialize_rescue_hidden_eval_task_package(
        task_id=GLUTAMATERGIC_TASK_ID,
        output_dir=tmp_path / "hidden_eval_task",
    )
    task_package_dir = Path(package_result["output_dir"])
    _tamper_task_package_ranking_input(
        task_package_dir,
        gene_id="ENSG00000183454",
        replacement_symbol="TAMPERED_TOP_GENE",
    )
    archive_result = materialize_hidden_eval_submission_archive(
        task_package_dir=task_package_dir,
        predictions_file=EXAMPLE_SUBMISSION_PATH,
        output_file=tmp_path / "example-submission.tar.gz",
        submitter_id="partner-demo",
        submission_id="example-submission-v1",
        scorer_id="glutamatergic-example-baseline",
    )

    with pytest.raises(
        ValueError,
        match=r"ranking_input.*operator-governed bundle",
    ):
        materialize_hidden_eval_simulation(
            task_package_dir=task_package_dir,
            submission_file=Path(archive_result["output_file"]),
            output_dir=tmp_path / "hidden_eval_run",
        )
