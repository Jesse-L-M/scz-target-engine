import csv
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys

from scz_target_engine.rescue.tasks import (
    NPC_SIGNATURE_REVERSAL_TASK_ID,
    NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID,
    materialize_npc_signature_reversal_baseline_pack,
    materialize_npc_signature_reversal_run,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_materialize_npc_signature_reversal_run_uses_only_frozen_bundle(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "npc_run"
    result = materialize_npc_signature_reversal_run(output_dir=output_dir)

    assert result["task_id"] == NPC_SIGNATURE_REVERSAL_TASK_ID
    assert result["baseline_ids"] == [
        "signature_weight_only",
        "reversal_fraction_only",
        "max_abs_reversal_rzs_only",
        "reversal_drug_count_only",
    ]
    assert result["model_id"] == NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID
    assert result["leakage_boundary"] == {
        "policy_id": "strict_rescue_task_boundary_v1",
        "raw_runtime_ingestion_enabled": False,
        "ranking_inputs_require_frozen_artifacts": True,
        "evaluation_labels_used_only_for_offline_metrics": True,
        "evaluation_labels_emitted_in_predictions": False,
        "principal_split": "test",
    }
    assert result["input_artifacts"]["ranking_input"]["path"].endswith(
        "data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/"
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31.csv"
    )
    assert result["input_artifacts"]["evaluation_target"]["path"].endswith(
        "data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/"
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23.csv"
    )
    assert "data/raw/" not in json.dumps(result, sort_keys=True)

    predictions_file = Path(result["outputs"]["predictions_file"])
    assert predictions_file.exists()
    prediction_rows = _read_csv_rows(predictions_file)
    assert len(prediction_rows) == 15614
    assert prediction_rows[0]["rank"] == "1"
    assert prediction_rows[0]["gene_id"]
    assert prediction_rows[0]["npc_abs_log_fc_priority_score"]
    assert "rescue_positive_label" not in prediction_rows[0]
    assert "label_status" not in prediction_rows[0]
    assert "evidence_source_id" not in prediction_rows[0]
    assert Path(result["comparison_outputs"]["comparison_rows_file"]).exists()
    assert Path(result["comparison_outputs"]["comparison_summary_file"]).exists()
    assert len(result["comparison_rows"]) == 20


def test_npc_signature_reversal_run_declares_test_supported_primary_scorer(
    tmp_path: Path,
) -> None:
    result = materialize_npc_signature_reversal_run(output_dir=tmp_path / "npc_run")
    all_slice = result["evaluation"]["slices"]["all"]
    test_slice = result["evaluation"]["slices"]["test"]
    all_scorers = all_slice["scorers"]
    test_scorers = test_slice["scorers"]

    model_metrics = all_scorers[NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID]["metrics"]
    signature_metrics = all_scorers["signature_weight_only"]["metrics"]
    reversal_metrics = all_scorers["reversal_fraction_only"]["metrics"]
    test_model_metrics = test_scorers[NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID]["metrics"]

    assert all_slice["entity_count"] == 15614
    assert all_slice["positive_count"] == 14
    assert test_slice["positive_count"] == 2

    assert model_metrics == {
        "average_precision": 0.00246,
        "mean_reciprocal_rank": 0.012658,
        "first_positive_rank": 79,
        "precision_at_50": 0.0,
        "recall_at_50": 0.0,
        "precision_at_100": 0.01,
        "recall_at_100": 0.071429,
    }
    assert test_model_metrics == {
        "average_precision": 0.003401,
        "mean_reciprocal_rank": 0.002695,
        "first_positive_rank": 371,
        "precision_at_50": 0.0,
        "recall_at_50": 0.0,
        "precision_at_100": 0.0,
        "recall_at_100": 0.0,
    }
    assert test_model_metrics["average_precision"] >= max(
        scorer_payload["metrics"]["average_precision"]
        for scorer_id, scorer_payload in test_scorers.items()
        if scorer_id != NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID
    )
    assert model_metrics["average_precision"] < signature_metrics["average_precision"]
    assert signature_metrics["average_precision"] > reversal_metrics["average_precision"]


def test_npc_signature_reversal_run_no_longer_exposes_scorers_override() -> None:
    assert "scorers" not in inspect.signature(
        materialize_npc_signature_reversal_run
    ).parameters


def test_npc_signature_reversal_cli_runs_end_to_end(tmp_path: Path) -> None:
    output_dir = tmp_path / "cli_run"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scz_target_engine.cli",
            "rescue",
            "npc-signature-reversal",
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    payload = json.loads(result.stdout)

    assert payload["task_id"] == NPC_SIGNATURE_REVERSAL_TASK_ID
    assert Path(payload["outputs"]["predictions_file"]).exists()
    assert Path(payload["outputs"]["summary_file"]).exists()
    assert payload["evaluation"]["principal_split"] == "test"


def test_npc_signature_reversal_baseline_pack_alias_runs(
    tmp_path: Path,
) -> None:
    result = materialize_npc_signature_reversal_baseline_pack(
        output_dir=tmp_path / "npc_pack"
    )

    assert result["task_id"] == NPC_SIGNATURE_REVERSAL_TASK_ID
    assert Path(result["comparison_outputs"]["comparison_rows_file"]).exists()
