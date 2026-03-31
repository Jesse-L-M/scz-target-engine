import csv
import hashlib
import json
from collections.abc import Callable
from collections import Counter
from pathlib import Path

import pytest

from scz_target_engine.rescue import (
    GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH,
    load_frozen_rescue_task_bundle,
    validate_rescue_governance_bundle,
)


SOURCE_MANIFEST_PATH = Path(
    "data/raw/rescue/npc_signature_reversal/source_manifest.json"
)
TASK_CARD_PATH = Path(
    "data/curated/rescue_tasks/governance/"
    "scz_npc_signature_reversal_rescue_task/task_card.json"
)
RANKING_OUTPUT_PATH = Path(
    "data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/"
    "scz_npc_signature_reversal_ranking_inputs_2020_12_31.csv"
)
EVALUATION_OUTPUT_PATH = Path(
    "data/processed/rescue/scz_npc_signature_reversal_rescue_task/frozen/"
    "scz_npc_signature_reversal_evaluation_labels_2022_02_23.csv"
)
GLUTAMATERGIC_RANKING_OUTPUT_PATH = Path(
    "data/curated/rescue_tasks/glutamatergic_convergence/frozen/"
    "glutamatergic_convergence_ranking_inputs_2025_01_15.csv"
)
GLUTAMATERGIC_EVALUATION_OUTPUT_PATH = Path(
    "data/curated/rescue_tasks/glutamatergic_convergence/frozen/"
    "glutamatergic_convergence_evaluation_labels_2025_06_30.csv"
)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _build_temp_bundle(
    tmp_path: Path,
    *,
    ranking_rows: list[dict[str, str]] | None = None,
    evaluation_rows: list[dict[str, str]] | None = None,
    use_materialized_integrity: bool = True,
) -> Path:
    task_card_payload = json.loads(TASK_CARD_PATH.read_text(encoding="utf-8"))
    ranking_card_payload = json.loads(
        Path(task_card_payload["dataset_card_paths"][0]).read_text(encoding="utf-8")
    )
    evaluation_card_payload = json.loads(
        Path(task_card_payload["dataset_card_paths"][1]).read_text(encoding="utf-8")
    )
    freeze_manifest_payload = json.loads(
        Path(task_card_payload["freeze_manifest_paths"][0]).read_text(encoding="utf-8")
    )
    split_manifest_payload = json.loads(
        Path(task_card_payload["split_manifest_paths"][0]).read_text(encoding="utf-8")
    )
    lineage_payload = json.loads(
        Path(task_card_payload["lineage_paths"][0]).read_text(encoding="utf-8")
    )

    ranking_fieldnames, original_ranking_rows = _read_csv_rows(RANKING_OUTPUT_PATH)
    evaluation_fieldnames, original_evaluation_rows = _read_csv_rows(EVALUATION_OUTPUT_PATH)
    ranking_rows = original_ranking_rows if ranking_rows is None else ranking_rows
    evaluation_rows = original_evaluation_rows if evaluation_rows is None else evaluation_rows

    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    ranking_output = bundle_dir / RANKING_OUTPUT_PATH.name
    evaluation_output = bundle_dir / EVALUATION_OUTPUT_PATH.name
    _write_csv(ranking_output, ranking_fieldnames, ranking_rows)
    _write_csv(evaluation_output, evaluation_fieldnames, evaluation_rows)

    expected_ranking_path = ranking_output if use_materialized_integrity else RANKING_OUTPUT_PATH
    expected_evaluation_path = (
        evaluation_output if use_materialized_integrity else EVALUATION_OUTPUT_PATH
    )
    _, expected_ranking_rows = _read_csv_rows(expected_ranking_path)
    _, expected_evaluation_rows = _read_csv_rows(expected_evaluation_path)

    ranking_card_path = bundle_dir / "ranking_card.json"
    evaluation_card_path = bundle_dir / "evaluation_card.json"
    freeze_manifest_path = bundle_dir / "freeze_manifest.json"
    split_manifest_path = bundle_dir / "split_manifest.json"
    lineage_path = bundle_dir / "lineage.json"
    task_card_path = bundle_dir / "task_card.json"

    ranking_card_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    ranking_card_payload["lineage_path"] = str(lineage_path)
    ranking_card_payload["expected_output_path"] = str(ranking_output)
    evaluation_card_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    evaluation_card_payload["lineage_path"] = str(lineage_path)
    evaluation_card_payload["expected_output_path"] = str(evaluation_output)

    freeze_manifest_payload["frozen_datasets"][0]["dataset_card_path"] = str(ranking_card_path)
    freeze_manifest_payload["frozen_datasets"][0]["expected_output_path"] = str(ranking_output)
    freeze_manifest_payload["frozen_datasets"][0]["expected_sha256"] = _sha256_path(
        expected_ranking_path
    )
    freeze_manifest_payload["frozen_datasets"][0]["expected_row_count"] = len(
        expected_ranking_rows
    )
    freeze_manifest_payload["frozen_datasets"][1]["dataset_card_path"] = str(
        evaluation_card_path
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_output_path"] = str(
        evaluation_output
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_sha256"] = _sha256_path(
        expected_evaluation_path
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_row_count"] = len(
        expected_evaluation_rows
    )

    split_manifest_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    lineage_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    lineage_payload["frozen_datasets"][0]["dataset_card_path"] = str(ranking_card_path)
    lineage_payload["frozen_datasets"][1]["dataset_card_path"] = str(evaluation_card_path)

    task_card_payload["dataset_card_paths"] = [
        str(ranking_card_path),
        str(evaluation_card_path),
    ]
    task_card_payload["freeze_manifest_paths"] = [str(freeze_manifest_path)]
    task_card_payload["split_manifest_paths"] = [str(split_manifest_path)]
    task_card_payload["lineage_paths"] = [str(lineage_path)]

    ranking_card_path.write_text(
        json.dumps(ranking_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    evaluation_card_path.write_text(
        json.dumps(evaluation_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    freeze_manifest_path.write_text(
        json.dumps(freeze_manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    split_manifest_path.write_text(
        json.dumps(split_manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    lineage_path.write_text(
        json.dumps(lineage_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    task_card_path.write_text(
        json.dumps(task_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return task_card_path


def _build_multi_freeze_bundle(
    tmp_path: Path,
    *,
    alternate_freeze_mutator: Callable[[dict[str, object]], None] | None = None,
) -> Path:
    task_card_path = _build_temp_bundle(tmp_path)
    task_card_payload = json.loads(task_card_path.read_text(encoding="utf-8"))
    base_freeze_path = Path(task_card_payload["freeze_manifest_paths"][0])
    base_lineage_path = Path(task_card_payload["lineage_paths"][0])
    alternate_freeze_path = base_freeze_path.with_name("alternate_freeze_manifest.json")
    alternate_lineage_path = base_lineage_path.with_name("alternate_lineage.json")

    alternate_freeze_payload = json.loads(base_freeze_path.read_text(encoding="utf-8"))
    alternate_freeze_payload["freeze_manifest_id"] = "alternate_freeze_manifest"
    if alternate_freeze_mutator is not None:
        alternate_freeze_mutator(alternate_freeze_payload)

    alternate_lineage_payload = json.loads(base_lineage_path.read_text(encoding="utf-8"))
    alternate_lineage_payload["lineage_id"] = "alternate_lineage"
    alternate_lineage_payload["freeze_manifest_path"] = str(alternate_freeze_path)

    alternate_freeze_path.write_text(
        json.dumps(alternate_freeze_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    alternate_lineage_path.write_text(
        json.dumps(alternate_lineage_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    task_card_payload["freeze_manifest_paths"].append(str(alternate_freeze_path))
    task_card_payload["lineage_paths"].append(str(alternate_lineage_path))
    task_card_path.write_text(
        json.dumps(task_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return task_card_path


def _build_glutamatergic_temp_bundle(
    tmp_path: Path,
    *,
    ranking_rows: list[dict[str, str]] | None = None,
    evaluation_rows: list[dict[str, str]] | None = None,
    use_materialized_integrity: bool = False,
) -> Path:
    task_card_payload = json.loads(
        GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH.read_text(encoding="utf-8")
    )
    ranking_card_payload = json.loads(
        Path(task_card_payload["dataset_card_paths"][0]).read_text(encoding="utf-8")
    )
    evaluation_card_payload = json.loads(
        Path(task_card_payload["dataset_card_paths"][1]).read_text(encoding="utf-8")
    )
    freeze_manifest_payload = json.loads(
        Path(task_card_payload["freeze_manifest_paths"][0]).read_text(encoding="utf-8")
    )
    split_manifest_payload = json.loads(
        Path(task_card_payload["split_manifest_paths"][0]).read_text(encoding="utf-8")
    )
    lineage_payload = json.loads(
        Path(task_card_payload["lineage_paths"][0]).read_text(encoding="utf-8")
    )

    ranking_fieldnames, original_ranking_rows = _read_csv_rows(
        GLUTAMATERGIC_RANKING_OUTPUT_PATH
    )
    evaluation_fieldnames, original_evaluation_rows = _read_csv_rows(
        GLUTAMATERGIC_EVALUATION_OUTPUT_PATH
    )
    ranking_rows = original_ranking_rows if ranking_rows is None else ranking_rows
    evaluation_rows = (
        original_evaluation_rows if evaluation_rows is None else evaluation_rows
    )

    bundle_dir = tmp_path / "glutamatergic-bundle"
    bundle_dir.mkdir()
    ranking_output = bundle_dir / GLUTAMATERGIC_RANKING_OUTPUT_PATH.name
    evaluation_output = bundle_dir / GLUTAMATERGIC_EVALUATION_OUTPUT_PATH.name
    _write_csv(ranking_output, ranking_fieldnames, ranking_rows)
    _write_csv(evaluation_output, evaluation_fieldnames, evaluation_rows)

    expected_ranking_path = (
        ranking_output if use_materialized_integrity else GLUTAMATERGIC_RANKING_OUTPUT_PATH
    )
    expected_evaluation_path = (
        evaluation_output
        if use_materialized_integrity
        else GLUTAMATERGIC_EVALUATION_OUTPUT_PATH
    )
    _, expected_ranking_rows = _read_csv_rows(expected_ranking_path)
    _, expected_evaluation_rows = _read_csv_rows(expected_evaluation_path)

    ranking_card_path = bundle_dir / "ranking_card.json"
    evaluation_card_path = bundle_dir / "evaluation_card.json"
    freeze_manifest_path = bundle_dir / "freeze_manifest.json"
    split_manifest_path = bundle_dir / "split_manifest.json"
    lineage_path = bundle_dir / "lineage.json"
    task_card_path = bundle_dir / "task_card.json"

    ranking_card_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    ranking_card_payload["lineage_path"] = str(lineage_path)
    ranking_card_payload["expected_output_path"] = str(ranking_output)
    evaluation_card_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    evaluation_card_payload["lineage_path"] = str(lineage_path)
    evaluation_card_payload["expected_output_path"] = str(evaluation_output)

    freeze_manifest_payload["frozen_datasets"][0]["dataset_card_path"] = str(ranking_card_path)
    freeze_manifest_payload["frozen_datasets"][0]["expected_output_path"] = str(
        ranking_output
    )
    freeze_manifest_payload["frozen_datasets"][0]["expected_sha256"] = _sha256_path(
        expected_ranking_path
    )
    freeze_manifest_payload["frozen_datasets"][0]["expected_row_count"] = len(
        expected_ranking_rows
    )
    freeze_manifest_payload["frozen_datasets"][1]["dataset_card_path"] = str(
        evaluation_card_path
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_output_path"] = str(
        evaluation_output
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_sha256"] = _sha256_path(
        expected_evaluation_path
    )
    freeze_manifest_payload["frozen_datasets"][1]["expected_row_count"] = len(
        expected_evaluation_rows
    )

    split_manifest_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    lineage_payload["freeze_manifest_path"] = str(freeze_manifest_path)
    lineage_payload["frozen_datasets"][0]["dataset_card_path"] = str(ranking_card_path)
    lineage_payload["frozen_datasets"][1]["dataset_card_path"] = str(
        evaluation_card_path
    )

    task_card_payload["dataset_card_paths"] = [
        str(ranking_card_path),
        str(evaluation_card_path),
    ]
    task_card_payload["freeze_manifest_paths"] = [str(freeze_manifest_path)]
    task_card_payload["split_manifest_paths"] = [str(split_manifest_path)]
    task_card_payload["lineage_paths"] = [str(lineage_path)]

    ranking_card_path.write_text(
        json.dumps(ranking_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    evaluation_card_path.write_text(
        json.dumps(evaluation_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    freeze_manifest_path.write_text(
        json.dumps(freeze_manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    split_manifest_path.write_text(
        json.dumps(split_manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    lineage_path.write_text(
        json.dumps(lineage_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    task_card_path.write_text(
        json.dumps(task_card_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return task_card_path


def test_npc_source_manifest_matches_checked_in_artifacts() -> None:
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text(encoding="utf-8"))

    assert manifest["task_id"] == "scz_npc_signature_reversal_rescue_task"
    assert manifest["cutoff_date"] == "2020-12-31"
    assert {source["availability"] for source in manifest["sources"]} == {
        "pre_cutoff",
        "post_cutoff",
    }
    assert {source["license"] for source in manifest["sources"]} == {
        "CC BY 4.0",
        "CC BY",
    }

    for source in manifest["sources"]:
        materialized_path = Path(source["materialized_path"])
        assert materialized_path.exists()
        assert _sha256_path(materialized_path) == source["materialized_sha256"]
        assert source["upstream_url"].startswith("https://")
        assert source["row_count"] > 0

    for frozen_output in manifest["frozen_outputs"]:
        output_path = Path(frozen_output["path"])
        assert output_path.exists()
        assert _sha256_path(output_path) == frozen_output["sha256"]
        assert frozen_output["row_count"] == 15614


def test_npc_frozen_bundle_loads_checked_in_csvs_only() -> None:
    bundle = load_frozen_rescue_task_bundle(
        rescue_task_id="scz_npc_signature_reversal_rescue_task"
    )

    assert bundle.ranking_input.card.dataset_id == (
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
    )
    assert bundle.evaluation_target.card.dataset_id == (
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23"
    )
    assert "split_name" in bundle.ranking_input.columns
    assert "split_name" in bundle.evaluation_target.columns

    ranking_ids = {row["gene_id"] for row in bundle.ranking_input.rows}
    evaluation_ids = {row["gene_id"] for row in bundle.evaluation_target.rows}
    assert ranking_ids == evaluation_ids

    split_counts = Counter(row["split_name"] for row in bundle.ranking_input.rows)
    assert split_counts == {"train": 10929, "validation": 2342, "test": 2343}

    positive_counts = Counter(
        row["split_name"]
        for row in bundle.evaluation_target.rows
        if row["rescue_positive_label"] == "1"
    )
    assert positive_counts == {"train": 9, "validation": 3, "test": 2}


def test_glutamatergic_frozen_bundle_derives_split_names_from_manifest() -> None:
    bundle = load_frozen_rescue_task_bundle(
        task_card_path=GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH
    )

    assert bundle.governance.task_card.task_id == "glutamatergic_convergence_rescue_task"
    assert "split_name" in bundle.ranking_input.columns
    assert "split_name" in bundle.evaluation_target.columns
    assert Counter(row["split_name"] for row in bundle.ranking_input.rows) == {
        "train": 2,
        "validation": 1,
        "test": 1,
    }
    assert {
        row["gene_symbol"]: row["split_name"] for row in bundle.ranking_input.rows
    } == {
        "GRIA1": "validation",
        "GRIN2A": "train",
        "GRM3": "test",
        "GRM5": "train",
    }
    assert {
        row["gene_symbol"]: row["split_name"] for row in bundle.evaluation_target.rows
    } == {
        "GRIA1": "validation",
        "GRIN2A": "train",
        "GRM3": "test",
        "GRM5": "train",
    }


def test_glutamatergic_frozen_bundle_rejects_checksum_drift(tmp_path: Path) -> None:
    _, ranking_rows = _read_csv_rows(GLUTAMATERGIC_RANKING_OUTPUT_PATH)
    drifted_ranking_rows = [dict(row) for row in ranking_rows]
    drifted_ranking_rows[0]["gene_symbol"] = "GRIA1_DRIFT"
    drifted_task_card = _build_glutamatergic_temp_bundle(
        tmp_path,
        ranking_rows=drifted_ranking_rows,
        use_materialized_integrity=False,
    )

    with pytest.raises(ValueError, match="checksum validation"):
        load_frozen_rescue_task_bundle(task_card_path=drifted_task_card)


def test_glutamatergic_frozen_bundle_rejects_row_count_drift(tmp_path: Path) -> None:
    _, ranking_rows = _read_csv_rows(GLUTAMATERGIC_RANKING_OUTPUT_PATH)
    drifted_task_card = _build_glutamatergic_temp_bundle(
        tmp_path,
        ranking_rows=ranking_rows[:-1],
        use_materialized_integrity=True,
    )
    task_card_payload = json.loads(drifted_task_card.read_text(encoding="utf-8"))
    freeze_manifest_path = Path(task_card_payload["freeze_manifest_paths"][0])
    freeze_manifest_payload = json.loads(freeze_manifest_path.read_text(encoding="utf-8"))
    freeze_manifest_payload["frozen_datasets"][0]["expected_row_count"] = len(ranking_rows)
    freeze_manifest_path.write_text(
        json.dumps(freeze_manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="row count drift detected"):
        load_frozen_rescue_task_bundle(task_card_path=drifted_task_card)


def test_frozen_loader_rejects_checksum_drift(tmp_path: Path) -> None:
    _, ranking_rows = _read_csv_rows(RANKING_OUTPUT_PATH)
    drifted_ranking_rows = [dict(row) for row in ranking_rows]
    drifted_ranking_rows[0]["split_name"] = "train"
    drifted_task_card = _build_temp_bundle(
        tmp_path,
        ranking_rows=drifted_ranking_rows,
        use_materialized_integrity=False,
    )

    with pytest.raises(ValueError, match="checksum validation"):
        load_frozen_rescue_task_bundle(task_card_path=drifted_task_card)


def test_frozen_loader_rejects_cross_file_split_inconsistency(tmp_path: Path) -> None:
    _, evaluation_rows = _read_csv_rows(EVALUATION_OUTPUT_PATH)
    drifted_evaluation_rows = [dict(row) for row in evaluation_rows]
    target_row = next(row for row in drifted_evaluation_rows if row["gene_id"] == "ENSG00000080493")
    target_row["split_name"] = "test"
    drifted_task_card = _build_temp_bundle(
        tmp_path,
        evaluation_rows=drifted_evaluation_rows,
        use_materialized_integrity=True,
    )

    with pytest.raises(ValueError, match="split_name drift detected"):
        load_frozen_rescue_task_bundle(task_card_path=drifted_task_card)


def test_frozen_loader_accepts_repo_relative_task_card_path_from_non_repo_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    bundle = load_frozen_rescue_task_bundle(
        task_card_path=Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/task_card.json"
        )
    )

    assert bundle.governance.task_card.task_id == "scz_npc_signature_reversal_rescue_task"


def test_frozen_loader_rejects_invalid_entry_point_arguments() -> None:
    with pytest.raises(ValueError, match="provide exactly one"):
        load_frozen_rescue_task_bundle()

    with pytest.raises(ValueError, match="provide exactly one"):
        load_frozen_rescue_task_bundle(
            rescue_task_id="scz_npc_signature_reversal_rescue_task",
            task_card_path=TASK_CARD_PATH,
        )


def test_frozen_loader_rejects_unknown_rescue_task_id() -> None:
    with pytest.raises(KeyError, match="unknown rescue_task_id"):
        load_frozen_rescue_task_bundle(rescue_task_id="missing_rescue_task")


def test_multi_freeze_bundle_that_passes_governance_also_loads(tmp_path: Path) -> None:
    task_card_path = _build_multi_freeze_bundle(tmp_path)

    governance_bundle = validate_rescue_governance_bundle(task_card_path)
    loaded_bundle = load_frozen_rescue_task_bundle(task_card_path=task_card_path)

    assert len(governance_bundle.freeze_manifests) == 2
    assert loaded_bundle.ranking_input.card.dataset_id == (
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
    )
    assert loaded_bundle.evaluation_target.card.dataset_id == (
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23"
    )


def test_loader_uses_dataset_card_declared_freeze_manifest_path(tmp_path: Path) -> None:
    def mutate_alternate_freeze(payload: dict[str, object]) -> None:
        frozen_datasets = payload["frozen_datasets"]
        assert isinstance(frozen_datasets, list)
        ranking_reference = frozen_datasets[0]
        assert isinstance(ranking_reference, dict)
        ranking_reference["expected_sha256"] = "0" * 64

    task_card_path = _build_multi_freeze_bundle(
        tmp_path,
        alternate_freeze_mutator=mutate_alternate_freeze,
    )

    bundle = load_frozen_rescue_task_bundle(task_card_path=task_card_path)

    assert len(bundle.ranking_input.rows) == 15614
    assert len(bundle.evaluation_target.rows) == 15614
