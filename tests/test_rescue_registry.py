import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.rescue.registry import (
    DEFAULT_RESCUE_TASK_REGISTRY_PATH,
    load_rescue_suite_contracts,
    load_rescue_task_contracts,
    resolve_rescue_task_contract,
)


EXAMPLE_CONTRACT_PATH = Path(
    "data/curated/rescue_tasks/contracts/example_scz_gene_rescue_task.json"
)


def test_rescue_registry_resolves_example_contract() -> None:
    task_contract = resolve_rescue_task_contract(
        rescue_task_id="example_scz_gene_rescue_task"
    )

    assert task_contract.suite_id == "scz_rescue_contract_suite"
    assert task_contract.contract_scope == "rescue_only"
    assert task_contract.entity_type == "gene"
    assert {artifact.channel for artifact in task_contract.artifact_contracts} == {
        "ranking_input",
        "evaluation_target",
        "task_output",
        "task_metadata",
    }
    assert task_contract.leakage_boundary.freeze_manifest_required is False
    assert (
        task_contract.leakage_boundary.freeze_manifest_policy
        == "deferred_until_pr40a"
    )


def test_rescue_registry_groups_example_task_under_single_suite() -> None:
    suites = load_rescue_suite_contracts()

    assert len(suites) == 1
    assert suites[0].suite_id == "scz_rescue_contract_suite"
    assert suites[0].tasks[0].task_id == "example_scz_gene_rescue_task"
    assert DEFAULT_RESCUE_TASK_REGISTRY_PATH.exists()


def test_rescue_contract_artifact_loads_through_artifact_registry() -> None:
    contract_artifact = load_artifact(EXAMPLE_CONTRACT_PATH.resolve())

    assert contract_artifact.artifact_name == "rescue_task_contract"
    assert contract_artifact.payload.task_id == "example_scz_gene_rescue_task"
    assert contract_artifact.payload.artifact_contracts[2].artifact_id == (
        "ranked_predictions"
    )


def test_rescue_contract_validation_rejects_missing_required_channel(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["artifact_contracts"] = [
        artifact
        for artifact in payload["artifact_contracts"]
        if artifact["channel"] != "evaluation_target"
    ]
    contract_path = tmp_path / "bad_rescue_contract.json"
    contract_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="artifact_contracts must include explicit rescue channels",
    ):
        load_artifact(
            contract_path,
            artifact_name="rescue_task_contract",
        )


def test_rescue_registry_stays_separate_from_benchmark_registry() -> None:
    benchmark_contract = resolve_benchmark_task_contract(
        benchmark_task_id="scz_translational_task"
    )
    rescue_contract = resolve_rescue_task_contract(
        rescue_task_id="example_scz_gene_rescue_task"
    )

    assert benchmark_contract.suite_id == "scz_translational_suite"
    assert rescue_contract.suite_id == "scz_rescue_contract_suite"
    assert benchmark_contract.task_id != rescue_contract.task_id


def test_rescue_registry_rejects_registry_contract_identity_mismatch(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["task_id"] = "mismatched_contract_task"
    contract_path = tmp_path / "mismatched_contract.json"
    contract_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    registry_path = tmp_path / "rescue_task_registry.csv"
    registry_path.write_text(
        "\n".join(
            [
                (
                    "suite_id,suite_label,task_id,task_label,task_type,disease,"
                    "entity_type,contract_scope,contract_file,registry_status,notes"
                ),
                (
                    "scz_rescue_contract_suite,Schizophrenia Rescue Task Suite,"
                    "example_scz_gene_rescue_task,Example schizophrenia gene rescue task,"
                    "gene_rescue_ranking,schizophrenia,gene,rescue_only,"
                    f"{contract_path},example,Intentional mismatch"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="rescue registry row did not match contract file fields",
    ):
        load_rescue_task_contracts(task_registry_path=registry_path)
