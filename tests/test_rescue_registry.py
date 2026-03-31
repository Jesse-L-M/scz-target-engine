import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.rescue import (
    load_frozen_rescue_task_bundle,
    validate_rescue_governance_bundle,
)
from scz_target_engine.rescue.registry import (
    DEFAULT_RESCUE_TASK_REGISTRY_PATH,
    load_rescue_suite_contracts,
    load_rescue_task_contracts,
    resolve_rescue_task_contract,
)


EXAMPLE_CONTRACT_PATH = Path(
    "data/curated/rescue_tasks/contracts/example_scz_gene_rescue_task.json"
)
EXAMPLE_TASK_CARD_PATH = Path(
    "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/task_card.json"
)
EXAMPLE_FREEZE_MANIFEST_PATH = Path(
    "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
    "freeze_manifests/example_scz_gene_rescue_freeze_2025_01_15.json"
)
EXAMPLE_SPLIT_MANIFEST_PATH = Path(
    "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
    "split_manifests/example_scz_gene_rescue_split_2025_01_16.json"
)
EXAMPLE_LINEAGE_PATH = Path(
    "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
    "lineage/example_scz_gene_raw_to_frozen_lineage_2025_01_16.json"
)
NPC_CONTRACT_PATH = Path(
    "data/curated/rescue_tasks/contracts/scz_npc_signature_reversal_rescue_task.json"
)
NPC_TASK_CARD_PATH = Path(
    "data/curated/rescue_tasks/governance/"
    "scz_npc_signature_reversal_rescue_task/task_card.json"
)
NPC_FREEZE_MANIFEST_PATH = Path(
    "data/curated/rescue_tasks/governance/"
    "scz_npc_signature_reversal_rescue_task/"
    "freeze_manifests/scz_npc_signature_reversal_freeze_2026_03_31.json"
)
NPC_SPLIT_MANIFEST_PATH = Path(
    "data/curated/rescue_tasks/governance/"
    "scz_npc_signature_reversal_rescue_task/"
    "split_manifests/scz_npc_signature_reversal_split_2026_03_31.json"
)
NPC_LINEAGE_PATH = Path(
    "data/curated/rescue_tasks/governance/"
    "scz_npc_signature_reversal_rescue_task/"
    "lineage/scz_npc_signature_reversal_raw_to_frozen_lineage_2026_03_31.json"
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
    assert task_contract.leakage_boundary.freeze_manifest_required is True
    assert (
        task_contract.leakage_boundary.freeze_manifest_policy
        == "schema_validated_rescue_governance_v1"
    )
    assert task_contract.leakage_boundary.dataset_cards_required is True
    assert task_contract.leakage_boundary.task_card_required is True
    assert task_contract.leakage_boundary.split_manifest_required is True
    assert task_contract.leakage_boundary.raw_to_frozen_lineage_required is True


def test_rescue_registry_resolves_active_npc_contract() -> None:
    task_contract = resolve_rescue_task_contract(
        rescue_task_id="scz_npc_signature_reversal_rescue_task"
    )

    assert task_contract.suite_id == "scz_rescue_contract_suite"
    assert task_contract.contract_scope == "rescue_only"
    assert task_contract.entity_type == "gene"
    assert task_contract.task_label == "Schizophrenia NPC signature-reversal rescue task"


def test_rescue_registry_groups_tasks_under_single_suite() -> None:
    suites = load_rescue_suite_contracts()

    assert len(suites) == 1
    assert suites[0].suite_id == "scz_rescue_contract_suite"
    assert {task.task_id for task in suites[0].tasks} == {
        "example_scz_gene_rescue_task",
        "scz_npc_signature_reversal_rescue_task",
    }
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


def test_example_rescue_governance_bundle_validates_from_task_card() -> None:
    bundle = validate_rescue_governance_bundle(EXAMPLE_TASK_CARD_PATH.resolve())

    assert bundle.task_card.task_id == "example_scz_gene_rescue_task"
    assert bundle.contract.task_id == "example_scz_gene_rescue_task"
    assert {dataset.dataset_id for dataset in bundle.dataset_cards} == {
        "example_scz_gene_ranking_inputs_2025_01_15",
        "example_scz_gene_evaluation_labels_2025_06_30",
    }
    assert bundle.freeze_manifests[0].freeze_manifest_id == (
        "example_scz_gene_rescue_freeze_2025_01_15"
    )
    assert bundle.split_manifests[0].source_dataset_id == (
        "example_scz_gene_ranking_inputs_2025_01_15"
    )
    assert bundle.lineages[0].lineage_id == (
        "example_scz_gene_raw_to_frozen_lineage_2025_01_16"
    )


def test_npc_rescue_governance_bundle_validates_from_task_card() -> None:
    bundle = validate_rescue_governance_bundle(NPC_TASK_CARD_PATH.resolve())

    assert bundle.task_card.task_id == "scz_npc_signature_reversal_rescue_task"
    assert bundle.contract.task_id == "scz_npc_signature_reversal_rescue_task"
    assert {dataset.dataset_id for dataset in bundle.dataset_cards} == {
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31",
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23",
    }
    assert bundle.freeze_manifests[0].freeze_manifest_id == (
        "scz_npc_signature_reversal_freeze_2026_03_31"
    )
    assert bundle.split_manifests[0].source_dataset_id == (
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
    )
    assert bundle.lineages[0].lineage_id == (
        "scz_npc_signature_reversal_raw_to_frozen_lineage_2026_03_31"
    )


def test_npc_frozen_bundle_loads_from_registry_id() -> None:
    bundle = load_frozen_rescue_task_bundle(
        rescue_task_id="scz_npc_signature_reversal_rescue_task"
    )

    assert bundle.governance.task_card.task_id == "scz_npc_signature_reversal_rescue_task"
    assert bundle.ranking_input.card.dataset_role == "ranking_input"
    assert bundle.evaluation_target.card.dataset_role == "evaluation_target"
    assert len(bundle.ranking_input.rows) == 15614
    assert len(bundle.evaluation_target.rows) == 15614


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
                    "entity_type,contract_scope,contract_file,task_card_file,"
                    "registry_status,notes"
                ),
                (
                    "scz_rescue_contract_suite,Schizophrenia Rescue Task Suite,"
                    "example_scz_gene_rescue_task,Example schizophrenia gene rescue task,"
                    "gene_rescue_ranking,schizophrenia,gene,rescue_only,"
                    f"{contract_path},{EXAMPLE_TASK_CARD_PATH},example,Intentional mismatch"
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


def test_rescue_registry_rejects_missing_task_card_file_on_normal_path(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "rescue_task_registry.csv"
    missing_task_card = tmp_path / "missing_task_card.json"
    registry_path.write_text(
        "\n".join(
            [
                (
                    "suite_id,suite_label,task_id,task_label,task_type,disease,"
                    "entity_type,contract_scope,contract_file,task_card_file,"
                    "registry_status,notes"
                ),
                (
                    "scz_rescue_contract_suite,Schizophrenia Rescue Task Suite,"
                    "example_scz_gene_rescue_task,Example schizophrenia gene rescue task,"
                    "gene_rescue_ranking,schizophrenia,gene,rescue_only,"
                    f"{EXAMPLE_CONTRACT_PATH},{missing_task_card},example,Missing governance bundle"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="task_card_file does not exist"):
        load_rescue_task_contracts(task_registry_path=registry_path)


def test_rescue_registry_rejects_broken_task_card_bundle_on_normal_path(
    tmp_path: Path,
) -> None:
    task_card_payload = json.loads(EXAMPLE_TASK_CARD_PATH.read_text(encoding="utf-8"))
    task_card_payload["dataset_card_paths"][0] = str(tmp_path / "missing_dataset_card.json")
    task_card_path = tmp_path / "broken_task_card.json"
    task_card_path.write_text(
        json.dumps(task_card_payload, indent=2),
        encoding="utf-8",
    )

    registry_path = tmp_path / "rescue_task_registry.csv"
    registry_path.write_text(
        "\n".join(
            [
                (
                    "suite_id,suite_label,task_id,task_label,task_type,disease,"
                    "entity_type,contract_scope,contract_file,task_card_file,"
                    "registry_status,notes"
                ),
                (
                    "scz_rescue_contract_suite,Schizophrenia Rescue Task Suite,"
                    "example_scz_gene_rescue_task,Example schizophrenia gene rescue task,"
                    "gene_rescue_ranking,schizophrenia,gene,rescue_only,"
                    f"{EXAMPLE_CONTRACT_PATH},{task_card_path},example,Broken governance bundle"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError):
        load_rescue_task_contracts(task_registry_path=registry_path)


def test_rescue_task_card_validation_rejects_missing_required_field(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_TASK_CARD_PATH.read_text(encoding="utf-8"))
    payload.pop("dataset_card_paths")
    task_card_path = tmp_path / "bad_task_card.json"
    task_card_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="missing required fields: dataset_card_paths",
    ):
        load_artifact(
            task_card_path,
            artifact_name="rescue_task_card",
        )


def test_rescue_task_card_load_rejects_missing_referenced_dataset_card(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_TASK_CARD_PATH.read_text(encoding="utf-8"))
    payload["dataset_card_paths"][0] = str(tmp_path / "missing_dataset_card.json")
    task_card_path = tmp_path / "task_card_with_missing_reference.json"
    task_card_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        load_artifact(
            task_card_path,
            artifact_name="rescue_task_card",
        )


def test_ranking_only_freeze_manifest_rejects_post_cutoff_source_snapshot(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_FREEZE_MANIFEST_PATH.read_text(encoding="utf-8"))
    payload["freeze_scope"] = "ranking_only"
    payload["frozen_datasets"] = [payload["frozen_datasets"][0]]
    freeze_manifest_path = tmp_path / "bad_ranking_only_freeze.json"
    freeze_manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="ranking_only freeze manifests must not include post_cutoff source snapshots",
    ):
        load_artifact(
            freeze_manifest_path,
            artifact_name="rescue_freeze_manifest",
        )


def test_rescue_governance_bundle_rejects_split_manifest_bound_to_wrong_freeze(
    tmp_path: Path,
) -> None:
    alternate_freeze_payload = json.loads(
        EXAMPLE_FREEZE_MANIFEST_PATH.read_text(encoding="utf-8")
    )
    alternate_freeze_payload["freeze_manifest_id"] = (
        "example_scz_gene_rescue_freeze_2025_01_15_alt"
    )
    alternate_freeze_path = tmp_path / "alternate_freeze_manifest.json"
    alternate_freeze_path.write_text(
        json.dumps(alternate_freeze_payload, indent=2),
        encoding="utf-8",
    )

    split_payload = json.loads(EXAMPLE_SPLIT_MANIFEST_PATH.read_text(encoding="utf-8"))
    split_payload["freeze_manifest_path"] = str(alternate_freeze_path)
    split_path = tmp_path / "bad_split_manifest.json"
    split_path.write_text(json.dumps(split_payload, indent=2), encoding="utf-8")

    task_card_payload = json.loads(EXAMPLE_TASK_CARD_PATH.read_text(encoding="utf-8"))
    task_card_payload["freeze_manifest_paths"] = [
        task_card_payload["freeze_manifest_paths"][0],
        str(alternate_freeze_path),
    ]
    task_card_payload["split_manifest_paths"] = [str(split_path)]
    task_card_path = tmp_path / "bad_task_card_freeze_binding.json"
    task_card_path.write_text(
        json.dumps(task_card_payload, indent=2),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="split manifests must reference the exact freeze_manifest_path declared by the source dataset card",
    ):
        validate_rescue_governance_bundle(task_card_path)


def test_rescue_lineage_rejects_produced_by_step_without_dataset_output(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_LINEAGE_PATH.read_text(encoding="utf-8"))
    payload["frozen_datasets"][0]["produced_by_step_id"] = "normalize_pre_cutoff_sources"
    lineage_path = tmp_path / "bad_lineage_step_binding.json"
    lineage_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="produced_by_step_id must point to a step that emits the dataset_id",
    ):
        load_artifact(
            lineage_path,
            artifact_name="rescue_raw_to_frozen_lineage",
        )


def test_rescue_lineage_rejects_raw_source_drift_from_freeze_manifest(
    tmp_path: Path,
) -> None:
    payload = json.loads(EXAMPLE_LINEAGE_PATH.read_text(encoding="utf-8"))
    payload["raw_sources"][0]["source_path"] = (
        "data/raw/sources/opentargets/drifted_snapshot.json"
    )
    lineage_path = tmp_path / "bad_lineage_source_drift.json"
    lineage_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="raw_sources must match the freeze manifest for source_path",
    ):
        load_artifact(
            lineage_path,
            artifact_name="rescue_raw_to_frozen_lineage",
        )
