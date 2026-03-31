import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact, list_artifact_schemas
from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_protocol import BENCHMARK_ARTIFACT_SCHEMAS_V1
from scz_target_engine.benchmark_runner import run_benchmark
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
)
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs


def _schema_signature(schema: object) -> tuple[object, ...]:
    return (
        schema.artifact_name,
        schema.schema_version,
        schema.file_format,
        schema.key_fields,
        {
            (field.name, field.field_type, field.required)
            for field in schema.fields
        },
    )


def test_artifact_registry_covers_current_output_and_contract_families() -> None:
    schemas = list_artifact_schemas()

    assert {schema.artifact_name for schema in schemas} == {
        "benchmark_snapshot_manifest",
        "benchmark_cohort_labels",
        "benchmark_model_run_manifest",
        "benchmark_metric_output_payload",
        "benchmark_confidence_interval_payload",
        "rescue_dataset_card",
        "rescue_freeze_manifest",
        "rescue_raw_to_frozen_lineage",
        "rescue_split_manifest",
        "rescue_task_contract",
        "rescue_task_card",
        "gene_target_ledgers",
        "decision_vectors_v1",
        "policy_decision_vectors_v2",
        "domain_head_rankings_v1",
        "policy_pareto_fronts_v1",
    }


def test_benchmark_schema_registrations_match_frozen_protocol_contract() -> None:
    registered = {
        schema.artifact_name: schema
        for schema in list_artifact_schemas()
        if schema.artifact_name.startswith("benchmark_")
    }
    frozen = {
        schema.artifact_name: schema
        for schema in BENCHMARK_ARTIFACT_SCHEMAS_V1
    }

    assert set(registered) == set(frozen)
    for artifact_name, frozen_schema in frozen.items():
        assert _schema_signature(registered[artifact_name]) == _schema_signature(
            frozen_schema
        )


def test_example_build_artifacts_validate_against_registered_schemas(
    tmp_path: Path,
) -> None:
    config = load_config(Path("config/v0.toml"))
    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )

    ledger_artifact = load_artifact(tmp_path / "gene_target_ledgers.json")
    decision_vector_artifact = load_artifact(tmp_path / "decision_vectors_v1.json")
    policy_vector_artifact = load_artifact(tmp_path / "policy_decision_vectors_v2.json")
    domain_ranking_artifact = load_artifact(tmp_path / "domain_head_rankings_v1.csv")
    policy_pareto_artifact = load_artifact(tmp_path / "policy_pareto_fronts_v1.json")

    assert ledger_artifact.artifact_name == "gene_target_ledgers"
    assert ledger_artifact.payload["target_count"] == len(ledger_artifact.payload["targets"])
    assert decision_vector_artifact.artifact_name == "decision_vectors_v1"
    assert set(decision_vector_artifact.payload["entities"]) == {"gene", "module"}
    assert policy_vector_artifact.artifact_name == "policy_decision_vectors_v2"
    assert set(policy_vector_artifact.payload["entities"]) == {"gene", "module"}
    assert domain_ranking_artifact.artifact_name == "domain_head_rankings_v1"
    assert len(domain_ranking_artifact.payload) > 0
    assert policy_pareto_artifact.artifact_name == "policy_pareto_fronts_v1"
    assert set(policy_pareto_artifact.payload["entity_types"]) == {"gene", "module"}


def test_policy_decision_vector_artifact_rejects_invalid_definition_payloads(
    tmp_path: Path,
) -> None:
    config = load_config(Path("config/v0.toml"))
    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )

    payload = json.loads((tmp_path / "policy_decision_vectors_v2.json").read_text())
    payload["policy_definitions"][0]["domain_weights"][0]["weight"] = 0.0
    invalid_weight_path = tmp_path / "invalid_policy_weight.json"
    invalid_weight_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="must be positive"):
        load_artifact(
            invalid_weight_path,
            artifact_name="policy_decision_vectors_v2",
        )

    payload = json.loads((tmp_path / "policy_decision_vectors_v2.json").read_text())
    del payload["policy_definitions"][0]["adjustment_weights"][
        "directionality_open_risk_penalty"
    ]
    missing_adjustment_path = tmp_path / "invalid_policy_adjustment.json"
    missing_adjustment_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="directionality_open_risk_penalty is required"):
        load_artifact(
            missing_adjustment_path,
            artifact_name="policy_decision_vectors_v2",
        )


def test_benchmark_fixture_artifacts_validate_against_registered_schemas(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "snapshot_manifest.json"
    cohort_labels_path = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

    snapshot_result = materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    snapshot_artifact = load_artifact(snapshot_path)
    assert snapshot_artifact.artifact_name == "benchmark_snapshot_manifest"
    assert snapshot_artifact.payload.snapshot_id == snapshot_result["snapshot_id"]
    assert snapshot_artifact.payload.benchmark_task_id == snapshot_result["benchmark_task_id"]

    materialize_benchmark_cohort_labels(
        manifest=snapshot_artifact.payload,
        cohort_members_file=Path(
            "data/benchmark/fixtures/scz_small/cohort_members.csv"
        ).resolve(),
        future_outcomes_file=Path(
            "data/benchmark/fixtures/scz_small/future_outcomes.csv"
        ).resolve(),
        output_file=cohort_labels_path,
    )
    cohort_artifact = load_artifact(cohort_labels_path)
    assert cohort_artifact.artifact_name == "benchmark_cohort_labels"
    assert len(cohort_artifact.payload) > 0

    benchmark_result = run_benchmark(
        manifest_file=snapshot_path,
        cohort_labels_file=cohort_labels_path,
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_dir=runner_output_dir,
        config_file=Path("config/v0.toml").resolve(),
        code_version="artifact-schema-test",
        deterministic_test_mode=True,
    )

    assert benchmark_result["executed_baselines"]
    assert benchmark_result["benchmark_task_id"] == "scz_translational_task"
    for manifest_file in benchmark_result["run_manifest_files"]:
        artifact = load_artifact(Path(manifest_file))
        assert artifact.artifact_name == "benchmark_model_run_manifest"
        assert artifact.payload.benchmark_task_id == "scz_translational_task"
    for metric_file in benchmark_result["metric_payload_files"]:
        assert (
            load_artifact(Path(metric_file)).artifact_name
            == "benchmark_metric_output_payload"
        )
    for interval_file in benchmark_result["confidence_interval_files"]:
        assert (
            load_artifact(Path(interval_file)).artifact_name
            == "benchmark_confidence_interval_payload"
        )


def test_example_rescue_task_contract_validates_against_registered_schema() -> None:
    rescue_contract_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/contracts/example_scz_gene_rescue_task.json"
        ).resolve()
    )

    assert rescue_contract_artifact.artifact_name == "rescue_task_contract"
    assert rescue_contract_artifact.payload.task_id == "example_scz_gene_rescue_task"
    assert rescue_contract_artifact.payload.leakage_boundary.policy_id == (
        "strict_rescue_task_boundary_v1"
    )
    assert rescue_contract_artifact.payload.leakage_boundary.freeze_manifest_required


def test_interneuron_rescue_task_contract_validates_against_registered_schema() -> None:
    rescue_contract_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/contracts/interneuron_gene_rescue_task.json"
        ).resolve()
    )

    assert rescue_contract_artifact.artifact_name == "rescue_task_contract"
    assert rescue_contract_artifact.payload.task_id == "interneuron_gene_rescue_task"
    assert rescue_contract_artifact.payload.entity_type == "gene"
    assert {
        artifact.artifact_id for artifact in rescue_contract_artifact.payload.artifact_contracts
    } == {
        "interneuron_synapse_candidates",
        "interneuron_arbor_candidates",
        "post_cutoff_followup_labels",
        "ranked_predictions",
        "task_context",
    }


def test_example_rescue_governance_artifacts_validate_against_registered_schemas() -> None:
    task_card_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "example_scz_gene_rescue_task/task_card.json"
        ).resolve()
    )
    ranking_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
            "dataset_cards/example_scz_gene_ranking_inputs.json"
        ).resolve()
    )
    evaluation_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
            "dataset_cards/example_scz_gene_evaluation_labels.json"
        ).resolve()
    )
    freeze_manifest_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
            "freeze_manifests/example_scz_gene_rescue_freeze_2025_01_15.json"
        ).resolve()
    )
    split_manifest_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
            "split_manifests/example_scz_gene_rescue_split_2025_01_16.json"
        ).resolve()
    )
    lineage_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/example_scz_gene_rescue_task/"
            "lineage/example_scz_gene_raw_to_frozen_lineage_2025_01_16.json"
        ).resolve()
    )

    assert task_card_artifact.artifact_name == "rescue_task_card"
    assert task_card_artifact.payload.task_id == "example_scz_gene_rescue_task"
    assert ranking_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert ranking_dataset_artifact.payload.dataset_role == "ranking_input"
    assert evaluation_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert evaluation_dataset_artifact.payload.dataset_role == "evaluation_target"
    assert freeze_manifest_artifact.artifact_name == "rescue_freeze_manifest"
    assert freeze_manifest_artifact.payload.freeze_manifest_id == (
        "example_scz_gene_rescue_freeze_2025_01_15"
    )
    assert split_manifest_artifact.artifact_name == "rescue_split_manifest"
    assert split_manifest_artifact.payload.source_dataset_id == (
        "example_scz_gene_ranking_inputs_2025_01_15"
    )
    assert lineage_artifact.artifact_name == "rescue_raw_to_frozen_lineage"
    assert {
        dataset.dataset_id for dataset in lineage_artifact.payload.frozen_datasets
    } == {
        "example_scz_gene_ranking_inputs_2025_01_15",
        "example_scz_gene_evaluation_labels_2025_06_30",
    }

def test_npc_rescue_task_contract_validates_against_registered_schema() -> None:
    rescue_contract_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/contracts/"
            "scz_npc_signature_reversal_rescue_task.json"
        ).resolve()
    )

    assert rescue_contract_artifact.artifact_name == "rescue_task_contract"
    assert rescue_contract_artifact.payload.task_id == (
        "scz_npc_signature_reversal_rescue_task"
    )
    assert rescue_contract_artifact.payload.leakage_boundary.policy_id == (
        "strict_rescue_task_boundary_v1"
    )


def test_npc_rescue_governance_artifacts_validate_against_registered_schemas() -> None:
    task_card_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/task_card.json"
        ).resolve()
    )
    ranking_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/"
            "dataset_cards/scz_npc_signature_reversal_ranking_inputs.json"
        ).resolve()
    )
    evaluation_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/"
            "dataset_cards/scz_npc_signature_reversal_evaluation_labels.json"
        ).resolve()
    )
    freeze_manifest_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/"
            "freeze_manifests/scz_npc_signature_reversal_freeze_2026_03_31.json"
        ).resolve()
    )
    split_manifest_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/"
            "split_manifests/scz_npc_signature_reversal_split_2026_03_31.json"
        ).resolve()
    )
    lineage_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "scz_npc_signature_reversal_rescue_task/"
            "lineage/scz_npc_signature_reversal_raw_to_frozen_lineage_2026_03_31.json"
        ).resolve()
    )

    assert task_card_artifact.artifact_name == "rescue_task_card"
    assert task_card_artifact.payload.task_id == "scz_npc_signature_reversal_rescue_task"
    assert ranking_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert ranking_dataset_artifact.payload.dataset_role == "ranking_input"
    assert evaluation_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert evaluation_dataset_artifact.payload.dataset_role == "evaluation_target"
    assert freeze_manifest_artifact.artifact_name == "rescue_freeze_manifest"
    assert freeze_manifest_artifact.payload.freeze_manifest_id == (
        "scz_npc_signature_reversal_freeze_2026_03_31"
    )
    assert split_manifest_artifact.artifact_name == "rescue_split_manifest"
    assert split_manifest_artifact.payload.source_dataset_id == (
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
    )
    assert lineage_artifact.artifact_name == "rescue_raw_to_frozen_lineage"
    assert {
        dataset.dataset_id for dataset in lineage_artifact.payload.frozen_datasets
    } == {
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31",
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23",
    }


def test_interneuron_rescue_governance_artifacts_validate_against_registered_schemas() -> None:
    task_card_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/"
            "interneuron_gene_rescue_task/task_card.json"
        ).resolve()
    )
    synapse_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "dataset_cards/interneuron_synapse_ranking_inputs.json"
        ).resolve()
    )
    arbor_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "dataset_cards/interneuron_arbor_ranking_inputs.json"
        ).resolve()
    )
    evaluation_dataset_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "dataset_cards/interneuron_followup_labels.json"
        ).resolve()
    )
    freeze_manifest_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "freeze_manifests/interneuron_gene_rescue_freeze_2023_12_31.json"
        ).resolve()
    )
    synapse_split_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "split_manifests/interneuron_synapse_split_2026_03_31.json"
        ).resolve()
    )
    arbor_split_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "split_manifests/interneuron_arbor_split_2026_03_31.json"
        ).resolve()
    )
    lineage_artifact = load_artifact(
        Path(
            "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/"
            "lineage/interneuron_gene_raw_to_frozen_lineage_2026_03_31.json"
        ).resolve()
    )

    assert task_card_artifact.artifact_name == "rescue_task_card"
    assert task_card_artifact.payload.task_id == "interneuron_gene_rescue_task"
    assert synapse_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert synapse_dataset_artifact.payload.dataset_role == "ranking_input"
    assert arbor_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert arbor_dataset_artifact.payload.dataset_role == "ranking_input"
    assert evaluation_dataset_artifact.artifact_name == "rescue_dataset_card"
    assert evaluation_dataset_artifact.payload.dataset_role == "evaluation_target"
    assert freeze_manifest_artifact.artifact_name == "rescue_freeze_manifest"
    assert freeze_manifest_artifact.payload.freeze_manifest_id == (
        "interneuron_gene_rescue_freeze_2023_12_31"
    )
    assert synapse_split_artifact.artifact_name == "rescue_split_manifest"
    assert (
        synapse_split_artifact.payload.source_dataset_id
        == "interneuron_synapse_ranking_inputs_2023_12_31"
    )
    assert arbor_split_artifact.artifact_name == "rescue_split_manifest"
    assert (
        arbor_split_artifact.payload.source_dataset_id
        == "interneuron_arbor_ranking_inputs_2023_12_31"
    )
    assert lineage_artifact.artifact_name == "rescue_raw_to_frozen_lineage"
    assert {
        dataset.dataset_id for dataset in lineage_artifact.payload.frozen_datasets
    } == {
        "interneuron_synapse_ranking_inputs_2023_12_31",
        "interneuron_arbor_ranking_inputs_2023_12_31",
        "interneuron_followup_labels_2026_03_31",
    }
