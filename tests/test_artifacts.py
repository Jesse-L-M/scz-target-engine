import hashlib
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
from scz_target_engine.program_memory import (
    materialize_program_memory_v3_adjudication_bundle,
    materialize_program_memory_v3_harvest_bundle,
    materialize_program_memory_v3_insight_packet,
)


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


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_release_manifest(
    path: Path,
    *,
    artifact_name: str,
    files: list[dict[str, object]],
) -> None:
    payload = {
        "schema_name": artifact_name,
        "schema_version": "v1",
        "release_id": f"{artifact_name}_2026_04_01",
        "release_family": artifact_name,
        "release_version": "2026.04.01",
        "materialized_at": "2026-04-01",
        "compatibility_phase": "dual_write",
        "files": files,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_schema_file(
    path: Path,
    *,
    artifact_name: str,
    schema_version: str | None = None,
) -> None:
    schema_path = Path("schemas/artifact_schemas") / f"{artifact_name}.json"
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    if schema_version is not None:
        payload["schema_version"] = schema_version
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_artifact_registry_covers_current_output_and_contract_families() -> None:
    schemas = list_artifact_schemas()

    assert {schema.artifact_name for schema in schemas} == {
        "program_memory_release",
        "program_memory_v3_source_manifest",
        "program_memory_v3_study_index",
        "program_memory_v3_result_observations",
        "program_memory_v3_harm_observations",
        "program_memory_v3_contradiction_log",
        "program_memory_v3_claim_ledger",
        "program_memory_v3_caveats",
        "program_memory_v3_belief_updates",
        "program_memory_v3_program_card",
        "program_memory_v3_insight_packet",
        "benchmark_release",
        "rescue_release",
        "variant_context_release",
        "policy_release",
        "hypothesis_release",
        "benchmark_snapshot_manifest",
        "benchmark_cohort_members",
        "benchmark_source_cohort_members",
        "benchmark_source_future_outcomes",
        "benchmark_cohort_manifest",
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
        "hypothesis_packets_v1",
        "prospective_prediction_registration",
        "prospective_forecast_outcome_log",
    }


def test_benchmark_schema_registrations_match_frozen_protocol_contract() -> None:
    benchmark_artifact_names = {
        schema.artifact_name for schema in BENCHMARK_ARTIFACT_SCHEMAS_V1
    }
    registered = {
        schema.artifact_name: schema
        for schema in list_artifact_schemas()
        if schema.artifact_name in benchmark_artifact_names
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


def test_load_artifact_rejects_schema_with_string_required_flag(
    tmp_path: Path,
) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_path = schema_dir / "benchmark_snapshot_manifest.json"
    _write_schema_file(
        schema_path,
        artifact_name="benchmark_snapshot_manifest",
    )
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    payload["required_fields"][0]["required"] = "false"
    schema_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="required must be an explicit boolean"):
        load_artifact(
            tmp_path / "unused.json",
            artifact_name="benchmark_snapshot_manifest",
            schema_dir=schema_dir,
        )


def test_list_artifact_schemas_rejects_required_fields_marked_optional(
    tmp_path: Path,
) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_path = schema_dir / "benchmark_snapshot_manifest.json"
    _write_schema_file(
        schema_path,
        artifact_name="benchmark_snapshot_manifest",
    )
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    payload["required_fields"][0]["required"] = False
    schema_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"required_fields\[0\]\.required must be true",
    ):
        list_artifact_schemas(schema_dir=schema_dir)


def test_load_artifact_rejects_non_string_metric_unit(tmp_path: Path) -> None:
    metric_path = tmp_path / "metric.json"
    metric_path.write_text(
        json.dumps(
            {
                "schema_name": "benchmark_metric_output_payload",
                "schema_version": "v1",
                "run_id": "fixture_run",
                "snapshot_id": "fixture_snapshot",
                "baseline_id": "v0_current",
                "entity_type": "gene",
                "horizon": "3y",
                "metric_name": "average_precision_any_positive_outcome",
                "metric_value": 0.75,
                "metric_unit": False,
                "cohort_size": 4,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="metric_unit must be a string",
    ):
        load_artifact(
            metric_path,
            artifact_name="benchmark_metric_output_payload",
        )


def test_load_artifact_rejects_missing_metric_unit(tmp_path: Path) -> None:
    metric_path = tmp_path / "metric.json"
    metric_path.write_text(
        json.dumps(
            {
                "schema_name": "benchmark_metric_output_payload",
                "schema_version": "v1",
                "run_id": "fixture_run",
                "snapshot_id": "fixture_snapshot",
                "baseline_id": "v0_current",
                "entity_type": "gene",
                "horizon": "3y",
                "metric_name": "average_precision_any_positive_outcome",
                "metric_value": 0.75,
                "cohort_size": 4,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="is missing required fields: metric_unit",
    ):
        load_artifact(
            metric_path,
            artifact_name="benchmark_metric_output_payload",
        )


def test_list_artifact_schemas_rejects_metric_unit_as_optional(tmp_path: Path) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_path = schema_dir / "benchmark_metric_output_payload.json"
    _write_schema_file(
        schema_path,
        artifact_name="benchmark_metric_output_payload",
    )
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    metric_unit_field = next(
        field
        for field in payload["required_fields"]
        if field["name"] == "metric_unit"
    )
    payload["required_fields"] = [
        field
        for field in payload["required_fields"]
        if field["name"] != "metric_unit"
    ]
    metric_unit_field["required"] = False
    payload["optional_fields"].append(metric_unit_field)
    schema_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="benchmark_metric_output_payload schema must require metric_unit",
    ):
        list_artifact_schemas(schema_dir=schema_dir)


def test_list_artifact_schemas_rejects_non_string_schema_field_metadata(
    tmp_path: Path,
) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    schema_path = schema_dir / "benchmark_snapshot_manifest.json"
    _write_schema_file(
        schema_path,
        artifact_name="benchmark_snapshot_manifest",
    )
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    payload["required_fields"][0]["description"] = False
    schema_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="description must be a string"):
        list_artifact_schemas(schema_dir=schema_dir)


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
    hypothesis_packets_artifact = load_artifact(tmp_path / "hypothesis_packets_v1.json")

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
    assert hypothesis_packets_artifact.artifact_name == "hypothesis_packets_v1"
    assert hypothesis_packets_artifact.payload["packet_count"] > 0


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
        manifest_file=snapshot_path,
        cohort_members_file=Path(
            "data/benchmark/fixtures/scz_small/cohort_members.csv"
        ).resolve(),
        future_outcomes_file=Path(
            "data/benchmark/fixtures/scz_small/future_outcomes.csv"
        ).resolve(),
        output_file=cohort_labels_path,
    )
    cohort_members_artifact = load_artifact(
        tmp_path / "benchmark_cohort_members.csv"
    )
    assert cohort_members_artifact.artifact_name == "benchmark_cohort_members"
    assert len(cohort_members_artifact.payload) == 3
    source_cohort_members_artifact = load_artifact(
        tmp_path / "source_cohort_members.csv"
    )
    assert (
        source_cohort_members_artifact.artifact_name
        == "benchmark_source_cohort_members"
    )
    assert len(source_cohort_members_artifact.payload) == 3
    source_future_outcomes_artifact = load_artifact(
        tmp_path / "source_future_outcomes.csv"
    )
    assert (
        source_future_outcomes_artifact.artifact_name
        == "benchmark_source_future_outcomes"
    )
    assert len(source_future_outcomes_artifact.payload) > 0
    cohort_manifest_artifact = load_artifact(
        tmp_path / "benchmark_cohort_manifest.json"
    )
    assert cohort_manifest_artifact.artifact_name == "benchmark_cohort_manifest"
    assert cohort_manifest_artifact.payload.cohort_id == snapshot_artifact.payload.cohort_id
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


def test_benchmark_source_future_outcomes_artifact_accepts_header_only_csv(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "snapshot_manifest.json"
    cohort_labels_path = tmp_path / "cohort_labels.csv"
    future_outcomes_path = tmp_path / "future_outcomes.csv"
    future_outcomes_path.write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n",
        encoding="utf-8",
    )

    snapshot_artifact = materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-04-03",
    )

    materialize_benchmark_cohort_labels(
        manifest=load_artifact(
            snapshot_path,
            artifact_name="benchmark_snapshot_manifest",
        ).payload,
        manifest_file=snapshot_path,
        cohort_members_file=Path(
            "data/benchmark/fixtures/scz_small/cohort_members.csv"
        ).resolve(),
        future_outcomes_file=future_outcomes_path,
        output_file=cohort_labels_path,
    )

    source_future_outcomes_artifact = load_artifact(
        tmp_path / "source_future_outcomes.csv",
        artifact_name="benchmark_source_future_outcomes",
    )

    assert snapshot_artifact["snapshot_id"] == "scz_fixture_2024_06_30"
    assert (
        source_future_outcomes_artifact.artifact_name
        == "benchmark_source_future_outcomes"
    )
    assert source_future_outcomes_artifact.payload == ()


def test_benchmark_source_future_outcomes_artifact_rejects_malformed_rows(
    tmp_path: Path,
) -> None:
    malformed_path = tmp_path / "source_future_outcomes.csv"
    malformed_path.write_text(
        (
            "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
            "gene,ENSG00000162946,future_schizophrenia_program_started,,manual,\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"outcome_date (is required|must be an ISO date in YYYY-MM-DD format)",
    ):
        load_artifact(
            malformed_path,
            artifact_name="benchmark_source_future_outcomes",
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


@pytest.mark.parametrize(
    "artifact_name, manifest_filename",
    [
        ("program_memory_release", "release_manifest.json"),
        ("benchmark_release", "benchmark_release_manifest.json"),
        ("rescue_release", "rescue_release_manifest.json"),
        ("variant_context_release", "atlas_release_manifest.json"),
        ("policy_release", "policy_manifest.json"),
        ("hypothesis_release", "release_manifest.json"),
    ],
)
def test_release_manifest_families_validate_top_level_entrypoints(
    tmp_path: Path,
    artifact_name: str,
    manifest_filename: str,
) -> None:
    bundle_dir = tmp_path / artifact_name
    bundle_dir.mkdir()
    readme_path = bundle_dir / "README.txt"
    readme_path.write_text(f"{artifact_name} bundle\n", encoding="utf-8")
    manifest_path = bundle_dir / manifest_filename
    _write_release_manifest(
        manifest_path,
        artifact_name=artifact_name,
        files=[
            {
                "artifact_id": "bundle_notes",
                "path": readme_path.name,
                "sha256": _sha256_path(readme_path),
            }
        ],
    )

    artifact = load_artifact(manifest_path)

    assert artifact.artifact_name == artifact_name
    assert artifact.payload.release_family == artifact_name
    assert artifact.payload.compatibility_phase == "dual_write"
    assert artifact.payload.files[0].artifact_id == "bundle_notes"


def test_release_manifest_validates_nested_registered_artifacts(
    tmp_path: Path,
) -> None:
    bundle_dir = tmp_path / "benchmark_release"
    bundle_dir.mkdir()
    snapshot_path = bundle_dir / "benchmark_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    summary_path = bundle_dir / "summary.md"
    summary_path.write_text("benchmark release summary\n", encoding="utf-8")
    manifest_path = bundle_dir / "benchmark_release_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="benchmark_release",
        files=[
            {
                "artifact_id": "snapshot_manifest",
                "path": snapshot_path.name,
                "sha256": _sha256_path(snapshot_path),
                "artifact_name": "benchmark_snapshot_manifest",
                "expected_schema_version": "v1",
            },
            {
                "artifact_id": "summary",
                "path": summary_path.name,
                "sha256": _sha256_path(summary_path),
            },
        ],
    )

    artifact = load_artifact(manifest_path)

    assert artifact.artifact_name == "benchmark_release"
    assert {entry.artifact_id for entry in artifact.payload.files} == {
        "snapshot_manifest",
        "summary",
    }


def test_release_manifest_rejects_missing_required_files(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "program_memory_release"
    bundle_dir.mkdir()
    manifest_path = bundle_dir / "release_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="program_memory_release",
        files=[
            {
                "artifact_id": "coverage_manifest",
                "path": "coverage_manifest.json",
                "sha256": "0" * 64,
            }
        ],
    )

    with pytest.raises(ValueError, match="missing required file"):
        load_artifact(manifest_path)


def test_release_manifest_rejects_checksum_drift(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "policy_release"
    bundle_dir.mkdir()
    summary_path = bundle_dir / "policy_notes.md"
    summary_path.write_text("policy bundle\n", encoding="utf-8")
    manifest_path = bundle_dir / "policy_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="policy_release",
        files=[
            {
                "artifact_id": "policy_notes",
                "path": summary_path.name,
                "sha256": _sha256_path(summary_path),
            }
        ],
    )
    summary_path.write_text("policy bundle changed\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksum mismatch"):
        load_artifact(manifest_path)


def test_benchmark_snapshot_manifest_rejects_declared_schema_version_drift(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "benchmark_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["schema_version"] = "v99"
    snapshot_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="benchmark_snapshot_manifest schema_version must be v1"):
        load_artifact(snapshot_path)


def test_release_manifest_rejects_expected_nested_schema_version_mismatch(
    tmp_path: Path,
) -> None:
    bundle_dir = tmp_path / "benchmark_release"
    bundle_dir.mkdir()
    snapshot_path = bundle_dir / "benchmark_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    manifest_path = bundle_dir / "benchmark_release_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="benchmark_release",
        files=[
            {
                "artifact_id": "snapshot_manifest",
                "path": snapshot_path.name,
                "sha256": _sha256_path(snapshot_path),
                "artifact_name": "benchmark_snapshot_manifest",
                "expected_schema_version": "v99",
            }
        ],
    )

    with pytest.raises(ValueError, match="schema version mismatch"):
        load_artifact(manifest_path)


def test_release_manifest_rejects_nested_declared_schema_version_drift(
    tmp_path: Path,
) -> None:
    bundle_dir = tmp_path / "benchmark_release"
    bundle_dir.mkdir()
    snapshot_path = bundle_dir / "benchmark_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["schema_version"] = "v99"
    snapshot_path.write_text(
        json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = bundle_dir / "benchmark_release_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="benchmark_release",
        files=[
            {
                "artifact_id": "snapshot_manifest",
                "path": snapshot_path.name,
                "sha256": _sha256_path(snapshot_path),
                "artifact_name": "benchmark_snapshot_manifest",
                "expected_schema_version": "v1",
            }
        ],
    )

    with pytest.raises(ValueError, match="benchmark_snapshot_manifest schema_version must be v1"):
        load_artifact(manifest_path)


def test_release_manifest_validates_nested_artifacts_with_custom_schema_dir(
    tmp_path: Path,
) -> None:
    schema_dir = tmp_path / "custom_schemas"
    schema_dir.mkdir()
    _write_schema_file(
        schema_dir / "benchmark_release.json",
        artifact_name="benchmark_release",
    )
    _write_schema_file(
        schema_dir / "benchmark_snapshot_manifest.json",
        artifact_name="benchmark_snapshot_manifest",
        schema_version="v99",
    )

    bundle_dir = tmp_path / "benchmark_release"
    bundle_dir.mkdir()
    snapshot_path = bundle_dir / "benchmark_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=Path(
            "data/benchmark/fixtures/scz_small/snapshot_request.json"
        ).resolve(),
        archive_index_file=Path(
            "data/benchmark/fixtures/scz_small/source_archives.json"
        ).resolve(),
        output_file=snapshot_path,
        materialized_at="2026-03-28",
    )
    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["schema_version"] = "v99"
    snapshot_path.write_text(
        json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest_path = bundle_dir / "benchmark_release_manifest.json"
    _write_release_manifest(
        manifest_path,
        artifact_name="benchmark_release",
        files=[
            {
                "artifact_id": "snapshot_manifest",
                "path": snapshot_path.name,
                "sha256": _sha256_path(snapshot_path),
                "artifact_name": "benchmark_snapshot_manifest",
                "expected_schema_version": "v99",
            }
        ],
    )

    artifact = load_artifact(manifest_path, schema_dir=schema_dir)

    assert artifact.artifact_name == "benchmark_release"
    assert artifact.schema.schema_dir == schema_dir.resolve()


def test_example_prospective_registration_validates_against_registered_schema() -> None:
    registration_artifact = load_artifact(
        Path(
            "data/prospective_registry/registrations/"
            "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
        ).resolve()
    )

    assert registration_artifact.artifact_name == "prospective_prediction_registration"
    assert registration_artifact.payload.registration_id == (
        "forecast_chrm4_acute_translation_guardrails_2026_03_31"
    )
    assert registration_artifact.payload.packet_artifact.packet_id == (
        "ENSG00000180720__acute_translation_guardrails_v1"
    )
    assert registration_artifact.payload.frozen_forecast_payload["predicted_outcome"] == (
        "advance"
    )

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


def test_program_memory_v3_stub_artifacts_validate_against_registered_schemas(
    tmp_path: Path,
) -> None:
    harvest_dir = tmp_path / "harvest"
    adjudicated_dir = tmp_path / "adjudicated"
    insight_packet_path = tmp_path / "packet" / "insight_packet.json"

    materialize_program_memory_v3_harvest_bundle(
        output_dir=harvest_dir,
        program_id="karxt",
        program_label="KarXT",
        materialized_at="2026-04-12",
        source_urls=("https://clinicaltrials.gov/study/NCT04659161",),
        corpus_tier="A",
    )
    materialize_program_memory_v3_adjudication_bundle(
        harvest_dir=harvest_dir,
        output_dir=adjudicated_dir,
        adjudication_id="karxt_review_v1",
        reviewer="tester@example.com",
        reviewed_at="2026-04-12",
    )
    materialize_program_memory_v3_insight_packet(
        program_dir=adjudicated_dir,
        output_file=insight_packet_path,
        packet_id="karxt_packet_v1",
        packet_question="What should change about beliefs for KarXT?",
        scope_summary="Single-program review packet.",
        generated_at="2026-04-12",
    )

    assert (
        load_artifact(harvest_dir / "source_manifest.json").artifact_name
        == "program_memory_v3_source_manifest"
    )
    assert (
        load_artifact(harvest_dir / "study_index.csv").artifact_name
        == "program_memory_v3_study_index"
    )
    assert (
        load_artifact(harvest_dir / "result_observations.csv").artifact_name
        == "program_memory_v3_result_observations"
    )
    assert (
        load_artifact(harvest_dir / "harm_observations.csv").artifact_name
        == "program_memory_v3_harm_observations"
    )
    assert (
        load_artifact(harvest_dir / "contradictions.csv").artifact_name
        == "program_memory_v3_contradiction_log"
    )
    assert (
        load_artifact(adjudicated_dir / "claims.csv").artifact_name
        == "program_memory_v3_claim_ledger"
    )
    assert (
        load_artifact(adjudicated_dir / "contradictions.csv").artifact_name
        == "program_memory_v3_contradiction_log"
    )
    assert (
        load_artifact(adjudicated_dir / "caveats.csv").artifact_name
        == "program_memory_v3_caveats"
    )
    assert (
        load_artifact(adjudicated_dir / "belief_updates.csv").artifact_name
        == "program_memory_v3_belief_updates"
    )
    assert (
        load_artifact(adjudicated_dir / "program_card.json").artifact_name
        == "program_memory_v3_program_card"
    )
    assert load_artifact(insight_packet_path).artifact_name == "program_memory_v3_insight_packet"
