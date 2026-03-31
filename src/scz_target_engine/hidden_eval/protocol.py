from __future__ import annotations

from pathlib import Path
from typing import Any

from scz_target_engine.io import read_json

HIDDEN_EVAL_PROTOCOL_ID = "rescue_hidden_eval_v1"
HIDDEN_EVAL_SCHEMA_VERSION = "v1"
HIDDEN_EVAL_TASK_PACKAGE_SCHEMA_NAME = "hidden_eval_task_package"
HIDDEN_EVAL_SUBMISSION_SCHEMA_NAME = "hidden_eval_submission_manifest"
HIDDEN_EVAL_SIMULATION_SCHEMA_NAME = "hidden_eval_simulation_manifest"
HIDDEN_EVAL_PUBLIC_SCORECARD_SCHEMA_NAME = "hidden_eval_public_scorecard"

REQUIRED_SUBMISSION_COLUMNS = ("task_id", "gene_id", "rank")
RECOMMENDED_SUBMISSION_COLUMNS = ("gene_symbol", "split_name", "score")


def _require_mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_int(value: object, field_name: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_list_of_text(value: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list")
    items: list[str] = []
    for index, item in enumerate(value):
        items.append(_require_text(item, f"{field_name}[{index}]"))
    return tuple(items)


def validate_hidden_eval_task_package_manifest(payload: object) -> dict[str, Any]:
    manifest = _require_mapping(payload, "hidden_eval_task_package")
    if _require_text(manifest.get("schema_name"), "schema_name") != (
        HIDDEN_EVAL_TASK_PACKAGE_SCHEMA_NAME
    ):
        raise ValueError(
            "schema_name must remain hidden_eval_task_package for public task packages"
        )
    if _require_text(manifest.get("schema_version"), "schema_version") != (
        HIDDEN_EVAL_SCHEMA_VERSION
    ):
        raise ValueError(
            f"schema_version must remain {HIDDEN_EVAL_SCHEMA_VERSION} for public task packages"
        )
    if _require_text(manifest.get("protocol_id"), "protocol_id") != HIDDEN_EVAL_PROTOCOL_ID:
        raise ValueError(f"protocol_id must remain {HIDDEN_EVAL_PROTOCOL_ID}")
    _require_text(manifest.get("package_id"), "package_id")
    _require_text(manifest.get("task_id"), "task_id")
    _require_text(manifest.get("task_label"), "task_label")
    _require_text(manifest.get("task_type"), "task_type")
    _require_text(manifest.get("entity_type"), "entity_type")
    _require_text(manifest.get("materialized_at"), "materialized_at")
    _require_text(manifest.get("cutoff_date"), "cutoff_date")
    _require_text(manifest.get("frozen_at"), "frozen_at")

    public_artifacts = _require_mapping(manifest.get("public_artifacts"), "public_artifacts")
    _require_text(public_artifacts.get("ranking_input_file"), "public_artifacts.ranking_input_file")
    _require_text(
        public_artifacts.get("submission_template_file"),
        "public_artifacts.submission_template_file",
    )
    _require_text(public_artifacts.get("ranking_dataset_id"), "public_artifacts.ranking_dataset_id")
    _require_text(
        public_artifacts.get("ranking_dataset_sha256"),
        "public_artifacts.ranking_dataset_sha256",
    )
    _require_int(public_artifacts.get("candidate_count"), "public_artifacts.candidate_count")
    _require_list_of_text(
        public_artifacts.get("primary_key_fields"),
        "public_artifacts.primary_key_fields",
    )

    submission_contract = _require_mapping(
        manifest.get("submission_contract"),
        "submission_contract",
    )
    required_columns = _require_list_of_text(
        submission_contract.get("required_columns"),
        "submission_contract.required_columns",
    )
    missing_columns = [
        column_name
        for column_name in REQUIRED_SUBMISSION_COLUMNS
        if column_name not in required_columns
    ]
    if missing_columns:
        raise ValueError(
            "submission_contract.required_columns must include: "
            + ", ".join(missing_columns)
        )
    _require_list_of_text(
        submission_contract.get("recommended_columns"),
        "submission_contract.recommended_columns",
    )
    _require_bool(
        submission_contract.get("ignored_additional_columns_allowed"),
        "submission_contract.ignored_additional_columns_allowed",
    )
    _require_text(
        submission_contract.get("entity_id_field"),
        "submission_contract.entity_id_field",
    )
    _require_int(
        submission_contract.get("expected_row_count"),
        "submission_contract.expected_row_count",
    )

    boundaries = _require_mapping(
        manifest.get("protocol_boundaries"),
        "protocol_boundaries",
    )
    _require_list_of_text(
        boundaries.get("submitter_visible"),
        "protocol_boundaries.submitter_visible",
    )
    _require_list_of_text(
        boundaries.get("hidden_evaluator_only"),
        "protocol_boundaries.hidden_evaluator_only",
    )
    frozen = _require_mapping(
        boundaries.get("frozen_by_contract"),
        "protocol_boundaries.frozen_by_contract",
    )
    _require_text(frozen.get("task_card_id"), "protocol_boundaries.frozen_by_contract.task_card_id")
    _require_list_of_text(
        frozen.get("freeze_manifest_ids"),
        "protocol_boundaries.frozen_by_contract.freeze_manifest_ids",
    )
    _require_list_of_text(
        frozen.get("split_manifest_ids"),
        "protocol_boundaries.frozen_by_contract.split_manifest_ids",
    )
    _require_list_of_text(
        frozen.get("lineage_ids"),
        "protocol_boundaries.frozen_by_contract.lineage_ids",
    )
    _require_text(
        frozen.get("leakage_boundary_policy_id"),
        "protocol_boundaries.frozen_by_contract.leakage_boundary_policy_id",
    )
    _require_list_of_text(manifest.get("compatibility_notes"), "compatibility_notes")
    _require_text(manifest.get("notes"), "notes")
    return manifest


def validate_hidden_eval_submission_manifest(payload: object) -> dict[str, Any]:
    manifest = _require_mapping(payload, "hidden_eval_submission_manifest")
    if _require_text(manifest.get("schema_name"), "schema_name") != (
        HIDDEN_EVAL_SUBMISSION_SCHEMA_NAME
    ):
        raise ValueError(
            "schema_name must remain hidden_eval_submission_manifest for submissions"
        )
    if _require_text(manifest.get("schema_version"), "schema_version") != (
        HIDDEN_EVAL_SCHEMA_VERSION
    ):
        raise ValueError(
            f"schema_version must remain {HIDDEN_EVAL_SCHEMA_VERSION} for submissions"
        )
    if _require_text(manifest.get("protocol_id"), "protocol_id") != HIDDEN_EVAL_PROTOCOL_ID:
        raise ValueError(f"protocol_id must remain {HIDDEN_EVAL_PROTOCOL_ID}")
    _require_text(manifest.get("submission_id"), "submission_id")
    _require_text(manifest.get("task_id"), "task_id")
    _require_text(manifest.get("package_id"), "package_id")
    _require_text(manifest.get("submitter_id"), "submitter_id")
    _require_text(manifest.get("scorer_id"), "scorer_id")
    _require_text(manifest.get("created_at"), "created_at")
    _require_text(manifest.get("predictions_file"), "predictions_file")
    _require_text(manifest.get("prediction_sha256"), "prediction_sha256")
    _require_int(manifest.get("prediction_row_count"), "prediction_row_count")
    _require_text(manifest.get("package_manifest_sha256"), "package_manifest_sha256")
    submitted_columns = _require_list_of_text(
        manifest.get("submitted_columns"),
        "submitted_columns",
    )
    missing_columns = [
        column_name
        for column_name in REQUIRED_SUBMISSION_COLUMNS
        if column_name not in submitted_columns
    ]
    if missing_columns:
        raise ValueError(
            "submitted_columns must include: " + ", ".join(missing_columns)
        )
    return manifest


def read_hidden_eval_task_package_manifest(path: Path) -> dict[str, Any]:
    return validate_hidden_eval_task_package_manifest(read_json(path))


def read_hidden_eval_submission_manifest(path: Path) -> dict[str, Any]:
    return validate_hidden_eval_submission_manifest(read_json(path))


__all__ = [
    "HIDDEN_EVAL_PROTOCOL_ID",
    "HIDDEN_EVAL_PUBLIC_SCORECARD_SCHEMA_NAME",
    "HIDDEN_EVAL_SCHEMA_VERSION",
    "HIDDEN_EVAL_SIMULATION_SCHEMA_NAME",
    "HIDDEN_EVAL_SUBMISSION_SCHEMA_NAME",
    "HIDDEN_EVAL_TASK_PACKAGE_SCHEMA_NAME",
    "RECOMMENDED_SUBMISSION_COLUMNS",
    "REQUIRED_SUBMISSION_COLUMNS",
    "read_hidden_eval_submission_manifest",
    "read_hidden_eval_task_package_manifest",
    "validate_hidden_eval_submission_manifest",
    "validate_hidden_eval_task_package_manifest",
]
