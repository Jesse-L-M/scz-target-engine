from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scz_target_engine.benchmark_protocol import VALID_ENTITY_TYPES
from scz_target_engine.io import read_json


RESCUE_TASK_CONTRACT_ARTIFACT_NAME = "rescue_task_contract"
STRICT_RESCUE_LEAKAGE_POLICY_ID = "strict_rescue_task_boundary_v1"
DEFERRED_FREEZE_MANIFEST_POLICY = "deferred_until_pr40a"
CURRENT_HEAD_POLICY = "reject_unfrozen_current_head_state"
VALID_CONTRACT_SCOPES = ("rescue_only", "benchmark_bridge_documented")
VALID_ARTIFACT_CHANNELS = (
    "ranking_input",
    "evaluation_target",
    "task_output",
    "task_metadata",
)
VALID_ARTIFACT_DIRECTIONS = ("consumes", "emits")
VALID_ARTIFACT_AVAILABILITIES = (
    "pre_cutoff",
    "post_cutoff",
    "derived",
    "operator_supplied",
)


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _require_non_empty_tuple(values: tuple[object, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")


def _require_mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


@dataclass(frozen=True)
class RescueArtifactContract:
    artifact_id: str
    artifact_label: str
    io_direction: str
    channel: str
    availability: str
    file_format: str
    required: bool
    description: str
    leakage_rule: str
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.artifact_id, "artifact_id")
        _require_text(self.artifact_label, "artifact_label")
        if self.io_direction not in VALID_ARTIFACT_DIRECTIONS:
            raise ValueError("io_direction must be a supported rescue artifact direction")
        if self.channel not in VALID_ARTIFACT_CHANNELS:
            raise ValueError("channel must be a supported rescue artifact channel")
        if self.availability not in VALID_ARTIFACT_AVAILABILITIES:
            raise ValueError(
                "availability must be a supported rescue artifact availability"
            )
        _require_text(self.file_format, "file_format")
        _require_text(self.description, "description")
        _require_text(self.leakage_rule, "leakage_rule")

        if self.channel == "ranking_input":
            if self.io_direction != "consumes":
                raise ValueError("ranking_input artifacts must be consumed by the task")
            if self.availability != "pre_cutoff":
                raise ValueError("ranking_input artifacts must remain pre_cutoff")
        if self.channel == "evaluation_target":
            if self.io_direction != "consumes":
                raise ValueError(
                    "evaluation_target artifacts must be consumed by the task"
                )
            if self.availability != "post_cutoff":
                raise ValueError(
                    "evaluation_target artifacts must remain post_cutoff"
                )
        if self.channel == "task_output":
            if self.io_direction != "emits":
                raise ValueError("task_output artifacts must be emitted by the task")
            if self.availability != "derived":
                raise ValueError("task_output artifacts must remain derived")
        if self.channel == "task_metadata" and self.availability == "post_cutoff":
            raise ValueError("task_metadata artifacts must not depend on post_cutoff data")

    def to_dict(self) -> dict[str, object]:
        payload = {
            "artifact_id": self.artifact_id,
            "artifact_label": self.artifact_label,
            "io_direction": self.io_direction,
            "channel": self.channel,
            "availability": self.availability,
            "file_format": self.file_format,
            "required": self.required,
            "description": self.description,
            "leakage_rule": self.leakage_rule,
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueArtifactContract:
        return cls(
            artifact_id=str(payload["artifact_id"]),
            artifact_label=str(payload["artifact_label"]),
            io_direction=str(payload["io_direction"]),
            channel=str(payload["channel"]),
            availability=str(payload["availability"]),
            file_format=str(payload["file_format"]),
            required=_require_bool(payload["required"], "required"),
            description=str(payload["description"]),
            leakage_rule=str(payload["leakage_rule"]),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueLeakageBoundary:
    policy_id: str = STRICT_RESCUE_LEAKAGE_POLICY_ID
    requires_explicit_cutoff: bool = True
    requires_explicit_artifact_contracts: bool = True
    forbid_post_cutoff_artifacts_in_ranking: bool = True
    forbid_evaluation_labels_in_ranking: bool = True
    freeze_manifest_required: bool = False
    freeze_manifest_policy: str = DEFERRED_FREEZE_MANIFEST_POLICY
    current_head_policy: str = CURRENT_HEAD_POLICY
    notes: str = ""

    def __post_init__(self) -> None:
        if self.policy_id != STRICT_RESCUE_LEAKAGE_POLICY_ID:
            raise ValueError(
                "policy_id must remain strict_rescue_task_boundary_v1 for rescue tasks"
            )
        if not self.requires_explicit_cutoff:
            raise ValueError("requires_explicit_cutoff must remain enabled")
        if not self.requires_explicit_artifact_contracts:
            raise ValueError("requires_explicit_artifact_contracts must remain enabled")
        if not self.forbid_post_cutoff_artifacts_in_ranking:
            raise ValueError(
                "forbid_post_cutoff_artifacts_in_ranking must remain enabled"
            )
        if not self.forbid_evaluation_labels_in_ranking:
            raise ValueError(
                "forbid_evaluation_labels_in_ranking must remain enabled"
            )
        if self.freeze_manifest_required:
            raise ValueError(
                "freeze_manifest_required must stay disabled until PR-40A lands"
            )
        if self.freeze_manifest_policy != DEFERRED_FREEZE_MANIFEST_POLICY:
            raise ValueError(
                "freeze_manifest_policy must remain deferred_until_pr40a"
            )
        if self.current_head_policy != CURRENT_HEAD_POLICY:
            raise ValueError(
                "current_head_policy must remain reject_unfrozen_current_head_state"
            )
        _require_text(self.notes, "notes")

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "requires_explicit_cutoff": self.requires_explicit_cutoff,
            "requires_explicit_artifact_contracts": self.requires_explicit_artifact_contracts,
            "forbid_post_cutoff_artifacts_in_ranking": self.forbid_post_cutoff_artifacts_in_ranking,
            "forbid_evaluation_labels_in_ranking": self.forbid_evaluation_labels_in_ranking,
            "freeze_manifest_required": self.freeze_manifest_required,
            "freeze_manifest_policy": self.freeze_manifest_policy,
            "current_head_policy": self.current_head_policy,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueLeakageBoundary:
        return cls(
            policy_id=str(payload["policy_id"]),
            requires_explicit_cutoff=_require_bool(
                payload["requires_explicit_cutoff"],
                "requires_explicit_cutoff",
            ),
            requires_explicit_artifact_contracts=_require_bool(
                payload["requires_explicit_artifact_contracts"],
                "requires_explicit_artifact_contracts",
            ),
            forbid_post_cutoff_artifacts_in_ranking=_require_bool(
                payload["forbid_post_cutoff_artifacts_in_ranking"],
                "forbid_post_cutoff_artifacts_in_ranking",
            ),
            forbid_evaluation_labels_in_ranking=_require_bool(
                payload["forbid_evaluation_labels_in_ranking"],
                "forbid_evaluation_labels_in_ranking",
            ),
            freeze_manifest_required=_require_bool(
                payload["freeze_manifest_required"],
                "freeze_manifest_required",
            ),
            freeze_manifest_policy=str(payload["freeze_manifest_policy"]),
            current_head_policy=str(payload["current_head_policy"]),
            notes=str(payload["notes"]),
        )


@dataclass(frozen=True)
class RescueTaskContract:
    schema_name: str
    schema_version: str
    contract_version: str
    suite_id: str
    suite_label: str
    task_id: str
    task_label: str
    task_type: str
    disease: str
    entity_type: str
    summary: str
    contract_scope: str
    artifact_contracts: tuple[RescueArtifactContract, ...]
    leakage_boundary: RescueLeakageBoundary
    benchmark_task_bridge_id: str = ""
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_TASK_CONTRACT_ARTIFACT_NAME:
            raise ValueError(
                "schema_name must remain rescue_task_contract for rescue contracts"
            )
        _require_text(self.schema_version, "schema_version")
        _require_text(self.contract_version, "contract_version")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.suite_label, "suite_label")
        _require_text(self.task_id, "task_id")
        _require_text(self.task_label, "task_label")
        _require_text(self.task_type, "task_type")
        _require_text(self.disease, "disease")
        _require_text(self.summary, "summary")
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must remain within the supported entity types")
        if self.contract_scope not in VALID_CONTRACT_SCOPES:
            raise ValueError("contract_scope must be a supported rescue contract scope")

        if self.contract_scope == "rescue_only" and self.benchmark_task_bridge_id:
            raise ValueError(
                "benchmark_task_bridge_id must be empty when contract_scope is rescue_only"
            )
        if (
            self.contract_scope == "benchmark_bridge_documented"
            and not self.benchmark_task_bridge_id
        ):
            raise ValueError(
                "benchmark_task_bridge_id is required for benchmark_bridge_documented scope"
            )

        _require_non_empty_tuple(self.artifact_contracts, "artifact_contracts")
        artifact_ids = [artifact.artifact_id for artifact in self.artifact_contracts]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("artifact_contracts must not repeat artifact_id")

        channels = {artifact.channel for artifact in self.artifact_contracts}
        required_channels = {"ranking_input", "evaluation_target", "task_output"}
        missing_channels = sorted(required_channels.difference(channels))
        if missing_channels:
            raise ValueError(
                "artifact_contracts must include explicit rescue channels: "
                + ", ".join(missing_channels)
            )

    def to_dict(self) -> dict[str, object]:
        payload = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "contract_version": self.contract_version,
            "suite_id": self.suite_id,
            "suite_label": self.suite_label,
            "task_id": self.task_id,
            "task_label": self.task_label,
            "task_type": self.task_type,
            "disease": self.disease,
            "entity_type": self.entity_type,
            "summary": self.summary,
            "contract_scope": self.contract_scope,
            "artifact_contracts": [
                artifact_contract.to_dict()
                for artifact_contract in self.artifact_contracts
            ],
            "leakage_boundary": self.leakage_boundary.to_dict(),
        }
        if self.benchmark_task_bridge_id:
            payload["benchmark_task_bridge_id"] = self.benchmark_task_bridge_id
        if self.notes:
            payload["notes"] = self.notes
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueTaskContract:
        leakage_boundary = RescueLeakageBoundary.from_dict(
            _require_mapping(payload["leakage_boundary"], "leakage_boundary")
        )
        artifact_payloads = payload["artifact_contracts"]
        if not isinstance(artifact_payloads, list):
            raise ValueError("artifact_contracts must be a list")
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            contract_version=str(payload["contract_version"]),
            suite_id=str(payload["suite_id"]),
            suite_label=str(payload["suite_label"]),
            task_id=str(payload["task_id"]),
            task_label=str(payload["task_label"]),
            task_type=str(payload["task_type"]),
            disease=str(payload["disease"]),
            entity_type=str(payload["entity_type"]),
            summary=str(payload["summary"]),
            contract_scope=str(payload["contract_scope"]),
            artifact_contracts=tuple(
                RescueArtifactContract.from_dict(
                    _require_mapping(item, "artifact_contracts[]")
                )
                for item in artifact_payloads
            ),
            leakage_boundary=leakage_boundary,
            benchmark_task_bridge_id=str(payload.get("benchmark_task_bridge_id", "")),
            notes=str(payload.get("notes", "")),
        )


def read_rescue_task_contract(path: Path) -> RescueTaskContract:
    payload = read_json(path)
    return RescueTaskContract.from_dict(
        _require_mapping(payload, "rescue_task_contract")
    )
