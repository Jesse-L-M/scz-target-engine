from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isclose
from pathlib import Path
from typing import Any

from scz_target_engine.benchmark_protocol import VALID_ENTITY_TYPES
from scz_target_engine.io import read_json
from scz_target_engine.rescue.contracts import (
    CURRENT_HEAD_POLICY,
    RESCUE_TASK_CONTRACT_ARTIFACT_NAME,
    STRICT_RESCUE_LEAKAGE_POLICY_ID,
    RescueTaskContract,
    read_rescue_task_contract,
)


REPO_ROOT = Path(__file__).resolve().parents[3]

RESCUE_DATASET_CARD_ARTIFACT_NAME = "rescue_dataset_card"
RESCUE_TASK_CARD_ARTIFACT_NAME = "rescue_task_card"
RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME = "rescue_freeze_manifest"
RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME = "rescue_split_manifest"
RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME = "rescue_raw_to_frozen_lineage"

VALID_GOVERNANCE_STATUSES = ("example", "planned", "active")
VALID_DATASET_ROLES = (
    "ranking_input",
    "evaluation_target",
    "task_output",
    "task_metadata",
)
VALID_DATASET_AVAILABILITIES = (
    "pre_cutoff",
    "post_cutoff",
    "derived",
    "operator_supplied",
)
VALID_SOURCE_STAGES = ("raw_snapshot", "frozen", "derived")
VALID_FREEZE_SCOPES = ("ranking_only", "task_bundle")
VALID_SPLIT_PURPOSES = ("train", "validation", "test")
VALID_LINEAGE_STEP_KINDS = (
    "extract",
    "normalize",
    "join",
    "filter",
    "label_holdout",
    "freeze",
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _require_mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_float(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    return float(value)


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_string_list(value: object, field_name: str) -> tuple[str, ...]:
    values = tuple(_require_text(item, f"{field_name}[]") for item in _require_list(value, field_name))
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    return values


def _require_iso_date(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date") from exc
    return text


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date") from exc


def _require_sha256(value: object, field_name: str) -> str:
    digest = _require_text(value, field_name)
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest.lower()):
        raise ValueError(f"{field_name} must be a lowercase hex sha256 digest")
    return digest.lower()


def _require_enum(value: str, field_name: str, valid_values: tuple[str, ...]) -> str:
    if value not in valid_values:
        joined = ", ".join(valid_values)
        raise ValueError(f"{field_name} must be one of: {joined}")
    return value


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _require_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not repeat values")


def _load_contract(contract_path: str) -> RescueTaskContract:
    return read_rescue_task_contract(_resolve_repo_relative_path(contract_path))


def _read_json_mapping(path: Path, artifact_name: str) -> dict[str, Any]:
    payload = read_json(path)
    return _require_mapping(payload, artifact_name)


def _validate_source_cutoff_alignment(
    source_snapshots: tuple["RescueSourceSnapshot", ...],
    *,
    cutoff: date,
    context: str,
) -> None:
    for source in source_snapshots:
        captured_at = _parse_date(source.captured_at, f"{context}.captured_at")
        if source.availability == "pre_cutoff" and captured_at > cutoff:
            raise ValueError(
                f"{context} pre_cutoff sources must not be captured after cutoff_date"
            )
        if source.availability == "post_cutoff" and captured_at <= cutoff:
            raise ValueError(
                f"{context} post_cutoff sources must be captured after cutoff_date"
            )


def _reconcile_sources_against_freeze_manifest(
    raw_sources: tuple["RescueSourceSnapshot", ...],
    freeze_manifest: "RescueFreezeManifest",
) -> None:
    raw_sources_by_id = {source.source_id: source for source in raw_sources}
    freeze_sources_by_id = {
        source.source_id: source for source in freeze_manifest.source_snapshots
    }
    if set(raw_sources_by_id) != set(freeze_sources_by_id):
        raise ValueError(
            "raw_sources must match the source_snapshot ids declared by the freeze manifest"
        )

    for source_id, raw_source in raw_sources_by_id.items():
        freeze_source = freeze_sources_by_id[source_id]
        for field_name in (
            "availability",
            "source_path",
            "captured_at",
            "snapshot_id",
            "sha256",
        ):
            if getattr(raw_source, field_name) != getattr(freeze_source, field_name):
                raise ValueError(
                    "raw_sources must match the freeze manifest for "
                    f"{field_name}: {source_id}"
                )


@dataclass(frozen=True)
class RescueDatasetCard:
    schema_name: str
    schema_version: str
    dataset_id: str
    dataset_label: str
    suite_id: str
    task_id: str
    contract_path: str
    artifact_contract_id: str
    dataset_role: str
    availability: str
    governance_status: str
    source_stage: str
    entity_type: str
    record_grain: str
    primary_key_fields: tuple[str, ...]
    contains_post_cutoff_labels: bool
    allowed_uses: tuple[str, ...]
    freeze_manifest_path: str
    lineage_path: str
    expected_output_path: str
    file_format: str
    description: str
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_DATASET_CARD_ARTIFACT_NAME:
            raise ValueError("schema_name must remain rescue_dataset_card for dataset cards")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.dataset_id, "dataset_id")
        _require_text(self.dataset_label, "dataset_label")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.contract_path, "contract_path")
        _require_text(self.artifact_contract_id, "artifact_contract_id")
        _require_enum(self.dataset_role, "dataset_role", VALID_DATASET_ROLES)
        _require_enum(self.availability, "availability", VALID_DATASET_AVAILABILITIES)
        _require_enum(
            self.governance_status,
            "governance_status",
            VALID_GOVERNANCE_STATUSES,
        )
        _require_enum(self.source_stage, "source_stage", VALID_SOURCE_STAGES)
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must remain within the supported entity types")
        _require_text(self.record_grain, "record_grain")
        _require_unique(self.primary_key_fields, "primary_key_fields")
        _require_unique(self.allowed_uses, "allowed_uses")
        _require_text(self.freeze_manifest_path, "freeze_manifest_path")
        _require_text(self.lineage_path, "lineage_path")
        _require_text(self.expected_output_path, "expected_output_path")
        _require_text(self.file_format, "file_format")
        _require_text(self.description, "description")

        contract = _load_contract(self.contract_path)
        if contract.schema_name != RESCUE_TASK_CONTRACT_ARTIFACT_NAME:
            raise ValueError("contract_path must point to a rescue_task_contract artifact")
        if contract.suite_id != self.suite_id:
            raise ValueError("suite_id must match the referenced rescue task contract")
        if contract.task_id != self.task_id:
            raise ValueError("task_id must match the referenced rescue task contract")
        if contract.entity_type != self.entity_type:
            raise ValueError("entity_type must match the referenced rescue task contract")

        artifact_contract = next(
            (
                artifact
                for artifact in contract.artifact_contracts
                if artifact.artifact_id == self.artifact_contract_id
            ),
            None,
        )
        if artifact_contract is None:
            raise ValueError(
                "artifact_contract_id must match an artifact contract declared by the rescue task contract"
            )
        if artifact_contract.channel != self.dataset_role:
            raise ValueError("dataset_role must match the referenced artifact contract channel")
        if artifact_contract.availability != self.availability:
            raise ValueError(
                "availability must match the referenced artifact contract availability"
            )

        if self.dataset_role in {"ranking_input", "evaluation_target"} and self.source_stage != "frozen":
            raise ValueError(
                "ranking_input and evaluation_target dataset cards must describe frozen datasets"
            )
        if self.dataset_role == "task_output" and self.source_stage != "derived":
            raise ValueError("task_output dataset cards must describe derived datasets")
        if self.dataset_role == "ranking_input" and self.contains_post_cutoff_labels:
            raise ValueError("ranking_input dataset cards must not expose post-cutoff labels")
        if self.dataset_role == "evaluation_target" and not self.contains_post_cutoff_labels:
            raise ValueError(
                "evaluation_target dataset cards must declare post-cutoff labels"
            )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueDatasetCard:
        return cls(
            schema_name=_require_text(payload["schema_name"], "schema_name"),
            schema_version=_require_text(payload["schema_version"], "schema_version"),
            dataset_id=_require_text(payload["dataset_id"], "dataset_id"),
            dataset_label=_require_text(payload["dataset_label"], "dataset_label"),
            suite_id=_require_text(payload["suite_id"], "suite_id"),
            task_id=_require_text(payload["task_id"], "task_id"),
            contract_path=_require_text(payload["contract_path"], "contract_path"),
            artifact_contract_id=_require_text(
                payload["artifact_contract_id"],
                "artifact_contract_id",
            ),
            dataset_role=_require_text(payload["dataset_role"], "dataset_role"),
            availability=_require_text(payload["availability"], "availability"),
            governance_status=_require_text(
                payload["governance_status"],
                "governance_status",
            ),
            source_stage=_require_text(payload["source_stage"], "source_stage"),
            entity_type=_require_text(payload["entity_type"], "entity_type"),
            record_grain=_require_text(payload["record_grain"], "record_grain"),
            primary_key_fields=_require_string_list(
                payload["primary_key_fields"],
                "primary_key_fields",
            ),
            contains_post_cutoff_labels=_require_bool(
                payload["contains_post_cutoff_labels"],
                "contains_post_cutoff_labels",
            ),
            allowed_uses=_require_string_list(payload["allowed_uses"], "allowed_uses"),
            freeze_manifest_path=_require_text(
                payload["freeze_manifest_path"],
                "freeze_manifest_path",
            ),
            lineage_path=_require_text(payload["lineage_path"], "lineage_path"),
            expected_output_path=_require_text(
                payload["expected_output_path"],
                "expected_output_path",
            ),
            file_format=_require_text(payload["file_format"], "file_format"),
            description=_require_text(payload["description"], "description"),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueTaskCard:
    schema_name: str
    schema_version: str
    task_card_id: str
    suite_id: str
    task_id: str
    contract_path: str
    governance_status: str
    owner: str
    summary: str
    leakage_boundary_policy_id: str
    dataset_card_paths: tuple[str, ...]
    freeze_manifest_paths: tuple[str, ...]
    split_manifest_paths: tuple[str, ...]
    lineage_paths: tuple[str, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_TASK_CARD_ARTIFACT_NAME:
            raise ValueError("schema_name must remain rescue_task_card for rescue task cards")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.task_card_id, "task_card_id")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.contract_path, "contract_path")
        _require_enum(
            self.governance_status,
            "governance_status",
            VALID_GOVERNANCE_STATUSES,
        )
        _require_text(self.owner, "owner")
        _require_text(self.summary, "summary")
        if self.leakage_boundary_policy_id != STRICT_RESCUE_LEAKAGE_POLICY_ID:
            raise ValueError(
                "leakage_boundary_policy_id must remain strict_rescue_task_boundary_v1"
            )
        _require_unique(self.dataset_card_paths, "dataset_card_paths")
        _require_unique(self.freeze_manifest_paths, "freeze_manifest_paths")
        _require_unique(self.split_manifest_paths, "split_manifest_paths")
        _require_unique(self.lineage_paths, "lineage_paths")

        contract = _load_contract(self.contract_path)
        if contract.suite_id != self.suite_id:
            raise ValueError("suite_id must match the referenced rescue task contract")
        if contract.task_id != self.task_id:
            raise ValueError("task_id must match the referenced rescue task contract")
        if contract.leakage_boundary.policy_id != self.leakage_boundary_policy_id:
            raise ValueError(
                "leakage_boundary_policy_id must match the referenced rescue task contract"
            )
        if not contract.leakage_boundary.freeze_manifest_required:
            raise ValueError(
                "rescue task contracts must require freeze manifests before task cards can validate"
            )
        if not contract.leakage_boundary.dataset_cards_required:
            raise ValueError(
                "rescue task contracts must require dataset cards before task cards can validate"
            )
        if not contract.leakage_boundary.task_card_required:
            raise ValueError(
                "rescue task contracts must require task cards before task cards can validate"
            )
        if not contract.leakage_boundary.split_manifest_required:
            raise ValueError(
                "rescue task contracts must require split manifests before task cards can validate"
            )
        if not contract.leakage_boundary.raw_to_frozen_lineage_required:
            raise ValueError(
                "rescue task contracts must require raw-to-frozen lineage before task cards can validate"
            )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueTaskCard:
        return cls(
            schema_name=_require_text(payload["schema_name"], "schema_name"),
            schema_version=_require_text(payload["schema_version"], "schema_version"),
            task_card_id=_require_text(payload["task_card_id"], "task_card_id"),
            suite_id=_require_text(payload["suite_id"], "suite_id"),
            task_id=_require_text(payload["task_id"], "task_id"),
            contract_path=_require_text(payload["contract_path"], "contract_path"),
            governance_status=_require_text(
                payload["governance_status"],
                "governance_status",
            ),
            owner=_require_text(payload["owner"], "owner"),
            summary=_require_text(payload["summary"], "summary"),
            leakage_boundary_policy_id=_require_text(
                payload["leakage_boundary_policy_id"],
                "leakage_boundary_policy_id",
            ),
            dataset_card_paths=_require_string_list(
                payload["dataset_card_paths"],
                "dataset_card_paths",
            ),
            freeze_manifest_paths=_require_string_list(
                payload["freeze_manifest_paths"],
                "freeze_manifest_paths",
            ),
            split_manifest_paths=_require_string_list(
                payload["split_manifest_paths"],
                "split_manifest_paths",
            ),
            lineage_paths=_require_string_list(payload["lineage_paths"], "lineage_paths"),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueSourceSnapshot:
    source_id: str
    source_name: str
    snapshot_id: str
    captured_at: str
    availability: str
    source_path: str
    sha256: str
    description: str

    def __post_init__(self) -> None:
        _require_text(self.source_id, "source_id")
        _require_text(self.source_name, "source_name")
        _require_text(self.snapshot_id, "snapshot_id")
        _require_iso_date(self.captured_at, "captured_at")
        _require_enum(self.availability, "availability", ("pre_cutoff", "post_cutoff"))
        _require_text(self.source_path, "source_path")
        _require_sha256(self.sha256, "sha256")
        _require_text(self.description, "description")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueSourceSnapshot:
        return cls(
            source_id=_require_text(payload["source_id"], "source_id"),
            source_name=_require_text(payload["source_name"], "source_name"),
            snapshot_id=_require_text(payload["snapshot_id"], "snapshot_id"),
            captured_at=_require_iso_date(payload["captured_at"], "captured_at"),
            availability=_require_text(payload["availability"], "availability"),
            source_path=_require_text(payload["source_path"], "source_path"),
            sha256=_require_sha256(payload["sha256"], "sha256"),
            description=_require_text(payload["description"], "description"),
        )


@dataclass(frozen=True)
class RescueFrozenDatasetReference:
    dataset_id: str
    artifact_contract_id: str
    dataset_role: str
    availability: str
    dataset_card_path: str
    expected_output_path: str

    def __post_init__(self) -> None:
        _require_text(self.dataset_id, "dataset_id")
        _require_text(self.artifact_contract_id, "artifact_contract_id")
        _require_enum(self.dataset_role, "dataset_role", VALID_DATASET_ROLES)
        _require_enum(self.availability, "availability", VALID_DATASET_AVAILABILITIES)
        _require_text(self.dataset_card_path, "dataset_card_path")
        _require_text(self.expected_output_path, "expected_output_path")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueFrozenDatasetReference:
        return cls(
            dataset_id=_require_text(payload["dataset_id"], "dataset_id"),
            artifact_contract_id=_require_text(
                payload["artifact_contract_id"],
                "artifact_contract_id",
            ),
            dataset_role=_require_text(payload["dataset_role"], "dataset_role"),
            availability=_require_text(payload["availability"], "availability"),
            dataset_card_path=_require_text(payload["dataset_card_path"], "dataset_card_path"),
            expected_output_path=_require_text(
                payload["expected_output_path"],
                "expected_output_path",
            ),
        )


@dataclass(frozen=True)
class RescueFreezeManifest:
    schema_name: str
    schema_version: str
    freeze_manifest_id: str
    suite_id: str
    task_id: str
    contract_path: str
    governance_status: str
    freeze_scope: str
    cutoff_date: str
    frozen_at: str
    leakage_boundary_policy_id: str
    current_head_policy: str
    source_snapshots: tuple[RescueSourceSnapshot, ...]
    frozen_datasets: tuple[RescueFrozenDatasetReference, ...]
    description: str
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME:
            raise ValueError(
                "schema_name must remain rescue_freeze_manifest for rescue freeze manifests"
            )
        _require_text(self.schema_version, "schema_version")
        _require_text(self.freeze_manifest_id, "freeze_manifest_id")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.contract_path, "contract_path")
        _require_enum(
            self.governance_status,
            "governance_status",
            VALID_GOVERNANCE_STATUSES,
        )
        _require_enum(self.freeze_scope, "freeze_scope", VALID_FREEZE_SCOPES)
        cutoff = _parse_date(self.cutoff_date, "cutoff_date")
        frozen_at = _parse_date(self.frozen_at, "frozen_at")
        if frozen_at < cutoff:
            raise ValueError("frozen_at must be on or after cutoff_date")
        if self.leakage_boundary_policy_id != STRICT_RESCUE_LEAKAGE_POLICY_ID:
            raise ValueError(
                "leakage_boundary_policy_id must remain strict_rescue_task_boundary_v1"
            )
        if self.current_head_policy != CURRENT_HEAD_POLICY:
            raise ValueError(
                "current_head_policy must remain reject_unfrozen_current_head_state"
            )
        _require_text(self.description, "description")

        source_ids = tuple(source.source_id for source in self.source_snapshots)
        dataset_ids = tuple(dataset.dataset_id for dataset in self.frozen_datasets)
        _require_unique(source_ids, "source_snapshots")
        _require_unique(dataset_ids, "frozen_datasets")
        if not any(source.availability == "pre_cutoff" for source in self.source_snapshots):
            raise ValueError("source_snapshots must include at least one pre_cutoff source")

        contract = _load_contract(self.contract_path)
        if contract.suite_id != self.suite_id:
            raise ValueError("suite_id must match the referenced rescue task contract")
        if contract.task_id != self.task_id:
            raise ValueError("task_id must match the referenced rescue task contract")
        if contract.leakage_boundary.policy_id != self.leakage_boundary_policy_id:
            raise ValueError(
                "leakage_boundary_policy_id must match the referenced rescue task contract"
            )
        if contract.leakage_boundary.current_head_policy != self.current_head_policy:
            raise ValueError(
                "current_head_policy must match the referenced rescue task contract"
            )

        contract_artifacts = {
            artifact.artifact_id: artifact for artifact in contract.artifact_contracts
        }
        includes_post_cutoff_output = False
        _validate_source_cutoff_alignment(
            self.source_snapshots,
            cutoff=cutoff,
            context="source_snapshots",
        )
        if (
            self.freeze_scope == "ranking_only"
            and any(source.availability == "post_cutoff" for source in self.source_snapshots)
        ):
            raise ValueError(
                "ranking_only freeze manifests must not include post_cutoff source snapshots"
            )

        for dataset in self.frozen_datasets:
            contract_artifact = contract_artifacts.get(dataset.artifact_contract_id)
            if contract_artifact is None:
                raise ValueError(
                    "frozen_datasets must reference an artifact_contract_id declared by the rescue task contract"
                )
            if contract_artifact.channel != dataset.dataset_role:
                raise ValueError(
                    "frozen_datasets dataset_role must match the rescue task contract channel"
                )
            if contract_artifact.availability != dataset.availability:
                raise ValueError(
                    "frozen_datasets availability must match the rescue task contract"
                )
            if dataset.availability == "post_cutoff":
                includes_post_cutoff_output = True

        if includes_post_cutoff_output and self.freeze_scope != "task_bundle":
            raise ValueError(
                "freeze_scope must be task_bundle when frozen_datasets include post_cutoff artifacts"
            )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueFreezeManifest:
        source_snapshots = tuple(
            RescueSourceSnapshot.from_dict(
                _require_mapping(item, "source_snapshots[]")
            )
            for item in _require_list(payload["source_snapshots"], "source_snapshots")
        )
        frozen_datasets = tuple(
            RescueFrozenDatasetReference.from_dict(
                _require_mapping(item, "frozen_datasets[]")
            )
            for item in _require_list(payload["frozen_datasets"], "frozen_datasets")
        )
        if not source_snapshots:
            raise ValueError("source_snapshots must contain at least one source snapshot")
        if not frozen_datasets:
            raise ValueError("frozen_datasets must contain at least one dataset")
        return cls(
            schema_name=_require_text(payload["schema_name"], "schema_name"),
            schema_version=_require_text(payload["schema_version"], "schema_version"),
            freeze_manifest_id=_require_text(
                payload["freeze_manifest_id"],
                "freeze_manifest_id",
            ),
            suite_id=_require_text(payload["suite_id"], "suite_id"),
            task_id=_require_text(payload["task_id"], "task_id"),
            contract_path=_require_text(payload["contract_path"], "contract_path"),
            governance_status=_require_text(
                payload["governance_status"],
                "governance_status",
            ),
            freeze_scope=_require_text(payload["freeze_scope"], "freeze_scope"),
            cutoff_date=_require_iso_date(payload["cutoff_date"], "cutoff_date"),
            frozen_at=_require_iso_date(payload["frozen_at"], "frozen_at"),
            leakage_boundary_policy_id=_require_text(
                payload["leakage_boundary_policy_id"],
                "leakage_boundary_policy_id",
            ),
            current_head_policy=_require_text(
                payload["current_head_policy"],
                "current_head_policy",
            ),
            source_snapshots=source_snapshots,
            frozen_datasets=frozen_datasets,
            description=_require_text(payload["description"], "description"),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueSplitDefinition:
    split_name: str
    purpose: str
    expected_fraction: float
    selection_rule: str
    leakage_rule: str

    def __post_init__(self) -> None:
        _require_text(self.split_name, "split_name")
        _require_enum(self.purpose, "purpose", VALID_SPLIT_PURPOSES)
        if self.expected_fraction <= 0 or self.expected_fraction >= 1:
            raise ValueError("expected_fraction must be between 0 and 1")
        _require_text(self.selection_rule, "selection_rule")
        _require_text(self.leakage_rule, "leakage_rule")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueSplitDefinition:
        return cls(
            split_name=_require_text(payload["split_name"], "split_name"),
            purpose=_require_text(payload["purpose"], "purpose"),
            expected_fraction=_require_float(
                payload["expected_fraction"],
                "expected_fraction",
            ),
            selection_rule=_require_text(payload["selection_rule"], "selection_rule"),
            leakage_rule=_require_text(payload["leakage_rule"], "leakage_rule"),
        )


@dataclass(frozen=True)
class RescueSplitManifest:
    schema_name: str
    schema_version: str
    split_manifest_id: str
    suite_id: str
    task_id: str
    contract_path: str
    governance_status: str
    freeze_manifest_path: str
    source_dataset_id: str
    split_strategy_id: str
    split_seed: int
    assignment_unit: str
    partitions: tuple[RescueSplitDefinition, ...]
    description: str
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME:
            raise ValueError(
                "schema_name must remain rescue_split_manifest for rescue split manifests"
            )
        _require_text(self.schema_version, "schema_version")
        _require_text(self.split_manifest_id, "split_manifest_id")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.contract_path, "contract_path")
        _require_enum(
            self.governance_status,
            "governance_status",
            VALID_GOVERNANCE_STATUSES,
        )
        _require_text(self.freeze_manifest_path, "freeze_manifest_path")
        _require_text(self.source_dataset_id, "source_dataset_id")
        _require_text(self.split_strategy_id, "split_strategy_id")
        if self.assignment_unit not in VALID_ENTITY_TYPES:
            raise ValueError("assignment_unit must remain within the supported entity types")
        _require_text(self.description, "description")

        contract = _load_contract(self.contract_path)
        if contract.suite_id != self.suite_id:
            raise ValueError("suite_id must match the referenced rescue task contract")
        if contract.task_id != self.task_id:
            raise ValueError("task_id must match the referenced rescue task contract")
        if self.assignment_unit != contract.entity_type:
            raise ValueError("assignment_unit must match the rescue task entity_type")

        freeze_manifest = read_rescue_freeze_manifest(
            _resolve_repo_relative_path(self.freeze_manifest_path)
        )
        if freeze_manifest.suite_id != self.suite_id:
            raise ValueError("freeze_manifest_path must match the rescue suite_id")
        if freeze_manifest.task_id != self.task_id:
            raise ValueError("freeze_manifest_path must match the rescue task_id")

        frozen_dataset = next(
            (
                dataset
                for dataset in freeze_manifest.frozen_datasets
                if dataset.dataset_id == self.source_dataset_id
            ),
            None,
        )
        if frozen_dataset is None:
            raise ValueError(
                "source_dataset_id must reference a dataset declared by the freeze manifest"
            )
        if frozen_dataset.dataset_role != "ranking_input":
            raise ValueError("source_dataset_id must point to a ranking_input dataset")

        purposes = tuple(partition.purpose for partition in self.partitions)
        _require_unique(tuple(partition.split_name for partition in self.partitions), "partitions")
        if set(purposes) != set(VALID_SPLIT_PURPOSES):
            raise ValueError("partitions must contain train, validation, and test purposes")
        if not isclose(
            sum(partition.expected_fraction for partition in self.partitions),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError("partitions expected_fraction values must sum to 1.0")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueSplitManifest:
        partitions = tuple(
            RescueSplitDefinition.from_dict(_require_mapping(item, "partitions[]"))
            for item in _require_list(payload["partitions"], "partitions")
        )
        if not partitions:
            raise ValueError("partitions must contain at least one split definition")
        return cls(
            schema_name=_require_text(payload["schema_name"], "schema_name"),
            schema_version=_require_text(payload["schema_version"], "schema_version"),
            split_manifest_id=_require_text(
                payload["split_manifest_id"],
                "split_manifest_id",
            ),
            suite_id=_require_text(payload["suite_id"], "suite_id"),
            task_id=_require_text(payload["task_id"], "task_id"),
            contract_path=_require_text(payload["contract_path"], "contract_path"),
            governance_status=_require_text(
                payload["governance_status"],
                "governance_status",
            ),
            freeze_manifest_path=_require_text(
                payload["freeze_manifest_path"],
                "freeze_manifest_path",
            ),
            source_dataset_id=_require_text(
                payload["source_dataset_id"],
                "source_dataset_id",
            ),
            split_strategy_id=_require_text(
                payload["split_strategy_id"],
                "split_strategy_id",
            ),
            split_seed=_require_int(payload["split_seed"], "split_seed"),
            assignment_unit=_require_text(payload["assignment_unit"], "assignment_unit"),
            partitions=partitions,
            description=_require_text(payload["description"], "description"),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueLineageStep:
    step_id: str
    step_label: str
    step_kind: str
    input_ids: tuple[str, ...]
    output_ids: tuple[str, ...]
    description: str

    def __post_init__(self) -> None:
        _require_text(self.step_id, "step_id")
        _require_text(self.step_label, "step_label")
        _require_enum(self.step_kind, "step_kind", VALID_LINEAGE_STEP_KINDS)
        _require_unique(self.input_ids, "input_ids")
        _require_unique(self.output_ids, "output_ids")
        _require_text(self.description, "description")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueLineageStep:
        return cls(
            step_id=_require_text(payload["step_id"], "step_id"),
            step_label=_require_text(payload["step_label"], "step_label"),
            step_kind=_require_text(payload["step_kind"], "step_kind"),
            input_ids=_require_string_list(payload["input_ids"], "input_ids"),
            output_ids=_require_string_list(payload["output_ids"], "output_ids"),
            description=_require_text(payload["description"], "description"),
        )


@dataclass(frozen=True)
class RescueLineageOutput:
    dataset_id: str
    dataset_card_path: str
    availability: str
    produced_by_step_id: str

    def __post_init__(self) -> None:
        _require_text(self.dataset_id, "dataset_id")
        _require_text(self.dataset_card_path, "dataset_card_path")
        _require_enum(self.availability, "availability", VALID_DATASET_AVAILABILITIES)
        _require_text(self.produced_by_step_id, "produced_by_step_id")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueLineageOutput:
        return cls(
            dataset_id=_require_text(payload["dataset_id"], "dataset_id"),
            dataset_card_path=_require_text(payload["dataset_card_path"], "dataset_card_path"),
            availability=_require_text(payload["availability"], "availability"),
            produced_by_step_id=_require_text(
                payload["produced_by_step_id"],
                "produced_by_step_id",
            ),
        )


@dataclass(frozen=True)
class RescueRawToFrozenLineage:
    schema_name: str
    schema_version: str
    lineage_id: str
    suite_id: str
    task_id: str
    contract_path: str
    governance_status: str
    freeze_manifest_path: str
    raw_sources: tuple[RescueSourceSnapshot, ...]
    transformation_steps: tuple[RescueLineageStep, ...]
    frozen_datasets: tuple[RescueLineageOutput, ...]
    description: str
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME:
            raise ValueError(
                "schema_name must remain rescue_raw_to_frozen_lineage for rescue lineage artifacts"
            )
        _require_text(self.schema_version, "schema_version")
        _require_text(self.lineage_id, "lineage_id")
        _require_text(self.suite_id, "suite_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.contract_path, "contract_path")
        _require_enum(
            self.governance_status,
            "governance_status",
            VALID_GOVERNANCE_STATUSES,
        )
        _require_text(self.freeze_manifest_path, "freeze_manifest_path")
        _require_text(self.description, "description")

        contract = _load_contract(self.contract_path)
        if contract.suite_id != self.suite_id:
            raise ValueError("suite_id must match the referenced rescue task contract")
        if contract.task_id != self.task_id:
            raise ValueError("task_id must match the referenced rescue task contract")

        freeze_manifest = read_rescue_freeze_manifest(
            _resolve_repo_relative_path(self.freeze_manifest_path)
        )
        if freeze_manifest.suite_id != self.suite_id:
            raise ValueError("freeze_manifest_path must match the rescue suite_id")
        if freeze_manifest.task_id != self.task_id:
            raise ValueError("freeze_manifest_path must match the rescue task_id")

        raw_source_ids = tuple(source.source_id for source in self.raw_sources)
        step_ids = tuple(step.step_id for step in self.transformation_steps)
        frozen_dataset_ids = tuple(dataset.dataset_id for dataset in self.frozen_datasets)
        _require_unique(raw_source_ids, "raw_sources")
        _require_unique(step_ids, "transformation_steps")
        _require_unique(frozen_dataset_ids, "frozen_datasets")

        cutoff = _parse_date(freeze_manifest.cutoff_date, "freeze_manifest.cutoff_date")
        _validate_source_cutoff_alignment(
            self.raw_sources,
            cutoff=cutoff,
            context="raw_sources",
        )
        _reconcile_sources_against_freeze_manifest(self.raw_sources, freeze_manifest)

        known_ids = set(raw_source_ids)
        outputs_by_step_id: dict[str, set[str]] = {}
        for step in self.transformation_steps:
            missing_inputs = sorted(input_id for input_id in step.input_ids if input_id not in known_ids)
            if missing_inputs:
                raise ValueError(
                    "transformation_steps must only reference known upstream ids; missing: "
                    + ", ".join(missing_inputs)
                )
            duplicate_outputs = sorted(output_id for output_id in step.output_ids if output_id in known_ids)
            if duplicate_outputs:
                raise ValueError(
                    "transformation_steps must not re-use upstream ids as outputs: "
                    + ", ".join(duplicate_outputs)
                )
            known_ids.update(step.output_ids)
            outputs_by_step_id[step.step_id] = set(step.output_ids)

        known_step_ids = set(step_ids)
        freeze_datasets = {dataset.dataset_id: dataset for dataset in freeze_manifest.frozen_datasets}
        for dataset in self.frozen_datasets:
            if dataset.produced_by_step_id not in known_step_ids:
                raise ValueError(
                    "frozen_datasets must reference a known transformation step"
                )
            if dataset.dataset_id not in outputs_by_step_id[dataset.produced_by_step_id]:
                raise ValueError(
                    "frozen_datasets produced_by_step_id must point to a step that emits the dataset_id"
                )
            freeze_dataset = freeze_datasets.get(dataset.dataset_id)
            if freeze_dataset is None:
                raise ValueError(
                    "frozen_datasets must match dataset ids declared by the freeze manifest"
                )
            if freeze_dataset.availability != dataset.availability:
                raise ValueError(
                    "frozen_datasets availability must match the freeze manifest"
                )

        if set(frozen_dataset_ids) != set(freeze_datasets):
            raise ValueError(
                "frozen_datasets must cover every dataset declared by the freeze manifest"
            )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RescueRawToFrozenLineage:
        raw_sources = tuple(
            RescueSourceSnapshot.from_dict(_require_mapping(item, "raw_sources[]"))
            for item in _require_list(payload["raw_sources"], "raw_sources")
        )
        transformation_steps = tuple(
            RescueLineageStep.from_dict(
                _require_mapping(item, "transformation_steps[]")
            )
            for item in _require_list(payload["transformation_steps"], "transformation_steps")
        )
        frozen_datasets = tuple(
            RescueLineageOutput.from_dict(
                _require_mapping(item, "frozen_datasets[]")
            )
            for item in _require_list(payload["frozen_datasets"], "frozen_datasets")
        )
        if not raw_sources:
            raise ValueError("raw_sources must contain at least one upstream source")
        if not transformation_steps:
            raise ValueError(
                "transformation_steps must contain at least one lineage transformation step"
            )
        if not frozen_datasets:
            raise ValueError("frozen_datasets must contain at least one frozen dataset")
        return cls(
            schema_name=_require_text(payload["schema_name"], "schema_name"),
            schema_version=_require_text(payload["schema_version"], "schema_version"),
            lineage_id=_require_text(payload["lineage_id"], "lineage_id"),
            suite_id=_require_text(payload["suite_id"], "suite_id"),
            task_id=_require_text(payload["task_id"], "task_id"),
            contract_path=_require_text(payload["contract_path"], "contract_path"),
            governance_status=_require_text(
                payload["governance_status"],
                "governance_status",
            ),
            freeze_manifest_path=_require_text(
                payload["freeze_manifest_path"],
                "freeze_manifest_path",
            ),
            raw_sources=raw_sources,
            transformation_steps=transformation_steps,
            frozen_datasets=frozen_datasets,
            description=_require_text(payload["description"], "description"),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class RescueGovernanceBundle:
    task_card: RescueTaskCard
    contract: RescueTaskContract
    dataset_cards: tuple[RescueDatasetCard, ...]
    freeze_manifests: tuple[RescueFreezeManifest, ...]
    split_manifests: tuple[RescueSplitManifest, ...]
    lineages: tuple[RescueRawToFrozenLineage, ...]


def read_rescue_dataset_card(path: Path) -> RescueDatasetCard:
    return RescueDatasetCard.from_dict(
        _read_json_mapping(path, RESCUE_DATASET_CARD_ARTIFACT_NAME)
    )


def read_rescue_task_card(path: Path) -> RescueTaskCard:
    return RescueTaskCard.from_dict(
        _read_json_mapping(path, RESCUE_TASK_CARD_ARTIFACT_NAME)
    )


def read_rescue_freeze_manifest(path: Path) -> RescueFreezeManifest:
    return RescueFreezeManifest.from_dict(
        _read_json_mapping(path, RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME)
    )


def read_rescue_split_manifest(path: Path) -> RescueSplitManifest:
    return RescueSplitManifest.from_dict(
        _read_json_mapping(path, RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME)
    )


def read_rescue_raw_to_frozen_lineage(path: Path) -> RescueRawToFrozenLineage:
    return RescueRawToFrozenLineage.from_dict(
        _read_json_mapping(path, RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME)
    )


def validate_rescue_governance_bundle(task_card_path: Path) -> RescueGovernanceBundle:
    task_card = read_rescue_task_card(task_card_path.resolve())
    contract = _load_contract(task_card.contract_path)

    dataset_cards = tuple(
        read_rescue_dataset_card(_resolve_repo_relative_path(path_text))
        for path_text in task_card.dataset_card_paths
    )
    freeze_manifests = tuple(
        read_rescue_freeze_manifest(_resolve_repo_relative_path(path_text))
        for path_text in task_card.freeze_manifest_paths
    )
    split_manifests = tuple(
        read_rescue_split_manifest(_resolve_repo_relative_path(path_text))
        for path_text in task_card.split_manifest_paths
    )
    lineages = tuple(
        read_rescue_raw_to_frozen_lineage(_resolve_repo_relative_path(path_text))
        for path_text in task_card.lineage_paths
    )

    dataset_by_id = {dataset.dataset_id: dataset for dataset in dataset_cards}
    if len(dataset_by_id) != len(dataset_cards):
        raise ValueError("task_card dataset_card_paths must resolve to unique dataset_id values")

    freeze_dataset_ids = {
        dataset.dataset_id
        for freeze_manifest in freeze_manifests
        for dataset in freeze_manifest.frozen_datasets
    }
    if freeze_dataset_ids != set(dataset_by_id):
        raise ValueError(
            "dataset cards must match the dataset ids declared by the referenced freeze manifests"
        )

    lineage_dataset_ids = {
        dataset.dataset_id
        for lineage in lineages
        for dataset in lineage.frozen_datasets
    }
    if lineage_dataset_ids != set(dataset_by_id):
        raise ValueError(
            "dataset cards must match the dataset ids declared by the referenced lineage artifacts"
        )

    task_card_freeze_paths = set(task_card.freeze_manifest_paths)
    task_card_lineage_paths = set(task_card.lineage_paths)
    dataset_path_by_id = {
        dataset.dataset_id: path_text
        for dataset, path_text in zip(dataset_cards, task_card.dataset_card_paths)
    }
    freeze_manifest_by_path = {
        path_text: freeze_manifest
        for path_text, freeze_manifest in zip(task_card.freeze_manifest_paths, freeze_manifests)
    }
    lineage_by_path = {
        path_text: lineage
        for path_text, lineage in zip(task_card.lineage_paths, lineages)
    }
    for dataset in dataset_cards:
        if dataset.freeze_manifest_path not in task_card_freeze_paths:
            raise ValueError(
                "dataset cards must reference a freeze manifest declared by the task card"
            )
        if dataset.lineage_path not in task_card_lineage_paths:
            raise ValueError(
                "dataset cards must reference a lineage artifact declared by the task card"
            )
        if dataset.suite_id != contract.suite_id or dataset.task_id != contract.task_id:
            raise ValueError("dataset cards must match the referenced rescue task contract")
        freeze_manifest = freeze_manifest_by_path[dataset.freeze_manifest_path]
        freeze_dataset = next(
            (
                item
                for item in freeze_manifest.frozen_datasets
                if item.dataset_id == dataset.dataset_id
            ),
            None,
        )
        if freeze_dataset is None:
            raise ValueError(
                "dataset cards must match dataset ids declared by their referenced freeze manifest"
            )
        if freeze_dataset.dataset_card_path != dataset_path_by_id[dataset.dataset_id]:
            raise ValueError(
                "freeze manifest dataset_card_path values must match the task card dataset paths"
            )
        if freeze_dataset.artifact_contract_id != dataset.artifact_contract_id:
            raise ValueError(
                "freeze manifest artifact_contract_id values must match the dataset cards"
            )
        if freeze_dataset.expected_output_path != dataset.expected_output_path:
            raise ValueError(
                "freeze manifest expected_output_path values must match the dataset cards"
            )
        lineage = lineage_by_path[dataset.lineage_path]
        lineage_dataset = next(
            (
                item for item in lineage.frozen_datasets if item.dataset_id == dataset.dataset_id
            ),
            None,
        )
        if lineage_dataset is None:
            raise ValueError(
                "dataset cards must match dataset ids declared by their referenced lineage artifact"
            )
        if lineage_dataset.dataset_card_path != dataset_path_by_id[dataset.dataset_id]:
            raise ValueError(
                "lineage dataset_card_path values must match the task-card dataset card paths"
            )

    for freeze_manifest in freeze_manifests:
        if freeze_manifest.suite_id != contract.suite_id:
            raise ValueError("freeze manifests must match the referenced rescue suite_id")
        if freeze_manifest.task_id != contract.task_id:
            raise ValueError("freeze manifests must match the referenced rescue task_id")

    for split_manifest in split_manifests:
        if split_manifest.suite_id != contract.suite_id:
            raise ValueError("split manifests must match the referenced rescue suite_id")
        if split_manifest.task_id != contract.task_id:
            raise ValueError("split manifests must match the referenced rescue task_id")
        if split_manifest.freeze_manifest_path not in task_card_freeze_paths:
            raise ValueError(
                "split manifests must reference a freeze manifest declared by the task card"
            )
        source_dataset = dataset_by_id.get(split_manifest.source_dataset_id)
        if source_dataset is None:
            raise ValueError(
                "split manifests must reference a dataset card declared by the task card"
            )
        if source_dataset.dataset_role != "ranking_input":
            raise ValueError("split manifests must split a ranking_input dataset")
        if source_dataset.freeze_manifest_path != split_manifest.freeze_manifest_path:
            raise ValueError(
                "split manifests must reference the exact freeze_manifest_path declared by the source dataset card"
            )

    for lineage in lineages:
        if lineage.suite_id != contract.suite_id:
            raise ValueError("lineage artifacts must match the referenced rescue suite_id")
        if lineage.task_id != contract.task_id:
            raise ValueError("lineage artifacts must match the referenced rescue task_id")

    return RescueGovernanceBundle(
        task_card=task_card,
        contract=contract,
        dataset_cards=dataset_cards,
        freeze_manifests=freeze_manifests,
        split_manifests=split_manifests,
        lineages=lineages,
    )


__all__ = [
    "RESCUE_DATASET_CARD_ARTIFACT_NAME",
    "RESCUE_FREEZE_MANIFEST_ARTIFACT_NAME",
    "RESCUE_RAW_TO_FROZEN_LINEAGE_ARTIFACT_NAME",
    "RESCUE_SPLIT_MANIFEST_ARTIFACT_NAME",
    "RESCUE_TASK_CARD_ARTIFACT_NAME",
    "RescueDatasetCard",
    "RescueFreezeManifest",
    "RescueGovernanceBundle",
    "RescueLineageOutput",
    "RescueLineageStep",
    "RescueRawToFrozenLineage",
    "RescueSourceSnapshot",
    "RescueSplitDefinition",
    "RescueSplitManifest",
    "RescueTaskCard",
    "read_rescue_dataset_card",
    "read_rescue_freeze_manifest",
    "read_rescue_raw_to_frozen_lineage",
    "read_rescue_split_manifest",
    "read_rescue_task_card",
    "validate_rescue_governance_bundle",
]
