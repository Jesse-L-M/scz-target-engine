from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.benchmark_protocol import VALID_ENTITY_TYPES
from scz_target_engine.io import read_csv_rows
from scz_target_engine.rescue.contracts import (
    RESCUE_TASK_CONTRACT_ARTIFACT_NAME,
    RescueTaskContract,
    VALID_CONTRACT_SCOPES,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESCUE_TASK_REGISTRY_PATH = (
    REPO_ROOT / "data" / "curated" / "rescue_tasks" / "rescue_task_registry.csv"
)
VALID_REGISTRY_STATUSES = ("example", "planned", "active")


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


@dataclass(frozen=True)
class RescueTaskRegistration:
    suite_id: str
    suite_label: str
    task_id: str
    task_label: str
    task_type: str
    disease: str
    entity_type: str
    contract_scope: str
    contract_file: Path
    registry_status: str
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.suite_id, "suite_id")
        _require_text(self.suite_label, "suite_label")
        _require_text(self.task_id, "task_id")
        _require_text(self.task_label, "task_label")
        _require_text(self.task_type, "task_type")
        _require_text(self.disease, "disease")
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must remain within the supported entity types")
        if self.contract_scope not in VALID_CONTRACT_SCOPES:
            raise ValueError("contract_scope must be a supported rescue contract scope")
        if not self.contract_file.exists():
            raise ValueError(f"contract_file does not exist: {self.contract_file}")
        if self.registry_status not in VALID_REGISTRY_STATUSES:
            raise ValueError("registry_status must be a supported rescue registry status")


@dataclass(frozen=True)
class RescueSuiteContract:
    suite_id: str
    suite_label: str
    tasks: tuple[RescueTaskContract, ...]

    def __post_init__(self) -> None:
        _require_text(self.suite_id, "suite_id")
        _require_text(self.suite_label, "suite_label")
        if not self.tasks:
            raise ValueError("tasks must contain at least one rescue task")
        if any(task.suite_id != self.suite_id for task in self.tasks):
            raise ValueError("all tasks must share the suite_id")
        if any(task.suite_label != self.suite_label for task in self.tasks):
            raise ValueError("all tasks must share the suite_label")

        task_ids = [task.task_id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("tasks must not repeat task_id")


def _build_registration(row: dict[str, str]) -> RescueTaskRegistration:
    return RescueTaskRegistration(
        suite_id=_require_text(row["suite_id"], "suite_id"),
        suite_label=_require_text(row["suite_label"], "suite_label"),
        task_id=_require_text(row["task_id"], "task_id"),
        task_label=_require_text(row["task_label"], "task_label"),
        task_type=_require_text(row["task_type"], "task_type"),
        disease=_require_text(row["disease"], "disease"),
        entity_type=_require_text(row["entity_type"], "entity_type"),
        contract_scope=_require_text(row["contract_scope"], "contract_scope"),
        contract_file=_resolve_repo_relative_path(row["contract_file"]),
        registry_status=_require_text(row["registry_status"], "registry_status"),
        notes=str(row.get("notes", "")).strip(),
    )


def _validate_registration_match(
    registration: RescueTaskRegistration,
    contract: RescueTaskContract,
) -> None:
    mismatches: list[str] = []
    for field_name in (
        "suite_id",
        "suite_label",
        "task_id",
        "task_label",
        "task_type",
        "disease",
        "entity_type",
        "contract_scope",
    ):
        if getattr(registration, field_name) != getattr(contract, field_name):
            mismatches.append(field_name)
    if mismatches:
        raise ValueError(
            "rescue registry row did not match contract file fields: "
            + ", ".join(mismatches)
        )


def _load_registry_rows(
    task_registry_path: Path,
) -> tuple[RescueTaskRegistration, ...]:
    if not task_registry_path.exists():
        raise FileNotFoundError(
            f"rescue task registry does not exist: {task_registry_path}"
        )

    rows = read_csv_rows(task_registry_path)
    registrations = tuple(_build_registration(row) for row in rows)
    if not registrations:
        raise ValueError("rescue task registry must contain at least one task")

    task_ids = [registration.task_id for registration in registrations]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("rescue task registry must not repeat task_id")
    return registrations


def _load_task_contracts(task_registry_path: Path) -> tuple[RescueTaskContract, ...]:
    contracts: list[RescueTaskContract] = []
    for registration in _load_registry_rows(task_registry_path):
        contract_artifact = load_artifact(
            registration.contract_file,
            artifact_name=RESCUE_TASK_CONTRACT_ARTIFACT_NAME,
        )
        contract = contract_artifact.payload
        if not isinstance(contract, RescueTaskContract):
            raise TypeError(
                "rescue task contract artifact did not return RescueTaskContract payload"
            )
        _validate_registration_match(registration, contract)
        contracts.append(contract)
    return tuple(contracts)


@lru_cache(maxsize=1)
def _load_default_rescue_task_registrations() -> tuple[RescueTaskRegistration, ...]:
    return _load_registry_rows(DEFAULT_RESCUE_TASK_REGISTRY_PATH)


@lru_cache(maxsize=1)
def _load_default_rescue_task_contracts() -> tuple[RescueTaskContract, ...]:
    return _load_task_contracts(DEFAULT_RESCUE_TASK_REGISTRY_PATH)


def load_rescue_task_registrations(
    task_registry_path: Path | None = None,
) -> tuple[RescueTaskRegistration, ...]:
    resolved_path = (
        DEFAULT_RESCUE_TASK_REGISTRY_PATH
        if task_registry_path is None
        else task_registry_path.resolve()
    )
    if resolved_path == DEFAULT_RESCUE_TASK_REGISTRY_PATH:
        return _load_default_rescue_task_registrations()
    return _load_registry_rows(resolved_path)


def load_rescue_task_contracts(
    task_registry_path: Path | None = None,
) -> tuple[RescueTaskContract, ...]:
    resolved_path = (
        DEFAULT_RESCUE_TASK_REGISTRY_PATH
        if task_registry_path is None
        else task_registry_path.resolve()
    )
    if resolved_path == DEFAULT_RESCUE_TASK_REGISTRY_PATH:
        return _load_default_rescue_task_contracts()
    return _load_task_contracts(resolved_path)


def load_rescue_suite_contracts(
    task_registry_path: Path | None = None,
) -> tuple[RescueSuiteContract, ...]:
    tasks = load_rescue_task_contracts(task_registry_path=task_registry_path)
    grouped: dict[str, list[RescueTaskContract]] = {}
    for task in tasks:
        grouped.setdefault(task.suite_id, []).append(task)
    return tuple(
        RescueSuiteContract(
            suite_id=suite_id,
            suite_label=grouped[suite_id][0].suite_label,
            tasks=tuple(grouped[suite_id]),
        )
        for suite_id in sorted(grouped)
    )


def resolve_rescue_suite_contract(
    rescue_suite_id: str,
    *,
    task_registry_path: Path | None = None,
) -> RescueSuiteContract:
    resolved_suite_id = _require_text(rescue_suite_id, "rescue_suite_id")
    for suite_contract in load_rescue_suite_contracts(
        task_registry_path=task_registry_path
    ):
        if suite_contract.suite_id == resolved_suite_id:
            return suite_contract
    raise ValueError(f"unknown rescue suite id: {resolved_suite_id}")


def resolve_rescue_task_contract(
    *,
    rescue_task_id: str | None = None,
    rescue_suite_id: str | None = None,
    task_registry_path: Path | None = None,
) -> RescueTaskContract:
    tasks = load_rescue_task_contracts(task_registry_path=task_registry_path)
    candidates = list(tasks)

    if rescue_suite_id:
        candidates = [
            task for task in candidates if task.suite_id == rescue_suite_id
        ]
    if rescue_task_id:
        candidates = [task for task in candidates if task.task_id == rescue_task_id]

    if not candidates:
        lookup_parts = []
        if rescue_suite_id:
            lookup_parts.append(f"suite_id={rescue_suite_id}")
        if rescue_task_id:
            lookup_parts.append(f"task_id={rescue_task_id}")
        lookup = ", ".join(lookup_parts) if lookup_parts else "no lookup key provided"
        raise ValueError(f"no rescue task contract matched: {lookup}")

    if len(candidates) > 1:
        matched_task_ids = ", ".join(sorted(task.task_id for task in candidates))
        raise ValueError(
            "rescue task contract lookup is ambiguous; matched task_ids: "
            f"{matched_task_ids}"
        )
    return candidates[0]


__all__ = [
    "DEFAULT_RESCUE_TASK_REGISTRY_PATH",
    "RescueSuiteContract",
    "RescueTaskRegistration",
    "load_rescue_suite_contracts",
    "load_rescue_task_contracts",
    "load_rescue_task_registrations",
    "resolve_rescue_suite_contract",
    "resolve_rescue_task_contract",
]
