from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from scz_target_engine.benchmark_protocol import (
    FROZEN_BENCHMARK_PROTOCOL,
    TRACK_B_BENCHMARK_PROTOCOL,
    VALID_ENTITY_TYPES,
    ArtifactSchema,
    BenchmarkProtocol,
)
from scz_target_engine.io import read_csv_rows


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TASK_REGISTRY_PATH = (
    REPO_ROOT / "data" / "curated" / "rescue_tasks" / "task_registry.csv"
)
PIPE_SEPARATOR = "|"
FROZEN_PROTOCOL_ID = "frozen_benchmark_protocol_v1"
TRACK_B_PROTOCOL_ID = "track_b_structural_replay_protocol_v1"
SUPPORTED_PROTOCOLS = {
    FROZEN_PROTOCOL_ID: FROZEN_BENCHMARK_PROTOCOL,
    TRACK_B_PROTOCOL_ID: TRACK_B_BENCHMARK_PROTOCOL,
}


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _split_pipe_list(value: str, field_name: str) -> tuple[str, ...]:
    items = tuple(
        item.strip()
        for item in value.split(PIPE_SEPARATOR)
        if item.strip()
    )
    if not items:
        raise ValueError(f"{field_name} must contain at least one value")
    return items


def _split_optional_pipe_list(
    value: str | None,
    field_name: str,
) -> tuple[str, ...]:
    cleaned = "" if value is None else str(value).strip()
    if not cleaned:
        return ()
    return _split_pipe_list(cleaned, field_name)


def _parse_optional_bool(value: str | None, field_name: str) -> bool:
    cleaned = "" if value is None else str(value).strip().lower()
    if cleaned in {"", "false", "0", "no"}:
        return False
    if cleaned in {"true", "1", "yes"}:
        return True
    raise ValueError(f"{field_name} must be one of true/false")


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _resolve_protocol(protocol_id: str) -> BenchmarkProtocol:
    resolved_protocol_id = _require_text(protocol_id, "protocol_id")
    protocol = SUPPORTED_PROTOCOLS.get(resolved_protocol_id)
    if protocol is None:
        raise ValueError(f"unsupported benchmark protocol id: {resolved_protocol_id}")
    return protocol


@dataclass(frozen=True)
class BenchmarkFixturePaths:
    snapshot_request_file: Path
    cohort_members_file: Path
    future_outcomes_file: Path
    archive_index_file: Path
    archive_index_sibling_file_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name, path in (
            ("snapshot_request_file", self.snapshot_request_file),
            ("cohort_members_file", self.cohort_members_file),
            ("future_outcomes_file", self.future_outcomes_file),
            ("archive_index_file", self.archive_index_file),
        ):
            if not path.exists():
                raise ValueError(f"{field_name} does not exist: {path}")
        for file_name in self.archive_index_sibling_file_names:
            _require_text(file_name, "archive_index_sibling_file_names")
        self.validate_archive_index_sibling_files(self.archive_index_file)

    def resolve_archive_index_sibling_paths(
        self,
        archive_index_file: Path,
    ) -> tuple[Path, ...]:
        base_dir = archive_index_file.resolve().parent
        return tuple(
            (base_dir / file_name).resolve()
            for file_name in self.archive_index_sibling_file_names
        )

    def validate_archive_index_sibling_files(
        self,
        archive_index_file: Path,
    ) -> None:
        missing_paths = [
            path.name
            for path in self.resolve_archive_index_sibling_paths(archive_index_file)
            if not path.exists()
        ]
        if missing_paths:
            raise ValueError(
                "benchmark fixture is missing required archive-index sibling files: "
                + ", ".join(sorted(missing_paths))
            )


@dataclass(frozen=True)
class BenchmarkTaskContract:
    suite_id: str
    suite_label: str
    task_id: str
    task_label: str
    protocol_id: str
    benchmark_question_id: str
    entity_types: tuple[str, ...]
    supported_baseline_ids: tuple[str, ...]
    emitted_artifact_names: tuple[str, ...]
    fixture_paths: BenchmarkFixturePaths
    protocol: BenchmarkProtocol
    notes: str = ""
    legacy_lookup_default: bool = False

    def __post_init__(self) -> None:
        _require_text(self.suite_id, "suite_id")
        _require_text(self.suite_label, "suite_label")
        _require_text(self.task_id, "task_id")
        _require_text(self.task_label, "task_label")
        if self.protocol_id not in SUPPORTED_PROTOCOLS:
            raise ValueError(f"unsupported protocol_id: {self.protocol_id}")
        if self.benchmark_question_id != self.protocol.question.question_id:
            raise ValueError(
                "benchmark_question_id must match the resolved protocol question id"
            )
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError(
                "entity_types must only contain supported benchmark entity types"
            )

        known_baselines = {
            baseline.baseline_id for baseline in self.protocol.baselines
        }
        unknown_baselines = sorted(
            set(self.supported_baseline_ids).difference(known_baselines)
        )
        if unknown_baselines:
            raise ValueError(
                "supported_baseline_ids referenced unknown baselines: "
                + ", ".join(unknown_baselines)
            )

        protocol_artifact_names = {
            artifact_schema.artifact_name
            for artifact_schema in self.protocol.artifact_schemas
        }
        unknown_artifact_names = sorted(
            set(self.emitted_artifact_names).difference(protocol_artifact_names)
        )
        if unknown_artifact_names:
            raise ValueError(
                "emitted_artifact_names referenced unknown protocol artifacts: "
                + ", ".join(unknown_artifact_names)
            )

        from scz_target_engine.artifacts.registry import get_artifact_schema

        for artifact_name in self.emitted_artifact_names:
            get_artifact_schema(artifact_name)

    @property
    def baseline_index(self) -> dict[str, object]:
        return {
            baseline.baseline_id: baseline
            for baseline in self.protocol.baselines
            if baseline.baseline_id in self.supported_baseline_ids
        }

    @property
    def artifact_schemas(self) -> tuple[ArtifactSchema, ...]:
        return tuple(
            artifact_schema
            for artifact_schema in self.protocol.artifact_schemas
            if artifact_schema.artifact_name in self.emitted_artifact_names
        )


@dataclass(frozen=True)
class BenchmarkSuiteContract:
    suite_id: str
    suite_label: str
    tasks: tuple[BenchmarkTaskContract, ...]

    def __post_init__(self) -> None:
        _require_text(self.suite_id, "suite_id")
        _require_text(self.suite_label, "suite_label")
        if not self.tasks:
            raise ValueError("tasks must contain at least one benchmark task")
        if any(task.suite_id != self.suite_id for task in self.tasks):
            raise ValueError("all tasks must share the suite_id")
        if any(task.suite_label != self.suite_label for task in self.tasks):
            raise ValueError("all tasks must share the suite_label")

        task_ids = [task.task_id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("tasks must not repeat task_id")


def _build_task_contract(row: dict[str, str]) -> BenchmarkTaskContract:
    protocol_id = _require_text(row["protocol_id"], "protocol_id")
    protocol = _resolve_protocol(protocol_id)
    return BenchmarkTaskContract(
        suite_id=_require_text(row["suite_id"], "suite_id"),
        suite_label=_require_text(row["suite_label"], "suite_label"),
        task_id=_require_text(row["task_id"], "task_id"),
        task_label=_require_text(row["task_label"], "task_label"),
        protocol_id=protocol_id,
        benchmark_question_id=_require_text(
            row["benchmark_question_id"],
            "benchmark_question_id",
        ),
        entity_types=_split_pipe_list(row["entity_types"], "entity_types"),
        supported_baseline_ids=_split_pipe_list(
            row["supported_baseline_ids"],
            "supported_baseline_ids",
        ),
        emitted_artifact_names=_split_pipe_list(
            row["emitted_artifact_names"],
            "emitted_artifact_names",
        ),
        fixture_paths=BenchmarkFixturePaths(
            snapshot_request_file=_resolve_repo_relative_path(
                row["fixture_snapshot_request_file"]
            ),
            cohort_members_file=_resolve_repo_relative_path(
                row["fixture_cohort_members_file"]
            ),
            future_outcomes_file=_resolve_repo_relative_path(
                row["fixture_future_outcomes_file"]
            ),
            archive_index_file=_resolve_repo_relative_path(
                row["fixture_archive_index_file"]
            ),
            archive_index_sibling_file_names=_split_optional_pipe_list(
                row.get("fixture_archive_index_sibling_file_names"),
                "fixture_archive_index_sibling_file_names",
            ),
        ),
        protocol=protocol,
        notes=str(row.get("notes", "")).strip(),
        legacy_lookup_default=_parse_optional_bool(
            row.get("legacy_lookup_default"),
            "legacy_lookup_default",
        ),
    )


def _load_task_contracts(task_registry_path: Path) -> tuple[BenchmarkTaskContract, ...]:
    if not task_registry_path.exists():
        raise FileNotFoundError(
            f"benchmark task registry does not exist: {task_registry_path}"
        )

    rows = read_csv_rows(task_registry_path)
    tasks = tuple(_build_task_contract(row) for row in rows)
    if not tasks:
        raise ValueError("benchmark task registry must contain at least one task")

    task_ids = [task.task_id for task in tasks]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("benchmark task registry must not repeat task_id")
    return tasks


@lru_cache(maxsize=1)
def _load_default_task_contracts() -> tuple[BenchmarkTaskContract, ...]:
    return _load_task_contracts(DEFAULT_TASK_REGISTRY_PATH)


def load_benchmark_task_contracts(
    task_registry_path: Path | None = None,
) -> tuple[BenchmarkTaskContract, ...]:
    resolved_path = (
        DEFAULT_TASK_REGISTRY_PATH
        if task_registry_path is None
        else task_registry_path.resolve()
    )
    if resolved_path == DEFAULT_TASK_REGISTRY_PATH:
        return _load_default_task_contracts()
    return _load_task_contracts(resolved_path)


def load_benchmark_suite_contracts(
    task_registry_path: Path | None = None,
) -> tuple[BenchmarkSuiteContract, ...]:
    tasks = load_benchmark_task_contracts(task_registry_path=task_registry_path)
    grouped: dict[str, list[BenchmarkTaskContract]] = {}
    for task in tasks:
        grouped.setdefault(task.suite_id, []).append(task)
    return tuple(
        BenchmarkSuiteContract(
            suite_id=suite_id,
            suite_label=grouped[suite_id][0].suite_label,
            tasks=tuple(grouped[suite_id]),
        )
        for suite_id in sorted(grouped)
    )


def resolve_benchmark_suite_contract(
    benchmark_suite_id: str,
    *,
    task_registry_path: Path | None = None,
) -> BenchmarkSuiteContract:
    resolved_suite_id = _require_text(benchmark_suite_id, "benchmark_suite_id")
    for suite_contract in load_benchmark_suite_contracts(
        task_registry_path=task_registry_path
    ):
        if suite_contract.suite_id == resolved_suite_id:
            return suite_contract
    raise ValueError(f"unknown benchmark suite id: {resolved_suite_id}")


def resolve_benchmark_task_contract(
    *,
    benchmark_task_id: str | None = None,
    benchmark_question_id: str | None = None,
    benchmark_suite_id: str | None = None,
    entity_types: tuple[str, ...] | None = None,
    baseline_ids: tuple[str, ...] | None = None,
    task_registry_path: Path | None = None,
) -> BenchmarkTaskContract:
    tasks = load_benchmark_task_contracts(task_registry_path=task_registry_path)
    candidates = list(tasks)

    if benchmark_suite_id:
        candidates = [
            task for task in candidates if task.suite_id == benchmark_suite_id
        ]
    if benchmark_task_id:
        candidates = [
            task for task in candidates if task.task_id == benchmark_task_id
        ]
    if benchmark_question_id:
        candidates = [
            task
            for task in candidates
            if task.benchmark_question_id == benchmark_question_id
        ]
    if entity_types:
        requested_entity_types = set(entity_types)
        candidates = [
            task
            for task in candidates
            if requested_entity_types.issubset(set(task.entity_types))
        ]
    if baseline_ids:
        requested_baseline_ids = set(baseline_ids)
        candidates = [
            task
            for task in candidates
            if requested_baseline_ids.issubset(set(task.supported_baseline_ids))
        ]

    if not candidates:
        lookup_parts = []
        if benchmark_suite_id:
            lookup_parts.append(f"suite_id={benchmark_suite_id}")
        if benchmark_task_id:
            lookup_parts.append(f"task_id={benchmark_task_id}")
        if benchmark_question_id:
            lookup_parts.append(f"benchmark_question_id={benchmark_question_id}")
        if entity_types:
            lookup_parts.append(
                "entity_types=" + "|".join(sorted(set(entity_types)))
            )
        if baseline_ids:
            lookup_parts.append(
                "baseline_ids=" + "|".join(sorted(set(baseline_ids)))
            )
        lookup = ", ".join(lookup_parts) if lookup_parts else "no lookup key provided"
        raise ValueError(f"no benchmark task contract matched: {lookup}")

    if len(candidates) > 1 and not benchmark_task_id and not baseline_ids:
        legacy_defaults = [
            task for task in candidates if task.legacy_lookup_default
        ]
        if len(legacy_defaults) == 1:
            return legacy_defaults[0]
        if len(legacy_defaults) > 1:
            matched_task_ids = ", ".join(
                sorted(task.task_id for task in legacy_defaults)
            )
            raise ValueError(
                "benchmark task registry matched multiple legacy lookup defaults: "
                f"{matched_task_ids}"
            )
    if len(candidates) > 1:
        matched_task_ids = ", ".join(sorted(task.task_id for task in candidates))
        raise ValueError(
            "benchmark task contract lookup is ambiguous; matched task_ids: "
            f"{matched_task_ids}"
        )
    return candidates[0]


__all__ = [
    "BenchmarkFixturePaths",
    "BenchmarkSuiteContract",
    "BenchmarkTaskContract",
    "DEFAULT_TASK_REGISTRY_PATH",
    "FROZEN_PROTOCOL_ID",
    "TRACK_B_PROTOCOL_ID",
    "load_benchmark_suite_contracts",
    "load_benchmark_task_contracts",
    "resolve_benchmark_suite_contract",
    "resolve_benchmark_task_contract",
]
