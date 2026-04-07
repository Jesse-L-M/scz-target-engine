from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import os
from pathlib import Path
import shutil
from typing import Any

from scz_target_engine.benchmark_protocol import (
    BENCHMARK_EVALUATION_HORIZONS,
    BENCHMARK_LABEL_NAMES,
    BENCHMARK_QUESTION_V1,
    TRACK_B_BENCHMARK_PROTOCOL,
    BenchmarkSnapshotManifest,
    VALID_ENTITY_TYPES,
)
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.benchmark_snapshots import (
    load_source_archive_descriptors,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_ASSETS_FILE_NAME,
    TRACK_B_CASEBOOK_FILE_NAME,
    TRACK_B_DIRECTIONALITY_HYPOTHESES_FILE_NAME,
    TRACK_B_EVENT_PROVENANCE_FILE_NAME,
    TRACK_B_EVENTS_FILE_NAME,
    TRACK_B_PROGRAM_UNIVERSE_FILE_NAME,
    is_track_b_task,
    load_track_b_casebook,
    validate_track_b_casebook_against_cohort_members,
)
from scz_target_engine.io import read_csv_rows, read_json, write_csv, write_json
from scz_target_engine.json_contract import (
    require_json_int,
    require_json_list,
    require_json_mapping,
    require_json_text,
    require_optional_json_string,
)


BENCHMARK_COHORT_MEMBERS_FIELDNAMES = [
    "entity_type",
    "entity_id",
    "entity_label",
]
BENCHMARK_COHORT_LABEL_FIELDNAMES = [
    "cohort_id",
    "snapshot_id",
    "entity_type",
    "entity_id",
    "entity_label",
    "label_name",
    "label_value",
    "horizon",
    "outcome_date",
    "label_source",
    "label_notes",
]
BENCHMARK_COHORT_MEMBERS_FILE_NAME = "benchmark_cohort_members.csv"
BENCHMARK_COHORT_MANIFEST_FILE_NAME = "benchmark_cohort_manifest.json"
BENCHMARK_SOURCE_COHORT_MEMBERS_FILE_NAME = "source_cohort_members.csv"
BENCHMARK_SOURCE_FUTURE_OUTCOMES_FILE_NAME = "source_future_outcomes.csv"
BENCHMARK_COHORT_MANIFEST_SCHEMA_NAME = "benchmark_cohort_manifest"
BENCHMARK_COHORT_MANIFEST_SCHEMA_VERSION = "v3"
NO_OUTCOME_LABEL = "no_qualifying_future_outcome"
OBSERVED_LABEL_VALUE = "true"
NOT_OBSERVED_LABEL_VALUE = "false"
TRACK_B_SOURCE_ARCHIVE_INDEX_FILE_NAME = "source_archives.json"
TRACK_B_SOURCE_ARCHIVE_DIR_NAME = "archives"
TRACK_B_SOURCE_ARTIFACT_SPECS = (
    ("source_archive_index", TRACK_B_SOURCE_ARCHIVE_INDEX_FILE_NAME),
    ("track_b_casebook", TRACK_B_CASEBOOK_FILE_NAME),
    ("track_b_program_universe", TRACK_B_PROGRAM_UNIVERSE_FILE_NAME),
    ("track_b_program_history_events", TRACK_B_EVENTS_FILE_NAME),
    ("program_memory_assets", TRACK_B_ASSETS_FILE_NAME),
    ("program_memory_event_provenance", TRACK_B_EVENT_PROVENANCE_FILE_NAME),
    (
        "program_memory_directionality_hypotheses",
        TRACK_B_DIRECTIONALITY_HYPOTHESES_FILE_NAME,
    ),
)
TRACK_B_SOURCE_ARTIFACT_FILE_NAMES = {
    artifact_name: file_name
    for artifact_name, file_name in TRACK_B_SOURCE_ARTIFACT_SPECS
}


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _require_sha256(value: str, field_name: str) -> str:
    cleaned = _require_text(value, field_name)
    if len(cleaned) != 64 or any(character not in "0123456789abcdef" for character in cleaned):
        raise ValueError(f"{field_name} must be a lowercase SHA256 hex digest")
    return cleaned


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def _parse_horizon_years(horizon: str) -> int:
    if not horizon.endswith("y"):
        raise ValueError(f"unsupported evaluation horizon: {horizon}")
    return int(horizon.removesuffix("y"))


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_reference_for_manifest(*, target_path: Path, anchor_path: Path) -> str:
    return Path(
        os.path.relpath(
            target_path.resolve(),
            start=anchor_path.resolve().parent,
        )
    ).as_posix()


def _resolve_manifest_path_reference(*, path_value: str, anchor_path: Path) -> Path:
    candidate = Path(_require_text(path_value, "path_value"))
    if candidate.is_absolute():
        return candidate.resolve()
    return (anchor_path.resolve().parent / candidate).resolve()


def _resolve_required_path(path_value: Path | str | None, field_name: str) -> Path:
    if path_value is None:
        raise ValueError(f"{field_name} is required")
    return Path(path_value).resolve()


def _copy_artifact_if_needed(*, source_path: Path, destination_path: Path) -> Path:
    resolved_source_path = source_path.resolve()
    resolved_destination_path = destination_path.resolve()
    if resolved_source_path != resolved_destination_path:
        shutil.copy2(resolved_source_path, resolved_destination_path)
    return resolved_destination_path


def _validate_snapshot_manifest_file_matches_manifest(
    *,
    snapshot_manifest: BenchmarkSnapshotManifest,
    snapshot_manifest_file: Path,
) -> None:
    snapshot_manifest_from_file = read_benchmark_snapshot_manifest(
        snapshot_manifest_file
    )
    snapshot_manifest_payload = snapshot_manifest.to_dict()
    snapshot_manifest_payload.pop("task_registry_path", None)
    snapshot_manifest_from_file_payload = snapshot_manifest_from_file.to_dict()
    snapshot_manifest_from_file_payload.pop("task_registry_path", None)
    if snapshot_manifest_from_file_payload != snapshot_manifest_payload:
        raise ValueError(
            "manifest_file must contain the same benchmark snapshot manifest as the "
            "supplied manifest object"
        )


def _resolve_task_contract_for_manifest(
    manifest: BenchmarkSnapshotManifest,
    *,
    task_registry_path: Path | None = None,
) -> object:
    return resolve_benchmark_task_contract(
        benchmark_task_id=manifest.benchmark_task_id or None,
        benchmark_question_id=manifest.benchmark_question_id,
        benchmark_suite_id=manifest.benchmark_suite_id or None,
        entity_types=manifest.entity_types,
        baseline_ids=manifest.baseline_ids,
        task_registry_path=(
            task_registry_path
            if task_registry_path is not None
            else (
                Path(manifest.task_registry_path).resolve()
                if getattr(manifest, "task_registry_path", "")
                else None
            )
        ),
    )


def _manifest_uses_track_b_protocol(manifest: BenchmarkSnapshotManifest) -> bool:
    benchmark_task_id = getattr(manifest, "benchmark_task_id", "")
    benchmark_question_id = getattr(manifest, "benchmark_question_id", "")
    return is_track_b_task(benchmark_task_id) or (
        benchmark_question_id == TRACK_B_BENCHMARK_PROTOCOL.question.question_id
    )


@dataclass(frozen=True)
class CohortMember:
    entity_type: str
    entity_id: str
    entity_label: str

    def __post_init__(self) -> None:
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        _require_text(self.entity_label, "entity_label")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CohortMember:
        mapping = require_json_mapping(payload, "cohort member")
        return cls(
            entity_type=require_json_text(mapping.get("entity_type"), "entity_type"),
            entity_id=require_json_text(mapping.get("entity_id"), "entity_id"),
            entity_label=require_json_text(mapping.get("entity_label"), "entity_label"),
        )


@dataclass(frozen=True)
class FutureOutcomeRecord:
    entity_type: str
    entity_id: str
    outcome_label: str
    outcome_date: str
    label_source: str
    label_notes: str = ""

    def __post_init__(self) -> None:
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        if self.outcome_label == NO_OUTCOME_LABEL:
            raise ValueError("future outcome inputs must not precompute no_qualifying_future_outcome")
        if self.outcome_label not in BENCHMARK_QUESTION_V1.translational_outcome_labels:
            raise ValueError("outcome_label must match the frozen benchmark question labels")
        _parse_iso_date(self.outcome_date, "outcome_date")
        _require_text(self.label_source, "label_source")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FutureOutcomeRecord:
        mapping = require_json_mapping(payload, "future outcome record")
        return cls(
            entity_type=require_json_text(mapping.get("entity_type"), "entity_type"),
            entity_id=require_json_text(mapping.get("entity_id"), "entity_id"),
            outcome_label=require_json_text(
                mapping.get("outcome_label"),
                "outcome_label",
            ),
            outcome_date=require_json_text(mapping.get("outcome_date"), "outcome_date"),
            label_source=require_json_text(
                mapping.get("label_source"),
                "label_source",
            ),
            label_notes=require_optional_json_string(
                mapping.get("label_notes"),
                "label_notes",
            ),
        )


@dataclass(frozen=True)
class BenchmarkCohortLabel:
    cohort_id: str
    snapshot_id: str
    entity_type: str
    entity_id: str
    entity_label: str
    label_name: str
    label_value: str
    horizon: str
    outcome_date: str
    label_source: str
    label_notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.cohort_id, "cohort_id")
        _require_text(self.snapshot_id, "snapshot_id")
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        _require_text(self.entity_label, "entity_label")
        if self.label_name not in BENCHMARK_LABEL_NAMES:
            raise ValueError("label_name must match a supported benchmark question label")
        if self.label_value not in {OBSERVED_LABEL_VALUE, NOT_OBSERVED_LABEL_VALUE}:
            raise ValueError("label_value must be true or false")
        if self.horizon not in BENCHMARK_EVALUATION_HORIZONS:
            raise ValueError("horizon must match a supported benchmark question horizon")
        if self.outcome_date:
            _parse_iso_date(self.outcome_date, "outcome_date")
        _require_text(self.label_source, "label_source")

    def to_dict(self) -> dict[str, object]:
        return {
            "cohort_id": self.cohort_id,
            "snapshot_id": self.snapshot_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "entity_label": self.entity_label,
            "label_name": self.label_name,
            "label_value": self.label_value,
            "horizon": self.horizon,
            "outcome_date": self.outcome_date,
            "label_source": self.label_source,
            "label_notes": self.label_notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkCohortLabel:
        mapping = require_json_mapping(payload, "benchmark cohort label")
        return cls(
            cohort_id=require_json_text(mapping.get("cohort_id"), "cohort_id"),
            snapshot_id=require_json_text(mapping.get("snapshot_id"), "snapshot_id"),
            entity_type=require_json_text(mapping.get("entity_type"), "entity_type"),
            entity_id=require_json_text(mapping.get("entity_id"), "entity_id"),
            entity_label=require_json_text(mapping.get("entity_label"), "entity_label"),
            label_name=require_json_text(mapping.get("label_name"), "label_name"),
            label_value=require_json_text(mapping.get("label_value"), "label_value"),
            horizon=require_json_text(mapping.get("horizon"), "horizon"),
            outcome_date=require_optional_json_string(
                mapping.get("outcome_date"),
                "outcome_date",
            ),
            label_source=require_json_text(
                mapping.get("label_source"),
                "label_source",
            ),
            label_notes=require_optional_json_string(
                mapping.get("label_notes"),
                "label_notes",
            ),
        )


@dataclass(frozen=True)
class CohortSourceArtifact:
    artifact_name: str
    artifact_path: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.artifact_name, "artifact_name")
        _require_text(self.artifact_path, "artifact_path")
        _require_sha256(self.artifact_sha256, "artifact_sha256")

    def to_dict(self) -> dict[str, str]:
        return {
            "artifact_name": self.artifact_name,
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CohortSourceArtifact":
        mapping = require_json_mapping(payload, "cohort source artifact")
        return cls(
            artifact_name=require_json_text(
                mapping.get("artifact_name"),
                "artifact_name",
            ),
            artifact_path=require_json_text(
                mapping.get("artifact_path"),
                "artifact_path",
            ),
            artifact_sha256=require_json_text(
                mapping.get("artifact_sha256"),
                "artifact_sha256",
            ),
        )


@dataclass(frozen=True)
class BenchmarkCohortManifest:
    snapshot_id: str
    cohort_id: str
    benchmark_question_id: str
    as_of_date: str
    outcome_observation_closed_at: str
    entity_types: tuple[str, ...]
    cohort_members_artifact_path: str
    cohort_members_artifact_sha256: str
    cohort_labels_artifact_path: str
    cohort_labels_artifact_sha256: str
    entity_count: int
    label_row_count: int
    observed_label_row_count: int
    schema_name: str = BENCHMARK_COHORT_MANIFEST_SCHEMA_NAME
    schema_version: str = BENCHMARK_COHORT_MANIFEST_SCHEMA_VERSION
    benchmark_suite_id: str = ""
    benchmark_task_id: str = ""
    snapshot_manifest_artifact_path: str = ""
    snapshot_manifest_artifact_sha256: str = ""
    source_cohort_members_path: str = ""
    source_cohort_members_sha256: str = ""
    source_future_outcomes_path: str = ""
    source_future_outcomes_sha256: str = ""
    auxiliary_source_artifacts: tuple[CohortSourceArtifact, ...] = ()
    notes: str = ""

    def __post_init__(self) -> None:
        if self.schema_name != BENCHMARK_COHORT_MANIFEST_SCHEMA_NAME:
            raise ValueError(
                "schema_name must remain "
                f"{BENCHMARK_COHORT_MANIFEST_SCHEMA_NAME}"
            )
        if self.schema_version != BENCHMARK_COHORT_MANIFEST_SCHEMA_VERSION:
            raise ValueError(
                "schema_version must remain "
                f"{BENCHMARK_COHORT_MANIFEST_SCHEMA_VERSION}"
            )
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.cohort_id, "cohort_id")
        _require_text(self.benchmark_question_id, "benchmark_question_id")
        _parse_iso_date(self.as_of_date, "as_of_date")
        _parse_iso_date(
            self.outcome_observation_closed_at,
            "outcome_observation_closed_at",
        )
        if not self.entity_types:
            raise ValueError("entity_types must contain at least one entity_type")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError("entity_types must only contain supported benchmark entity types")
        if self.benchmark_suite_id:
            _require_text(self.benchmark_suite_id, "benchmark_suite_id")
        if self.benchmark_task_id:
            _require_text(self.benchmark_task_id, "benchmark_task_id")
        _require_text(
            self.snapshot_manifest_artifact_path,
            "snapshot_manifest_artifact_path",
        )
        _require_sha256(
            self.snapshot_manifest_artifact_sha256,
            "snapshot_manifest_artifact_sha256",
        )
        _require_text(self.cohort_members_artifact_path, "cohort_members_artifact_path")
        _require_sha256(
            self.cohort_members_artifact_sha256,
            "cohort_members_artifact_sha256",
        )
        _require_text(self.cohort_labels_artifact_path, "cohort_labels_artifact_path")
        _require_sha256(
            self.cohort_labels_artifact_sha256,
            "cohort_labels_artifact_sha256",
        )
        _require_text(self.source_cohort_members_path, "source_cohort_members_path")
        _require_sha256(
            self.source_cohort_members_sha256,
            "source_cohort_members_sha256",
        )
        _require_text(self.source_future_outcomes_path, "source_future_outcomes_path")
        _require_sha256(
            self.source_future_outcomes_sha256,
            "source_future_outcomes_sha256",
        )
        seen_auxiliary_artifact_names: set[str] = set()
        for artifact in self.auxiliary_source_artifacts:
            if artifact.artifact_name in seen_auxiliary_artifact_names:
                raise ValueError(
                    "auxiliary_source_artifacts must not repeat artifact_name"
                )
            seen_auxiliary_artifact_names.add(artifact.artifact_name)
        if self.entity_count <= 0:
            raise ValueError("entity_count must be positive")
        if self.label_row_count <= 0:
            raise ValueError("label_row_count must be positive")
        if self.observed_label_row_count < 0:
            raise ValueError("observed_label_row_count must be non-negative")
        if self.observed_label_row_count > self.label_row_count:
            raise ValueError(
                "observed_label_row_count cannot exceed label_row_count"
            )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "benchmark_question_id": self.benchmark_question_id,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "entity_types": list(self.entity_types),
            "cohort_members_artifact_path": self.cohort_members_artifact_path,
            "cohort_members_artifact_sha256": self.cohort_members_artifact_sha256,
            "cohort_labels_artifact_path": self.cohort_labels_artifact_path,
            "cohort_labels_artifact_sha256": self.cohort_labels_artifact_sha256,
            "entity_count": self.entity_count,
            "label_row_count": self.label_row_count,
            "observed_label_row_count": self.observed_label_row_count,
        }
        if self.benchmark_suite_id:
            payload["benchmark_suite_id"] = self.benchmark_suite_id
        if self.benchmark_task_id:
            payload["benchmark_task_id"] = self.benchmark_task_id
        payload["snapshot_manifest_artifact_path"] = (
            self.snapshot_manifest_artifact_path
        )
        payload["snapshot_manifest_artifact_sha256"] = (
            self.snapshot_manifest_artifact_sha256
        )
        payload["source_cohort_members_path"] = self.source_cohort_members_path
        payload["source_cohort_members_sha256"] = self.source_cohort_members_sha256
        payload["source_future_outcomes_path"] = self.source_future_outcomes_path
        payload["source_future_outcomes_sha256"] = self.source_future_outcomes_sha256
        if self.auxiliary_source_artifacts:
            payload["auxiliary_source_artifacts"] = [
                artifact.to_dict()
                for artifact in self.auxiliary_source_artifacts
            ]
        if self.notes:
            payload["notes"] = self.notes
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkCohortManifest:
        mapping = require_json_mapping(payload, "benchmark cohort manifest")
        required_fields = (
            "schema_name",
            "schema_version",
            "snapshot_id",
            "cohort_id",
            "benchmark_question_id",
            "as_of_date",
            "outcome_observation_closed_at",
            "entity_types",
            "snapshot_manifest_artifact_path",
            "snapshot_manifest_artifact_sha256",
            "cohort_members_artifact_path",
            "cohort_members_artifact_sha256",
            "cohort_labels_artifact_path",
            "cohort_labels_artifact_sha256",
            "source_cohort_members_path",
            "source_cohort_members_sha256",
            "source_future_outcomes_path",
            "source_future_outcomes_sha256",
            "entity_count",
            "label_row_count",
            "observed_label_row_count",
        )
        for field_name in required_fields:
            if field_name not in mapping:
                raise ValueError(
                    f"benchmark cohort manifest is missing required field: {field_name}"
                )
        return cls(
            schema_name=require_json_text(mapping.get("schema_name"), "schema_name"),
            schema_version=require_json_text(
                mapping.get("schema_version"),
                "schema_version",
            ),
            snapshot_id=require_json_text(mapping.get("snapshot_id"), "snapshot_id"),
            cohort_id=require_json_text(mapping.get("cohort_id"), "cohort_id"),
            benchmark_question_id=require_json_text(
                mapping.get("benchmark_question_id"),
                "benchmark_question_id",
            ),
            as_of_date=require_json_text(mapping.get("as_of_date"), "as_of_date"),
            outcome_observation_closed_at=require_json_text(
                mapping.get("outcome_observation_closed_at"),
                "outcome_observation_closed_at",
            ),
            entity_types=tuple(
                require_json_text(item, "entity_types[]")
                for item in require_json_list(mapping.get("entity_types"), "entity_types")
            ),
            benchmark_suite_id=require_optional_json_string(
                mapping.get("benchmark_suite_id"),
                "benchmark_suite_id",
            ),
            benchmark_task_id=require_optional_json_string(
                mapping.get("benchmark_task_id"),
                "benchmark_task_id",
            ),
            snapshot_manifest_artifact_path=require_json_text(
                mapping.get("snapshot_manifest_artifact_path"),
                "snapshot_manifest_artifact_path",
            ),
            snapshot_manifest_artifact_sha256=require_json_text(
                mapping.get("snapshot_manifest_artifact_sha256"),
                "snapshot_manifest_artifact_sha256",
            ),
            cohort_members_artifact_path=require_json_text(
                mapping.get("cohort_members_artifact_path"),
                "cohort_members_artifact_path",
            ),
            cohort_members_artifact_sha256=require_json_text(
                mapping.get("cohort_members_artifact_sha256"),
                "cohort_members_artifact_sha256",
            ),
            cohort_labels_artifact_path=require_json_text(
                mapping.get("cohort_labels_artifact_path"),
                "cohort_labels_artifact_path",
            ),
            cohort_labels_artifact_sha256=require_json_text(
                mapping.get("cohort_labels_artifact_sha256"),
                "cohort_labels_artifact_sha256",
            ),
            source_cohort_members_path=require_json_text(
                mapping.get("source_cohort_members_path"),
                "source_cohort_members_path",
            ),
            source_cohort_members_sha256=require_json_text(
                mapping.get("source_cohort_members_sha256"),
                "source_cohort_members_sha256",
            ),
            source_future_outcomes_path=require_json_text(
                mapping.get("source_future_outcomes_path"),
                "source_future_outcomes_path",
            ),
            source_future_outcomes_sha256=require_json_text(
                mapping.get("source_future_outcomes_sha256"),
                "source_future_outcomes_sha256",
            ),
            auxiliary_source_artifacts=tuple(
                CohortSourceArtifact.from_dict(item)
                for item in require_json_list(
                    mapping.get("auxiliary_source_artifacts", []),
                    "auxiliary_source_artifacts",
                )
            ),
            entity_count=require_json_int(mapping.get("entity_count"), "entity_count"),
            label_row_count=require_json_int(
                mapping.get("label_row_count"),
                "label_row_count",
            ),
            observed_label_row_count=require_json_int(
                mapping.get("observed_label_row_count"),
                "observed_label_row_count",
            ),
            notes=require_optional_json_string(mapping.get("notes"), "notes"),
        )


@dataclass(frozen=True)
class MaterializedBenchmarkCohortArtifacts:
    cohort_manifest_path: Path
    cohort_members_path: Path
    source_cohort_members_path: Path
    source_future_outcomes_path: Path
    cohort_manifest: BenchmarkCohortManifest
    cohort_members: tuple[CohortMember, ...]
    cohort_labels: tuple[BenchmarkCohortLabel, ...]
    auxiliary_source_artifacts: tuple[CohortSourceArtifact, ...] = ()


def load_cohort_members(path: Path) -> tuple[CohortMember, ...]:
    members = tuple(CohortMember.from_dict(row) for row in read_csv_rows(path))
    seen = set()
    for member in members:
        key = (member.entity_type, member.entity_id)
        if key in seen:
            raise ValueError("cohort members must not repeat entity_type/entity_id")
        seen.add(key)
    return members


def load_future_outcomes(path: Path) -> tuple[FutureOutcomeRecord, ...]:
    return tuple(FutureOutcomeRecord.from_dict(row) for row in read_csv_rows(path))


def write_benchmark_cohort_members(
    path: Path,
    cohort_members: tuple[CohortMember, ...],
) -> None:
    write_csv(
        path,
        [
            {
                "entity_type": member.entity_type,
                "entity_id": member.entity_id,
                "entity_label": member.entity_label,
            }
            for member in sorted(
                cohort_members,
                key=lambda item: (
                    item.entity_type,
                    item.entity_id,
                    item.entity_label.lower(),
                ),
            )
        ],
        BENCHMARK_COHORT_MEMBERS_FIELDNAMES,
    )


def read_benchmark_cohort_members(path: Path) -> tuple[CohortMember, ...]:
    return load_cohort_members(path)


def write_benchmark_cohort_labels(
    path: Path,
    labels: tuple[BenchmarkCohortLabel, ...],
) -> None:
    write_csv(
        path,
        [label.to_dict() for label in labels],
        BENCHMARK_COHORT_LABEL_FIELDNAMES,
    )


def read_benchmark_cohort_labels(path: Path) -> tuple[BenchmarkCohortLabel, ...]:
    return tuple(BenchmarkCohortLabel.from_dict(row) for row in read_csv_rows(path))


def benchmark_cohort_members_path_for_labels_file(labels_file: Path) -> Path:
    return labels_file.resolve().parent / BENCHMARK_COHORT_MEMBERS_FILE_NAME


def benchmark_cohort_manifest_path_for_labels_file(labels_file: Path) -> Path:
    return labels_file.resolve().parent / BENCHMARK_COHORT_MANIFEST_FILE_NAME


def benchmark_source_cohort_members_path_for_labels_file(labels_file: Path) -> Path:
    return labels_file.resolve().parent / BENCHMARK_SOURCE_COHORT_MEMBERS_FILE_NAME


def benchmark_source_future_outcomes_path_for_labels_file(labels_file: Path) -> Path:
    return (
        labels_file.resolve().parent / BENCHMARK_SOURCE_FUTURE_OUTCOMES_FILE_NAME
    )


def benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
    labels_file: Path,
    *,
    artifact_name: str,
) -> Path:
    file_name = TRACK_B_SOURCE_ARTIFACT_FILE_NAMES.get(artifact_name)
    if file_name is None:
        raise ValueError(f"unknown Track B auxiliary source artifact: {artifact_name}")
    return labels_file.resolve().parent / file_name


def benchmark_track_b_source_archives_dir_for_labels_file(labels_file: Path) -> Path:
    return labels_file.resolve().parent / TRACK_B_SOURCE_ARCHIVE_DIR_NAME


def _require_canonical_bundle_artifact_path(
    *,
    actual_path: Path,
    expected_path: Path,
    artifact_name: str,
) -> None:
    if actual_path != expected_path:
        raise ValueError(
            "benchmark cohort manifest must point to the canonical "
            f"{artifact_name} artifact beside cohort labels"
        )


def _copy_directory_tree_if_needed(
    *,
    source_dir: Path,
    destination_dir: Path,
) -> Path:
    resolved_source_dir = source_dir.resolve()
    resolved_destination_dir = destination_dir.resolve()
    if resolved_source_dir == resolved_destination_dir:
        return resolved_destination_dir
    if resolved_destination_dir.exists():
        if not resolved_destination_dir.is_dir():
            raise ValueError(
                f"destination_dir must be a directory: {resolved_destination_dir}"
            )
        shutil.rmtree(resolved_destination_dir)
    shutil.copytree(resolved_source_dir, resolved_destination_dir)
    return resolved_destination_dir


def write_benchmark_cohort_manifest(
    path: Path,
    cohort_manifest: BenchmarkCohortManifest,
) -> None:
    write_json(path, cohort_manifest.to_dict())


def read_benchmark_cohort_manifest(path: Path) -> BenchmarkCohortManifest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return BenchmarkCohortManifest.from_dict(payload)


def validate_benchmark_cohort_members_against_manifest(
    manifest: BenchmarkSnapshotManifest,
    cohort_members: tuple[CohortMember, ...],
) -> None:
    if not cohort_members:
        raise ValueError("benchmark cohort members must contain at least one row")
    manifest_entity_types = set(manifest.entity_types)
    member_entity_types = {member.entity_type for member in cohort_members}
    if member_entity_types != manifest_entity_types:
        details: list[str] = []
        missing_entity_types = sorted(manifest_entity_types.difference(member_entity_types))
        extra_entity_types = sorted(member_entity_types.difference(manifest_entity_types))
        if missing_entity_types:
            details.append("missing=" + ", ".join(missing_entity_types))
        if extra_entity_types:
            details.append("unexpected=" + ", ".join(extra_entity_types))
        raise ValueError(
            "benchmark cohort members must match the manifest entity_types"
            + (f" ({'; '.join(details)})" if details else "")
        )


def validate_benchmark_cohort_manifest_against_snapshot_manifest(
    snapshot_manifest: BenchmarkSnapshotManifest,
    cohort_manifest: BenchmarkCohortManifest,
) -> None:
    if cohort_manifest.snapshot_id != snapshot_manifest.snapshot_id:
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "snapshot_id"
        )
    if cohort_manifest.cohort_id != snapshot_manifest.cohort_id:
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "cohort_id"
        )
    if (
        cohort_manifest.benchmark_question_id
        != snapshot_manifest.benchmark_question_id
    ):
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "benchmark_question_id"
        )
    if cohort_manifest.as_of_date != snapshot_manifest.as_of_date:
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "as_of_date"
        )
    if (
        cohort_manifest.outcome_observation_closed_at
        != snapshot_manifest.outcome_observation_closed_at
    ):
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "outcome_observation_closed_at"
        )
    if tuple(cohort_manifest.entity_types) != tuple(snapshot_manifest.entity_types):
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "entity_types"
        )
    if (
        cohort_manifest.benchmark_suite_id
        and cohort_manifest.benchmark_suite_id != snapshot_manifest.benchmark_suite_id
    ):
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "benchmark_suite_id"
        )
    if (
        cohort_manifest.benchmark_task_id
        and cohort_manifest.benchmark_task_id != snapshot_manifest.benchmark_task_id
    ):
        raise ValueError(
            "benchmark cohort manifest must match the supplied snapshot manifest "
            "benchmark_task_id"
        )


def validate_benchmark_cohort_labels_against_manifest(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    *,
    cohort_members: tuple[CohortMember, ...],
) -> None:
    question = (
        TRACK_B_BENCHMARK_PROTOCOL.question
        if _manifest_uses_track_b_protocol(manifest)
        else _resolve_task_contract_for_manifest(manifest).protocol.question
    )
    if not cohort_labels:
        raise ValueError("benchmark cohort labels must contain at least one row")
    validate_benchmark_cohort_members_against_manifest(manifest, cohort_members)
    snapshot_ids = {label.snapshot_id for label in cohort_labels}
    if snapshot_ids != {manifest.snapshot_id}:
        raise ValueError("benchmark cohort labels must match the manifest snapshot_id")
    cohort_ids = {label.cohort_id for label in cohort_labels}
    if cohort_ids != {manifest.cohort_id}:
        raise ValueError("benchmark cohort labels must match the manifest cohort_id")
    expected_members = {
        (member.entity_type, member.entity_id): member.entity_label
        for member in cohort_members
    }
    expected_label_pairs = {
        (horizon, label_name)
        for horizon in question.evaluation_horizons
        for label_name in question.translational_outcome_labels
    }
    seen_label_keys: set[tuple[str, str, str, str]] = set()
    entity_label_pairs: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for label in cohort_labels:
        entity_key = (label.entity_type, label.entity_id)
        expected_entity_label = expected_members.get(entity_key)
        if expected_entity_label is None:
            raise ValueError(
                "benchmark cohort labels contain an entity outside the benchmark "
                f"cohort members artifact: {label.entity_type}/{label.entity_id}"
            )
        if expected_entity_label != label.entity_label:
            raise ValueError(
                "benchmark cohort labels must keep a stable entity_label per "
                f"entity_type/entity_id: {label.entity_type}/{label.entity_id}"
            )
        label_key = (
            label.entity_type,
            label.entity_id,
            label.horizon,
            label.label_name,
        )
        if label_key in seen_label_keys:
            raise ValueError(
                "benchmark cohort labels must not repeat "
                "entity_type/entity_id/horizon/label_name rows"
            )
        seen_label_keys.add(label_key)
        entity_label_pairs.setdefault(entity_key, set()).add(
            (label.horizon, label.label_name)
        )
    missing_entities = sorted(set(expected_members).difference(entity_label_pairs))
    extra_entities = sorted(set(entity_label_pairs).difference(expected_members))
    if missing_entities or extra_entities:
        details: list[str] = []
        if missing_entities:
            details.append(
                "missing="
                + ", ".join(
                    f"{entity_type}/{entity_id}"
                    for entity_type, entity_id in missing_entities[:5]
                )
            )
        if extra_entities:
            details.append(
                "unexpected="
                + ", ".join(
                    f"{entity_type}/{entity_id}"
                    for entity_type, entity_id in extra_entities[:5]
                )
            )
        raise ValueError(
            "benchmark cohort labels must align with the benchmark_cohort_members "
            "artifact"
            + (f" ({'; '.join(details)})" if details else "")
        )
    for entity_type, entity_id in sorted(expected_members):
        observed_pairs = entity_label_pairs.get((entity_type, entity_id), set())
        if observed_pairs == expected_label_pairs:
            continue
        missing_pairs = sorted(expected_label_pairs.difference(observed_pairs))
        unexpected_pairs = sorted(observed_pairs.difference(expected_label_pairs))
        details: list[str] = []
        if missing_pairs:
            details.append(
                "missing="
                + ", ".join(
                    f"{horizon}/{label_name}"
                    for horizon, label_name in missing_pairs[:5]
                )
            )
        if unexpected_pairs:
            details.append(
                "unexpected="
                + ", ".join(
                    f"{horizon}/{label_name}"
                    for horizon, label_name in unexpected_pairs[:5]
                )
            )
        raise ValueError(
            "benchmark cohort labels must include the full protocol label matrix per "
            f"entity: {entity_type}/{entity_id}"
            + (f" ({'; '.join(details)})" if details else "")
        )


def build_benchmark_cohort_manifest(
    *,
    snapshot_manifest: BenchmarkSnapshotManifest,
    snapshot_manifest_file: Path,
    cohort_members: tuple[CohortMember, ...],
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    cohort_manifest_artifact_file: Path,
    cohort_members_artifact_file: Path,
    cohort_labels_artifact_file: Path,
    source_cohort_members_file: Path,
    source_future_outcomes_file: Path,
    auxiliary_source_artifact_files: tuple[tuple[str, Path], ...] = (),
) -> BenchmarkCohortManifest:
    resolved_cohort_manifest_artifact_file = _resolve_required_path(
        cohort_manifest_artifact_file,
        "cohort_manifest_artifact_file",
    )
    resolved_cohort_members_artifact_file = _resolve_required_path(
        cohort_members_artifact_file,
        "cohort_members_artifact_file",
    )
    resolved_cohort_labels_artifact_file = _resolve_required_path(
        cohort_labels_artifact_file,
        "cohort_labels_artifact_file",
    )
    resolved_source_cohort_members_file = _resolve_required_path(
        source_cohort_members_file,
        "source_cohort_members_file",
    )
    resolved_source_future_outcomes_file = _resolve_required_path(
        source_future_outcomes_file,
        "source_future_outcomes_file",
    )
    resolved_auxiliary_source_artifact_files = tuple(
        (
            _require_text(artifact_name, "artifact_name"),
            _resolve_required_path(artifact_file, f"{artifact_name}_artifact_file"),
        )
        for artifact_name, artifact_file in auxiliary_source_artifact_files
    )
    resolved_snapshot_manifest_file = _resolve_required_path(
        snapshot_manifest_file,
        "snapshot_manifest_file",
    )
    _validate_snapshot_manifest_file_matches_manifest(
        snapshot_manifest=snapshot_manifest,
        snapshot_manifest_file=resolved_snapshot_manifest_file,
    )
    expected_cohort_members_artifact_file = benchmark_cohort_members_path_for_labels_file(
        resolved_cohort_labels_artifact_file
    )
    _require_canonical_bundle_artifact_path(
        actual_path=resolved_cohort_members_artifact_file,
        expected_path=expected_cohort_members_artifact_file,
        artifact_name="benchmark_cohort_members",
    )
    expected_source_cohort_members_file = benchmark_source_cohort_members_path_for_labels_file(
        resolved_cohort_labels_artifact_file
    )
    _require_canonical_bundle_artifact_path(
        actual_path=resolved_source_cohort_members_file,
        expected_path=expected_source_cohort_members_file,
        artifact_name="benchmark_source_cohort_members",
    )
    expected_source_future_outcomes_file = (
        benchmark_source_future_outcomes_path_for_labels_file(
            resolved_cohort_labels_artifact_file
        )
    )
    _require_canonical_bundle_artifact_path(
        actual_path=resolved_source_future_outcomes_file,
        expected_path=expected_source_future_outcomes_file,
        artifact_name="benchmark_source_future_outcomes",
    )
    observed_label_row_count = sum(
        label.label_value == OBSERVED_LABEL_VALUE for label in cohort_labels
    )
    return BenchmarkCohortManifest(
        snapshot_id=snapshot_manifest.snapshot_id,
        cohort_id=snapshot_manifest.cohort_id,
        benchmark_question_id=snapshot_manifest.benchmark_question_id,
        as_of_date=snapshot_manifest.as_of_date,
        outcome_observation_closed_at=snapshot_manifest.outcome_observation_closed_at,
        entity_types=tuple(snapshot_manifest.entity_types),
        benchmark_suite_id=snapshot_manifest.benchmark_suite_id,
        benchmark_task_id=snapshot_manifest.benchmark_task_id,
        snapshot_manifest_artifact_path=_path_reference_for_manifest(
            target_path=resolved_snapshot_manifest_file,
            anchor_path=resolved_cohort_manifest_artifact_file,
        ),
        snapshot_manifest_artifact_sha256=_file_sha256(
            resolved_snapshot_manifest_file
        ),
        cohort_members_artifact_path=_path_reference_for_manifest(
            target_path=resolved_cohort_members_artifact_file,
            anchor_path=resolved_cohort_manifest_artifact_file,
        ),
        cohort_members_artifact_sha256=_file_sha256(
            resolved_cohort_members_artifact_file
        ),
        cohort_labels_artifact_path=_path_reference_for_manifest(
            target_path=resolved_cohort_labels_artifact_file,
            anchor_path=resolved_cohort_manifest_artifact_file,
        ),
        cohort_labels_artifact_sha256=_file_sha256(resolved_cohort_labels_artifact_file),
        source_cohort_members_path=_path_reference_for_manifest(
            target_path=resolved_source_cohort_members_file,
            anchor_path=resolved_cohort_manifest_artifact_file,
        ),
        source_cohort_members_sha256=_file_sha256(resolved_source_cohort_members_file),
        source_future_outcomes_path=_path_reference_for_manifest(
            target_path=resolved_source_future_outcomes_file,
            anchor_path=resolved_cohort_manifest_artifact_file,
        ),
        source_future_outcomes_sha256=_file_sha256(resolved_source_future_outcomes_file),
        auxiliary_source_artifacts=tuple(
            CohortSourceArtifact(
                artifact_name=artifact_name,
                artifact_path=_path_reference_for_manifest(
                    target_path=artifact_file,
                    anchor_path=resolved_cohort_manifest_artifact_file,
                ),
                artifact_sha256=_file_sha256(artifact_file),
            )
            for artifact_name, artifact_file in resolved_auxiliary_source_artifact_files
        ),
        entity_count=len(cohort_members),
        label_row_count=len(cohort_labels),
        observed_label_row_count=observed_label_row_count,
    )


def load_materialized_benchmark_cohort_artifacts(
    *,
    snapshot_manifest: BenchmarkSnapshotManifest,
    snapshot_manifest_file: Path,
    cohort_labels_file: Path,
) -> MaterializedBenchmarkCohortArtifacts:
    resolved_snapshot_manifest_file = _resolve_required_path(
        snapshot_manifest_file,
        "snapshot_manifest_file",
    )
    resolved_cohort_labels_file = _resolve_required_path(
        cohort_labels_file,
        "cohort_labels_file",
    )
    cohort_manifest_path = benchmark_cohort_manifest_path_for_labels_file(
        resolved_cohort_labels_file
    )
    if not cohort_manifest_path.exists():
        raise ValueError(
            "benchmark runner/reporting requires a materialized "
            f"{BENCHMARK_COHORT_MANIFEST_FILE_NAME} beside cohort labels; rerun "
            "build-benchmark-cohort"
        )
    cohort_manifest = read_benchmark_cohort_manifest(cohort_manifest_path)
    validate_benchmark_cohort_manifest_against_snapshot_manifest(
        snapshot_manifest,
        cohort_manifest,
    )
    resolved_snapshot_manifest_artifact_path = _resolve_manifest_path_reference(
        path_value=cohort_manifest.snapshot_manifest_artifact_path,
        anchor_path=cohort_manifest_path,
    )
    if resolved_snapshot_manifest_artifact_path != resolved_snapshot_manifest_file:
        raise ValueError(
            "benchmark cohort manifest does not point to the supplied "
            "benchmark_snapshot_manifest artifact"
        )
    if _file_sha256(resolved_snapshot_manifest_file) != (
        cohort_manifest.snapshot_manifest_artifact_sha256
    ):
        raise ValueError(
            "benchmark cohort manifest snapshot manifest sha256 does not match the "
            "supplied benchmark_snapshot_manifest"
        )
    resolved_cohort_labels_artifact_path = _resolve_manifest_path_reference(
        path_value=cohort_manifest.cohort_labels_artifact_path,
        anchor_path=cohort_manifest_path,
    )
    if resolved_cohort_labels_artifact_path != resolved_cohort_labels_file:
        raise ValueError(
            "benchmark cohort manifest does not point to the supplied "
            "benchmark_cohort_labels artifact"
        )
    if _file_sha256(resolved_cohort_labels_file) != (
        cohort_manifest.cohort_labels_artifact_sha256
    ):
        raise ValueError(
            "benchmark cohort labels sha256 does not match benchmark_cohort_manifest"
        )
    cohort_members_path = _resolve_manifest_path_reference(
        path_value=cohort_manifest.cohort_members_artifact_path,
        anchor_path=cohort_manifest_path,
    )
    _require_canonical_bundle_artifact_path(
        actual_path=cohort_members_path,
        expected_path=benchmark_cohort_members_path_for_labels_file(
            resolved_cohort_labels_file
        ),
        artifact_name="benchmark_cohort_members",
    )
    if not cohort_members_path.exists():
        raise ValueError(
            "benchmark cohort manifest references a missing benchmark_cohort_members "
            f"artifact: {cohort_members_path}"
        )
    if _file_sha256(cohort_members_path) != cohort_manifest.cohort_members_artifact_sha256:
        raise ValueError(
            "benchmark cohort members sha256 does not match benchmark_cohort_manifest"
        )
    source_cohort_members_path = _resolve_manifest_path_reference(
        path_value=cohort_manifest.source_cohort_members_path,
        anchor_path=cohort_manifest_path,
    )
    _require_canonical_bundle_artifact_path(
        actual_path=source_cohort_members_path,
        expected_path=benchmark_source_cohort_members_path_for_labels_file(
            resolved_cohort_labels_file
        ),
        artifact_name="benchmark_source_cohort_members",
    )
    if not source_cohort_members_path.exists():
        raise ValueError(
            "benchmark cohort manifest references a missing source cohort members "
            f"artifact: {source_cohort_members_path}"
        )
    if _file_sha256(source_cohort_members_path) != (
        cohort_manifest.source_cohort_members_sha256
    ):
        raise ValueError(
            "benchmark cohort source cohort members sha256 does not match "
            "benchmark_cohort_manifest"
        )
    source_future_outcomes_path = _resolve_manifest_path_reference(
        path_value=cohort_manifest.source_future_outcomes_path,
        anchor_path=cohort_manifest_path,
    )
    _require_canonical_bundle_artifact_path(
        actual_path=source_future_outcomes_path,
        expected_path=benchmark_source_future_outcomes_path_for_labels_file(
            resolved_cohort_labels_file
        ),
        artifact_name="benchmark_source_future_outcomes",
    )
    if not source_future_outcomes_path.exists():
        raise ValueError(
            "benchmark cohort manifest references a missing source future "
            f"outcomes artifact: {source_future_outcomes_path}"
        )
    if _file_sha256(source_future_outcomes_path) != (
        cohort_manifest.source_future_outcomes_sha256
    ):
        raise ValueError(
            "benchmark cohort source future outcomes sha256 does not match "
            "benchmark_cohort_manifest"
        )
    auxiliary_source_artifacts: list[CohortSourceArtifact] = []
    for artifact in cohort_manifest.auxiliary_source_artifacts:
        resolved_artifact_path = _resolve_manifest_path_reference(
            path_value=artifact.artifact_path,
            anchor_path=cohort_manifest_path,
        )
        if not resolved_artifact_path.exists():
            raise ValueError(
                "benchmark cohort manifest references a missing auxiliary source "
                f"artifact {artifact.artifact_name}: {resolved_artifact_path}"
            )
        if _file_sha256(resolved_artifact_path) != artifact.artifact_sha256:
            raise ValueError(
                "benchmark cohort auxiliary source artifact sha256 does not match "
                f"benchmark_cohort_manifest for {artifact.artifact_name}"
            )
        auxiliary_source_artifacts.append(
            CohortSourceArtifact(
                artifact_name=artifact.artifact_name,
                artifact_path=str(resolved_artifact_path),
                artifact_sha256=artifact.artifact_sha256,
            )
        )
    if _manifest_uses_track_b_protocol(snapshot_manifest):
        _validate_track_b_auxiliary_source_artifacts(
            auxiliary_source_artifacts=tuple(auxiliary_source_artifacts),
            cohort_labels_file=resolved_cohort_labels_file,
        )
    cohort_members = read_benchmark_cohort_members(cohort_members_path)
    cohort_labels = read_benchmark_cohort_labels(resolved_cohort_labels_file)
    if cohort_manifest.entity_count != len(cohort_members):
        raise ValueError(
            "benchmark cohort manifest entity_count does not match the materialized "
            "benchmark_cohort_members artifact"
        )
    if cohort_manifest.label_row_count != len(cohort_labels):
        raise ValueError(
            "benchmark cohort manifest label_row_count does not match the materialized "
            "benchmark_cohort_labels artifact"
        )
    observed_label_row_count = sum(
        label.label_value == OBSERVED_LABEL_VALUE for label in cohort_labels
    )
    if cohort_manifest.observed_label_row_count != observed_label_row_count:
        raise ValueError(
            "benchmark cohort manifest observed_label_row_count does not match the "
            "materialized benchmark_cohort_labels artifact"
        )
    validate_benchmark_cohort_labels_against_manifest(
        snapshot_manifest,
        cohort_labels,
        cohort_members=cohort_members,
    )
    return MaterializedBenchmarkCohortArtifacts(
        cohort_manifest_path=cohort_manifest_path,
        cohort_members_path=cohort_members_path,
        source_cohort_members_path=source_cohort_members_path,
        source_future_outcomes_path=source_future_outcomes_path,
        cohort_manifest=cohort_manifest,
        cohort_members=cohort_members,
        cohort_labels=cohort_labels,
        auxiliary_source_artifacts=tuple(auxiliary_source_artifacts),
    )


def _track_b_casebook_paths_for_cohort_members_file(
    cohort_members_file: Path,
) -> tuple[Path, Path, Path]:
    fixture_dir = cohort_members_file.resolve().parent
    return (
        fixture_dir / TRACK_B_CASEBOOK_FILE_NAME,
        fixture_dir / TRACK_B_PROGRAM_UNIVERSE_FILE_NAME,
        fixture_dir / TRACK_B_EVENTS_FILE_NAME,
    )


def _track_b_source_artifact_paths_for_cohort_members_file(
    cohort_members_file: Path,
) -> tuple[tuple[str, Path], ...]:
    fixture_dir = cohort_members_file.resolve().parent
    return tuple(
        (artifact_name, (fixture_dir / file_name).resolve())
        for artifact_name, file_name in TRACK_B_SOURCE_ARTIFACT_SPECS
    )


def _resolve_track_b_source_artifacts(
    *,
    cohort_members_file: Path,
) -> tuple[tuple[str, Path], ...]:
    source_artifact_paths = _track_b_source_artifact_paths_for_cohort_members_file(
        cohort_members_file
    )
    missing_paths = [
        source_path.name
        for _, source_path in source_artifact_paths
        if not source_path.exists()
    ]
    if missing_paths:
        raise ValueError(
            "Track B benchmark requires source archive index and pinned source files "
            "beside cohort_members.csv, missing: "
            + ", ".join(sorted(missing_paths))
        )
    return source_artifact_paths


def _materialize_track_b_source_artifacts(
    *,
    cohort_members_file: Path,
    labels_file: Path,
) -> tuple[tuple[str, Path], ...]:
    resolved_source_artifacts = _resolve_track_b_source_artifacts(
        cohort_members_file=cohort_members_file,
    )
    source_artifact_index = {
        artifact_name: source_path
        for artifact_name, source_path in resolved_source_artifacts
    }
    source_archive_index_file = source_artifact_index["source_archive_index"]
    source_archives_dir = (
        source_archive_index_file.resolve().parent / TRACK_B_SOURCE_ARCHIVE_DIR_NAME
    )
    if not source_archives_dir.exists():
        raise ValueError(
            "Track B benchmark requires archived source fixture files under "
            "archives/ beside source_archives.json"
        )
    _copy_directory_tree_if_needed(
        source_dir=source_archives_dir,
        destination_dir=benchmark_track_b_source_archives_dir_for_labels_file(
            labels_file
        ),
    )
    materialized_artifacts: list[tuple[str, Path]] = []
    for artifact_name, _ in TRACK_B_SOURCE_ARTIFACT_SPECS:
        materialized_artifacts.append(
            (
                artifact_name,
                _copy_artifact_if_needed(
                    source_path=source_artifact_index[artifact_name],
                    destination_path=(
                        benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
                            labels_file,
                            artifact_name=artifact_name,
                        )
                    ),
                ),
            )
        )
    return tuple(materialized_artifacts)


def _build_track_b_cohort_labels(
    *,
    manifest: BenchmarkSnapshotManifest,
    cohort_members: tuple[CohortMember, ...],
    cohort_members_file: Path,
    future_outcomes: tuple[FutureOutcomeRecord, ...],
    question: object,
) -> tuple[BenchmarkCohortLabel, ...]:
    if future_outcomes:
        raise ValueError(
            "Track B benchmark does not source cohort labels from future_outcomes.csv; "
            "the file must remain empty"
        )
    casebook_path, program_universe_path, events_path = (
        _track_b_casebook_paths_for_cohort_members_file(cohort_members_file)
    )
    missing_paths = [
        path.name
        for path in (casebook_path, program_universe_path, events_path)
        if not path.exists()
    ]
    if missing_paths:
        raise ValueError(
            "Track B benchmark requires casebook and slice-local fixture files beside "
            "cohort_members.csv, missing: "
            + ", ".join(sorted(missing_paths))
        )
    cases = load_track_b_casebook(
        casebook_path,
        as_of_date=manifest.as_of_date,
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    validate_track_b_casebook_against_cohort_members(
        cases=cases,
        cohort_members=cohort_members,
    )
    case_index = {
        case.proposal_entity_id: case
        for case in cases
    }
    labels: list[BenchmarkCohortLabel] = []
    for member in sorted(
        cohort_members,
        key=lambda item: (item.entity_type, item.entity_id, item.entity_label.lower()),
    ):
        case = case_index[member.entity_id]
        for label_name in question.translational_outcome_labels:
            label_is_observed = case.gold_replay_status == label_name
            labels.append(
                BenchmarkCohortLabel(
                    cohort_id=manifest.cohort_id,
                    snapshot_id=manifest.snapshot_id,
                    entity_type=member.entity_type,
                    entity_id=member.entity_id,
                    entity_label=member.entity_label,
                    label_name=label_name,
                    label_value=(
                        OBSERVED_LABEL_VALUE
                        if label_is_observed
                        else NOT_OBSERVED_LABEL_VALUE
                    ),
                    horizon=question.evaluation_horizons[0],
                    outcome_date="",
                    label_source=(
                        "track_b_casebook"
                        if label_is_observed
                        else "not_observed_in_track_b_casebook"
                    ),
                    label_notes=(
                        f"case_id={case.case_id}; source_program_universe_id="
                        f"{case.source_program_universe_id}; gold_failure_scope="
                        f"{case.gold_failure_scope}"
                    ),
                )
            )
    return tuple(labels)


def _validate_track_b_auxiliary_source_artifacts(
    *,
    auxiliary_source_artifacts: tuple[CohortSourceArtifact, ...],
    cohort_labels_file: Path,
) -> None:
    artifact_index: dict[str, Path] = {}
    for artifact in auxiliary_source_artifacts:
        resolved_artifact_path = Path(artifact.artifact_path).resolve()
        expected_path = benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
            cohort_labels_file,
            artifact_name=artifact.artifact_name,
        )
        _require_canonical_bundle_artifact_path(
            actual_path=resolved_artifact_path,
            expected_path=expected_path,
            artifact_name=artifact.artifact_name,
        )
        artifact_index[artifact.artifact_name] = resolved_artifact_path
    missing_artifact_names = sorted(
        set(TRACK_B_SOURCE_ARTIFACT_FILE_NAMES).difference(artifact_index)
    )
    if missing_artifact_names:
        raise ValueError(
            "Track B benchmark cohort manifest must capture every pinned Track B "
            "source artifact, missing: "
            + ", ".join(missing_artifact_names)
        )
    load_source_archive_descriptors(artifact_index["source_archive_index"])


def build_benchmark_cohort_labels(
    manifest: BenchmarkSnapshotManifest,
    cohort_members: tuple[CohortMember, ...],
    future_outcomes: tuple[FutureOutcomeRecord, ...],
    *,
    task_registry_path: Path | None = None,
    cohort_members_file: Path | None = None,
) -> tuple[BenchmarkCohortLabel, ...]:
    if _manifest_uses_track_b_protocol(manifest):
        if cohort_members_file is None:
            raise ValueError(
                "Track B cohort label materialization requires cohort_members_file"
            )
        return _build_track_b_cohort_labels(
            manifest=manifest,
            cohort_members=cohort_members,
            cohort_members_file=cohort_members_file,
            future_outcomes=future_outcomes,
            question=TRACK_B_BENCHMARK_PROTOCOL.question,
        )
    question = _resolve_task_contract_for_manifest(
        manifest,
        task_registry_path=task_registry_path,
    ).protocol.question
    as_of_date = _parse_iso_date(manifest.as_of_date, "as_of_date")
    outcome_closed_at = _parse_iso_date(
        manifest.outcome_observation_closed_at,
        "outcome_observation_closed_at",
    )
    snapshot_entity_types = set(manifest.entity_types)
    member_keys = {
        (member.entity_type, member.entity_id)
        for member in cohort_members
    }
    horizon_cutoffs = {
        horizon: min(
            _add_years(as_of_date, _parse_horizon_years(horizon)),
            outcome_closed_at,
        )
        for horizon in question.evaluation_horizons
    }
    grouped_outcomes: dict[tuple[str, str], list[FutureOutcomeRecord]] = {}
    for outcome in future_outcomes:
        outcome_date = _parse_iso_date(outcome.outcome_date, "outcome_date")
        if outcome.entity_type not in snapshot_entity_types:
            raise ValueError(
                f"future outcome {outcome.entity_type}/{outcome.entity_id} is outside the snapshot entity_types"
            )
        if (outcome.entity_type, outcome.entity_id) not in member_keys:
            raise ValueError(
                f"future outcome {outcome.entity_type}/{outcome.entity_id} does not match any cohort member"
            )
        if outcome_date <= as_of_date:
            raise ValueError(
                f"future outcome {outcome.entity_type}/{outcome.entity_id} must be after as_of_date"
            )
        if outcome_date > outcome_closed_at:
            raise ValueError(
                "future outcome "
                f"{outcome.entity_type}/{outcome.entity_id} exceeds outcome_observation_closed_at"
            )
        grouped_outcomes.setdefault((outcome.entity_type, outcome.entity_id), []).append(outcome)

    labels: list[BenchmarkCohortLabel] = []
    for member in sorted(
        cohort_members,
        key=lambda item: (item.entity_type, item.entity_id, item.entity_label.lower()),
    ):
        if member.entity_type not in snapshot_entity_types:
            raise ValueError(
                f"cohort member {member.entity_type}/{member.entity_id} is outside the snapshot entity_types"
            )
        member_outcomes = sorted(
            grouped_outcomes.get((member.entity_type, member.entity_id), []),
            key=lambda item: (item.outcome_date, item.outcome_label, item.label_source),
        )
        for horizon in question.evaluation_horizons:
            horizon_cutoff = horizon_cutoffs[horizon]
            qualifying_outcomes = [
                outcome
                for outcome in member_outcomes
                if as_of_date
                < _parse_iso_date(outcome.outcome_date, "outcome_date")
                <= horizon_cutoff
            ]
            outcomes_by_label: dict[str, list[FutureOutcomeRecord]] = {}
            for outcome in qualifying_outcomes:
                outcomes_by_label.setdefault(outcome.outcome_label, []).append(outcome)

            for label_name in question.translational_outcome_labels:
                if label_name == NO_OUTCOME_LABEL:
                    label_is_observed = not qualifying_outcomes
                    outcome_date = ""
                    label_source = (
                        "benchmark_label_builder"
                        if label_is_observed
                        else "qualifying_outcome_observed"
                    )
                    label_notes = (
                        f"no qualifying future outcome observed through {horizon_cutoff.isoformat()}"
                        if label_is_observed
                        else ""
                    )
                else:
                    matched_outcomes = outcomes_by_label.get(label_name, [])
                    label_is_observed = bool(matched_outcomes)
                    outcome_date = (
                        matched_outcomes[0].outcome_date
                        if matched_outcomes
                        else ""
                    )
                    label_source = (
                        "; ".join(
                            sorted({outcome.label_source for outcome in matched_outcomes})
                        )
                        if matched_outcomes
                        else "not_observed_within_horizon"
                    )
                    observed_dates = ",".join(
                        outcome.outcome_date for outcome in matched_outcomes
                    )
                    observed_notes = "; ".join(
                        note
                        for note in sorted(
                            {
                                outcome.label_notes
                                for outcome in matched_outcomes
                                if outcome.label_notes
                            }
                        )
                    )
                    label_notes = ""
                    if observed_dates:
                        label_notes = f"observed_dates={observed_dates}"
                    if observed_notes:
                        label_notes = (
                            f"{label_notes}; {observed_notes}"
                            if label_notes
                            else observed_notes
                        )
                labels.append(
                    BenchmarkCohortLabel(
                        cohort_id=manifest.cohort_id,
                        snapshot_id=manifest.snapshot_id,
                        entity_type=member.entity_type,
                        entity_id=member.entity_id,
                        entity_label=member.entity_label,
                        label_name=label_name,
                        label_value=(
                            OBSERVED_LABEL_VALUE
                            if label_is_observed
                            else NOT_OBSERVED_LABEL_VALUE
                        ),
                        horizon=horizon,
                        outcome_date=outcome_date,
                        label_source=label_source,
                        label_notes=label_notes,
                    )
                )
    return tuple(labels)


def materialize_benchmark_cohort_labels(
    *,
    manifest: BenchmarkSnapshotManifest,
    manifest_file: Path,
    cohort_members_file: Path,
    future_outcomes_file: Path,
    output_file: Path,
    task_registry_path: Path | None = None,
) -> dict[str, object]:
    resolved_manifest_file = _resolve_required_path(manifest_file, "manifest_file")
    _validate_snapshot_manifest_file_matches_manifest(
        snapshot_manifest=manifest,
        snapshot_manifest_file=resolved_manifest_file,
    )
    resolved_cohort_members_file = _resolve_required_path(
        cohort_members_file,
        "cohort_members_file",
    )
    resolved_future_outcomes_file = _resolve_required_path(
        future_outcomes_file,
        "future_outcomes_file",
    )
    resolved_output_file = _resolve_required_path(output_file, "output_file")
    cohort_members = load_cohort_members(resolved_cohort_members_file)
    validate_benchmark_cohort_members_against_manifest(manifest, cohort_members)
    labels = build_benchmark_cohort_labels(
        manifest,
        cohort_members,
        load_future_outcomes(resolved_future_outcomes_file),
        task_registry_path=task_registry_path,
        cohort_members_file=resolved_cohort_members_file,
    )
    write_benchmark_cohort_labels(resolved_output_file, labels)
    cohort_members_output_file = benchmark_cohort_members_path_for_labels_file(
        resolved_output_file
    )
    write_benchmark_cohort_members(cohort_members_output_file, cohort_members)
    source_cohort_members_output_file = _copy_artifact_if_needed(
        source_path=resolved_cohort_members_file,
        destination_path=benchmark_source_cohort_members_path_for_labels_file(
            resolved_output_file
        ),
    )
    source_future_outcomes_output_file = _copy_artifact_if_needed(
        source_path=resolved_future_outcomes_file,
        destination_path=benchmark_source_future_outcomes_path_for_labels_file(
            resolved_output_file
        ),
    )
    auxiliary_source_artifact_files: tuple[tuple[str, Path], ...] = ()
    if _manifest_uses_track_b_protocol(manifest):
        auxiliary_source_artifact_files = _materialize_track_b_source_artifacts(
            cohort_members_file=resolved_cohort_members_file,
            labels_file=resolved_output_file,
        )
    cohort_manifest_output_file = benchmark_cohort_manifest_path_for_labels_file(
        resolved_output_file
    )
    cohort_manifest = build_benchmark_cohort_manifest(
        snapshot_manifest=manifest,
        snapshot_manifest_file=resolved_manifest_file,
        cohort_manifest_artifact_file=cohort_manifest_output_file,
        cohort_members=cohort_members,
        cohort_labels=labels,
        cohort_members_artifact_file=cohort_members_output_file,
        cohort_labels_artifact_file=resolved_output_file,
        source_cohort_members_file=source_cohort_members_output_file,
        source_future_outcomes_file=source_future_outcomes_output_file,
        auxiliary_source_artifact_files=auxiliary_source_artifact_files,
    )
    write_benchmark_cohort_manifest(cohort_manifest_output_file, cohort_manifest)
    observed_label_rows = sum(
        label.label_value == OBSERVED_LABEL_VALUE for label in labels
    )
    return {
        "benchmark_suite_id": manifest.benchmark_suite_id,
        "benchmark_task_id": manifest.benchmark_task_id,
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_file": str(resolved_output_file),
        "benchmark_cohort_members_file": str(cohort_members_output_file),
        "benchmark_cohort_manifest_file": str(cohort_manifest_output_file),
        "source_cohort_members_file": str(source_cohort_members_output_file),
        "source_future_outcomes_file": str(source_future_outcomes_output_file),
        "entity_count": len(cohort_members),
        "row_count": len(labels),
        "observed_label_rows": observed_label_rows,
    }
