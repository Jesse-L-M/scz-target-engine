from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from scz_target_engine.benchmark_intervention_objects import (
    INTERVENTION_OBJECT_BUNDLE_FILE_NAME,
    INTERVENTION_OBJECT_ENTITY_TYPE,
    PROGRAM_HISTORY_EVENTS_PATH,
    PROGRAM_UNIVERSE_PATH,
    materialize_intervention_object_feature_bundle,
)
from scz_target_engine.benchmark_protocol import (
    FROZEN_BASELINE_IDS,
    BenchmarkSnapshotManifest,
    SourceCutoffRule,
    SourceSnapshot,
    VALID_ENTITY_TYPES,
    resolve_benchmark_question,
)
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.io import read_json, write_json
from scz_target_engine.json_contract import (
    require_json_list,
    require_json_mapping,
    require_json_text,
    require_optional_json_string,
    require_optional_json_text,
)


SNAPSHOT_MANIFEST_SCHEMA_NAME = "benchmark_snapshot_manifest"
SNAPSHOT_MANIFEST_SCHEMA_VERSION = "v1"


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _resolve_optional_path(
    value: str | None,
    *,
    base_dir: Path | None = None,
) -> str:
    if value in {None, ""}:
        return ""
    path = Path(str(value))
    if not path.is_absolute():
        if base_dir is None:
            path = path.resolve()
        else:
            path = (base_dir / path).resolve()
    return str(path)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class SnapshotBuildRequest:
    snapshot_id: str
    cohort_id: str
    as_of_date: str
    outcome_observation_closed_at: str
    entity_types: tuple[str, ...]
    baseline_ids: tuple[str, ...]
    notes: str = ""
    benchmark_question_id: str = "scz_translational_ranking_v1"
    benchmark_suite_id: str = ""
    benchmark_task_id: str = ""
    task_registry_path: str = ""
    program_universe_file: str = ""
    program_history_events_file: str = ""

    def __post_init__(self) -> None:
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.cohort_id, "cohort_id")
        _require_text(self.benchmark_question_id, "benchmark_question_id")
        resolve_benchmark_question(self.benchmark_question_id)
        if not self.entity_types:
            raise ValueError("entity_types must contain at least one value")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError("entity_types must only contain supported benchmark entity types")
        if not self.baseline_ids:
            raise ValueError("baseline_ids must contain at least one benchmark baseline")
        if len(self.baseline_ids) != len(set(self.baseline_ids)):
            raise ValueError("baseline_ids must not repeat baseline_id")
        unknown_baseline_ids = sorted(
            set(self.baseline_ids).difference(FROZEN_BASELINE_IDS)
        )
        if unknown_baseline_ids:
            raise ValueError(
                "baseline_ids must only contain supported benchmark baselines: "
                + ", ".join(unknown_baseline_ids)
            )
        task_contract = resolve_benchmark_task_contract(
            benchmark_task_id=self.benchmark_task_id or None,
            benchmark_question_id=self.benchmark_question_id,
            benchmark_suite_id=self.benchmark_suite_id or None,
            entity_types=self.entity_types,
            baseline_ids=self.baseline_ids,
            task_registry_path=(
                Path(self.task_registry_path).resolve()
                if self.task_registry_path
                else None
            ),
        )
        if self.benchmark_question_id != task_contract.benchmark_question_id:
            raise ValueError(
                "benchmark_question_id must match the resolved benchmark task contract"
            )
        as_of_date = _parse_iso_date(self.as_of_date, "as_of_date")
        outcome_closed_at = _parse_iso_date(
            self.outcome_observation_closed_at,
            "outcome_observation_closed_at",
        )
        if outcome_closed_at < as_of_date:
            raise ValueError(
                "outcome_observation_closed_at must be on or after as_of_date"
            )
        unsupported_entity_types = sorted(
            set(self.entity_types).difference(task_contract.entity_types)
        )
        if unsupported_entity_types:
            raise ValueError(
                "entity_types must be supported by the benchmark task contract: "
                + ", ".join(unsupported_entity_types)
            )
        unsupported_baselines = sorted(
            set(self.baseline_ids).difference(task_contract.supported_baseline_ids)
        )
        if unsupported_baselines:
            raise ValueError(
                "baseline_ids must be supported by the benchmark task contract: "
                + ", ".join(unsupported_baselines)
            )
        if bool(self.program_universe_file) != bool(self.program_history_events_file):
            raise ValueError(
                "program_universe_file and program_history_events_file must be provided together"
            )
        if (
            INTERVENTION_OBJECT_ENTITY_TYPE in self.entity_types
            and not self.program_universe_file
        ):
            raise ValueError(
                "intervention_object snapshot requests must provide "
                "program_universe_file and program_history_events_file"
            )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "benchmark_question_id": self.benchmark_question_id,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "entity_types": list(self.entity_types),
            "baseline_ids": list(self.baseline_ids),
            "notes": self.notes,
        }
        if self.benchmark_suite_id:
            payload["benchmark_suite_id"] = self.benchmark_suite_id
        if self.benchmark_task_id:
            payload["benchmark_task_id"] = self.benchmark_task_id
        if self.task_registry_path:
            payload["task_registry_path"] = self.task_registry_path
        if self.program_universe_file:
            payload["program_universe_file"] = self.program_universe_file
        if self.program_history_events_file:
            payload["program_history_events_file"] = self.program_history_events_file
        return payload

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        task_registry_path: Path | None = None,
        base_dir: Path | None = None,
    ) -> SnapshotBuildRequest:
        mapping = require_json_mapping(payload, "snapshot build request")
        return cls(
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
            baseline_ids=tuple(
                require_json_text(item, "baseline_ids[]")
                for item in require_json_list(mapping.get("baseline_ids"), "baseline_ids")
            ),
            notes=require_optional_json_string(mapping.get("notes"), "notes"),
            benchmark_suite_id=require_optional_json_string(
                mapping.get("benchmark_suite_id"),
                "benchmark_suite_id",
            ),
            benchmark_task_id=require_optional_json_string(
                mapping.get("benchmark_task_id"),
                "benchmark_task_id",
            ),
            task_registry_path=(
                str(task_registry_path.resolve())
                if task_registry_path is not None
                else _resolve_optional_path(
                    require_optional_json_text(
                        mapping.get("task_registry_path"),
                        "task_registry_path",
                    ),
                    base_dir=base_dir,
                )
            ),
            program_universe_file=_resolve_optional_path(
                require_optional_json_text(
                    mapping.get("program_universe_file"),
                    "program_universe_file",
                ),
                base_dir=base_dir,
            ),
            program_history_events_file=_resolve_optional_path(
                require_optional_json_text(
                    mapping.get("program_history_events_file"),
                    "program_history_events_file",
                ),
                base_dir=base_dir,
            ),
        )


@dataclass(frozen=True)
class SourceArchiveDescriptor:
    source_name: str
    source_version: str
    archive_file: str
    archive_format: str
    allowed_data_through: str
    evidence_frozen_at: str
    sha256: str
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.source_name, "source_name")
        _require_text(self.source_version, "source_version")
        _require_text(self.archive_file, "archive_file")
        _require_text(self.archive_format, "archive_format")
        _parse_iso_date(self.allowed_data_through, "allowed_data_through")
        _parse_iso_date(self.evidence_frozen_at, "evidence_frozen_at")
        if len(self.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256.lower()
        ):
            raise ValueError("sha256 must be a lowercase hexadecimal SHA256 digest")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "source_version": self.source_version,
            "archive_file": self.archive_file,
            "archive_format": self.archive_format,
            "allowed_data_through": self.allowed_data_through,
            "evidence_frozen_at": self.evidence_frozen_at,
            "sha256": self.sha256,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        base_dir: Path,
    ) -> SourceArchiveDescriptor:
        mapping = require_json_mapping(payload, "source archive descriptor")
        archive_file = Path(
            require_json_text(mapping.get("archive_file"), "archive_file")
        )
        if not archive_file.is_absolute():
            archive_file = (base_dir / archive_file).resolve()
        return cls(
            source_name=require_json_text(mapping.get("source_name"), "source_name"),
            source_version=require_json_text(
                mapping.get("source_version"),
                "source_version",
            ),
            archive_file=str(archive_file),
            archive_format=require_json_text(
                mapping.get("archive_format"),
                "archive_format",
            ),
            allowed_data_through=require_json_text(
                mapping.get("allowed_data_through"),
                "allowed_data_through",
            ),
            evidence_frozen_at=require_json_text(
                mapping.get("evidence_frozen_at"),
                "evidence_frozen_at",
            ),
            sha256=require_json_text(mapping.get("sha256"), "sha256"),
            notes=require_optional_json_string(mapping.get("notes"), "notes"),
        )


def load_snapshot_build_request(
    path: Path,
    *,
    task_registry_path: Path | None = None,
) -> SnapshotBuildRequest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("snapshot build request must be a JSON object")
    return SnapshotBuildRequest.from_dict(
        payload,
        task_registry_path=task_registry_path,
        base_dir=path.parent,
    )


def load_source_archive_descriptors(path: Path) -> tuple[SourceArchiveDescriptor, ...]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("source archive index must be a JSON object")
    archive_payloads = payload.get("archives")
    if not isinstance(archive_payloads, list):
        raise ValueError("source archive index must define an archives list")
    descriptors = tuple(
        SourceArchiveDescriptor.from_dict(item, base_dir=path.parent)
        for item in archive_payloads
    )
    source_versions = {
        (descriptor.source_name, descriptor.source_version)
        for descriptor in descriptors
    }
    if len(descriptors) != len(source_versions):
        raise ValueError("source archive index must not repeat source_name/source_version")
    return descriptors


def write_benchmark_snapshot_manifest(
    path: Path,
    manifest: BenchmarkSnapshotManifest,
) -> None:
    write_json(path, manifest.to_dict())


def read_benchmark_snapshot_manifest(
    path: Path,
    *,
    task_registry_path: Path | None = None,
) -> BenchmarkSnapshotManifest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("benchmark snapshot manifest must be a JSON object")
    return BenchmarkSnapshotManifest.from_dict(
        payload,
        task_registry_path=task_registry_path,
    )


def _build_excluded_source_snapshot(
    *,
    source_rule: SourceCutoffRule,
    source_version: str,
    allowed_data_through: str,
    materialized_at: str,
    exclusion_reason: str,
) -> SourceSnapshot:
    return SourceSnapshot(
        source_name=source_rule.source_name,
        source_version=source_version,
        cutoff_mode=source_rule.cutoff_mode,
        allowed_data_through=allowed_data_through,
        evidence_frozen_at=None,
        materialized_at=materialized_at,
        evidence_timestamp_field=source_rule.evidence_timestamp_field,
        missing_date_policy=source_rule.missing_date_policy,
        future_record_policy=source_rule.future_record_policy,
        included=False,
        exclusion_reason=exclusion_reason,
    )


def _resolve_source_snapshot(
    *,
    source_rule: SourceCutoffRule,
    as_of_date: str,
    materialized_at: str,
    descriptors: tuple[SourceArchiveDescriptor, ...],
) -> SourceSnapshot:
    cutoff_date = _parse_iso_date(as_of_date, "as_of_date")
    sorted_descriptors = sorted(
        descriptors,
        key=lambda descriptor: (
            _parse_iso_date(descriptor.allowed_data_through, "allowed_data_through"),
            _parse_iso_date(descriptor.evidence_frozen_at, "evidence_frozen_at"),
            descriptor.source_version,
        ),
        reverse=True,
    )
    eligible = [
        descriptor
        for descriptor in sorted_descriptors
        if _parse_iso_date(descriptor.allowed_data_through, "allowed_data_through")
        <= cutoff_date
        and _parse_iso_date(descriptor.evidence_frozen_at, "evidence_frozen_at")
        <= cutoff_date
    ]
    if eligible:
        newest_allowed_data_through = eligible[0].allowed_data_through
        newest_evidence_frozen_at = eligible[0].evidence_frozen_at
        ambiguous_descriptors = [
            descriptor
            for descriptor in eligible
            if descriptor.allowed_data_through == newest_allowed_data_through
            and descriptor.evidence_frozen_at == newest_evidence_frozen_at
        ]
        if len(ambiguous_descriptors) > 1:
            versions = ", ".join(
                descriptor.source_version for descriptor in ambiguous_descriptors
            )
            raise ValueError(
                f"{source_rule.source_name} has multiple eligible archive descriptors for "
                f"{newest_allowed_data_through}/{newest_evidence_frozen_at}: {versions}"
            )

    validation_failures: list[str] = []
    for descriptor in eligible:
        archive_path = Path(descriptor.archive_file)
        if not archive_path.exists():
            validation_failures.append(f"archive file missing: {archive_path}")
            continue
        actual_sha256 = _file_sha256(archive_path)
        if actual_sha256 != descriptor.sha256:
            validation_failures.append(
                f"archive checksum mismatch for {archive_path.name}"
            )
            continue
        return SourceSnapshot(
            source_name=source_rule.source_name,
            source_version=descriptor.source_version,
            cutoff_mode=source_rule.cutoff_mode,
            allowed_data_through=descriptor.allowed_data_through,
            evidence_frozen_at=descriptor.evidence_frozen_at,
            materialized_at=materialized_at,
            evidence_timestamp_field=source_rule.evidence_timestamp_field,
            missing_date_policy=source_rule.missing_date_policy,
            future_record_policy=source_rule.future_record_policy,
            included=True,
        )

    if not sorted_descriptors:
        return _build_excluded_source_snapshot(
            source_rule=source_rule,
            source_version="unavailable",
            allowed_data_through=as_of_date,
            materialized_at=materialized_at,
            exclusion_reason=(
                f"no archived release descriptor available on or before {as_of_date}"
            ),
        )

    candidate = eligible[0] if eligible else sorted_descriptors[0]
    if eligible and validation_failures:
        exclusion_reason = (
            "no valid pre-cutoff archive passed validation: "
            f"{validation_failures[0]}"
        )
    else:
        exclusion_reason = (
            f"latest archived release {candidate.allowed_data_through} is after "
            f"requested cutoff {as_of_date}"
        )

    return _build_excluded_source_snapshot(
        source_rule=source_rule,
        source_version=candidate.source_version,
        allowed_data_through=(
            candidate.allowed_data_through if eligible else as_of_date
        ),
        materialized_at=materialized_at,
        exclusion_reason=exclusion_reason,
    )


def build_benchmark_snapshot_manifest(
    request: SnapshotBuildRequest,
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    *,
    materialized_at: str,
    task_registry_path: Path | None = None,
) -> BenchmarkSnapshotManifest:
    _parse_iso_date(materialized_at, "materialized_at")
    effective_task_registry_path = task_registry_path
    if effective_task_registry_path is None and request.task_registry_path:
        effective_task_registry_path = Path(request.task_registry_path).resolve()
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=request.benchmark_task_id or None,
        benchmark_question_id=request.benchmark_question_id,
        benchmark_suite_id=request.benchmark_suite_id or None,
        entity_types=request.entity_types,
        baseline_ids=request.baseline_ids,
        task_registry_path=effective_task_registry_path,
    )
    protocol = task_contract.protocol
    descriptors_by_source: dict[str, tuple[SourceArchiveDescriptor, ...]] = {
        source_rule.source_name: tuple(
            descriptor
            for descriptor in archive_descriptors
            if descriptor.source_name == source_rule.source_name
        )
        for source_rule in protocol.source_cutoff_rules
    }
    unknown_descriptor_sources = sorted(
        {
            descriptor.source_name
            for descriptor in archive_descriptors
            if descriptor.source_name not in descriptors_by_source
        }
    )
    if unknown_descriptor_sources:
        raise ValueError(
            "source archive index included unknown sources: "
            + ", ".join(unknown_descriptor_sources)
        )

    source_snapshots = tuple(
        _resolve_source_snapshot(
            source_rule=source_rule,
            as_of_date=request.as_of_date,
            materialized_at=materialized_at,
            descriptors=descriptors_by_source[source_rule.source_name],
        )
        for source_rule in protocol.source_cutoff_rules
    )
    return BenchmarkSnapshotManifest(
        schema_name=SNAPSHOT_MANIFEST_SCHEMA_NAME,
        schema_version=SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        snapshot_id=request.snapshot_id,
        cohort_id=request.cohort_id,
        benchmark_suite_id=task_contract.suite_id,
        benchmark_task_id=task_contract.task_id,
        benchmark_question_id=request.benchmark_question_id,
        as_of_date=request.as_of_date,
        outcome_observation_closed_at=request.outcome_observation_closed_at,
        entity_types=request.entity_types,
        source_snapshots=source_snapshots,
        leakage_controls=protocol.leakage_controls,
        baseline_ids=request.baseline_ids,
        notes=request.notes,
        task_registry_path=(
            str(effective_task_registry_path.resolve())
            if effective_task_registry_path is not None
            else request.task_registry_path
        ),
    )


def materialize_benchmark_snapshot_manifest(
    *,
    request_file: Path,
    archive_index_file: Path,
    output_file: Path,
    materialized_at: str,
    task_registry_path: Path | None = None,
) -> dict[str, object]:
    request = load_snapshot_build_request(
        request_file,
        task_registry_path=task_registry_path,
    )
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=request.benchmark_task_id or None,
        benchmark_question_id=request.benchmark_question_id,
        benchmark_suite_id=request.benchmark_suite_id or None,
        entity_types=request.entity_types,
        baseline_ids=request.baseline_ids,
        task_registry_path=(
            task_registry_path
            if task_registry_path is not None
            else (
                Path(request.task_registry_path).resolve()
                if request.task_registry_path
                else None
            )
        ),
    )
    task_contract.fixture_paths.validate_archive_index_sibling_files(
        archive_index_file.resolve()
    )
    archive_descriptors = load_source_archive_descriptors(archive_index_file)
    manifest = build_benchmark_snapshot_manifest(
        request,
        archive_descriptors,
        materialized_at=materialized_at,
        task_registry_path=task_registry_path,
    )
    write_benchmark_snapshot_manifest(output_file, manifest)
    bundle_output_file: Path | None = None
    if INTERVENTION_OBJECT_ENTITY_TYPE in request.entity_types:
        bundle_output_file = output_file.parent / INTERVENTION_OBJECT_BUNDLE_FILE_NAME
        materialize_intervention_object_feature_bundle(
            output_file=bundle_output_file,
            as_of_date=request.as_of_date,
            source_snapshots=manifest.source_snapshots,
            archive_descriptors=archive_descriptors,
            program_universe_path=(
                Path(request.program_universe_file).resolve()
                if request.program_universe_file
                else PROGRAM_UNIVERSE_PATH
            ),
            events_path=(
                Path(request.program_history_events_file).resolve()
                if request.program_history_events_file
                else PROGRAM_HISTORY_EVENTS_PATH
            ),
        )
    included_sources = [
        source_snapshot.source_name
        for source_snapshot in manifest.source_snapshots
        if source_snapshot.included
    ]
    excluded_sources = [
        {
            "source_name": source_snapshot.source_name,
            "source_version": source_snapshot.source_version,
            "exclusion_reason": source_snapshot.exclusion_reason,
        }
        for source_snapshot in manifest.source_snapshots
        if not source_snapshot.included
    ]
    return {
        "benchmark_suite_id": manifest.benchmark_suite_id,
        "benchmark_task_id": manifest.benchmark_task_id,
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_file": str(output_file),
        "intervention_object_feature_bundle": (
            str(bundle_output_file) if bundle_output_file is not None else ""
        ),
        "included_sources": included_sources,
        "excluded_sources": excluded_sources,
    }
