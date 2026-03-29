from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from scz_target_engine.benchmark_protocol import (
    BENCHMARK_QUESTION_V1,
    BenchmarkSnapshotManifest,
    LeakageControls,
    SOURCE_CUTOFF_RULES_V1,
    SourceSnapshot,
    VALID_ENTITY_TYPES,
)
from scz_target_engine.io import read_json, write_json


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
    benchmark_question_id: str = BENCHMARK_QUESTION_V1.question_id

    def __post_init__(self) -> None:
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.cohort_id, "cohort_id")
        if self.benchmark_question_id != BENCHMARK_QUESTION_V1.question_id:
            raise ValueError(
                "benchmark_question_id must match the frozen benchmark question id "
                f"{BENCHMARK_QUESTION_V1.question_id}"
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
        if not self.entity_types:
            raise ValueError("entity_types must contain at least one value")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError("entity_types must only contain supported benchmark entity types")
        if not self.baseline_ids:
            raise ValueError("baseline_ids must contain at least one benchmark baseline")
        if len(self.baseline_ids) != len(set(self.baseline_ids)):
            raise ValueError("baseline_ids must not repeat baseline_id")

    def to_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "benchmark_question_id": self.benchmark_question_id,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "entity_types": list(self.entity_types),
            "baseline_ids": list(self.baseline_ids),
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SnapshotBuildRequest:
        return cls(
            snapshot_id=str(payload["snapshot_id"]),
            cohort_id=str(payload["cohort_id"]),
            benchmark_question_id=str(payload["benchmark_question_id"]),
            as_of_date=str(payload["as_of_date"]),
            outcome_observation_closed_at=str(payload["outcome_observation_closed_at"]),
            entity_types=tuple(str(item) for item in payload["entity_types"]),
            baseline_ids=tuple(str(item) for item in payload["baseline_ids"]),
            notes=str(payload.get("notes", "")),
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
        archive_file = Path(str(payload["archive_file"]))
        if not archive_file.is_absolute():
            archive_file = (base_dir / archive_file).resolve()
        return cls(
            source_name=str(payload["source_name"]),
            source_version=str(payload["source_version"]),
            archive_file=str(archive_file),
            archive_format=str(payload["archive_format"]),
            allowed_data_through=str(payload["allowed_data_through"]),
            evidence_frozen_at=str(payload["evidence_frozen_at"]),
            sha256=str(payload["sha256"]),
            notes=str(payload.get("notes", "")),
        )


def load_snapshot_build_request(path: Path) -> SnapshotBuildRequest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("snapshot build request must be a JSON object")
    return SnapshotBuildRequest.from_dict(payload)


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


def read_benchmark_snapshot_manifest(path: Path) -> BenchmarkSnapshotManifest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("benchmark snapshot manifest must be a JSON object")
    return BenchmarkSnapshotManifest.from_dict(payload)


def _build_excluded_source_snapshot(
    *,
    source_name: str,
    source_version: str,
    allowed_data_through: str,
    materialized_at: str,
    exclusion_reason: str,
) -> SourceSnapshot:
    return SourceSnapshot(
        source_name=source_name,
        source_version=source_version,
        cutoff_mode=next(
            rule.cutoff_mode
            for rule in SOURCE_CUTOFF_RULES_V1
            if rule.source_name == source_name
        ),
        allowed_data_through=allowed_data_through,
        evidence_frozen_at=None,
        materialized_at=materialized_at,
        evidence_timestamp_field=None,
        missing_date_policy=next(
            rule.missing_date_policy
            for rule in SOURCE_CUTOFF_RULES_V1
            if rule.source_name == source_name
        ),
        future_record_policy=next(
            rule.future_record_policy
            for rule in SOURCE_CUTOFF_RULES_V1
            if rule.source_name == source_name
        ),
        included=False,
        exclusion_reason=exclusion_reason,
    )


def _resolve_source_snapshot(
    *,
    source_name: str,
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
                f"{source_name} has multiple eligible archive descriptors for "
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
            source_name=source_name,
            source_version=descriptor.source_version,
            cutoff_mode=next(
                rule.cutoff_mode
                for rule in SOURCE_CUTOFF_RULES_V1
                if rule.source_name == source_name
            ),
            allowed_data_through=descriptor.allowed_data_through,
            evidence_frozen_at=descriptor.evidence_frozen_at,
            materialized_at=materialized_at,
            evidence_timestamp_field=None,
            missing_date_policy=next(
                rule.missing_date_policy
                for rule in SOURCE_CUTOFF_RULES_V1
                if rule.source_name == source_name
            ),
            future_record_policy=next(
                rule.future_record_policy
                for rule in SOURCE_CUTOFF_RULES_V1
                if rule.source_name == source_name
            ),
            included=True,
        )

    if not sorted_descriptors:
        return _build_excluded_source_snapshot(
            source_name=source_name,
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
        source_name=source_name,
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
) -> BenchmarkSnapshotManifest:
    _parse_iso_date(materialized_at, "materialized_at")
    descriptors_by_source: dict[str, tuple[SourceArchiveDescriptor, ...]] = {
        rule.source_name: tuple(
            descriptor
            for descriptor in archive_descriptors
            if descriptor.source_name == rule.source_name
        )
        for rule in SOURCE_CUTOFF_RULES_V1
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
            source_name=rule.source_name,
            as_of_date=request.as_of_date,
            materialized_at=materialized_at,
            descriptors=descriptors_by_source[rule.source_name],
        )
        for rule in SOURCE_CUTOFF_RULES_V1
    )
    return BenchmarkSnapshotManifest(
        schema_name=SNAPSHOT_MANIFEST_SCHEMA_NAME,
        schema_version=SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        snapshot_id=request.snapshot_id,
        cohort_id=request.cohort_id,
        benchmark_question_id=request.benchmark_question_id,
        as_of_date=request.as_of_date,
        outcome_observation_closed_at=request.outcome_observation_closed_at,
        entity_types=request.entity_types,
        source_snapshots=source_snapshots,
        leakage_controls=LeakageControls(),
        baseline_ids=request.baseline_ids,
        notes=request.notes,
    )


def materialize_benchmark_snapshot_manifest(
    *,
    request_file: Path,
    archive_index_file: Path,
    output_file: Path,
    materialized_at: str,
) -> dict[str, object]:
    manifest = build_benchmark_snapshot_manifest(
        load_snapshot_build_request(request_file),
        load_source_archive_descriptors(archive_index_file),
        materialized_at=materialized_at,
    )
    write_benchmark_snapshot_manifest(output_file, manifest)
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
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_file": str(output_file),
        "included_sources": included_sources,
        "excluded_sources": excluded_sources,
    }
