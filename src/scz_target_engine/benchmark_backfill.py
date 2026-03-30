from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import shutil

from scz_target_engine.benchmark_labels import (
    build_benchmark_cohort_labels,
    load_cohort_members,
    load_future_outcomes,
)
from scz_target_engine.benchmark_registry import (
    BenchmarkTaskContract,
    resolve_benchmark_task_contract,
)
from scz_target_engine.benchmark_snapshots import (
    SourceArchiveDescriptor,
    SnapshotBuildRequest,
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
)
from scz_target_engine.io import write_json


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PUBLIC_SLICE_OUTPUT_DIR = (
    REPO_ROOT / "data" / "benchmark" / "public_slices"
)
DEFAULT_PUBLIC_SLICE_TASK_ID = "scz_translational_task"
PUBLIC_SLICE_CATALOG_FILE_NAME = "catalog.json"


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _date_to_slug(value: str) -> str:
    return value.replace("-", "_")


def _task_slice_prefix(task_id: str) -> str:
    normalized = _require_text(task_id, "task_id")
    if normalized.endswith("_task"):
        return normalized[:-5]
    return normalized


def _repo_relative_path(path: Path) -> str:
    resolved_path = path.resolve()
    try:
        return str(resolved_path.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved_path)


@dataclass(frozen=True)
class PublicSliceSourceStatus:
    source_name: str
    source_version: str
    included: bool
    allowed_data_through: str
    evidence_frozen_at: str = ""
    exclusion_reason: str = ""

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "source_name": self.source_name,
            "source_version": self.source_version,
            "included": self.included,
            "allowed_data_through": self.allowed_data_through,
        }
        if self.evidence_frozen_at:
            payload["evidence_frozen_at"] = self.evidence_frozen_at
        if self.exclusion_reason:
            payload["exclusion_reason"] = self.exclusion_reason
        return payload


@dataclass(frozen=True)
class PublicBenchmarkSliceSpec:
    slice_id: str
    benchmark_suite_id: str
    benchmark_task_id: str
    as_of_date: str
    snapshot_request: SnapshotBuildRequest
    source_fixture_dir: Path
    archive_descriptors: tuple[SourceArchiveDescriptor, ...]
    source_statuses: tuple[PublicSliceSourceStatus, ...]
    notes: str = ""

    @property
    def included_sources(self) -> tuple[str, ...]:
        return tuple(
            source_status.source_name
            for source_status in self.source_statuses
            if source_status.included
        )

    @property
    def excluded_sources(self) -> tuple[PublicSliceSourceStatus, ...]:
        return tuple(
            source_status
            for source_status in self.source_statuses
            if not source_status.included
        )

    def to_dict(self, *, output_dir: Path) -> dict[str, object]:
        slice_dir = output_dir / self.slice_id
        return {
            "slice_id": self.slice_id,
            "benchmark_suite_id": self.benchmark_suite_id,
            "benchmark_task_id": self.benchmark_task_id,
            "as_of_date": self.as_of_date,
            "snapshot_id": self.snapshot_request.snapshot_id,
            "cohort_id": self.snapshot_request.cohort_id,
            "slice_dir": _repo_relative_path(slice_dir),
            "snapshot_request_file": _repo_relative_path(
                slice_dir / "snapshot_request.json"
            ),
            "cohort_members_file": _repo_relative_path(
                slice_dir / "cohort_members.csv"
            ),
            "future_outcomes_file": _repo_relative_path(
                slice_dir / "future_outcomes.csv"
            ),
            "archive_index_file": _repo_relative_path(
                slice_dir / "source_archives.json"
            ),
            "source_fixture_dir": _repo_relative_path(self.source_fixture_dir),
            "included_sources": list(self.included_sources),
            "excluded_sources": [
                source_status.to_dict() for source_status in self.excluded_sources
            ],
            "notes": self.notes,
        }


@dataclass(frozen=True)
class PublicBenchmarkSlicePlan:
    benchmark_suite_id: str
    benchmark_task_id: str
    source_fixture_dir: Path
    slices: tuple[PublicBenchmarkSliceSpec, ...]
    coverage_limitation: str = ""

    def to_dict(self, *, output_dir: Path) -> dict[str, object]:
        payload: dict[str, object] = {
            "benchmark_suite_id": self.benchmark_suite_id,
            "benchmark_task_id": self.benchmark_task_id,
            "source_fixture_dir": _repo_relative_path(self.source_fixture_dir),
            "output_dir": _repo_relative_path(output_dir),
            "catalog_file": _repo_relative_path(
                output_dir / PUBLIC_SLICE_CATALOG_FILE_NAME
            ),
            "public_slice_ids": [slice_spec.slice_id for slice_spec in self.slices],
            "slices": [
                slice_spec.to_dict(output_dir=output_dir) for slice_spec in self.slices
            ],
        }
        if self.coverage_limitation:
            payload["coverage_limitation"] = self.coverage_limitation
        return payload


def _archive_activation_date(
    *,
    allowed_data_through: str,
    evidence_frozen_at: str,
) -> date:
    return max(
        _parse_iso_date(allowed_data_through, "allowed_data_through"),
        _parse_iso_date(evidence_frozen_at, "evidence_frozen_at"),
    )


def _build_slice_request(
    *,
    base_request: SnapshotBuildRequest,
    task_contract: BenchmarkTaskContract,
    as_of_date: str,
    task_registry_path: Path | None = None,
) -> SnapshotBuildRequest:
    prefix = _task_slice_prefix(task_contract.task_id)
    date_slug = _date_to_slug(as_of_date)
    slice_id = f"{prefix}_{date_slug}"
    return SnapshotBuildRequest(
        snapshot_id=slice_id,
        cohort_id=f"{slice_id}_cohort",
        benchmark_question_id=base_request.benchmark_question_id,
        benchmark_suite_id=task_contract.suite_id,
        benchmark_task_id=task_contract.task_id,
        as_of_date=as_of_date,
        outcome_observation_closed_at=base_request.outcome_observation_closed_at,
        entity_types=base_request.entity_types,
        baseline_ids=base_request.baseline_ids,
        notes=(
            "Public historical benchmark slice derived from "
            f"{task_contract.task_id} with cutoff {as_of_date}. "
            "Frozen benchmark question, leakage rules, and baseline ids remain unchanged."
        ),
        task_registry_path=(
            str(task_registry_path.resolve())
            if task_registry_path is not None
            else ""
        ),
    )


def _descriptors_allowed_at_cutoff(
    descriptors: tuple[SourceArchiveDescriptor, ...],
    *,
    as_of_date: str,
) -> tuple[SourceArchiveDescriptor, ...]:
    cutoff_date = _parse_iso_date(as_of_date, "as_of_date")
    return tuple(
        descriptor
        for descriptor in descriptors
        if _archive_activation_date(
            allowed_data_through=descriptor.allowed_data_through,
            evidence_frozen_at=descriptor.evidence_frozen_at,
        )
        <= cutoff_date
    )


def _source_statuses_from_manifest(
    manifest: object,
) -> tuple[PublicSliceSourceStatus, ...]:
    return tuple(
        PublicSliceSourceStatus(
            source_name=source_snapshot.source_name,
            source_version=source_snapshot.source_version,
            included=source_snapshot.included,
            allowed_data_through=source_snapshot.allowed_data_through,
            evidence_frozen_at=source_snapshot.evidence_frozen_at or "",
            exclusion_reason=source_snapshot.exclusion_reason or "",
        )
        for source_snapshot in manifest.source_snapshots
    )


def _source_status_fingerprint(
    source_statuses: tuple[PublicSliceSourceStatus, ...],
) -> tuple[tuple[str, str, bool, str, str, str], ...]:
    return tuple(
        (
            source_status.source_name,
            source_status.source_version,
            source_status.included,
            source_status.allowed_data_through,
            source_status.evidence_frozen_at,
            source_status.exclusion_reason,
        )
        for source_status in source_statuses
    )


def _load_fixture_inputs(
    task_contract: BenchmarkTaskContract,
    *,
    task_registry_path: Path | None = None,
) -> tuple[
    SnapshotBuildRequest,
    tuple[object, ...],
    tuple[object, ...],
    tuple[object, ...],
]:
    base_request = load_snapshot_build_request(
        task_contract.fixture_paths.snapshot_request_file,
        task_registry_path=task_registry_path,
    )
    archive_descriptors = load_source_archive_descriptors(
        task_contract.fixture_paths.archive_index_file
    )
    cohort_members = tuple(
        load_cohort_members(task_contract.fixture_paths.cohort_members_file)
    )
    future_outcomes = tuple(
        load_future_outcomes(task_contract.fixture_paths.future_outcomes_file)
    )
    return base_request, archive_descriptors, cohort_members, future_outcomes


def _build_public_slice_specs(
    task_contract: BenchmarkTaskContract,
    *,
    task_registry_path: Path | None = None,
) -> tuple[PublicBenchmarkSliceSpec, ...]:
    (
        base_request,
        archive_descriptors,
        cohort_members,
        future_outcomes,
    ) = _load_fixture_inputs(
        task_contract,
        task_registry_path=task_registry_path,
    )
    max_cutoff = _parse_iso_date(base_request.as_of_date, "as_of_date")
    candidate_cutoff_dates = sorted(
        {
            _archive_activation_date(
                allowed_data_through=descriptor.allowed_data_through,
                evidence_frozen_at=descriptor.evidence_frozen_at,
            ).isoformat()
            for descriptor in archive_descriptors
            if _archive_activation_date(
                allowed_data_through=descriptor.allowed_data_through,
                evidence_frozen_at=descriptor.evidence_frozen_at,
            )
            <= max_cutoff
        }
    )
    source_fixture_dir = task_contract.fixture_paths.snapshot_request_file.parent
    seen_fingerprints: set[tuple[tuple[str, str, bool, str, str, str], ...]] = set()
    slice_specs: list[PublicBenchmarkSliceSpec] = []

    for as_of_date in candidate_cutoff_dates:
        slice_archive_descriptors = _descriptors_allowed_at_cutoff(
            archive_descriptors,
            as_of_date=as_of_date,
        )
        slice_request = _build_slice_request(
            base_request=base_request,
            task_contract=task_contract,
            as_of_date=as_of_date,
            task_registry_path=task_registry_path,
        )
        manifest = build_benchmark_snapshot_manifest(
            slice_request,
            slice_archive_descriptors,
            materialized_at=as_of_date,
            task_registry_path=task_registry_path,
        )
        build_benchmark_cohort_labels(
            manifest,
            cohort_members,
            future_outcomes,
            task_registry_path=task_registry_path,
        )
        source_statuses = _source_statuses_from_manifest(manifest)
        fingerprint = _source_status_fingerprint(source_statuses)
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        slice_specs.append(
            PublicBenchmarkSliceSpec(
                slice_id=slice_request.snapshot_id,
                benchmark_suite_id=task_contract.suite_id,
                benchmark_task_id=task_contract.task_id,
                as_of_date=as_of_date,
                snapshot_request=slice_request,
                source_fixture_dir=source_fixture_dir,
                archive_descriptors=slice_archive_descriptors,
                source_statuses=source_statuses,
                notes=slice_request.notes,
            )
        )

    return tuple(slice_specs)


def plan_public_benchmark_slices(
    *,
    benchmark_task_id: str | None = None,
    task_registry_path: Path | None = None,
) -> PublicBenchmarkSlicePlan:
    resolved_task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=benchmark_task_id or DEFAULT_PUBLIC_SLICE_TASK_ID,
        task_registry_path=task_registry_path,
    )
    slice_specs = _build_public_slice_specs(
        resolved_task_contract,
        task_registry_path=task_registry_path,
    )
    source_fixture_dir = (
        resolved_task_contract.fixture_paths.snapshot_request_file.parent
    )
    coverage_limitation = ""
    if len(slice_specs) < 2:
        base_request = load_snapshot_build_request(
            resolved_task_contract.fixture_paths.snapshot_request_file,
            task_registry_path=task_registry_path,
        )
        coverage_limitation = (
            "Archived descriptor coverage is too sparse for multiple public slices: "
            f"discovered {len(slice_specs)} honest slice(s) on or before "
            f"{base_request.as_of_date} for {resolved_task_contract.task_id}."
        )
    return PublicBenchmarkSlicePlan(
        benchmark_suite_id=resolved_task_contract.suite_id,
        benchmark_task_id=resolved_task_contract.task_id,
        source_fixture_dir=source_fixture_dir,
        slices=slice_specs,
        coverage_limitation=coverage_limitation,
    )


def _copy_fixture_file(source_path: Path, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)


def _slice_archive_descriptor_payload(
    descriptor: SourceArchiveDescriptor,
    *,
    archive_index_base_dir: Path,
) -> dict[str, object]:
    archive_path = Path(descriptor.archive_file).resolve()
    archive_file = archive_path.relative_to(archive_index_base_dir.resolve())
    return {
        "source_name": descriptor.source_name,
        "source_version": descriptor.source_version,
        "archive_file": str(archive_file),
        "archive_format": descriptor.archive_format,
        "allowed_data_through": descriptor.allowed_data_through,
        "evidence_frozen_at": descriptor.evidence_frozen_at,
        "sha256": descriptor.sha256,
        "notes": descriptor.notes,
    }


def materialize_public_benchmark_slices(
    *,
    output_dir: Path | None = None,
    benchmark_task_id: str | None = None,
    task_registry_path: Path | None = None,
) -> dict[str, object]:
    resolved_output_dir = (
        DEFAULT_PUBLIC_SLICE_OUTPUT_DIR if output_dir is None else output_dir.resolve()
    )
    plan = plan_public_benchmark_slices(
        benchmark_task_id=benchmark_task_id,
        task_registry_path=task_registry_path,
    )
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=benchmark_task_id or DEFAULT_PUBLIC_SLICE_TASK_ID,
        task_registry_path=task_registry_path,
    )
    archive_index_base_dir = task_contract.fixture_paths.archive_index_file.parent

    for slice_spec in plan.slices:
        slice_dir = resolved_output_dir / slice_spec.slice_id
        if slice_dir.exists():
            shutil.rmtree(slice_dir)
        write_json(
            slice_dir / "snapshot_request.json",
            slice_spec.snapshot_request.to_dict(),
        )
        _copy_fixture_file(
            task_contract.fixture_paths.cohort_members_file,
            slice_dir / "cohort_members.csv",
        )
        _copy_fixture_file(
            task_contract.fixture_paths.future_outcomes_file,
            slice_dir / "future_outcomes.csv",
        )
        write_json(
            slice_dir / "source_archives.json",
            {
                "archives": [
                    _slice_archive_descriptor_payload(
                        descriptor,
                        archive_index_base_dir=archive_index_base_dir,
                    )
                    for descriptor in slice_spec.archive_descriptors
                ]
            },
        )
        for descriptor in slice_spec.archive_descriptors:
            source_archive_path = Path(descriptor.archive_file).resolve()
            relative_archive_path = source_archive_path.relative_to(
                archive_index_base_dir.resolve()
            )
            _copy_fixture_file(
                source_archive_path,
                slice_dir / relative_archive_path,
            )

    result = plan.to_dict(output_dir=resolved_output_dir)
    write_json(
        resolved_output_dir / PUBLIC_SLICE_CATALOG_FILE_NAME,
        result,
    )
    return result


__all__ = [
    "DEFAULT_PUBLIC_SLICE_OUTPUT_DIR",
    "DEFAULT_PUBLIC_SLICE_TASK_ID",
    "PUBLIC_SLICE_CATALOG_FILE_NAME",
    "PublicBenchmarkSlicePlan",
    "PublicBenchmarkSliceSpec",
    "PublicSliceSourceStatus",
    "materialize_public_benchmark_slices",
    "plan_public_benchmark_slices",
]
