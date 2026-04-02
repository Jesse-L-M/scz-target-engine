from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import shutil

from scz_target_engine.benchmark_labels import (
    CohortMember,
    FutureOutcomeRecord,
    build_benchmark_cohort_labels,
    load_cohort_members,
    load_future_outcomes,
)
from scz_target_engine.benchmark_intervention_objects import (
    INTERVENTION_OBJECT_ENTITY_TYPE,
    build_intervention_object_candidate_cutoff_dates,
    build_intervention_object_public_slice_rows,
)
from scz_target_engine.benchmark_metrics import build_positive_relevance_index
from scz_target_engine.benchmark_registry import (
    DEFAULT_TASK_REGISTRY_PATH,
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
from scz_target_engine.io import write_csv, write_json


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PUBLIC_SLICE_OUTPUT_DIR = (
    REPO_ROOT / "data" / "benchmark" / "public_slices"
)
DEFAULT_PUBLIC_SLICE_TASK_ID = "scz_translational_task"
PUBLIC_SLICE_CATALOG_FILE_NAME = "catalog.json"
PUBLIC_SLICE_ENTITY_TYPES = (INTERVENTION_OBJECT_ENTITY_TYPE,)
PUBLIC_SLICE_BASELINE_IDS = ("v0_current", "v1_current", "random_with_coverage")
PUBLIC_SLICE_PRINCIPAL_HORIZON = "3y"
PUBLIC_SLICE_PROGRAM_UNIVERSE_FILE_NAME = "program_universe.csv"
PUBLIC_SLICE_PROGRAM_HISTORY_EVENTS_FILE_NAME = "events.csv"
PUBLIC_SLICE_TASK_REGISTRY_FILE_NAME = "task_registry.csv"


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
    principal_positive_entity_count: int = 0
    uses_intervention_object_replay: bool = False
    program_universe_source_path: Path | None = None
    program_history_events_source_path: Path | None = None
    task_registry_source_path: Path | None = None
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
        payload = {
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
            "principal_positive_horizon": PUBLIC_SLICE_PRINCIPAL_HORIZON,
            "principal_positive_entity_count": self.principal_positive_entity_count,
            "principal_horizon_evaluable": self.principal_positive_entity_count > 0,
            "notes": self.notes,
        }
        if self.snapshot_request.program_universe_file:
            payload["program_universe_file"] = _repo_relative_path(
                slice_dir / self.snapshot_request.program_universe_file
            )
        if self.snapshot_request.program_history_events_file:
            payload["program_history_events_file"] = _repo_relative_path(
                slice_dir / self.snapshot_request.program_history_events_file
            )
        if self.task_registry_source_path is not None:
            payload["task_registry_file"] = _repo_relative_path(
                slice_dir / PUBLIC_SLICE_TASK_REGISTRY_FILE_NAME
            )
        return payload


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


def _resolve_optional_fixture_path(
    path_text: str,
    *,
    base_dir: Path,
) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path.resolve()


def _uses_track_a_public_slice_replay(
    *,
    task_contract: BenchmarkTaskContract,
    task_registry_path: Path | None,
) -> bool:
    resolved_registry_path = (
        None if task_registry_path is None else task_registry_path.resolve()
    )
    return (
        task_contract.task_id == DEFAULT_PUBLIC_SLICE_TASK_ID
        and (
            resolved_registry_path is None
            or resolved_registry_path == DEFAULT_TASK_REGISTRY_PATH
        )
    )


def _slice_program_input_source_paths(
    *,
    base_request: SnapshotBuildRequest,
    source_fixture_dir: Path,
    uses_track_a_replay: bool,
) -> tuple[Path | None, Path | None]:
    program_universe_path = _resolve_optional_fixture_path(
        base_request.program_universe_file,
        base_dir=source_fixture_dir,
    )
    program_history_events_path = _resolve_optional_fixture_path(
        base_request.program_history_events_file,
        base_dir=source_fixture_dir,
    )
    if program_universe_path is not None or program_history_events_path is not None:
        if program_universe_path is None or program_history_events_path is None:
            raise ValueError(
                "snapshot request must provide both program_universe_file and "
                "program_history_events_file when either is set"
            )
        return program_universe_path, program_history_events_path
    if uses_track_a_replay:
        return (
            (
                REPO_ROOT
                / "data"
                / "curated"
                / "program_history"
                / "v2"
                / "program_universe.csv"
            ).resolve(),
            (
                REPO_ROOT
                / "data"
                / "curated"
                / "program_history"
                / "v2"
                / "events.csv"
            ).resolve(),
        )
    return None, None


def _candidate_cutoff_dates(
    *,
    base_request: SnapshotBuildRequest,
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    uses_track_a_replay: bool,
    program_universe_path: Path | None,
    program_history_events_path: Path | None,
) -> tuple[str, ...]:
    maximum_cutoff = _parse_iso_date(base_request.as_of_date, "as_of_date")
    archive_cutoff_dates = {
        _archive_activation_date(
            allowed_data_through=descriptor.allowed_data_through,
            evidence_frozen_at=descriptor.evidence_frozen_at,
        ).isoformat()
        for descriptor in archive_descriptors
        if _archive_activation_date(
            allowed_data_through=descriptor.allowed_data_through,
            evidence_frozen_at=descriptor.evidence_frozen_at,
        )
        <= maximum_cutoff
    }
    if not archive_cutoff_dates:
        return ()
    candidate_cutoff_dates = set(archive_cutoff_dates)
    if uses_track_a_replay:
        minimum_cutoff_date = min(archive_cutoff_dates, default=base_request.as_of_date)
        if program_universe_path is None or program_history_events_path is None:
            raise ValueError(
                "Track A public-slice replay requires pinned program_universe and events inputs"
            )
        candidate_cutoff_dates.update(
            build_intervention_object_candidate_cutoff_dates(
                as_of_date=base_request.as_of_date,
                minimum_cutoff_date=minimum_cutoff_date,
                program_universe_path=program_universe_path,
                events_path=program_history_events_path,
            )
        )
    return tuple(sorted(candidate_cutoff_dates))


def _coverage_limitation(
    *,
    slice_specs: tuple[PublicBenchmarkSliceSpec, ...],
    as_of_date: str,
    benchmark_task_id: str,
) -> str:
    evaluable_slice_specs = tuple(
        slice_spec
        for slice_spec in slice_specs
        if slice_spec.principal_positive_entity_count > 0
    )
    if not evaluable_slice_specs and slice_specs:
        return (
            "Honest public slices were discovered on or before "
            f"{as_of_date} for {benchmark_task_id}, but none are evaluable on the "
            f"principal {PUBLIC_SLICE_PRINCIPAL_HORIZON} horizon: "
            + ", ".join(
                f"{slice_spec.slice_id}={slice_spec.principal_positive_entity_count}"
                for slice_spec in slice_specs
            )
        )
    if len(slice_specs) < 2:
        return (
            "Archived descriptor coverage is too sparse for multiple public slices: "
            f"discovered {len(slice_specs)} honest slice(s) on or before "
            f"{as_of_date} for {benchmark_task_id}."
        )
    if len(evaluable_slice_specs) < 2:
        return (
            "Honest public slices were discovered on or before "
            f"{as_of_date} for {benchmark_task_id}, but fewer than two are evaluable "
            f"on the principal {PUBLIC_SLICE_PRINCIPAL_HORIZON} horizon: "
            + ", ".join(
                f"{slice_spec.slice_id}={slice_spec.principal_positive_entity_count}"
                for slice_spec in slice_specs
            )
        )
    return ""


def _cohort_members_fingerprint(
    cohort_members: tuple[CohortMember, ...],
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(
            (
                member.entity_type,
                member.entity_id,
                member.entity_label,
            )
            for member in cohort_members
        )
    )


def _future_outcomes_fingerprint(
    future_outcomes: tuple[FutureOutcomeRecord, ...],
) -> tuple[tuple[str, str, str, str, str, str], ...]:
    return tuple(
        sorted(
            (
                outcome.entity_type,
                outcome.entity_id,
                outcome.outcome_label,
                outcome.outcome_date,
                outcome.label_source,
                outcome.label_notes,
            )
            for outcome in future_outcomes
        )
    )


def _public_slice_state_fingerprint(
    *,
    source_statuses: tuple[PublicSliceSourceStatus, ...],
    cohort_members: tuple[CohortMember, ...],
    future_outcomes: tuple[FutureOutcomeRecord, ...],
) -> tuple[object, ...]:
    return (
        _source_status_fingerprint(source_statuses),
        _cohort_members_fingerprint(cohort_members),
        _future_outcomes_fingerprint(future_outcomes),
    )


def _build_slice_request(
    *,
    base_request: SnapshotBuildRequest,
    task_contract: BenchmarkTaskContract,
    as_of_date: str,
    uses_track_a_replay: bool,
    task_registry_path: Path | None = None,
) -> SnapshotBuildRequest:
    prefix = _task_slice_prefix(task_contract.task_id)
    date_slug = _date_to_slug(as_of_date)
    slice_id = f"{prefix}_{date_slug}"
    entity_types = base_request.entity_types
    baseline_ids = base_request.baseline_ids
    notes = (
        "Public historical benchmark slice derived from "
        f"{task_contract.task_id} with cutoff {as_of_date}. "
        "Frozen benchmark question, leakage rules, and baseline ids remain unchanged."
    )
    program_universe_file = ""
    program_history_events_file = ""
    if base_request.program_universe_file and base_request.program_history_events_file:
        program_universe_file = PUBLIC_SLICE_PROGRAM_UNIVERSE_FILE_NAME
        program_history_events_file = PUBLIC_SLICE_PROGRAM_HISTORY_EVENTS_FILE_NAME
    if uses_track_a_replay:
        entity_types = PUBLIC_SLICE_ENTITY_TYPES
        baseline_ids = PUBLIC_SLICE_BASELINE_IDS
        program_universe_file = PUBLIC_SLICE_PROGRAM_UNIVERSE_FILE_NAME
        program_history_events_file = PUBLIC_SLICE_PROGRAM_HISTORY_EVENTS_FILE_NAME
        notes = (
            "Public historical Track A benchmark slice derived from "
            f"{task_contract.task_id} with cutoff {as_of_date}. "
            "Intervention-object cohort rows are derived from pinned program-history "
            "inputs and future program-history events while preserving frozen leakage "
            "rules and explicit archive exclusions."
        )
    return SnapshotBuildRequest(
        snapshot_id=slice_id,
        cohort_id=f"{slice_id}_cohort",
        benchmark_question_id=base_request.benchmark_question_id,
        benchmark_suite_id=task_contract.suite_id,
        benchmark_task_id=task_contract.task_id,
        as_of_date=as_of_date,
        outcome_observation_closed_at=base_request.outcome_observation_closed_at,
        entity_types=entity_types,
        baseline_ids=baseline_ids,
        notes=notes,
        task_registry_path=(
            str(task_registry_path.resolve())
            if task_registry_path is not None
            else ""
        ),
        program_universe_file=program_universe_file,
        program_history_events_file=program_history_events_file,
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
) -> tuple[tuple[str, bool, str, str, str], ...]:
    return tuple(
        (
            source_status.source_name,
            source_status.included,
            source_status.source_version if source_status.included else "",
            source_status.allowed_data_through if source_status.included else "",
            source_status.evidence_frozen_at if source_status.included else "",
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


def _principal_positive_entity_count(
    labels: tuple[object, ...],
    *,
    entity_types: tuple[str, ...],
) -> int:
    return sum(
        sum(
            build_positive_relevance_index(
                labels,
                entity_type=entity_type,
                horizon=PUBLIC_SLICE_PRINCIPAL_HORIZON,
            ).values()
        )
        for entity_type in entity_types
    )


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
    uses_track_a_replay = _uses_track_a_public_slice_replay(
        task_contract=task_contract,
        task_registry_path=task_registry_path,
    )
    resolved_task_registry_path = (
        None if task_registry_path is None else task_registry_path.resolve()
    )
    source_fixture_dir = task_contract.fixture_paths.snapshot_request_file.parent
    (
        program_universe_source_path,
        program_history_events_source_path,
    ) = _slice_program_input_source_paths(
        base_request=base_request,
        source_fixture_dir=source_fixture_dir,
        uses_track_a_replay=uses_track_a_replay,
    )
    candidate_cutoff_dates = _candidate_cutoff_dates(
        base_request=base_request,
        archive_descriptors=archive_descriptors,
        uses_track_a_replay=uses_track_a_replay,
        program_universe_path=program_universe_source_path,
        program_history_events_path=program_history_events_source_path,
    )
    seen_fingerprints: set[tuple[object, ...]] = set()
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
            uses_track_a_replay=uses_track_a_replay,
            task_registry_path=task_registry_path,
        )
        manifest = build_benchmark_snapshot_manifest(
            slice_request,
            slice_archive_descriptors,
            materialized_at=as_of_date,
            task_registry_path=task_registry_path,
        )
        resolved_cohort_members = cohort_members
        resolved_future_outcomes = future_outcomes
        if uses_track_a_replay:
            cohort_member_rows, future_outcome_rows = (
                build_intervention_object_public_slice_rows(
                    as_of_date=as_of_date,
                    outcome_observation_closed_at=base_request.outcome_observation_closed_at,
                    program_universe_path=program_universe_source_path,
                    events_path=program_history_events_source_path,
                )
            )
            resolved_cohort_members = tuple(
                CohortMember.from_dict(row) for row in cohort_member_rows
            )
            resolved_future_outcomes = tuple(
                FutureOutcomeRecord.from_dict(row) for row in future_outcome_rows
            )
        source_statuses = _source_statuses_from_manifest(manifest)
        fingerprint = _public_slice_state_fingerprint(
            source_statuses=source_statuses,
            cohort_members=resolved_cohort_members,
            future_outcomes=resolved_future_outcomes,
        )
        if fingerprint in seen_fingerprints:
            continue
        labels = build_benchmark_cohort_labels(
            manifest,
            resolved_cohort_members,
            resolved_future_outcomes,
            task_registry_path=task_registry_path,
        )
        positive_count = _principal_positive_entity_count(
            labels,
            entity_types=manifest.entity_types,
        )
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
                principal_positive_entity_count=positive_count,
                uses_intervention_object_replay=uses_track_a_replay,
                program_universe_source_path=program_universe_source_path,
                program_history_events_source_path=program_history_events_source_path,
                task_registry_source_path=resolved_task_registry_path,
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
    base_request = load_snapshot_build_request(
        resolved_task_contract.fixture_paths.snapshot_request_file,
        task_registry_path=task_registry_path,
    )
    coverage_limitation = _coverage_limitation(
        slice_specs=slice_specs,
        as_of_date=base_request.as_of_date,
        benchmark_task_id=resolved_task_contract.task_id,
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


def _looks_like_public_slice_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    return any(
        (path / file_name).exists()
        for file_name in (
            "snapshot_request.json",
            "source_archives.json",
            "cohort_members.csv",
            "future_outcomes.csv",
        )
    )


def _prune_obsolete_slice_dirs(
    *,
    output_dir: Path,
    active_slice_ids: set[str],
) -> None:
    if not output_dir.exists():
        return
    for child in output_dir.iterdir():
        if child.name in active_slice_ids:
            continue
        if _looks_like_public_slice_dir(child):
            shutil.rmtree(child)


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
    active_slice_ids = {slice_spec.slice_id for slice_spec in plan.slices}
    _prune_obsolete_slice_dirs(
        output_dir=resolved_output_dir,
        active_slice_ids=active_slice_ids,
    )

    for slice_spec in plan.slices:
        slice_dir = resolved_output_dir / slice_spec.slice_id
        if slice_dir.exists():
            shutil.rmtree(slice_dir)
        snapshot_request_payload = slice_spec.snapshot_request.to_dict()
        if (
            slice_spec.task_registry_source_path is not None
            and slice_spec.snapshot_request.task_registry_path
        ):
            snapshot_request_payload["task_registry_path"] = (
                PUBLIC_SLICE_TASK_REGISTRY_FILE_NAME
            )
        write_json(
            slice_dir / "snapshot_request.json",
            snapshot_request_payload,
        )
        if (
            slice_spec.task_registry_source_path is not None
            and slice_spec.snapshot_request.task_registry_path
        ):
            _copy_fixture_file(
                slice_spec.task_registry_source_path,
                slice_dir / PUBLIC_SLICE_TASK_REGISTRY_FILE_NAME,
            )
        if (
            slice_spec.program_universe_source_path is not None
            and slice_spec.snapshot_request.program_universe_file
        ):
            _copy_fixture_file(
                slice_spec.program_universe_source_path,
                slice_dir / slice_spec.snapshot_request.program_universe_file,
            )
        if (
            slice_spec.program_history_events_source_path is not None
            and slice_spec.snapshot_request.program_history_events_file
        ):
            _copy_fixture_file(
                slice_spec.program_history_events_source_path,
                slice_dir / slice_spec.snapshot_request.program_history_events_file,
            )
        if slice_spec.uses_intervention_object_replay:
            cohort_rows, future_outcome_rows = build_intervention_object_public_slice_rows(
                as_of_date=slice_spec.as_of_date,
                outcome_observation_closed_at=(
                    slice_spec.snapshot_request.outcome_observation_closed_at
                ),
                program_universe_path=slice_spec.program_universe_source_path,
                events_path=slice_spec.program_history_events_source_path,
            )
            write_csv(
                slice_dir / "cohort_members.csv",
                cohort_rows,
                ["entity_type", "entity_id", "entity_label"],
            )
            write_csv(
                slice_dir / "future_outcomes.csv",
                future_outcome_rows,
                [
                    "entity_type",
                    "entity_id",
                    "outcome_label",
                    "outcome_date",
                    "label_source",
                    "label_notes",
                ],
            )
        else:
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
