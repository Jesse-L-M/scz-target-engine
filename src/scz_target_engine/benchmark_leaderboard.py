from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
from typing import Any

from scz_target_engine.benchmark_labels import (
    MaterializedBenchmarkCohortArtifacts,
    load_materialized_benchmark_cohort_artifacts,
)
from scz_target_engine.benchmark_intervention_objects import (
    INTERVENTION_OBJECT_ENTITY_TYPE,
    build_intervention_object_bundle_source_snapshot_provenance,
    read_intervention_object_feature_bundle,
    read_intervention_object_projection_payload,
)
from scz_target_engine.benchmark_metrics import (
    BOOTSTRAP_INTERVAL_METHOD,
    BenchmarkConfidenceIntervalPayload,
    BenchmarkMetricOutputPayload,
    RETRIEVAL_METRIC_NAMES,
    build_positive_relevance_index,
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_protocol import (
    SourceSnapshot,
)
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.benchmark_runner import (
    BenchmarkModelRunManifest,
    InputArtifactReference,
    build_track_b_run_id,
    derive_track_b_slice_random_seed,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import (
    load_source_archive_descriptors,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_CASE_OUTPUT_SCHEMA_NAME,
    TRACK_B_CONFUSION_SCHEMA_NAME,
    TRACK_B_ENTITY_TYPE,
    TRACK_B_HORIZON,
    TRACK_B_METRIC_NAMES,
    TRACK_B_METRIC_UNITS,
    TRACK_B_RUN_PARAMETERIZATION_FIELDS,
    TRACK_B_BOOTSTRAP_RESAMPLE_UNIT,
    build_track_b_case_outputs,
    build_track_b_confusion_summary,
    build_track_b_error_analysis_markdown,
    build_track_b_program_memory_dataset,
    estimate_track_b_metric_intervals,
    is_track_b_task,
    load_track_b_casebook,
    read_track_b_case_output_payload,
    read_track_b_confusion_summary,
    score_track_b_case_outputs,
    track_b_metric_cohort_sizes,
    track_b_case_output_path,
    track_b_confusion_summary_path,
    validate_track_b_case_output_payload,
    validate_track_b_case_output_payload_against_expected_cases,
)
from scz_target_engine.io import read_json, write_json


REPORT_CARD_SCHEMA_NAME = "benchmark_report_card_payload"
REPORT_CARD_SCHEMA_VERSION = "v1"
LEADERBOARD_SCHEMA_NAME = "benchmark_leaderboard_payload"
LEADERBOARD_SCHEMA_VERSION = "v1"
RANKING_ORDER_DESCENDING = "descending"
INTERVENTION_OBJECT_ERROR_ANALYSIS_HORIZON = "3y"

_SLICE_NOTES_PATTERN = re.compile(
    r"relevance=any_positive_outcome;"
    r"\s*positives=(?P<positives>\d+);"
    r"\s*covered_entities=(?P<covered>\d+)/(?P<admissible>\d+)"
    r"(?:;\s*deterministic_test_mode=true)?$"
)
_INTERVAL_METHOD_PATTERN = re.compile(r"method=(?P<method>[a-z_]+);")


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_component(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _parameter_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return sha256(encoded).hexdigest()[:8]


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _index_artifacts_by_name(
    artifacts: tuple[InputArtifactReference, ...],
    *,
    context: str,
) -> dict[str, InputArtifactReference]:
    artifact_index: dict[str, InputArtifactReference] = {}
    duplicate_names: list[str] = []
    for artifact in artifacts:
        if artifact.artifact_name in artifact_index:
            duplicate_names.append(artifact.artifact_name)
            continue
        artifact_index[artifact.artifact_name] = artifact
    if duplicate_names:
        raise ValueError(
            f"{context} contains duplicate artifact_name entries: "
            + ", ".join(sorted(set(duplicate_names)))
        )
    return artifact_index


def _build_artifact_reference(
    *,
    artifact_name: str,
    path: Path,
    schema_name: str = "",
    notes: str = "",
) -> InputArtifactReference:
    resolved_path = path.resolve()
    return InputArtifactReference(
        artifact_name=artifact_name,
        artifact_path=str(resolved_path),
        sha256=_file_sha256(resolved_path),
        schema_name=schema_name,
        notes=notes,
    )


def _build_track_b_slice_notes(
    *,
    case_count: int,
    included_coverage_count: int,
    replay_supported_case_count: int,
    analog_evaluable_case_count: int,
    deterministic_test_mode: bool,
) -> str:
    notes = (
        f"track_b_cases={case_count};"
        f" included_coverage_cases={included_coverage_count};"
        f" replay_supported_cases={replay_supported_case_count};"
        f" analog_evaluable_cases={analog_evaluable_case_count}"
    )
    if deterministic_test_mode:
        notes += "; deterministic_test_mode=true"
    return notes


@dataclass(frozen=True)
class _ValidatedTrackBRunContract:
    bootstrap_iterations: int
    bootstrap_confidence_level: float
    deterministic_test_mode: bool
    slice_random_seed: int
    validated_parameterization: dict[str, object]


@dataclass(frozen=True)
class _ValidatedTrackBRunBundle:
    run_manifest_path: Path
    run_manifest: BenchmarkModelRunManifest
    validated_parameterization: dict[str, object]
    metric_items: tuple[tuple[Path, BenchmarkMetricOutputPayload], ...]
    interval_paths_by_metric: dict[str, Path]
    case_output_payload: Any
    confusion_summary: Any
    validated_metric_values: dict[str, float]
    validated_intervals: dict[str, tuple[float, float, float]]
    slice_notes: str


@dataclass(frozen=True)
class _ValidatedTrackBReportingBundle:
    evaluation_input_artifacts: tuple[InputArtifactReference, ...]
    case_count: int
    included_coverage_count: int
    replay_supported_case_count: int
    validated_runs_by_run_id: dict[str, _ValidatedTrackBRunBundle]


def _require_track_b_parameterization_mapping(
    run_manifest: BenchmarkModelRunManifest,
) -> dict[str, object]:
    parameterization = run_manifest.parameterization
    if not isinstance(parameterization, dict):
        raise ValueError(
            f"Track B run manifest parameterization must be a JSON object for {run_manifest.run_id}"
        )
    missing_field_names = [
        field_name
        for field_name in TRACK_B_RUN_PARAMETERIZATION_FIELDS
        if field_name not in parameterization
    ]
    unexpected_field_names = sorted(
        set(parameterization).difference(TRACK_B_RUN_PARAMETERIZATION_FIELDS)
    )
    if missing_field_names or unexpected_field_names:
        details: list[str] = []
        if missing_field_names:
            details.append("missing=" + ", ".join(missing_field_names))
        if unexpected_field_names:
            details.append("unexpected=" + ", ".join(unexpected_field_names))
        raise ValueError(
            "Track B run manifest parameterization does not match the exact "
            f"reporting contract for {run_manifest.run_id}"
            + (f" ({'; '.join(details)})" if details else "")
        )
    return parameterization


def _require_track_b_parameterization_text(
    parameterization: dict[str, object],
    *,
    field_name: str,
    run_id: str,
) -> str:
    value = parameterization.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Track B run manifest parameterization {field_name} must be a non-empty string for {run_id}"
        )
    return value


def _require_track_b_parameterization_int(
    parameterization: dict[str, object],
    *,
    field_name: str,
    run_id: str,
) -> int:
    value = parameterization.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"Track B run manifest parameterization {field_name} must be an integer for {run_id}"
        )
    return value


def _require_track_b_parameterization_float(
    parameterization: dict[str, object],
    *,
    field_name: str,
    run_id: str,
) -> float:
    value = parameterization.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"Track B run manifest parameterization {field_name} must be a float for {run_id}"
        )
    return float(value)


def _require_track_b_parameterization_bool(
    parameterization: dict[str, object],
    *,
    field_name: str,
    run_id: str,
) -> bool:
    value = parameterization.get(field_name)
    if not isinstance(value, bool):
        raise ValueError(
            f"Track B run manifest parameterization {field_name} must be a boolean for {run_id}"
        )
    return value


def _build_track_b_expected_evaluation_input_artifacts(
    *,
    manifest_file: Path,
    cohort_labels_file: Path,
    materialized_cohort: MaterializedBenchmarkCohortArtifacts,
) -> tuple[InputArtifactReference, ...]:
    source_artifact_index = {
        artifact_name: Path(str(artifact.artifact_path)).resolve()
        for artifact_name, artifact in _index_artifacts_by_name(
            tuple(materialized_cohort.auxiliary_source_artifacts),
            context="Track B auxiliary source artifacts",
        ).items()
    }
    expected_artifacts = [
        _build_artifact_reference(
            artifact_name="benchmark_snapshot_manifest",
            path=manifest_file.resolve(),
            schema_name="benchmark_snapshot_manifest",
        ),
        _build_artifact_reference(
            artifact_name="benchmark_cohort_manifest",
            path=materialized_cohort.cohort_manifest_path,
            schema_name="benchmark_cohort_manifest",
        ),
        _build_artifact_reference(
            artifact_name="benchmark_cohort_members",
            path=materialized_cohort.cohort_members_path,
            schema_name="benchmark_cohort_members",
        ),
        _build_artifact_reference(
            artifact_name="benchmark_source_cohort_members",
            path=materialized_cohort.source_cohort_members_path,
            schema_name="benchmark_source_cohort_members",
        ),
        _build_artifact_reference(
            artifact_name="benchmark_source_future_outcomes",
            path=materialized_cohort.source_future_outcomes_path,
            schema_name="benchmark_source_future_outcomes",
        ),
        _build_artifact_reference(
            artifact_name="benchmark_cohort_labels",
            path=cohort_labels_file.resolve(),
            schema_name="benchmark_cohort_labels",
        ),
    ]
    for artifact_name in (
        "source_archive_index",
        "track_b_casebook",
        "track_b_program_universe",
        "track_b_program_history_events",
        "program_memory_assets",
        "program_memory_event_provenance",
        "program_memory_directionality_hypotheses",
    ):
        artifact_path = source_artifact_index.get(artifact_name)
        if artifact_path is None:
            raise ValueError(
                "Track B reporting requires the materialized benchmark cohort bundle to "
                "capture the pinned Track B source artifacts: "
                f"{artifact_name}"
            )
        expected_artifacts.append(
            _build_artifact_reference(
                artifact_name=artifact_name,
                path=artifact_path,
            )
        )
    expected_artifacts.sort(
        key=lambda artifact: (
            artifact.artifact_name,
            artifact.source_name,
            artifact.artifact_path,
        )
    )
    return tuple(expected_artifacts)


def _validate_track_b_input_artifact_contract(
    *,
    run_manifest: BenchmarkModelRunManifest,
    expected_input_artifacts: tuple[InputArtifactReference, ...],
) -> None:
    expected_by_name = _index_artifacts_by_name(
        expected_input_artifacts,
        context="Track B expected evaluation_input_artifacts",
    )
    observed_by_name = _index_artifacts_by_name(
        run_manifest.input_artifacts,
        context=(
            "Track B run manifest input_artifacts"
            f" for {run_manifest.run_id}"
        ),
    )
    missing_names = sorted(set(expected_by_name).difference(observed_by_name))
    unexpected_names = sorted(set(observed_by_name).difference(expected_by_name))
    if missing_names or unexpected_names:
        details: list[str] = []
        if missing_names:
            details.append("missing=" + ", ".join(missing_names))
        if unexpected_names:
            details.append("unexpected=" + ", ".join(unexpected_names))
        raise ValueError(
            "Track B run manifest input_artifacts do not match the pinned cohort/source "
            f"artifact contract for {run_manifest.run_id}"
            + (f" ({'; '.join(details)})" if details else "")
        )
    for artifact_name, expected_artifact in sorted(expected_by_name.items()):
        observed_artifact = observed_by_name[artifact_name]
        if (
            Path(observed_artifact.artifact_path).resolve()
            != Path(expected_artifact.artifact_path).resolve()
        ):
            raise ValueError(
                "Track B run manifest input_artifact path does not match the pinned "
                f"cohort/source artifact contract for {run_manifest.run_id}/{artifact_name}"
            )
        if observed_artifact.sha256 != expected_artifact.sha256:
            raise ValueError(
                "Track B run manifest input_artifact sha256 does not match the pinned "
                f"cohort/source artifact contract for {run_manifest.run_id}/{artifact_name}"
            )
        if (
            expected_artifact.schema_name
            and observed_artifact.schema_name != expected_artifact.schema_name
        ):
            raise ValueError(
                "Track B run manifest input_artifact schema_name does not match the "
                f"pinned cohort/source artifact contract for {run_manifest.run_id}/{artifact_name}"
            )


def _validate_track_b_run_manifest_ownership(
    *,
    manifest: object,
    run_manifest: BenchmarkModelRunManifest,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    baseline: object,
    expected_case_count: int,
    expected_casebook_sha256: str,
) -> _ValidatedTrackBRunContract:
    if run_manifest.benchmark_suite_id != benchmark_suite_id:
        raise ValueError(
            "Track B run manifest benchmark_suite_id does not match the reporting "
            f"surface for {run_manifest.run_id}"
        )
    if run_manifest.benchmark_task_id != benchmark_task_id:
        raise ValueError(
            "Track B run manifest benchmark_task_id does not match the reporting "
            f"surface for {run_manifest.run_id}"
        )
    if run_manifest.model_family != str(getattr(baseline, "family")):
        raise ValueError(
            f"Track B run manifest model_family does not match the baseline contract for {run_manifest.run_id}"
        )
    parameterization = _require_track_b_parameterization_mapping(run_manifest)
    benchmark_question_id = _require_track_b_parameterization_text(
        parameterization,
        field_name="benchmark_question_id",
        run_id=run_manifest.run_id,
    )
    if benchmark_question_id != str(getattr(manifest, "benchmark_question_id")):
        raise ValueError(
            "Track B run manifest benchmark_question_id does not match snapshot "
            f"for {run_manifest.run_id}"
        )
    track_b_baseline_mode = _require_track_b_parameterization_text(
        parameterization,
        field_name="track_b_baseline_mode",
        run_id=run_manifest.run_id,
    )
    if track_b_baseline_mode != run_manifest.baseline_id:
        raise ValueError(
            "Track B run manifest baseline_id does not match track_b_baseline_mode "
            f"for {run_manifest.run_id}"
        )
    track_b_horizon = _require_track_b_parameterization_text(
        parameterization,
        field_name="track_b_horizon",
        run_id=run_manifest.run_id,
    )
    if track_b_horizon != TRACK_B_HORIZON:
        raise ValueError(
            "Track B run manifest track_b_horizon does not match the Track B "
            f"reporting surface for {run_manifest.run_id}"
        )
    interval_method = _require_track_b_parameterization_text(
        parameterization,
        field_name="interval_method",
        run_id=run_manifest.run_id,
    )
    if interval_method != BOOTSTRAP_INTERVAL_METHOD:
        raise ValueError(
            "Track B run manifest parameterization interval_method does not match the "
            f"Track B reporting contract for {run_manifest.run_id}"
        )
    resample_unit = _require_track_b_parameterization_text(
        parameterization,
        field_name="resample_unit",
        run_id=run_manifest.run_id,
    )
    if resample_unit != TRACK_B_BOOTSTRAP_RESAMPLE_UNIT:
        raise ValueError(
            "Track B run manifest parameterization resample_unit does not match the "
            f"Track B reporting contract for {run_manifest.run_id}"
        )
    bootstrap_iterations = _require_track_b_parameterization_int(
        parameterization,
        field_name="bootstrap_iterations",
        run_id=run_manifest.run_id,
    )
    if bootstrap_iterations <= 0:
        raise ValueError(
            f"Track B run manifest parameterization bootstrap_iterations must be positive for {run_manifest.run_id}"
        )
    bootstrap_confidence_level = _require_track_b_parameterization_float(
        parameterization,
        field_name="bootstrap_confidence_level",
        run_id=run_manifest.run_id,
    )
    if not 0.0 < bootstrap_confidence_level < 1.0:
        raise ValueError(
            "Track B run manifest parameterization bootstrap_confidence_level must be "
            f"between 0 and 1 for {run_manifest.run_id}"
        )
    base_random_seed = _require_track_b_parameterization_int(
        parameterization,
        field_name="random_seed",
        run_id=run_manifest.run_id,
    )
    deterministic_test_mode = _require_track_b_parameterization_bool(
        parameterization,
        field_name="deterministic_test_mode",
        run_id=run_manifest.run_id,
    )
    track_b_case_count = _require_track_b_parameterization_int(
        parameterization,
        field_name="track_b_case_count",
        run_id=run_manifest.run_id,
    )
    if track_b_case_count != expected_case_count:
        raise ValueError(
            "Track B run manifest parameterization track_b_case_count does not match "
            f"the pinned Track B casebook for {run_manifest.run_id}"
        )
    track_b_casebook_sha256 = _require_track_b_parameterization_text(
        parameterization,
        field_name="track_b_casebook_sha256",
        run_id=run_manifest.run_id,
    )
    if track_b_casebook_sha256 != expected_casebook_sha256:
        raise ValueError(
            "Track B run manifest parameterization track_b_casebook_sha256 does not "
            f"match the pinned Track B casebook for {run_manifest.run_id}"
        )
    validated_parameterization = {
        "benchmark_question_id": benchmark_question_id,
        "bootstrap_confidence_level": bootstrap_confidence_level,
        "bootstrap_iterations": bootstrap_iterations,
        "deterministic_test_mode": deterministic_test_mode,
        "interval_method": interval_method,
        "random_seed": base_random_seed,
        "resample_unit": resample_unit,
        "track_b_casebook_sha256": track_b_casebook_sha256,
        "track_b_horizon": track_b_horizon,
        "track_b_case_count": track_b_case_count,
        "track_b_baseline_mode": track_b_baseline_mode,
    }
    expected_run_id = build_track_b_run_id(
        snapshot_id=run_manifest.snapshot_id,
        baseline_id=run_manifest.baseline_id,
        code_version=run_manifest.code_version,
        parameterization=validated_parameterization,
    )
    if run_manifest.run_id != expected_run_id:
        raise ValueError(
            "Track B run manifest run_id does not match its baseline/code_version/"
            f"parameterization contract for {run_manifest.run_id}"
        )
    return _ValidatedTrackBRunContract(
        bootstrap_iterations=bootstrap_iterations,
        bootstrap_confidence_level=bootstrap_confidence_level,
        deterministic_test_mode=deterministic_test_mode,
        slice_random_seed=derive_track_b_slice_random_seed(
            base_random_seed=base_random_seed,
            baseline_id=run_manifest.baseline_id,
        ),
        validated_parameterization=validated_parameterization,
    )


def _validate_track_b_runner_artifact_ownership(
    *,
    run_manifest: BenchmarkModelRunManifest,
    metric_items: list[tuple[Path, BenchmarkMetricOutputPayload]],
    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ],
    case_output_payload: Any,
    confusion_summary: Any,
) -> None:
    expected_run_id = run_manifest.run_id
    expected_baseline_id = run_manifest.baseline_id
    expected_snapshot_id = run_manifest.snapshot_id
    if case_output_payload.run_id != expected_run_id:
        raise ValueError(
            f"Track B case output payload run_id does not match runner manifest for {expected_run_id}"
        )
    if case_output_payload.baseline_id != expected_baseline_id:
        raise ValueError(
            "Track B case output payload baseline_id does not match runner manifest "
            f"for {expected_run_id}"
        )
    if case_output_payload.snapshot_id != expected_snapshot_id:
        raise ValueError(
            "Track B case output payload snapshot_id does not match runner manifest "
            f"for {expected_run_id}"
        )
    if confusion_summary.run_id != expected_run_id:
        raise ValueError(
            f"Track B confusion summary run_id does not match runner manifest for {expected_run_id}"
        )
    if confusion_summary.baseline_id != expected_baseline_id:
        raise ValueError(
            "Track B confusion summary baseline_id does not match runner manifest "
            f"for {expected_run_id}"
        )
    if confusion_summary.snapshot_id != expected_snapshot_id:
        raise ValueError(
            "Track B confusion summary snapshot_id does not match runner manifest "
            f"for {expected_run_id}"
        )
    for _, metric_payload in metric_items:
        if metric_payload.run_id != expected_run_id:
            raise ValueError(
                f"Track B metric payload run_id does not match runner manifest for {expected_run_id}"
            )
        if metric_payload.baseline_id != expected_baseline_id:
            raise ValueError(
                "Track B metric payload baseline_id does not match runner manifest "
                f"for {expected_run_id}/{metric_payload.metric_name}"
            )
        if metric_payload.snapshot_id != expected_snapshot_id:
            raise ValueError(
                "Track B metric payload snapshot_id does not match runner manifest "
                f"for {expected_run_id}/{metric_payload.metric_name}"
            )
    for metric_name in TRACK_B_METRIC_NAMES:
        interval_payload = interval_index[
            (expected_run_id, TRACK_B_ENTITY_TYPE, TRACK_B_HORIZON, metric_name)
        ][1]
        if interval_payload.run_id != expected_run_id:
            raise ValueError(
                "Track B confidence interval payload run_id does not match runner "
                f"manifest for {expected_run_id}/{metric_name}"
            )
        if interval_payload.baseline_id != expected_baseline_id:
            raise ValueError(
                "Track B confidence interval payload baseline_id does not match "
                f"runner manifest for {expected_run_id}/{metric_name}"
            )
        if interval_payload.snapshot_id != expected_snapshot_id:
            raise ValueError(
                "Track B confidence interval payload snapshot_id does not match "
                f"runner manifest for {expected_run_id}/{metric_name}"
            )


def _parse_slice_counts(notes: str) -> tuple[int, int, int] | None:
    match = _SLICE_NOTES_PATTERN.fullmatch(notes.strip())
    if match is None:
        return None
    return (
        int(match.group("covered")),
        int(match.group("admissible")),
        int(match.group("positives")),
    )


def _parse_interval_method(notes: str) -> str:
    match = _INTERVAL_METHOD_PATTERN.search(notes)
    if match is None:
        return ""
    return match.group("method")


def _require_json_mapping(path: Path) -> dict[str, object]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a single JSON object")
    return payload


def _task_registry_path_from_manifest(manifest: object) -> Path | None:
    task_registry_path = getattr(manifest, "task_registry_path", "")
    if not task_registry_path:
        return None
    return Path(str(task_registry_path)).resolve()


def _discover_runner_artifact_files(
    runner_output_dir: Path,
) -> tuple[tuple[Path, ...], tuple[Path, ...], tuple[Path, ...]]:
    resolved_output_dir = runner_output_dir.resolve()
    run_manifest_files = tuple(
        sorted((resolved_output_dir / "run_manifests").glob("*.json"))
    )
    metric_payload_files = tuple(
        sorted((resolved_output_dir / "metric_payloads").rglob("*.json"))
    )
    confidence_interval_files = tuple(
        sorted(
            (resolved_output_dir / "confidence_interval_payloads").rglob("*.json")
        )
    )
    if not run_manifest_files:
        raise ValueError(
            f"no run manifest files found under {resolved_output_dir / 'run_manifests'}"
        )
    if not metric_payload_files:
        raise ValueError(
            f"no metric payload files found under {resolved_output_dir / 'metric_payloads'}"
        )
    if not confidence_interval_files:
        raise ValueError(
            "no confidence interval payload files found under "
            f"{resolved_output_dir / 'confidence_interval_payloads'}"
        )
    return (
        run_manifest_files,
        metric_payload_files,
        confidence_interval_files,
    )


def _missing_required_metric_names(
    present_metric_names: set[str],
) -> tuple[str, ...]:
    return tuple(
        metric_name
        for metric_name in RETRIEVAL_METRIC_NAMES
        if metric_name not in present_metric_names
    )


def _validate_reporting_slice_completeness(
    *,
    run_manifest_index: dict[str, tuple[Path, BenchmarkModelRunManifest]],
    baseline_index: dict[str, object],
    slice_keys: tuple[tuple[str, str], ...],
    metrics_by_run_slice: dict[
        tuple[str, str, str],
        list[tuple[Path, BenchmarkMetricOutputPayload]],
    ],
    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ],
) -> None:
    incomplete_slices: list[str] = []
    for run_id, (_, run_manifest) in sorted(
        run_manifest_index.items(),
        key=lambda item: (item[1][1].baseline_id, item[0]),
    ):
        baseline = baseline_index[run_manifest.baseline_id]
        expected_slice_keys = tuple(
            (entity_type, horizon)
            for entity_type, horizon in slice_keys
            if entity_type in baseline.entity_types
        )
        for entity_type, horizon in expected_slice_keys:
            metric_items = metrics_by_run_slice.get((run_id, entity_type, horizon), [])
            missing_metric_names = _missing_required_metric_names(
                {
                    metric_payload.metric_name
                    for _, metric_payload in metric_items
                }
            )
            missing_interval_names = tuple(
                metric_name
                for metric_name in RETRIEVAL_METRIC_NAMES
                if (run_id, entity_type, horizon, metric_name) not in interval_index
            )
            if not missing_metric_names and not missing_interval_names:
                continue

            detail_parts: list[str] = []
            if missing_metric_names:
                detail_parts.append(
                    "missing metric payloads: "
                    + ", ".join(missing_metric_names)
                )
            if missing_interval_names:
                detail_parts.append(
                    "missing confidence interval payloads: "
                    + ", ".join(missing_interval_names)
                )
            incomplete_slices.append(
                f"{run_id}/{entity_type}/{horizon} ({'; '.join(detail_parts)})"
            )

    if incomplete_slices:
        raise ValueError(
            "incomplete benchmark runner output for reporting: "
            + "; ".join(incomplete_slices)
        )


def _build_report_card_path(
    output_dir: Path,
    *,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    snapshot_id: str,
    run_id: str,
) -> Path:
    return (
        output_dir
        / "report_cards"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id)
        / f"{run_id}.json"
    )


def _build_leaderboard_path(
    output_dir: Path,
    *,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    snapshot_id: str,
    entity_type: str,
    horizon: str,
    metric_name: str,
) -> Path:
    return (
        output_dir
        / "leaderboards"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id)
        / _normalize_component(entity_type)
        / _normalize_component(horizon)
        / f"{_normalize_component(metric_name)}.json"
    )


def _build_error_analysis_path(
    output_dir: Path,
    *,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    snapshot_id: str,
    run_id: str,
) -> Path:
    return (
        output_dir
        / "error_analysis"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id)
        / f"{run_id}.md"
    )


def _build_error_analysis_json_path(
    output_dir: Path,
    *,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    snapshot_id: str,
    run_id: str,
) -> Path:
    return (
        output_dir
        / "error_analysis"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id)
        / f"{run_id}.json"
    )


def _clear_reporting_snapshot_outputs(
    output_dir: Path,
    *,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    snapshot_id: str,
) -> None:
    snapshot_dirs = (
        output_dir
        / "report_cards"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id),
        output_dir
        / "leaderboards"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id),
        output_dir
        / "error_analysis"
        / _normalize_component(benchmark_suite_id)
        / _normalize_component(benchmark_task_id)
        / _normalize_component(snapshot_id),
    )
    for snapshot_dir in snapshot_dirs:
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir)


def _artifact_path_for_name(
    artifacts: tuple[InputArtifactReference, ...],
    artifact_name: str,
) -> Path | None:
    for artifact in artifacts:
        if artifact.artifact_name == artifact_name:
            return Path(artifact.artifact_path).resolve()
    return None


def _cohort_entity_labels(
    cohort_labels: tuple[object, ...],
    *,
    entity_type: str,
) -> dict[str, str]:
    entity_labels: dict[str, str] = {}
    for label in cohort_labels:
        if getattr(label, "entity_type", "") != entity_type:
            continue
        entity_id = str(getattr(label, "entity_id", ""))
        entity_label = str(getattr(label, "entity_label", ""))
        if entity_id not in entity_labels:
            entity_labels[entity_id] = entity_label
            continue
        if entity_labels[entity_id] != entity_label:
            raise ValueError(
                "cohort labels must keep a stable entity_label per entity_type/entity_id: "
                f"{entity_type}/{entity_id}"
            )
    return entity_labels


def _render_intervention_object_error_analysis(
    *,
    manifest: object,
    run_manifest: BenchmarkModelRunManifest,
    bundle_path: Path,
    projection_path: Path,
    cohort_labels: tuple[object, ...],
    horizon: str,
) -> str:
    source_archive_index_path = _artifact_path_for_name(
        run_manifest.input_artifacts,
        "source_archive_index",
    )
    if source_archive_index_path is None:
        raise ValueError(
            "run manifest is missing source_archive_index required for "
            "intervention-object error analysis validation"
        )
    archive_descriptors = load_source_archive_descriptors(source_archive_index_path)
    expected_source_snapshot_provenance_json = (
        build_intervention_object_bundle_source_snapshot_provenance(
            getattr(manifest, "source_snapshots"),
            archive_descriptors,
        )
    )
    expected_included_sources = tuple(
        sorted(
            source_snapshot.source_name
            for source_snapshot in getattr(manifest, "source_snapshots")
            if source_snapshot.included
        )
    )
    expected_excluded_sources = tuple(
        sorted(
            source_snapshot.source_name
            for source_snapshot in getattr(manifest, "source_snapshots")
            if not source_snapshot.included
        )
    )
    bundle_rows = read_intervention_object_feature_bundle(
        bundle_path,
        expected_as_of_date=str(getattr(manifest, "as_of_date")),
        expected_entities=_cohort_entity_labels(
            cohort_labels,
            entity_type=INTERVENTION_OBJECT_ENTITY_TYPE,
        ),
        expected_included_sources=expected_included_sources,
        expected_excluded_sources=expected_excluded_sources,
        expected_source_snapshot_provenance_json=(
            expected_source_snapshot_provenance_json
        ),
    )
    bundle_index = {
        str(row["entity_id"]): row
        for row in bundle_rows
    }
    projection_payload = read_intervention_object_projection_payload(projection_path)
    projection_rows = projection_payload.get("rows", [])
    if not isinstance(projection_rows, list):
        raise ValueError("projection payload rows must be a list")
    relevance_index = build_positive_relevance_index(
        cohort_labels,
        entity_type=INTERVENTION_OBJECT_ENTITY_TYPE,
        horizon=horizon,
    )
    positives = {
        entity_id for entity_id, relevant in relevance_index.items() if relevant
    }
    ranked_rows = [
        row
        for row in projection_rows
        if isinstance(row, dict) and bool(row.get("covered"))
    ]
    ranked_rows.sort(
        key=lambda row: (
            int(row.get("rank") or 999999),
            str(row.get("entity_label", "")).lower(),
            str(row.get("entity_id", "")),
        )
    )
    false_positives = [
        row for row in ranked_rows[:5] if str(row.get("entity_id")) not in positives
    ]
    true_positives = [
        row for row in ranked_rows[:5] if str(row.get("entity_id")) in positives
    ]
    false_negatives = [
        row
        for row in projection_rows
        if isinstance(row, dict)
        and str(row.get("entity_id")) in positives
        and not bool(row.get("covered"))
    ]

    def render_row(row: dict[str, object]) -> str:
        bundle_row = bundle_index.get(str(row.get("entity_id")), {})
        return (
            f"- rank={row.get('rank', '-')}, score={row.get('projected_score', '-')}, "
            f"{row.get('entity_label', row.get('entity_id'))} "
            f"[domain={bundle_row.get('domain', '')}; "
            f"stage={bundle_row.get('stage_bucket', '')}; "
            f"target={bundle_row.get('target', '')}]"
        )

    lines = [
        f"# Track A Error Analysis: {run_manifest.run_id}",
        "",
        f"- baseline: `{run_manifest.baseline_id}`",
        f"- principal horizon: `{horizon}`",
        "- entity type: `intervention_object`",
        "- projection source: explicit archived gene/module baseline projection",
        "",
        "## Top True Positives",
    ]
    lines.extend(
        render_row(row) for row in true_positives
    )
    if not true_positives:
        lines.append("- none in top 5")
    lines.extend(["", "## Top False Positives"])
    lines.extend(render_row(row) for row in false_positives)
    if not false_positives:
        lines.append("- none in top 5")
    lines.extend(["", "## Missed Positives"])
    lines.extend(render_row(row) for row in false_negatives[:5])
    if not false_negatives:
        lines.append("- none")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class BenchmarkMetricSummary:
    metric_name: str
    metric_value: float
    interval_low: float
    interval_high: float
    metric_unit: str
    cohort_size: int
    confidence_level: float
    bootstrap_iterations: int
    interval_method: str
    resample_unit: str
    random_seed: int | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.metric_name, "metric_name")
        _require_text(self.metric_unit, "metric_unit")
        _require_text(self.interval_method, "interval_method")
        _require_text(self.resample_unit, "resample_unit")
        if self.cohort_size < 0:
            raise ValueError("cohort_size must be non-negative")
        if self.bootstrap_iterations <= 0:
            raise ValueError("bootstrap_iterations must be positive")
        if self.interval_low > self.interval_high:
            raise ValueError("interval_low cannot exceed interval_high")

    def to_dict(self) -> dict[str, object]:
        return {
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "interval_low": self.interval_low,
            "interval_high": self.interval_high,
            "metric_unit": self.metric_unit,
            "cohort_size": self.cohort_size,
            "confidence_level": self.confidence_level,
            "bootstrap_iterations": self.bootstrap_iterations,
            "interval_method": self.interval_method,
            "resample_unit": self.resample_unit,
            "random_seed": self.random_seed,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkMetricSummary:
        random_seed = payload.get("random_seed")
        return cls(
            metric_name=str(payload["metric_name"]),
            metric_value=float(payload["metric_value"]),
            interval_low=float(payload["interval_low"]),
            interval_high=float(payload["interval_high"]),
            metric_unit=str(payload["metric_unit"]),
            cohort_size=int(payload["cohort_size"]),
            confidence_level=float(payload["confidence_level"]),
            bootstrap_iterations=int(payload["bootstrap_iterations"]),
            interval_method=str(payload["interval_method"]),
            resample_unit=str(payload["resample_unit"]),
            random_seed=None if random_seed in {None, ""} else int(random_seed),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkReportCardSlice:
    entity_type: str
    horizon: str
    admissible_entity_count: int
    positive_entity_count: int
    covered_entity_count: int | None
    metrics: tuple[BenchmarkMetricSummary, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.entity_type, "entity_type")
        _require_text(self.horizon, "horizon")
        if self.admissible_entity_count < 0:
            raise ValueError("admissible_entity_count must be non-negative")
        if self.positive_entity_count < 0:
            raise ValueError("positive_entity_count must be non-negative")
        if self.covered_entity_count is not None and self.covered_entity_count < 0:
            raise ValueError("covered_entity_count must be non-negative when present")
        if not self.metrics:
            raise ValueError("metrics must contain at least one metric summary")

    def to_dict(self) -> dict[str, object]:
        return {
            "entity_type": self.entity_type,
            "horizon": self.horizon,
            "admissible_entity_count": self.admissible_entity_count,
            "positive_entity_count": self.positive_entity_count,
            "covered_entity_count": self.covered_entity_count,
            "metrics": [metric.to_dict() for metric in self.metrics],
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkReportCardSlice:
        return cls(
            entity_type=str(payload["entity_type"]),
            horizon=str(payload["horizon"]),
            admissible_entity_count=int(payload["admissible_entity_count"]),
            positive_entity_count=int(payload["positive_entity_count"]),
            covered_entity_count=(
                None
                if payload.get("covered_entity_count") is None
                else int(payload["covered_entity_count"])
            ),
            metrics=tuple(
                BenchmarkMetricSummary.from_dict(item) for item in payload["metrics"]
            ),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkReportCardPayload:
    report_card_id: str
    benchmark_suite_id: str
    benchmark_task_id: str
    benchmark_question_id: str
    snapshot_id: str
    cohort_id: str
    run_id: str
    baseline_id: str
    baseline_label: str
    model_family: str
    baseline_status: str
    baseline_coverage_rule: str
    baseline_description: str
    code_version: str
    started_at: str
    completed_at: str
    generated_at: str
    as_of_date: str
    outcome_observation_closed_at: str
    source_snapshots: tuple[SourceSnapshot, ...]
    evaluation_input_artifacts: tuple[InputArtifactReference, ...]
    derived_from_artifacts: tuple[InputArtifactReference, ...]
    slices: tuple[BenchmarkReportCardSlice, ...]
    run_parameterization: dict[str, object] | None = None
    run_notes: str = ""
    notes: str = ""
    schema_name: str = REPORT_CARD_SCHEMA_NAME
    schema_version: str = REPORT_CARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "report_card_id",
            "benchmark_suite_id",
            "benchmark_task_id",
            "benchmark_question_id",
            "snapshot_id",
            "cohort_id",
            "run_id",
            "baseline_id",
            "baseline_label",
            "model_family",
            "baseline_status",
            "baseline_coverage_rule",
            "baseline_description",
            "code_version",
            "started_at",
            "completed_at",
            "generated_at",
            "as_of_date",
            "outcome_observation_closed_at",
            "schema_name",
            "schema_version",
        ):
            _require_text(str(getattr(self, field_name)), field_name)
        if not self.source_snapshots:
            raise ValueError("source_snapshots must contain at least one source snapshot")
        if not self.evaluation_input_artifacts:
            raise ValueError(
                "evaluation_input_artifacts must contain at least one artifact reference"
            )
        if not self.derived_from_artifacts:
            raise ValueError(
                "derived_from_artifacts must contain at least one artifact reference"
            )
        if not self.slices:
            raise ValueError("slices must contain at least one report slice")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "report_card_id": self.report_card_id,
            "benchmark_suite_id": self.benchmark_suite_id,
            "benchmark_task_id": self.benchmark_task_id,
            "benchmark_question_id": self.benchmark_question_id,
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "run_id": self.run_id,
            "baseline_id": self.baseline_id,
            "baseline_label": self.baseline_label,
            "model_family": self.model_family,
            "baseline_status": self.baseline_status,
            "baseline_coverage_rule": self.baseline_coverage_rule,
            "baseline_description": self.baseline_description,
            "code_version": self.code_version,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "generated_at": self.generated_at,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "source_snapshots": [
                source_snapshot.to_dict() for source_snapshot in self.source_snapshots
            ],
            "evaluation_input_artifacts": [
                reference.to_dict() for reference in self.evaluation_input_artifacts
            ],
            "derived_from_artifacts": [
                reference.to_dict() for reference in self.derived_from_artifacts
            ],
            "slices": [slice_report.to_dict() for slice_report in self.slices],
            "run_notes": self.run_notes,
            "notes": self.notes,
        }
        if self.run_parameterization is not None:
            payload["run_parameterization"] = self.run_parameterization
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkReportCardPayload:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            report_card_id=str(payload["report_card_id"]),
            benchmark_suite_id=str(payload["benchmark_suite_id"]),
            benchmark_task_id=str(payload["benchmark_task_id"]),
            benchmark_question_id=str(payload["benchmark_question_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            cohort_id=str(payload["cohort_id"]),
            run_id=str(payload["run_id"]),
            baseline_id=str(payload["baseline_id"]),
            baseline_label=str(payload["baseline_label"]),
            model_family=str(payload["model_family"]),
            baseline_status=str(payload["baseline_status"]),
            baseline_coverage_rule=str(payload["baseline_coverage_rule"]),
            baseline_description=str(payload["baseline_description"]),
            code_version=str(payload["code_version"]),
            started_at=str(payload["started_at"]),
            completed_at=str(payload["completed_at"]),
            generated_at=str(payload["generated_at"]),
            as_of_date=str(payload["as_of_date"]),
            outcome_observation_closed_at=str(
                payload["outcome_observation_closed_at"]
            ),
            source_snapshots=tuple(
                SourceSnapshot.from_dict(item) for item in payload["source_snapshots"]
            ),
            evaluation_input_artifacts=tuple(
                InputArtifactReference.from_dict(item)
                for item in payload["evaluation_input_artifacts"]
            ),
            derived_from_artifacts=tuple(
                InputArtifactReference.from_dict(item)
                for item in payload["derived_from_artifacts"]
            ),
            slices=tuple(
                BenchmarkReportCardSlice.from_dict(item) for item in payload["slices"]
            ),
            run_parameterization=payload.get("run_parameterization"),
            run_notes=str(payload.get("run_notes", "")),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkLeaderboardEntry:
    rank: int
    report_card_id: str
    report_card_path: str
    run_id: str
    baseline_id: str
    baseline_label: str
    model_family: str
    code_version: str
    baseline_status: str
    metric_value: float
    interval_low: float
    interval_high: float
    cohort_size: int
    admissible_entity_count: int
    positive_entity_count: int
    covered_entity_count: int | None
    notes: str = ""

    def __post_init__(self) -> None:
        if self.rank <= 0:
            raise ValueError("rank must be positive")
        for field_name in (
            "report_card_id",
            "report_card_path",
            "run_id",
            "baseline_id",
            "baseline_label",
            "model_family",
            "code_version",
            "baseline_status",
        ):
            _require_text(str(getattr(self, field_name)), field_name)
        if self.cohort_size < 0:
            raise ValueError("cohort_size must be non-negative")
        if self.admissible_entity_count < 0:
            raise ValueError("admissible_entity_count must be non-negative")
        if self.positive_entity_count < 0:
            raise ValueError("positive_entity_count must be non-negative")
        if self.covered_entity_count is not None and self.covered_entity_count < 0:
            raise ValueError("covered_entity_count must be non-negative when present")
        if self.interval_low > self.interval_high:
            raise ValueError("interval_low cannot exceed interval_high")

    def to_dict(self) -> dict[str, object]:
        return {
            "rank": self.rank,
            "report_card_id": self.report_card_id,
            "report_card_path": self.report_card_path,
            "run_id": self.run_id,
            "baseline_id": self.baseline_id,
            "baseline_label": self.baseline_label,
            "model_family": self.model_family,
            "code_version": self.code_version,
            "baseline_status": self.baseline_status,
            "metric_value": self.metric_value,
            "interval_low": self.interval_low,
            "interval_high": self.interval_high,
            "cohort_size": self.cohort_size,
            "admissible_entity_count": self.admissible_entity_count,
            "positive_entity_count": self.positive_entity_count,
            "covered_entity_count": self.covered_entity_count,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkLeaderboardEntry:
        return cls(
            rank=int(payload["rank"]),
            report_card_id=str(payload["report_card_id"]),
            report_card_path=str(payload["report_card_path"]),
            run_id=str(payload["run_id"]),
            baseline_id=str(payload["baseline_id"]),
            baseline_label=str(payload["baseline_label"]),
            model_family=str(payload["model_family"]),
            code_version=str(payload["code_version"]),
            baseline_status=str(payload["baseline_status"]),
            metric_value=float(payload["metric_value"]),
            interval_low=float(payload["interval_low"]),
            interval_high=float(payload["interval_high"]),
            cohort_size=int(payload["cohort_size"]),
            admissible_entity_count=int(payload["admissible_entity_count"]),
            positive_entity_count=int(payload["positive_entity_count"]),
            covered_entity_count=(
                None
                if payload.get("covered_entity_count") is None
                else int(payload["covered_entity_count"])
            ),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkLeaderboardPayload:
    leaderboard_id: str
    benchmark_suite_id: str
    benchmark_task_id: str
    benchmark_question_id: str
    snapshot_id: str
    cohort_id: str
    entity_type: str
    horizon: str
    metric_name: str
    metric_unit: str
    confidence_level: float
    bootstrap_iterations: int
    interval_method: str
    resample_unit: str
    as_of_date: str
    outcome_observation_closed_at: str
    generated_at: str
    report_card_files: tuple[str, ...]
    entries: tuple[BenchmarkLeaderboardEntry, ...]
    ranking_order: str = RANKING_ORDER_DESCENDING
    notes: str = ""
    schema_name: str = LEADERBOARD_SCHEMA_NAME
    schema_version: str = LEADERBOARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "leaderboard_id",
            "benchmark_suite_id",
            "benchmark_task_id",
            "benchmark_question_id",
            "snapshot_id",
            "cohort_id",
            "entity_type",
            "horizon",
            "metric_name",
            "metric_unit",
            "interval_method",
            "resample_unit",
            "as_of_date",
            "outcome_observation_closed_at",
            "generated_at",
            "ranking_order",
            "schema_name",
            "schema_version",
        ):
            _require_text(str(getattr(self, field_name)), field_name)
        if self.ranking_order != RANKING_ORDER_DESCENDING:
            raise ValueError("ranking_order must be descending")
        if self.bootstrap_iterations <= 0:
            raise ValueError("bootstrap_iterations must be positive")
        if not self.report_card_files:
            raise ValueError("report_card_files must contain at least one report card")
        if not self.entries:
            raise ValueError("entries must contain at least one leaderboard entry")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "leaderboard_id": self.leaderboard_id,
            "benchmark_suite_id": self.benchmark_suite_id,
            "benchmark_task_id": self.benchmark_task_id,
            "benchmark_question_id": self.benchmark_question_id,
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "entity_type": self.entity_type,
            "horizon": self.horizon,
            "metric_name": self.metric_name,
            "metric_unit": self.metric_unit,
            "confidence_level": self.confidence_level,
            "bootstrap_iterations": self.bootstrap_iterations,
            "interval_method": self.interval_method,
            "resample_unit": self.resample_unit,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "generated_at": self.generated_at,
            "report_card_files": list(self.report_card_files),
            "entries": [entry.to_dict() for entry in self.entries],
            "ranking_order": self.ranking_order,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkLeaderboardPayload:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            leaderboard_id=str(payload["leaderboard_id"]),
            benchmark_suite_id=str(payload["benchmark_suite_id"]),
            benchmark_task_id=str(payload["benchmark_task_id"]),
            benchmark_question_id=str(payload["benchmark_question_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            cohort_id=str(payload["cohort_id"]),
            entity_type=str(payload["entity_type"]),
            horizon=str(payload["horizon"]),
            metric_name=str(payload["metric_name"]),
            metric_unit=str(payload["metric_unit"]),
            confidence_level=float(payload["confidence_level"]),
            bootstrap_iterations=int(payload["bootstrap_iterations"]),
            interval_method=str(payload["interval_method"]),
            resample_unit=str(payload["resample_unit"]),
            as_of_date=str(payload["as_of_date"]),
            outcome_observation_closed_at=str(
                payload["outcome_observation_closed_at"]
            ),
            generated_at=str(payload["generated_at"]),
            report_card_files=tuple(
                str(path_text) for path_text in payload["report_card_files"]
            ),
            entries=tuple(
                BenchmarkLeaderboardEntry.from_dict(item)
                for item in payload["entries"]
            ),
            ranking_order=str(payload.get("ranking_order", RANKING_ORDER_DESCENDING)),
            notes=str(payload.get("notes", "")),
        )


def write_benchmark_report_card_payload(
    path: Path,
    payload: BenchmarkReportCardPayload,
) -> None:
    write_json(path, payload.to_dict())


def read_benchmark_report_card_payload(path: Path) -> BenchmarkReportCardPayload:
    payload = _require_json_mapping(path)
    return BenchmarkReportCardPayload.from_dict(payload)


def write_benchmark_leaderboard_payload(
    path: Path,
    payload: BenchmarkLeaderboardPayload,
) -> None:
    write_json(path, payload.to_dict())


def read_benchmark_leaderboard_payload(path: Path) -> BenchmarkLeaderboardPayload:
    payload = _require_json_mapping(path)
    return BenchmarkLeaderboardPayload.from_dict(payload)


def _validate_track_b_runner_outputs(
    *,
    run_manifest: BenchmarkModelRunManifest,
    validated_run_contract: _ValidatedTrackBRunContract,
    expected_as_of_date: str,
    expected_case_outputs: tuple[Any, ...],
    metric_items: list[tuple[Path, BenchmarkMetricOutputPayload]],
    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ],
    case_output_payload: Any,
    confusion_summary: Any,
) -> tuple[dict[str, float], dict[str, tuple[float, float, float]], str]:
    _validate_track_b_runner_artifact_ownership(
        run_manifest=run_manifest,
        metric_items=metric_items,
        interval_index=interval_index,
        case_output_payload=case_output_payload,
        confusion_summary=confusion_summary,
    )
    run_id = run_manifest.run_id
    validate_track_b_case_output_payload(
        case_output_payload,
        expected_as_of_date=expected_as_of_date,
    )
    validate_track_b_case_output_payload_against_expected_cases(
        case_output_payload,
        expected_cases=expected_case_outputs,
    )
    if case_output_payload.baseline_id != confusion_summary.baseline_id:
        raise ValueError(
            f"Track B runner outputs disagree on baseline_id for run_id {run_id}"
        )
    if case_output_payload.snapshot_id != confusion_summary.snapshot_id:
        raise ValueError(
            f"Track B runner outputs disagree on snapshot_id for run_id {run_id}"
        )
    expected_metric_values = score_track_b_case_outputs(case_output_payload.cases)
    expected_metric_cohort_sizes = track_b_metric_cohort_sizes(case_output_payload.cases)
    expected_confusion_summary = build_track_b_confusion_summary(
        run_id=run_id,
        baseline_id=case_output_payload.baseline_id,
        snapshot_id=case_output_payload.snapshot_id,
        case_outputs=case_output_payload.cases,
    )
    if confusion_summary != expected_confusion_summary:
        raise ValueError(
            f"Track B confusion summary does not match runner case outputs for {run_id}"
        )
    expected_slice_notes = _build_track_b_slice_notes(
        case_count=len(case_output_payload.cases),
        included_coverage_count=sum(
            1
            for case_output in case_output_payload.cases
            if case_output.coverage_state_at_cutoff == "included"
        ),
        replay_supported_case_count=sum(
            1
            for case_output in case_output_payload.cases
            if case_output.gold_replay_status == "replay_supported"
        ),
        analog_evaluable_case_count=sum(
            1
            for case_output in case_output_payload.cases
            if case_output.analog_recall_at_3 is not None
        ),
        deterministic_test_mode=validated_run_contract.deterministic_test_mode,
    )
    interval_payloads = [
        interval_index[(run_id, TRACK_B_ENTITY_TYPE, TRACK_B_HORIZON, metric_name)][1]
        for metric_name in TRACK_B_METRIC_NAMES
    ]
    for interval_payload in interval_payloads:
        if (
            interval_payload.bootstrap_iterations
            != validated_run_contract.bootstrap_iterations
        ):
            raise ValueError(
                "Track B confidence interval payload bootstrap_iterations does not "
                f"match the run manifest contract for {run_id}"
            )
        if (
            interval_payload.confidence_level
            != validated_run_contract.bootstrap_confidence_level
        ):
            raise ValueError(
                "Track B confidence interval payload confidence_level does not match "
                f"the run manifest contract for {run_id}"
            )
        if interval_payload.random_seed != validated_run_contract.slice_random_seed:
            raise ValueError(
                "Track B confidence interval payload random_seed does not match the "
                f"run manifest contract for {run_id}"
            )
        if interval_payload.resample_unit != TRACK_B_BOOTSTRAP_RESAMPLE_UNIT:
            raise ValueError(
                f"Track B confidence interval payload has unexpected resample_unit for {run_id}"
            )
        if interval_payload.notes != (
            f"method={BOOTSTRAP_INTERVAL_METHOD}; {expected_slice_notes}"
        ):
            raise ValueError(
                f"Track B confidence interval payload notes do not match runner case outputs for {run_id}"
            )
    expected_intervals = estimate_track_b_metric_intervals(
        case_output_payload.cases,
        iterations=validated_run_contract.bootstrap_iterations,
        confidence_level=validated_run_contract.bootstrap_confidence_level,
        random_seed=validated_run_contract.slice_random_seed,
    )
    for _, metric_payload in metric_items:
        if metric_payload.run_id != run_id:
            raise ValueError(
                f"Track B metric payload run_id does not match runner manifest for {run_id}"
            )
        if metric_payload.snapshot_id != case_output_payload.snapshot_id:
            raise ValueError(
                f"Track B metric payload snapshot_id does not match runner case outputs for {run_id}"
            )
        if metric_payload.baseline_id != case_output_payload.baseline_id:
            raise ValueError(
                f"Track B metric payload baseline_id does not match runner case outputs for {run_id}"
            )
        expected_metric_unit = TRACK_B_METRIC_UNITS[metric_payload.metric_name]
        if metric_payload.metric_unit != expected_metric_unit:
            raise ValueError(
                "Track B metric payload metric_unit does not match the Track B "
                f"metric contract for {run_id}/{metric_payload.metric_name}"
            )
        if metric_payload.metric_value != expected_metric_values[metric_payload.metric_name]:
            raise ValueError(
                "Track B metric payload does not match runner case outputs for "
                f"{run_id}/{metric_payload.metric_name}"
            )
        if (
            metric_payload.cohort_size
            != expected_metric_cohort_sizes[metric_payload.metric_name]
        ):
            raise ValueError(
                "Track B metric payload cohort_size does not match runner case outputs "
                f"for {run_id}/{metric_payload.metric_name}"
            )
        if metric_payload.notes != expected_slice_notes:
            raise ValueError(
                "Track B metric payload notes do not match runner case outputs for "
                f"{run_id}/{metric_payload.metric_name}"
            )
        interval_payload = interval_index[
            (run_id, TRACK_B_ENTITY_TYPE, TRACK_B_HORIZON, metric_payload.metric_name)
        ][1]
        expected_point_estimate, expected_interval_low, expected_interval_high = (
            expected_intervals[metric_payload.metric_name]
        )
        if (
            interval_payload.point_estimate,
            interval_payload.interval_low,
            interval_payload.interval_high,
        ) != (
            expected_point_estimate,
            expected_interval_low,
            expected_interval_high,
        ):
            raise ValueError(
                "Track B confidence interval payload does not match runner case "
                f"outputs for {run_id}/{metric_payload.metric_name}"
            )
    return expected_metric_values, expected_intervals, expected_slice_notes


def _validate_track_b_expected_baseline_set(
    *,
    manifest: object,
    task_contract: object,
    run_manifest_index: dict[str, tuple[Path, BenchmarkModelRunManifest]],
) -> dict[str, tuple[Path, BenchmarkModelRunManifest]]:
    baseline_index = {
        baseline.baseline_id: baseline for baseline in task_contract.protocol.baselines
    }
    expected_baseline_ids = tuple(
        baseline_id
        for baseline_id in getattr(manifest, "baseline_ids")
        if baseline_index[baseline_id].status == "available_now"
    )
    observed_by_baseline: dict[str, tuple[Path, BenchmarkModelRunManifest]] = {}
    duplicate_baseline_ids: list[str] = []
    for path, run_manifest in run_manifest_index.values():
        if run_manifest.baseline_id in observed_by_baseline:
            duplicate_baseline_ids.append(run_manifest.baseline_id)
            continue
        observed_by_baseline[run_manifest.baseline_id] = (path, run_manifest)
    missing_baseline_ids = sorted(
        set(expected_baseline_ids).difference(observed_by_baseline)
    )
    unexpected_baseline_ids = sorted(
        set(observed_by_baseline).difference(expected_baseline_ids)
    )
    if missing_baseline_ids or unexpected_baseline_ids or duplicate_baseline_ids:
        details: list[str] = []
        if missing_baseline_ids:
            details.append("missing=" + ", ".join(missing_baseline_ids))
        if unexpected_baseline_ids:
            details.append("unexpected=" + ", ".join(unexpected_baseline_ids))
        if duplicate_baseline_ids:
            details.append(
                "duplicate=" + ", ".join(sorted(set(duplicate_baseline_ids)))
            )
        raise ValueError(
            "Track B reporting requires the complete expected baseline set"
            + (f" ({'; '.join(details)})" if details else "")
        )
    return {
        baseline_id: observed_by_baseline[baseline_id]
        for baseline_id in expected_baseline_ids
    }


def _validate_track_b_reporting_bundle(
    *,
    manifest: object,
    manifest_file: Path,
    cohort_labels_file: Path,
    materialized_cohort: MaterializedBenchmarkCohortArtifacts,
    cohort_labels: tuple[object, ...],
    benchmark_suite_id: str,
    benchmark_task_id: str,
    task_contract: object,
    resolved_runner_output_dir: Path,
    run_manifest_index: dict[str, tuple[Path, BenchmarkModelRunManifest]],
    metrics_by_run_slice: dict[
        tuple[str, str, str],
        list[tuple[Path, BenchmarkMetricOutputPayload]],
    ],
    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ],
) -> _ValidatedTrackBReportingBundle:
    evaluation_input_artifacts = _build_track_b_expected_evaluation_input_artifacts(
        manifest_file=manifest_file,
        cohort_labels_file=cohort_labels_file,
        materialized_cohort=materialized_cohort,
    )
    source_artifact_index = {
        artifact_name: Path(artifact.artifact_path).resolve()
        for artifact_name, artifact in _index_artifacts_by_name(
            tuple(materialized_cohort.auxiliary_source_artifacts),
            context="Track B auxiliary source artifacts",
        ).items()
    }
    expected_cases = load_track_b_casebook(
        source_artifact_index["track_b_casebook"],
        as_of_date=str(getattr(manifest, "as_of_date")),
        program_universe_path=source_artifact_index["track_b_program_universe"],
        events_path=source_artifact_index["track_b_program_history_events"],
    )
    expected_dataset = build_track_b_program_memory_dataset(
        as_of_date=str(getattr(manifest, "as_of_date")),
        events_path=source_artifact_index["track_b_program_history_events"],
        dataset_dir=source_artifact_index["track_b_casebook"].parent,
    )
    expected_cohort_surface_from_cases = tuple(
        sorted(
            (
                case.proposal_entity_id,
                case.proposal_entity_label,
                case.gold_replay_status,
            )
            for case in expected_cases
        )
    )
    cohort_entity_labels: dict[str, str] = {}
    cohort_replay_statuses: dict[str, str] = {}
    for label in cohort_labels:
        if str(getattr(label, "entity_type")) != TRACK_B_ENTITY_TYPE:
            continue
        if str(getattr(label, "horizon")) != TRACK_B_HORIZON:
            raise ValueError(
                "Track B reporting requires benchmark_cohort_labels to use the "
                f"{TRACK_B_HORIZON} horizon"
            )
        entity_id = str(getattr(label, "entity_id"))
        entity_label = str(getattr(label, "entity_label"))
        existing_label = cohort_entity_labels.get(entity_id)
        if existing_label is not None and existing_label != entity_label:
            raise ValueError(
                "Track B reporting requires a stable entity_label per cohort entity"
            )
        cohort_entity_labels[entity_id] = entity_label
        if str(getattr(label, "label_value")) == "true":
            if entity_id in cohort_replay_statuses:
                raise ValueError(
                    "Track B reporting requires exactly one observed replay-status "
                    "label per cohort entity"
                )
            cohort_replay_statuses[entity_id] = str(getattr(label, "label_name"))
    observed_cohort_surface = tuple(
        sorted(
            (
                entity_id,
                cohort_entity_labels[entity_id],
                cohort_replay_statuses[entity_id],
            )
            for entity_id in cohort_entity_labels
            if entity_id in cohort_replay_statuses
        )
    )
    if observed_cohort_surface != expected_cohort_surface_from_cases:
        raise ValueError(
            "Track B reporting requires the materialized cohort bundle to match the "
            "pinned Track B casebook entity ids, labels, and replay-status labels"
        )
    baseline_index = {
        baseline.baseline_id: baseline for baseline in task_contract.protocol.baselines
    }
    casebook_sha256 = next(
        artifact.sha256
        for artifact in evaluation_input_artifacts
        if artifact.artifact_name == "track_b_casebook"
    )
    validated_run_contracts_by_run_id: dict[str, _ValidatedTrackBRunContract] = {}
    for run_manifest_path, run_manifest in sorted(
        run_manifest_index.values(),
        key=lambda item: (item[1].baseline_id, item[1].run_id),
    ):
        baseline = baseline_index.get(run_manifest.baseline_id)
        if baseline is None:
            raise ValueError(
                f"Track B run manifest baseline_id is not part of the Track B contract for {run_manifest.run_id}"
            )
        validated_run_contracts_by_run_id[run_manifest.run_id] = (
            _validate_track_b_run_manifest_ownership(
                manifest=manifest,
                run_manifest=run_manifest,
                benchmark_suite_id=benchmark_suite_id,
                benchmark_task_id=benchmark_task_id,
                baseline=baseline,
                expected_case_count=len(expected_cases),
                expected_casebook_sha256=casebook_sha256,
            )
        )
        _validate_track_b_input_artifact_contract(
            run_manifest=run_manifest,
            expected_input_artifacts=evaluation_input_artifacts,
        )
    run_manifests_by_baseline = _validate_track_b_expected_baseline_set(
        manifest=manifest,
        task_contract=task_contract,
        run_manifest_index=run_manifest_index,
    )
    validated_runs_by_run_id: dict[str, _ValidatedTrackBRunBundle] = {}
    for baseline_id, (run_manifest_path, run_manifest) in sorted(
        run_manifests_by_baseline.items()
    ):
        validated_run_contract = validated_run_contracts_by_run_id[run_manifest.run_id]
        metric_items = metrics_by_run_slice.get(
            (run_manifest.run_id, TRACK_B_ENTITY_TYPE, TRACK_B_HORIZON)
        )
        if not metric_items:
            raise ValueError(
                f"Track B reporting is missing metric payloads for run_id {run_manifest.run_id}"
            )
        present_metric_names = {
            metric_payload.metric_name for _, metric_payload in metric_items
        }
        unexpected_metric_names = tuple(
            metric_name
            for metric_name in sorted(present_metric_names)
            if metric_name not in TRACK_B_METRIC_NAMES
        )
        missing_metric_names = tuple(
            metric_name
            for metric_name in TRACK_B_METRIC_NAMES
            if metric_name not in present_metric_names
        )
        missing_interval_names = tuple(
            metric_name
            for metric_name in TRACK_B_METRIC_NAMES
            if (
                run_manifest.run_id,
                TRACK_B_ENTITY_TYPE,
                TRACK_B_HORIZON,
                metric_name,
            )
            not in interval_index
        )
        if unexpected_metric_names or missing_metric_names or missing_interval_names:
            details: list[str] = []
            if unexpected_metric_names:
                details.append(
                    "unexpected metric payloads: "
                    + ", ".join(unexpected_metric_names)
                )
            if missing_metric_names:
                details.append("missing metric payloads: " + ", ".join(missing_metric_names))
            if missing_interval_names:
                details.append(
                    "missing confidence interval payloads: "
                    + ", ".join(missing_interval_names)
                )
            raise ValueError(
                "incomplete Track B reporting bundle for "
                f"{run_manifest.run_id} ({'; '.join(details)})"
            )
        case_output_file = track_b_case_output_path(
            resolved_runner_output_dir,
            run_id=run_manifest.run_id,
        )
        if not case_output_file.exists():
            raise ValueError(
                f"missing Track B case output payload for run_id {run_manifest.run_id}"
            )
        confusion_summary_file = track_b_confusion_summary_path(
            resolved_runner_output_dir,
            run_id=run_manifest.run_id,
        )
        if not confusion_summary_file.exists():
            raise ValueError(
                f"missing Track B confusion summary for run_id {run_manifest.run_id}"
            )
        case_output_payload = read_track_b_case_output_payload(case_output_file)
        confusion_summary = read_track_b_confusion_summary(confusion_summary_file)
        validated_metric_values, validated_intervals, slice_notes = (
            _validate_track_b_runner_outputs(
                run_manifest=run_manifest,
                validated_run_contract=validated_run_contract,
                expected_as_of_date=str(getattr(manifest, "as_of_date")),
                expected_case_outputs=build_track_b_case_outputs(
                    cases=expected_cases,
                    dataset=expected_dataset,
                    baseline_id=run_manifest.baseline_id,
                ),
                metric_items=metric_items,
                interval_index=interval_index,
                case_output_payload=case_output_payload,
                confusion_summary=confusion_summary,
            )
        )
        validated_runs_by_run_id[run_manifest.run_id] = _ValidatedTrackBRunBundle(
            run_manifest_path=run_manifest_path,
            run_manifest=run_manifest,
            validated_parameterization=validated_run_contract.validated_parameterization,
            metric_items=tuple(
                sorted(metric_items, key=lambda item: item[1].metric_name)
            ),
            interval_paths_by_metric={
                metric_name: interval_index[
                    (
                        run_manifest.run_id,
                        TRACK_B_ENTITY_TYPE,
                        TRACK_B_HORIZON,
                        metric_name,
                    )
                ][0]
                for metric_name in TRACK_B_METRIC_NAMES
            },
            case_output_payload=case_output_payload,
            confusion_summary=confusion_summary,
            validated_metric_values=validated_metric_values,
            validated_intervals=validated_intervals,
            slice_notes=slice_notes,
        )
    return _ValidatedTrackBReportingBundle(
        evaluation_input_artifacts=evaluation_input_artifacts,
        case_count=len(expected_cases),
        included_coverage_count=sum(
            case.coverage_state_at_cutoff == "included" for case in expected_cases
        ),
        replay_supported_case_count=sum(
            case.gold_replay_status == "replay_supported" for case in expected_cases
        ),
        validated_runs_by_run_id=validated_runs_by_run_id,
    )


def _materialize_track_b_reporting(
    *,
    manifest: object,
    manifest_file: Path,
    materialized_cohort: MaterializedBenchmarkCohortArtifacts,
    cohort_labels: tuple[object, ...],
    cohort_labels_file: Path,
    benchmark_suite_id: str,
    benchmark_task_id: str,
    task_contract: object,
    resolved_runner_output_dir: Path,
    resolved_output_dir: Path,
    resolved_generated_at: str,
    run_manifest_files: tuple[Path, ...],
    metric_payload_files: tuple[Path, ...],
    confidence_interval_files: tuple[Path, ...],
    run_manifest_index: dict[str, tuple[Path, BenchmarkModelRunManifest]],
    metric_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkMetricOutputPayload],
    ],
    metrics_by_run_slice: dict[
        tuple[str, str, str],
        list[tuple[Path, BenchmarkMetricOutputPayload]],
    ],
    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ],
) -> dict[str, object]:
    validated_bundle = _validate_track_b_reporting_bundle(
        manifest=manifest,
        manifest_file=manifest_file,
        cohort_labels_file=cohort_labels_file,
        materialized_cohort=materialized_cohort,
        cohort_labels=cohort_labels,
        benchmark_suite_id=benchmark_suite_id,
        benchmark_task_id=benchmark_task_id,
        task_contract=task_contract,
        resolved_runner_output_dir=resolved_runner_output_dir,
        run_manifest_index=run_manifest_index,
        metrics_by_run_slice=metrics_by_run_slice,
        interval_index=interval_index,
    )

    _clear_reporting_snapshot_outputs(
        resolved_output_dir,
        benchmark_suite_id=benchmark_suite_id,
        benchmark_task_id=benchmark_task_id,
        snapshot_id=str(getattr(manifest, "snapshot_id")),
    )

    report_card_records: list[tuple[BenchmarkReportCardPayload, Path]] = []
    report_card_files: list[str] = []
    error_analysis_files: list[str] = []

    baseline_index = {
        baseline.baseline_id: baseline for baseline in task_contract.protocol.baselines
    }

    for run_id, validated_run in sorted(
        validated_bundle.validated_runs_by_run_id.items(),
        key=lambda item: (item[1].run_manifest.baseline_id, item[0]),
    ):
        run_manifest_path = validated_run.run_manifest_path
        run_manifest = validated_run.run_manifest
        baseline = baseline_index[run_manifest.baseline_id]
        metric_summaries: list[BenchmarkMetricSummary] = []
        metric_paths_for_run: list[Path] = []
        interval_paths_for_run: list[Path] = []
        metric_intervals: dict[str, tuple[float, float, float]] = {}
        for metric_path, metric_payload in validated_run.metric_items:
            interval_path = validated_run.interval_paths_by_metric[
                metric_payload.metric_name
            ]
            interval_payload = interval_index[
                (
                    metric_payload.run_id,
                    metric_payload.entity_type,
                    metric_payload.horizon,
                    metric_payload.metric_name,
                )
            ][1]
            metric_paths_for_run.append(metric_path)
            interval_paths_for_run.append(interval_path)
            point_estimate, interval_low, interval_high = validated_run.validated_intervals[
                metric_payload.metric_name
            ]
            metric_summaries.append(
                BenchmarkMetricSummary(
                    metric_name=metric_payload.metric_name,
                    metric_value=validated_run.validated_metric_values[
                        metric_payload.metric_name
                    ],
                    interval_low=interval_low,
                    interval_high=interval_high,
                    metric_unit=metric_payload.metric_unit,
                    cohort_size=metric_payload.cohort_size,
                    confidence_level=interval_payload.confidence_level,
                    bootstrap_iterations=interval_payload.bootstrap_iterations,
                    interval_method=_parse_interval_method(interval_payload.notes),
                    resample_unit=interval_payload.resample_unit,
                    random_seed=interval_payload.random_seed,
                    notes=validated_run.slice_notes,
                )
            )
            metric_intervals[metric_payload.metric_name] = (
                validated_run.validated_metric_values[metric_payload.metric_name],
                interval_low,
                interval_high,
            )

        slice_notes = validated_run.slice_notes if validated_run.metric_items else ""
        slice_report = BenchmarkReportCardSlice(
            entity_type=TRACK_B_ENTITY_TYPE,
            horizon=TRACK_B_HORIZON,
            admissible_entity_count=validated_bundle.case_count,
            positive_entity_count=validated_bundle.replay_supported_case_count,
            covered_entity_count=validated_bundle.included_coverage_count,
            metrics=tuple(metric_summaries),
            notes=slice_notes,
        )

        derived_from_artifacts = [
            _build_artifact_reference(
                artifact_name="benchmark_model_run_manifest",
                path=run_manifest_path,
                schema_name="benchmark_model_run_manifest",
                notes="Runner-emitted baseline manifest consumed for Track B reporting.",
            )
        ]
        for path in sorted(metric_paths_for_run):
            derived_from_artifacts.append(
                _build_artifact_reference(
                    artifact_name="benchmark_metric_output_payload",
                    path=path,
                    schema_name="benchmark_metric_output_payload",
                )
            )
        for path in sorted(interval_paths_for_run):
            derived_from_artifacts.append(
                _build_artifact_reference(
                    artifact_name="benchmark_confidence_interval_payload",
                    path=path,
                    schema_name="benchmark_confidence_interval_payload",
                )
            )
        case_output_payload_path = track_b_case_output_path(
            resolved_runner_output_dir,
            run_id=run_id,
        )
        confusion_summary_path = track_b_confusion_summary_path(
            resolved_runner_output_dir,
            run_id=run_id,
        )
        derived_from_artifacts.append(
            _build_artifact_reference(
                artifact_name="track_b_case_output_payload",
                path=case_output_payload_path,
                schema_name=TRACK_B_CASE_OUTPUT_SCHEMA_NAME,
                notes="Structured per-case Track B runner outputs.",
            )
        )
        derived_from_artifacts.append(
            _build_artifact_reference(
                artifact_name="track_b_confusion_summary",
                path=confusion_summary_path,
                schema_name=TRACK_B_CONFUSION_SCHEMA_NAME,
                notes="Track B confusion summary derived directly from the runner case outputs.",
            )
        )

        report_card = BenchmarkReportCardPayload(
            report_card_id=run_id,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            benchmark_question_id=str(getattr(manifest, "benchmark_question_id")),
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            cohort_id=str(getattr(manifest, "cohort_id")),
            run_id=run_manifest.run_id,
            baseline_id=run_manifest.baseline_id,
            baseline_label=baseline.label,
            model_family=run_manifest.model_family,
            baseline_status=baseline.status,
            baseline_coverage_rule=baseline.coverage_rule,
            baseline_description=baseline.description,
            code_version=run_manifest.code_version,
            started_at=run_manifest.started_at,
            completed_at=run_manifest.completed_at,
            generated_at=resolved_generated_at,
            as_of_date=str(getattr(manifest, "as_of_date")),
            outcome_observation_closed_at=str(
                getattr(manifest, "outcome_observation_closed_at")
            ),
            source_snapshots=tuple(getattr(manifest, "source_snapshots")),
            evaluation_input_artifacts=validated_bundle.evaluation_input_artifacts,
            derived_from_artifacts=tuple(derived_from_artifacts),
            slices=(slice_report,),
            run_parameterization=validated_run.validated_parameterization,
            run_notes=run_manifest.notes,
            notes=(
                "Derived from Track B runner artifacts without rerunning structural replay scoring."
            ),
        )
        report_card_path = _build_report_card_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            run_id=run_id,
        )
        write_benchmark_report_card_payload(report_card_path, report_card)
        report_card_records.append((report_card, report_card_path))
        report_card_files.append(str(report_card_path))

        case_output_payload = validated_run.case_output_payload
        confusion_summary = validated_run.confusion_summary
        error_analysis_path = _build_error_analysis_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            run_id=run_id,
        )
        error_analysis_path.parent.mkdir(parents=True, exist_ok=True)
        error_analysis_path.write_text(
            build_track_b_error_analysis_markdown(
                payload=case_output_payload,
                confusion_summary=confusion_summary,
                metric_intervals=metric_intervals,
            ),
            encoding="utf-8",
        )
        error_analysis_files.append(str(error_analysis_path))

        error_analysis_json_path = _build_error_analysis_json_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            run_id=run_id,
        )
        write_json(error_analysis_json_path, confusion_summary.to_dict())
        error_analysis_files.append(str(error_analysis_json_path))

    leaderboard_groups: dict[
        tuple[str, str, str],
        list[
            tuple[
                BenchmarkReportCardPayload,
                Path,
                BenchmarkReportCardSlice,
                BenchmarkMetricSummary,
            ]
        ],
    ] = {}
    for report_card, report_card_path in report_card_records:
        for slice_report in report_card.slices:
            for metric_summary in slice_report.metrics:
                leaderboard_groups.setdefault(
                    (
                        slice_report.entity_type,
                        slice_report.horizon,
                        metric_summary.metric_name,
                    ),
                    [],
                ).append(
                    (
                        report_card,
                        report_card_path,
                        slice_report,
                        metric_summary,
                    )
                )

    leaderboard_payload_files: list[str] = []
    for (
        entity_type,
        horizon,
        metric_name,
    ), group in sorted(leaderboard_groups.items()):
        if not group:
            continue
        metric_unit = group[0][3].metric_unit
        confidence_level = group[0][3].confidence_level
        bootstrap_iterations = group[0][3].bootstrap_iterations
        interval_method = group[0][3].interval_method
        resample_unit = group[0][3].resample_unit
        for _, _, _, metric_summary in group:
            if metric_summary.metric_unit != metric_unit:
                raise ValueError(
                    f"inconsistent metric_unit for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.confidence_level != confidence_level:
                raise ValueError(
                    "inconsistent confidence_level for leaderboard slice "
                    f"{entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.bootstrap_iterations != bootstrap_iterations:
                raise ValueError(
                    "inconsistent bootstrap_iterations for leaderboard slice "
                    f"{entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.interval_method != interval_method:
                raise ValueError(
                    f"inconsistent interval_method for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.resample_unit != resample_unit:
                raise ValueError(
                    f"inconsistent resample_unit for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )
        sorted_group = sorted(
            group,
            key=lambda item: (
                -item[3].metric_value,
                item[0].baseline_id,
                item[0].run_id,
            ),
        )
        entries: list[BenchmarkLeaderboardEntry] = []
        for rank, (
            report_card,
            report_card_path,
            slice_report,
            metric_summary,
        ) in enumerate(sorted_group, start=1):
            entries.append(
                BenchmarkLeaderboardEntry(
                    rank=rank,
                    report_card_id=report_card.report_card_id,
                    report_card_path=str(report_card_path),
                    run_id=report_card.run_id,
                    baseline_id=report_card.baseline_id,
                    baseline_label=report_card.baseline_label,
                    model_family=report_card.model_family,
                    code_version=report_card.code_version,
                    baseline_status=report_card.baseline_status,
                    metric_value=metric_summary.metric_value,
                    interval_low=metric_summary.interval_low,
                    interval_high=metric_summary.interval_high,
                    cohort_size=metric_summary.cohort_size,
                    admissible_entity_count=slice_report.admissible_entity_count,
                    positive_entity_count=slice_report.positive_entity_count,
                    covered_entity_count=slice_report.covered_entity_count,
                    notes=slice_report.notes,
                )
            )
        leaderboard_payload = BenchmarkLeaderboardPayload(
            leaderboard_id="__".join(
                (
                    _normalize_component(benchmark_suite_id),
                    _normalize_component(benchmark_task_id),
                    _normalize_component(str(getattr(manifest, "snapshot_id"))),
                    _normalize_component(entity_type),
                    _normalize_component(horizon),
                    _normalize_component(metric_name),
                )
            ),
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            benchmark_question_id=str(getattr(manifest, "benchmark_question_id")),
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            cohort_id=str(getattr(manifest, "cohort_id")),
            entity_type=entity_type,
            horizon=horizon,
            metric_name=metric_name,
            metric_unit=metric_unit,
            confidence_level=confidence_level,
            bootstrap_iterations=bootstrap_iterations,
            interval_method=interval_method,
            resample_unit=resample_unit,
            as_of_date=str(getattr(manifest, "as_of_date")),
            outcome_observation_closed_at=str(
                getattr(manifest, "outcome_observation_closed_at")
            ),
            generated_at=resolved_generated_at,
            report_card_files=tuple(
                str(report_card_path) for _, report_card_path, _, _ in sorted_group
            ),
            entries=tuple(entries),
            notes=(
                "Derived from Track B report cards, which themselves are derived from "
                "runner-emitted benchmark artifacts."
            ),
        )
        leaderboard_path = _build_leaderboard_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=str(getattr(manifest, "snapshot_id")),
            entity_type=entity_type,
            horizon=horizon,
            metric_name=metric_name,
        )
        write_benchmark_leaderboard_payload(leaderboard_path, leaderboard_payload)
        leaderboard_payload_files.append(str(leaderboard_path))

    return {
        "benchmark_suite_id": benchmark_suite_id,
        "benchmark_task_id": benchmark_task_id,
        "benchmark_question_id": str(getattr(manifest, "benchmark_question_id")),
        "snapshot_id": str(getattr(manifest, "snapshot_id")),
        "cohort_id": str(getattr(manifest, "cohort_id")),
        "runner_output_dir": str(resolved_runner_output_dir),
        "output_dir": str(resolved_output_dir),
        "report_card_files": sorted(report_card_files),
        "leaderboard_payload_files": sorted(leaderboard_payload_files),
        "error_analysis_files": sorted(error_analysis_files),
        "discovered_run_manifest_files": [str(path) for path in run_manifest_files],
        "discovered_metric_payload_files": [str(path) for path in metric_payload_files],
        "discovered_confidence_interval_files": [
            str(path) for path in confidence_interval_files
        ],
    }


def materialize_benchmark_reporting(
    *,
    manifest_file: Path,
    cohort_labels_file: Path,
    runner_output_dir: Path,
    output_dir: Path,
    generated_at: str | None = None,
) -> dict[str, object]:
    manifest = read_benchmark_snapshot_manifest(manifest_file.resolve())
    materialized_cohort = load_materialized_benchmark_cohort_artifacts(
        snapshot_manifest=manifest,
        snapshot_manifest_file=manifest_file.resolve(),
        cohort_labels_file=cohort_labels_file.resolve(),
    )
    cohort_labels = materialized_cohort.cohort_labels
    resolved_runner_output_dir = runner_output_dir.resolve()
    resolved_output_dir = output_dir.resolve()
    resolved_generated_at = generated_at or _utc_now()
    task_registry_path = _task_registry_path_from_manifest(manifest)
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=manifest.benchmark_task_id or None,
        benchmark_question_id=manifest.benchmark_question_id,
        benchmark_suite_id=manifest.benchmark_suite_id or None,
        entity_types=manifest.entity_types,
        baseline_ids=manifest.baseline_ids,
        task_registry_path=task_registry_path,
    )
    benchmark_suite_id = manifest.benchmark_suite_id or task_contract.suite_id
    benchmark_task_id = manifest.benchmark_task_id or task_contract.task_id
    baseline_index = {
        baseline.baseline_id: baseline for baseline in task_contract.protocol.baselines
    }

    for label in cohort_labels:
        if label.snapshot_id != manifest.snapshot_id:
            raise ValueError(
                "cohort label snapshot_id must match the supplied snapshot manifest"
            )
        if label.cohort_id != manifest.cohort_id:
            raise ValueError(
                "cohort label cohort_id must match the supplied snapshot manifest"
            )

    (
        run_manifest_files,
        metric_payload_files,
        confidence_interval_files,
    ) = _discover_runner_artifact_files(resolved_runner_output_dir)

    run_manifest_index: dict[str, tuple[Path, BenchmarkModelRunManifest]] = {}
    for path in run_manifest_files:
        run_manifest = read_benchmark_model_run_manifest(path)
        if run_manifest.snapshot_id != manifest.snapshot_id:
            raise ValueError(
                f"run manifest {path} does not match snapshot_id {manifest.snapshot_id}"
            )
        if run_manifest.run_id in run_manifest_index:
            raise ValueError(f"duplicate run_id found in runner outputs: {run_manifest.run_id}")
        run_manifest_index[run_manifest.run_id] = (path, run_manifest)

    metric_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkMetricOutputPayload],
    ] = {}
    metrics_by_run_slice: dict[
        tuple[str, str, str],
        list[tuple[Path, BenchmarkMetricOutputPayload]],
    ] = {}
    for path in metric_payload_files:
        metric_payload = read_benchmark_metric_output_payload(path)
        if metric_payload.snapshot_id != manifest.snapshot_id:
            raise ValueError(
                f"metric payload {path} does not match snapshot_id {manifest.snapshot_id}"
            )
        if metric_payload.run_id not in run_manifest_index:
            raise ValueError(
                f"metric payload {path} references unknown run_id {metric_payload.run_id}"
            )
        key = (
            metric_payload.run_id,
            metric_payload.entity_type,
            metric_payload.horizon,
            metric_payload.metric_name,
        )
        if key in metric_index:
            raise ValueError(f"duplicate metric payload for key {key}")
        metric_index[key] = (path, metric_payload)
        metrics_by_run_slice.setdefault(
            (metric_payload.run_id, metric_payload.entity_type, metric_payload.horizon),
            [],
        ).append((path, metric_payload))

    interval_index: dict[
        tuple[str, str, str, str],
        tuple[Path, BenchmarkConfidenceIntervalPayload],
    ] = {}
    for path in confidence_interval_files:
        interval_payload = read_benchmark_confidence_interval_payload(path)
        if interval_payload.snapshot_id != manifest.snapshot_id:
            raise ValueError(
                "confidence interval payload "
                f"{path} does not match snapshot_id {manifest.snapshot_id}"
            )
        if interval_payload.run_id not in run_manifest_index:
            raise ValueError(
                "confidence interval payload "
                f"{path} references unknown run_id {interval_payload.run_id}"
            )
        key = (
            interval_payload.run_id,
            interval_payload.entity_type,
            interval_payload.horizon,
            interval_payload.metric_name,
        )
        if key in interval_index:
            raise ValueError(f"duplicate confidence interval payload for key {key}")
        interval_index[key] = (path, interval_payload)

    if is_track_b_task(benchmark_task_id):
        return _materialize_track_b_reporting(
            manifest=manifest,
            manifest_file=manifest_file.resolve(),
            materialized_cohort=materialized_cohort,
            cohort_labels=cohort_labels,
            cohort_labels_file=cohort_labels_file.resolve(),
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            task_contract=task_contract,
            resolved_runner_output_dir=resolved_runner_output_dir,
            resolved_output_dir=resolved_output_dir,
            resolved_generated_at=resolved_generated_at,
            run_manifest_files=run_manifest_files,
            metric_payload_files=metric_payload_files,
            confidence_interval_files=confidence_interval_files,
            run_manifest_index=run_manifest_index,
            metric_index=metric_index,
            metrics_by_run_slice=metrics_by_run_slice,
            interval_index=interval_index,
        )

    label_counts: dict[tuple[str, str], tuple[int, int]] = {}
    slice_keys = tuple(
        sorted(
            {
                (label.entity_type, label.horizon)
                for label in cohort_labels
            }
        )
    )
    for entity_type, horizon in slice_keys:
        admissible_entity_ids = {
            label.entity_id
            for label in cohort_labels
            if label.entity_type == entity_type and label.horizon == horizon
        }
        positive_count = sum(
            build_positive_relevance_index(
                cohort_labels,
                entity_type=entity_type,
                horizon=horizon,
            ).values()
        )
        label_counts[(entity_type, horizon)] = (
            len(admissible_entity_ids),
            positive_count,
        )

    _validate_reporting_slice_completeness(
        run_manifest_index=run_manifest_index,
        baseline_index=baseline_index,
        slice_keys=slice_keys,
        metrics_by_run_slice=metrics_by_run_slice,
        interval_index=interval_index,
    )
    _clear_reporting_snapshot_outputs(
        resolved_output_dir,
        benchmark_suite_id=benchmark_suite_id,
        benchmark_task_id=benchmark_task_id,
        snapshot_id=manifest.snapshot_id,
    )

    report_card_records: list[tuple[BenchmarkReportCardPayload, Path]] = []
    report_card_files: list[str] = []
    error_analysis_files: list[str] = []

    for run_id, (
        run_manifest_path,
        run_manifest,
    ) in sorted(
        run_manifest_index.items(),
        key=lambda item: (item[1][1].baseline_id, item[0]),
    ):
        baseline = baseline_index[run_manifest.baseline_id]
        slice_reports: list[BenchmarkReportCardSlice] = []
        metric_paths_for_run: list[Path] = []
        interval_paths_for_run: list[Path] = []

        for (
            slice_run_id,
            entity_type,
            horizon,
        ), metric_items in sorted(
            metrics_by_run_slice.items(),
            key=lambda item: (item[0][1], item[0][2], item[0][0]),
        ):
            if slice_run_id != run_id:
                continue
            sorted_metric_items = sorted(metric_items, key=lambda item: item[1].metric_name)
            metric_summaries: list[BenchmarkMetricSummary] = []
            slice_notes = sorted_metric_items[0][1].notes
            covered_entity_count: int | None = None
            parsed_counts = _parse_slice_counts(slice_notes)
            if parsed_counts is not None:
                covered_entity_count, parsed_admissible_count, parsed_positive_count = parsed_counts
            else:
                parsed_admissible_count = None
                parsed_positive_count = None

            admissible_entity_count, positive_entity_count = label_counts[
                (entity_type, horizon)
            ]
            if (
                parsed_admissible_count is not None
                and parsed_admissible_count != admissible_entity_count
            ):
                raise ValueError(
                    "metric notes admissible count does not match cohort labels for "
                    f"{run_id}/{entity_type}/{horizon}"
                )
            if (
                parsed_positive_count is not None
                and parsed_positive_count != positive_entity_count
            ):
                raise ValueError(
                    "metric notes positive count does not match cohort labels for "
                    f"{run_id}/{entity_type}/{horizon}"
                )

            for metric_path, metric_payload in sorted_metric_items:
                interval_key = (
                    metric_payload.run_id,
                    metric_payload.entity_type,
                    metric_payload.horizon,
                    metric_payload.metric_name,
                )
                interval_match = interval_index.get(interval_key)
                if interval_match is None:
                    raise ValueError(
                        "missing confidence interval payload for "
                        f"{run_id}/{entity_type}/{horizon}/{metric_payload.metric_name}"
                    )
                interval_path, interval_payload = interval_match
                metric_paths_for_run.append(metric_path)
                interval_paths_for_run.append(interval_path)
                if metric_payload.cohort_size != admissible_entity_count:
                    raise ValueError(
                        "metric payload cohort_size does not match the admissible "
                        f"cohort size for {run_id}/{entity_type}/{horizon}"
                    )
                metric_summaries.append(
                    BenchmarkMetricSummary(
                        metric_name=metric_payload.metric_name,
                        metric_value=metric_payload.metric_value,
                        interval_low=interval_payload.interval_low,
                        interval_high=interval_payload.interval_high,
                        metric_unit=metric_payload.metric_unit,
                        cohort_size=metric_payload.cohort_size,
                        confidence_level=interval_payload.confidence_level,
                        bootstrap_iterations=interval_payload.bootstrap_iterations,
                        interval_method=_parse_interval_method(interval_payload.notes),
                        resample_unit=interval_payload.resample_unit,
                        random_seed=interval_payload.random_seed,
                        notes=metric_payload.notes,
                    )
                )

            slice_reports.append(
                BenchmarkReportCardSlice(
                    entity_type=entity_type,
                    horizon=horizon,
                    admissible_entity_count=admissible_entity_count,
                    positive_entity_count=positive_entity_count,
                    covered_entity_count=covered_entity_count,
                    metrics=tuple(metric_summaries),
                    notes=slice_notes,
                )
            )

        if not slice_reports:
            raise ValueError(f"no metric slices found for run_id {run_id}")

        derived_from_artifacts = [
            _build_artifact_reference(
                artifact_name="benchmark_model_run_manifest",
                path=run_manifest_path,
                schema_name="benchmark_model_run_manifest",
                notes="Runner-emitted baseline manifest consumed for public report-card derivation",
            )
        ]
        for path in sorted(metric_paths_for_run):
            derived_from_artifacts.append(
                _build_artifact_reference(
                    artifact_name="benchmark_metric_output_payload",
                    path=path,
                    schema_name="benchmark_metric_output_payload",
                )
            )
        for path in sorted(interval_paths_for_run):
            derived_from_artifacts.append(
                _build_artifact_reference(
                    artifact_name="benchmark_confidence_interval_payload",
                    path=path,
                    schema_name="benchmark_confidence_interval_payload",
                )
            )

        report_card = BenchmarkReportCardPayload(
            report_card_id=run_id,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            benchmark_question_id=manifest.benchmark_question_id,
            snapshot_id=manifest.snapshot_id,
            cohort_id=manifest.cohort_id,
            run_id=run_manifest.run_id,
            baseline_id=run_manifest.baseline_id,
            baseline_label=baseline.label,
            model_family=run_manifest.model_family,
            baseline_status=baseline.status,
            baseline_coverage_rule=baseline.coverage_rule,
            baseline_description=baseline.description,
            code_version=run_manifest.code_version,
            started_at=run_manifest.started_at,
            completed_at=run_manifest.completed_at,
            generated_at=resolved_generated_at,
            as_of_date=manifest.as_of_date,
            outcome_observation_closed_at=manifest.outcome_observation_closed_at,
            source_snapshots=manifest.source_snapshots,
            evaluation_input_artifacts=run_manifest.input_artifacts,
            derived_from_artifacts=tuple(derived_from_artifacts),
            slices=tuple(
                sorted(slice_reports, key=lambda item: (item.entity_type, item.horizon))
            ),
            run_parameterization=run_manifest.parameterization,
            run_notes=run_manifest.notes,
            notes=(
                "Derived from benchmark runner artifacts without rerunning benchmark "
                "scoring logic."
            ),
        )
        report_card_path = _build_report_card_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=manifest.snapshot_id,
            run_id=run_id,
        )
        write_benchmark_report_card_payload(report_card_path, report_card)
        report_card_records.append((report_card, report_card_path))
        report_card_files.append(str(report_card_path))
        principal_intervention_slice = next(
            (
                slice_report
                for slice_report in report_card.slices
                if slice_report.entity_type == INTERVENTION_OBJECT_ENTITY_TYPE
                and slice_report.horizon == INTERVENTION_OBJECT_ERROR_ANALYSIS_HORIZON
            ),
            None,
        )
        if (
            principal_intervention_slice is not None
            and principal_intervention_slice.positive_entity_count > 0
        ):
            bundle_path = _artifact_path_for_name(
                run_manifest.input_artifacts,
                "intervention_object_feature_bundle",
            )
            projection_path = _artifact_path_for_name(
                run_manifest.input_artifacts,
                "benchmark_intervention_object_baseline_projection",
            )
            if bundle_path is not None and projection_path is not None:
                error_analysis_path = _build_error_analysis_path(
                    resolved_output_dir,
                    benchmark_suite_id=benchmark_suite_id,
                    benchmark_task_id=benchmark_task_id,
                    snapshot_id=manifest.snapshot_id,
                    run_id=run_id,
                )
                error_analysis_path.parent.mkdir(parents=True, exist_ok=True)
                error_analysis_path.write_text(
                    _render_intervention_object_error_analysis(
                        manifest=manifest,
                        run_manifest=run_manifest,
                        bundle_path=bundle_path,
                        projection_path=projection_path,
                        cohort_labels=cohort_labels,
                        horizon=principal_intervention_slice.horizon,
                    ),
                    encoding="utf-8",
                )
                error_analysis_files.append(str(error_analysis_path))

    leaderboard_groups: dict[
        tuple[str, str, str],
        list[
            tuple[
                BenchmarkReportCardPayload,
                Path,
                BenchmarkReportCardSlice,
                BenchmarkMetricSummary,
            ]
        ],
    ] = {}
    for report_card, report_card_path in report_card_records:
        for slice_report in report_card.slices:
            for metric_summary in slice_report.metrics:
                leaderboard_groups.setdefault(
                    (
                        slice_report.entity_type,
                        slice_report.horizon,
                        metric_summary.metric_name,
                    ),
                    [],
                ).append(
                    (
                        report_card,
                        report_card_path,
                        slice_report,
                        metric_summary,
                    )
                )

    leaderboard_payload_files: list[str] = []
    for (
        entity_type,
        horizon,
        metric_name,
    ), group in sorted(leaderboard_groups.items()):
        if not group:
            continue
        metric_unit = group[0][3].metric_unit
        confidence_level = group[0][3].confidence_level
        bootstrap_iterations = group[0][3].bootstrap_iterations
        interval_method = group[0][3].interval_method
        resample_unit = group[0][3].resample_unit
        for _, _, _, metric_summary in group:
            if metric_summary.metric_unit != metric_unit:
                raise ValueError(
                    f"inconsistent metric_unit for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.confidence_level != confidence_level:
                raise ValueError(
                    "inconsistent confidence_level for leaderboard slice "
                    f"{entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.bootstrap_iterations != bootstrap_iterations:
                raise ValueError(
                    "inconsistent bootstrap_iterations for leaderboard slice "
                    f"{entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.interval_method != interval_method:
                raise ValueError(
                    f"inconsistent interval_method for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )
            if metric_summary.resample_unit != resample_unit:
                raise ValueError(
                    f"inconsistent resample_unit for leaderboard slice {entity_type}/{horizon}/{metric_name}"
                )

        sorted_group = sorted(
            group,
            key=lambda item: (
                -item[3].metric_value,
                item[0].baseline_id,
                item[0].run_id,
            ),
        )
        entries: list[BenchmarkLeaderboardEntry] = []
        for rank, (
            report_card,
            report_card_path,
            slice_report,
            metric_summary,
        ) in enumerate(sorted_group, start=1):
            entries.append(
                BenchmarkLeaderboardEntry(
                    rank=rank,
                    report_card_id=report_card.report_card_id,
                    report_card_path=str(report_card_path),
                    run_id=report_card.run_id,
                    baseline_id=report_card.baseline_id,
                    baseline_label=report_card.baseline_label,
                    model_family=report_card.model_family,
                    code_version=report_card.code_version,
                    baseline_status=report_card.baseline_status,
                    metric_value=metric_summary.metric_value,
                    interval_low=metric_summary.interval_low,
                    interval_high=metric_summary.interval_high,
                    cohort_size=metric_summary.cohort_size,
                    admissible_entity_count=slice_report.admissible_entity_count,
                    positive_entity_count=slice_report.positive_entity_count,
                    covered_entity_count=slice_report.covered_entity_count,
                    notes=slice_report.notes,
                )
            )

        leaderboard_payload = BenchmarkLeaderboardPayload(
            leaderboard_id="__".join(
                (
                    _normalize_component(benchmark_suite_id),
                    _normalize_component(benchmark_task_id),
                    _normalize_component(manifest.snapshot_id),
                    _normalize_component(entity_type),
                    _normalize_component(horizon),
                    _normalize_component(metric_name),
                )
            ),
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            benchmark_question_id=manifest.benchmark_question_id,
            snapshot_id=manifest.snapshot_id,
            cohort_id=manifest.cohort_id,
            entity_type=entity_type,
            horizon=horizon,
            metric_name=metric_name,
            metric_unit=metric_unit,
            confidence_level=confidence_level,
            bootstrap_iterations=bootstrap_iterations,
            interval_method=interval_method,
            resample_unit=resample_unit,
            as_of_date=manifest.as_of_date,
            outcome_observation_closed_at=manifest.outcome_observation_closed_at,
            generated_at=resolved_generated_at,
            report_card_files=tuple(
                str(report_card_path)
                for _, report_card_path, _, _ in sorted_group
            ),
            entries=tuple(entries),
            notes=(
                "Derived from benchmark report cards, which themselves are derived from "
                "runner-emitted benchmark artifacts."
            ),
        )
        leaderboard_path = _build_leaderboard_path(
            resolved_output_dir,
            benchmark_suite_id=benchmark_suite_id,
            benchmark_task_id=benchmark_task_id,
            snapshot_id=manifest.snapshot_id,
            entity_type=entity_type,
            horizon=horizon,
            metric_name=metric_name,
        )
        write_benchmark_leaderboard_payload(leaderboard_path, leaderboard_payload)
        leaderboard_payload_files.append(str(leaderboard_path))

    return {
        "benchmark_suite_id": benchmark_suite_id,
        "benchmark_task_id": benchmark_task_id,
        "benchmark_question_id": manifest.benchmark_question_id,
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "runner_output_dir": str(resolved_runner_output_dir),
        "output_dir": str(resolved_output_dir),
        "report_card_files": sorted(report_card_files),
        "leaderboard_payload_files": sorted(leaderboard_payload_files),
        "error_analysis_files": sorted(error_analysis_files),
        "discovered_run_manifest_files": [str(path) for path in run_manifest_files],
        "discovered_metric_payload_files": [str(path) for path in metric_payload_files],
        "discovered_confidence_interval_files": [
            str(path) for path in confidence_interval_files
        ],
    }


__all__ = [
    "BenchmarkLeaderboardEntry",
    "BenchmarkLeaderboardPayload",
    "BenchmarkMetricSummary",
    "BenchmarkReportCardPayload",
    "BenchmarkReportCardSlice",
    "LEADERBOARD_SCHEMA_NAME",
    "LEADERBOARD_SCHEMA_VERSION",
    "REPORT_CARD_SCHEMA_NAME",
    "REPORT_CARD_SCHEMA_VERSION",
    "materialize_benchmark_reporting",
    "read_benchmark_leaderboard_payload",
    "read_benchmark_report_card_payload",
    "write_benchmark_leaderboard_payload",
    "write_benchmark_report_card_payload",
]
