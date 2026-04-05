from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from scz_target_engine.benchmark_labels import (
    BenchmarkCohortLabel,
    load_materialized_benchmark_cohort_artifacts,
)
from scz_target_engine.benchmark_intervention_objects import (
    INTERVENTION_OBJECT_ENTITY_TYPE,
    build_intervention_object_bundle_source_snapshot_provenance,
    build_intervention_object_projection_payload,
    intervention_object_bundle_path_for_manifest_file,
    intervention_object_projection_path,
    read_intervention_object_feature_bundle,
    write_intervention_object_projection_payload,
)
from scz_target_engine.benchmark_metrics import (
    BOOTSTRAP_INTERVAL_METHOD,
    BOOTSTRAP_RESAMPLE_UNIT,
    DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL,
    DEFAULT_BOOTSTRAP_ITERATIONS,
    DETERMINISTIC_TEST_BOOTSTRAP_ITERATIONS,
    BenchmarkConfidenceIntervalPayload,
    BenchmarkMetricOutputPayload,
    build_positive_relevance_index,
    build_ranked_evaluation_rows,
    count_relevant,
    estimate_bootstrap_intervals,
    write_benchmark_confidence_interval_payload,
    write_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_protocol import (
    AVAILABLE_NOW_STATUS,
    GENE_ENTITY_TYPE,
    MODULE_ENTITY_TYPE,
    PROTOCOL_ONLY_STATUS,
    BaselineDefinition,
    BenchmarkSnapshotManifest,
)
from scz_target_engine.benchmark_registry import resolve_benchmark_task_contract
from scz_target_engine.benchmark_snapshots import (
    SourceArchiveDescriptor,
    load_source_archive_descriptors,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_BOOTSTRAP_RESAMPLE_UNIT,
    TRACK_B_CASEBOOK_FILE_NAME,
    TRACK_B_ENTITY_TYPE,
    TRACK_B_HORIZON,
    TRACK_B_METRIC_NAMES,
    TrackBCaseOutputPayload,
    build_track_b_case_outputs,
    build_track_b_confusion_summary,
    build_track_b_program_memory_dataset,
    estimate_track_b_metric_intervals,
    is_track_b_task,
    track_b_assets_path_for_archive_index_file,
    load_track_b_casebook,
    track_b_case_output_path,
    track_b_casebook_path_for_archive_index_file,
    track_b_confusion_summary_path,
    track_b_directionality_hypotheses_path_for_archive_index_file,
    track_b_event_provenance_path_for_archive_index_file,
    track_b_events_path_for_archive_index_file,
    track_b_metric_cohort_sizes,
    track_b_program_universe_path_for_archive_index_file,
    write_track_b_case_output_payload,
    write_track_b_confusion_summary,
)
from scz_target_engine.config import EngineConfig, load_config
from scz_target_engine.decision_vector import build_decision_vectors
from scz_target_engine.io import read_csv_rows, read_json, write_json
from scz_target_engine.scoring import (
    GENE_REQUIRED_GROUPS,
    MODULE_REQUIRED_GROUPS,
    EntityRecord,
    annotate_ranked_entities,
    parse_optional_float,
    rank_records,
    run_stability_analysis,
)


RUN_MANIFEST_SCHEMA_NAME = "benchmark_model_run_manifest"
RUN_MANIFEST_SCHEMA_VERSION = "v1"
DEFAULT_RANDOM_BASELINE_SEED = 17

KNOWN_GENE_LAYER_FIELDS = (
    "common_variant_support",
    "rare_variant_support",
    "cell_state_support",
    "developmental_regulatory_support",
    "tractability_compoundability",
)
KNOWN_MODULE_LAYER_FIELDS = (
    "member_gene_genetic_enrichment",
    "cell_state_specificity",
    "developmental_regulatory_relevance",
)
KNOWN_METADATA_FIELDS = (
    "generic_platform_baseline",
)
BASELINE_SOURCE_DEPENDENCIES = {
    "pgc_only": ("PGC",),
    "schema_only": ("SCHEMA",),
    "opentargets_only": ("Open Targets",),
    "chembl_only": ("ChEMBL",),
    "v0_current": ("PGC", "SCHEMA", "PsychENCODE", "Open Targets", "ChEMBL"),
    "v1_current": ("PGC", "SCHEMA", "PsychENCODE", "Open Targets", "ChEMBL"),
    "random_with_coverage": (),
    "track_b_exact_target": (),
    "track_b_target_class": (),
    "track_b_nearest_history": (),
    "track_b_structural_current": (),
}
V1_RESOLUTION_METHOD = "mean_available_domain_head_score"


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_component(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parameter_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return sha256(encoded).hexdigest()[:8]


def _task_registry_path_from_manifest(
    manifest: BenchmarkSnapshotManifest,
) -> Path | None:
    if not manifest.task_registry_path:
        return None
    return Path(manifest.task_registry_path).resolve()


def _score_to_string(value: float | str) -> str:
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


@dataclass(frozen=True)
class InputArtifactReference:
    artifact_name: str
    artifact_path: str
    sha256: str
    schema_name: str = ""
    source_name: str = ""
    source_version: str = ""
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.artifact_name, "artifact_name")
        _require_text(self.artifact_path, "artifact_path")
        _require_text(self.sha256, "sha256")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_name": self.artifact_name,
            "artifact_path": self.artifact_path,
            "sha256": self.sha256,
            "schema_name": self.schema_name,
            "source_name": self.source_name,
            "source_version": self.source_version,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> InputArtifactReference:
        return cls(
            artifact_name=str(payload["artifact_name"]),
            artifact_path=str(payload["artifact_path"]),
            sha256=str(payload["sha256"]),
            schema_name=str(payload.get("schema_name", "")),
            source_name=str(payload.get("source_name", "")),
            source_version=str(payload.get("source_version", "")),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkModelRunManifest:
    run_id: str
    snapshot_id: str
    baseline_id: str
    model_family: str
    code_version: str
    input_artifacts: tuple[InputArtifactReference, ...]
    started_at: str
    completed_at: str
    benchmark_suite_id: str = ""
    benchmark_task_id: str = ""
    parameterization: dict[str, object] | None = None
    notes: str = ""
    schema_name: str = RUN_MANIFEST_SCHEMA_NAME
    schema_version: str = RUN_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.baseline_id, "baseline_id")
        _require_text(self.model_family, "model_family")
        _require_text(self.code_version, "code_version")
        _require_text(self.started_at, "started_at")
        _require_text(self.completed_at, "completed_at")
        if self.benchmark_suite_id:
            _require_text(self.benchmark_suite_id, "benchmark_suite_id")
        if self.benchmark_task_id:
            _require_text(self.benchmark_task_id, "benchmark_task_id")
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "baseline_id": self.baseline_id,
            "model_family": self.model_family,
            "code_version": self.code_version,
            "parameterization": self.parameterization,
            "input_artifacts": [
                artifact.to_dict() for artifact in self.input_artifacts
            ],
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "notes": self.notes,
        }
        if self.benchmark_suite_id:
            payload["benchmark_suite_id"] = self.benchmark_suite_id
        if self.benchmark_task_id:
            payload["benchmark_task_id"] = self.benchmark_task_id
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkModelRunManifest:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            run_id=str(payload["run_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            baseline_id=str(payload["baseline_id"]),
            model_family=str(payload["model_family"]),
            code_version=str(payload["code_version"]),
            benchmark_suite_id=str(payload.get("benchmark_suite_id", "")),
            benchmark_task_id=str(payload.get("benchmark_task_id", "")),
            parameterization=payload.get("parameterization"),
            input_artifacts=tuple(
                InputArtifactReference.from_dict(item)
                for item in payload["input_artifacts"]
            ),
            started_at=str(payload["started_at"]),
            completed_at=str(payload["completed_at"]),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class PredictionRow:
    entity_type: str
    entity_id: str
    entity_label: str
    score: float
    rank: int


@dataclass(frozen=True)
class BenchmarkExecutionContext:
    manifest: BenchmarkSnapshotManifest
    cohort_labels: tuple[BenchmarkCohortLabel, ...]
    config: EngineConfig
    gene_records: list[EntityRecord]
    module_records: list[EntityRecord]
    cohort_entities: dict[str, tuple[tuple[str, str], ...]]
    included_source_refs: dict[str, InputArtifactReference]
    v0_ranked_rows: dict[str, list[dict[str, object]]]
    ranked_entities: dict[str, list[Any]]
    intervention_object_bundle_rows: list[dict[str, object]]
    intervention_object_bundle_ref: InputArtifactReference | None = None


def write_benchmark_model_run_manifest(
    path: Path,
    manifest: BenchmarkModelRunManifest,
) -> None:
    write_json(path, manifest.to_dict())


def read_benchmark_model_run_manifest(path: Path) -> BenchmarkModelRunManifest:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("benchmark model run manifest must be a JSON object")
    return BenchmarkModelRunManifest.from_dict(payload)


def _resolve_code_version(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            cwd=repo_root,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def _build_artifact_reference(
    *,
    artifact_name: str,
    path: Path,
    schema_name: str = "",
    source_name: str = "",
    source_version: str = "",
    notes: str = "",
) -> InputArtifactReference:
    return InputArtifactReference(
        artifact_name=artifact_name,
        artifact_path=str(path),
        sha256=_file_sha256(path),
        schema_name=schema_name,
        source_name=source_name,
        source_version=source_version,
        notes=notes,
    )


def _resolve_included_source_descriptors(
    manifest: BenchmarkSnapshotManifest,
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
) -> dict[str, SourceArchiveDescriptor]:
    descriptors_by_key = {
        (descriptor.source_name, descriptor.source_version): descriptor
        for descriptor in archive_descriptors
    }
    included: dict[str, SourceArchiveDescriptor] = {}
    for source_snapshot in manifest.source_snapshots:
        if not source_snapshot.included:
            continue
        key = (source_snapshot.source_name, source_snapshot.source_version)
        descriptor = descriptors_by_key.get(key)
        if descriptor is None:
            raise ValueError(
                "snapshot manifest references an included source archive that is "
                "missing from the source archive index: "
                f"{source_snapshot.source_name}/{source_snapshot.source_version}"
            )
        archive_path = Path(descriptor.archive_file)
        if not archive_path.exists():
            raise ValueError(f"source archive file missing: {archive_path}")
        actual_sha256 = _file_sha256(archive_path)
        if actual_sha256 != descriptor.sha256:
            raise ValueError(
                f"source archive checksum mismatch for {archive_path.name}"
            )
        included[source_snapshot.source_name] = descriptor
    return included


def _load_archive_rows(
    descriptor: SourceArchiveDescriptor,
) -> tuple[dict[str, str], ...]:
    archive_path = Path(descriptor.archive_file)
    if descriptor.archive_format == "csv":
        rows = read_csv_rows(archive_path)
        if any("entity_type" in row for row in rows):
            return tuple(rows)
        inferred_entity_type = GENE_ENTITY_TYPE
        return tuple(
            {
                **row,
                "entity_type": inferred_entity_type,
            }
            for row in rows
        )
    if descriptor.archive_format == "json":
        payload = read_json(archive_path)
        if isinstance(payload, list):
            return tuple(
                {
                    **item,
                    "entity_type": item.get("entity_type", GENE_ENTITY_TYPE),
                }
                for item in payload
                if isinstance(item, dict)
            )
        if not isinstance(payload, dict):
            raise ValueError(
                f"unsupported JSON archive payload for {archive_path.name}"
            )
        rows: list[dict[str, str]] = []
        for key, entity_type in (("genes", GENE_ENTITY_TYPE), ("modules", MODULE_ENTITY_TYPE)):
            entity_rows = payload.get(key, [])
            if not isinstance(entity_rows, list):
                continue
            for entity_row in entity_rows:
                if not isinstance(entity_row, dict):
                    continue
                rows.append(
                    {
                        **entity_row,
                        "entity_type": str(entity_row.get("entity_type", entity_type)),
                    }
                )
        return tuple(rows)
    raise ValueError(f"unsupported archive format: {descriptor.archive_format}")


def _build_entity_states(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    config: EngineConfig,
) -> dict[tuple[str, str], dict[str, object]]:
    states: dict[tuple[str, str], dict[str, object]] = {}
    for label in cohort_labels:
        key = (label.entity_type, label.entity_id)
        if key in states:
            continue
        if label.entity_type == GENE_ENTITY_TYPE:
            layer_names = tuple(config.gene_layers)
        else:
            layer_names = tuple(config.module_layers)
        states[key] = {
            "entity_type": label.entity_type,
            "entity_id": label.entity_id,
            "entity_label": label.entity_label,
            "layer_values": {layer_name: None for layer_name in layer_names},
            "metadata": {},
        }
    manifest_entity_types = set(manifest.entity_types)
    unexpected_entity_types = {
        entity_type
        for entity_type, _ in states
        if entity_type not in manifest_entity_types
    }
    if unexpected_entity_types:
        raise ValueError("cohort labels include entity types outside the manifest")
    return states


def _apply_archive_row(
    states: dict[tuple[str, str], dict[str, object]],
    row: dict[str, str],
) -> None:
    entity_type = str(row.get("entity_type", "")).strip()
    entity_id = str(row.get("entity_id", "")).strip()
    if not entity_type or not entity_id:
        raise ValueError("archive rows must include entity_type and entity_id")
    key = (entity_type, entity_id)
    if key not in states:
        return

    state = states[key]
    layer_values = dict(state["layer_values"])
    metadata = dict(state["metadata"])
    for field_name in KNOWN_GENE_LAYER_FIELDS + KNOWN_MODULE_LAYER_FIELDS:
        if field_name not in row:
            continue
        raw_value = str(row[field_name]).strip()
        if not raw_value:
            continue
        if field_name not in layer_values:
            continue
        parsed_value = float(raw_value)
        if layer_values[field_name] is not None:
            raise ValueError(
                f"duplicate archive value for {entity_type}/{entity_id} field {field_name}"
            )
        layer_values[field_name] = parsed_value
    for field_name in KNOWN_METADATA_FIELDS:
        if field_name not in row:
            continue
        raw_value = str(row[field_name]).strip()
        if not raw_value:
            continue
        if field_name in metadata:
            raise ValueError(
                f"duplicate archive metadata for {entity_type}/{entity_id} field {field_name}"
            )
        metadata[field_name] = raw_value

    state["layer_values"] = layer_values
    state["metadata"] = metadata


def _assemble_snapshot_records(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    config: EngineConfig,
) -> tuple[
    list[EntityRecord],
    list[EntityRecord],
    dict[str, tuple[tuple[str, str], ...]],
    dict[str, InputArtifactReference],
]:
    states = _build_entity_states(manifest, cohort_labels, config)
    included_descriptors = _resolve_included_source_descriptors(
        manifest,
        archive_descriptors,
    )
    included_source_refs = {
        source_name: _build_artifact_reference(
            artifact_name="source_archive",
            path=Path(descriptor.archive_file),
            source_name=descriptor.source_name,
            source_version=descriptor.source_version,
            notes=descriptor.notes,
        )
        for source_name, descriptor in included_descriptors.items()
    }

    for descriptor in included_descriptors.values():
        for row in _load_archive_rows(descriptor):
            _apply_archive_row(states, row)

    gene_records: list[EntityRecord] = []
    module_records: list[EntityRecord] = []
    cohort_entities: dict[str, tuple[tuple[str, str], ...]] = {
        GENE_ENTITY_TYPE: (),
        MODULE_ENTITY_TYPE: (),
    }
    grouped_cohort_entities: dict[str, list[tuple[str, str]]] = {
        GENE_ENTITY_TYPE: [],
        MODULE_ENTITY_TYPE: [],
    }
    for (entity_type, entity_id), state in sorted(
        states.items(),
        key=lambda item: (item[0][0], str(item[1]["entity_label"]).lower(), item[0][1]),
    ):
        record = EntityRecord(
            entity_type=str(state["entity_type"]),
            entity_id=str(state["entity_id"]),
            entity_label=str(state["entity_label"]),
            layer_values=dict(state["layer_values"]),
            metadata={
                key: _score_to_string(value)
                for key, value in dict(state["metadata"]).items()
            },
        )
        grouped_cohort_entities[entity_type].append((record.entity_id, record.entity_label))
        if entity_type == GENE_ENTITY_TYPE:
            gene_records.append(record)
            continue
        module_records.append(record)
    cohort_entities = {
        entity_type: tuple(values)
        for entity_type, values in grouped_cohort_entities.items()
    }
    return gene_records, module_records, cohort_entities, included_source_refs


def _cohort_entities_from_labels(
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
) -> dict[str, tuple[tuple[str, str], ...]]:
    grouped: dict[str, list[tuple[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for label in sorted(
        cohort_labels,
        key=lambda item: (item.entity_type, item.entity_label.lower(), item.entity_id),
    ):
        key = (label.entity_type, label.entity_id)
        if key in seen:
            continue
        seen.add(key)
        grouped.setdefault(label.entity_type, []).append(
            (label.entity_id, label.entity_label)
        )
    return {
        entity_type: tuple(rows)
        for entity_type, rows in grouped.items()
    }


def _assemble_projection_records(
    manifest: BenchmarkSnapshotManifest,
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    config: EngineConfig,
) -> tuple[
    list[EntityRecord],
    list[EntityRecord],
    dict[str, InputArtifactReference],
]:
    included_descriptors = _resolve_included_source_descriptors(
        manifest,
        archive_descriptors,
    )
    included_source_refs = {
        source_name: _build_artifact_reference(
            artifact_name="source_archive",
            path=Path(descriptor.archive_file),
            source_name=descriptor.source_name,
            source_version=descriptor.source_version,
            notes=descriptor.notes,
        )
        for source_name, descriptor in included_descriptors.items()
    }
    states: dict[tuple[str, str], dict[str, object]] = {}
    for descriptor in included_descriptors.values():
        for row in _load_archive_rows(descriptor):
            entity_type = str(row.get("entity_type", "")).strip()
            entity_id = str(row.get("entity_id", "")).strip()
            entity_label = str(row.get("entity_label", "")).strip()
            if entity_type not in {GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE}:
                continue
            if not entity_id or not entity_label:
                continue
            if (entity_type, entity_id) not in states:
                layer_names = (
                    tuple(config.gene_layers)
                    if entity_type == GENE_ENTITY_TYPE
                    else tuple(config.module_layers)
                )
                states[(entity_type, entity_id)] = {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "entity_label": entity_label,
                    "layer_values": {layer_name: None for layer_name in layer_names},
                    "metadata": {},
                }
            _apply_archive_row(states, row)

    gene_records: list[EntityRecord] = []
    module_records: list[EntityRecord] = []
    for (entity_type, entity_id), state in sorted(
        states.items(),
        key=lambda item: (item[0][0], str(item[1]["entity_label"]).lower(), item[0][1]),
    ):
        record = EntityRecord(
            entity_type=str(state["entity_type"]),
            entity_id=str(state["entity_id"]),
            entity_label=str(state["entity_label"]),
            layer_values=dict(state["layer_values"]),
            metadata={
                key: _score_to_string(value)
                for key, value in dict(state["metadata"]).items()
            },
        )
        if entity_type == GENE_ENTITY_TYPE:
            gene_records.append(record)
        else:
            module_records.append(record)
    return gene_records, module_records, included_source_refs


def _build_context(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    config: EngineConfig,
    *,
    manifest_file: Path,
) -> BenchmarkExecutionContext:
    intervention_object_bundle_rows: list[dict[str, object]] = []
    intervention_object_bundle_ref: InputArtifactReference | None = None
    if INTERVENTION_OBJECT_ENTITY_TYPE in manifest.entity_types:
        gene_records, module_records, included_source_refs = _assemble_projection_records(
            manifest,
            archive_descriptors,
            config,
        )
        cohort_entities = _cohort_entities_from_labels(cohort_labels)
        bundle_path = intervention_object_bundle_path_for_manifest_file(manifest_file)
        if not bundle_path.exists():
            raise ValueError(
                "intervention-object snapshot requires a materialized feature bundle: "
                f"{bundle_path}"
            )
        expected_included_sources = tuple(
            sorted(
                source_snapshot.source_name
                for source_snapshot in manifest.source_snapshots
                if source_snapshot.included
            )
        )
        expected_excluded_sources = tuple(
            sorted(
                source_snapshot.source_name
                for source_snapshot in manifest.source_snapshots
                if not source_snapshot.included
            )
        )
        expected_source_snapshot_provenance_json = (
            build_intervention_object_bundle_source_snapshot_provenance(
                manifest.source_snapshots,
                archive_descriptors,
            )
        )
        intervention_object_bundle_rows = read_intervention_object_feature_bundle(
            bundle_path,
            expected_as_of_date=manifest.as_of_date,
            expected_entities=dict(
                cohort_entities.get(INTERVENTION_OBJECT_ENTITY_TYPE, ())
            ),
            expected_included_sources=expected_included_sources,
            expected_excluded_sources=expected_excluded_sources,
            expected_source_snapshot_provenance_json=(
                expected_source_snapshot_provenance_json
            ),
        )
        intervention_object_bundle_ref = _build_artifact_reference(
            artifact_name="intervention_object_feature_bundle",
            path=bundle_path,
            schema_name="intervention_object_feature_bundle",
            notes="Snapshot-side intervention-object feature bundle materialized from archived inputs only",
        )
    else:
        gene_records, module_records, cohort_entities, included_source_refs = (
            _assemble_snapshot_records(
                manifest,
                cohort_labels,
                archive_descriptors,
                config,
            )
        )
    gene_ranked_rows = rank_records(
        gene_records,
        layer_weights=config.gene_layers,
        required_groups=GENE_REQUIRED_GROUPS,
    )
    module_ranked_rows = rank_records(
        module_records,
        layer_weights=config.module_layers,
        required_groups=MODULE_REQUIRED_GROUPS,
    )
    gene_stability = run_stability_analysis(
        gene_records,
        layer_weights=config.gene_layers,
        required_groups=GENE_REQUIRED_GROUPS,
        top_n=config.build.top_n,
        perturbation_fraction=config.stability.perturbation_fraction,
        decision_grade_threshold=config.stability.heuristic_stability_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )
    module_stability = run_stability_analysis(
        module_records,
        layer_weights=config.module_layers,
        required_groups=MODULE_REQUIRED_GROUPS,
        top_n=config.build.top_n,
        perturbation_fraction=config.stability.perturbation_fraction,
        decision_grade_threshold=config.stability.heuristic_stability_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )
    ranked_entities = {
        GENE_ENTITY_TYPE: annotate_ranked_entities(
            gene_ranked_rows,
            {},
            gene_stability,
            config.stability.heuristic_stability_threshold,
        ),
        MODULE_ENTITY_TYPE: annotate_ranked_entities(
            module_ranked_rows,
            {},
            module_stability,
            config.stability.heuristic_stability_threshold,
        ),
    }
    return BenchmarkExecutionContext(
        manifest=manifest,
        cohort_labels=cohort_labels,
        config=config,
        gene_records=gene_records,
        module_records=module_records,
        cohort_entities=cohort_entities,
        included_source_refs=included_source_refs,
        v0_ranked_rows={
            GENE_ENTITY_TYPE: gene_ranked_rows,
            MODULE_ENTITY_TYPE: module_ranked_rows,
        },
        ranked_entities=ranked_entities,
        intervention_object_bundle_rows=intervention_object_bundle_rows,
        intervention_object_bundle_ref=intervention_object_bundle_ref,
    )


def _sort_prediction_candidates(
    candidates: list[tuple[str, str, float]],
) -> tuple[PredictionRow, ...]:
    candidates.sort(
        key=lambda item: (
            -item[2],
            item[1].lower(),
            item[0],
        )
    )
    return tuple(
        PredictionRow(
            entity_type="",
            entity_id=entity_id,
            entity_label=entity_label,
            score=round(score, 6),
            rank=index,
        )
        for index, (entity_id, entity_label, score) in enumerate(candidates, start=1)
    )


def _with_entity_type(
    entity_type: str,
    predictions: tuple[PredictionRow, ...],
) -> tuple[PredictionRow, ...]:
    return tuple(
        PredictionRow(
            entity_type=entity_type,
            entity_id=prediction.entity_id,
            entity_label=prediction.entity_label,
            score=prediction.score,
            rank=prediction.rank,
        )
        for prediction in predictions
    )


def _execute_source_only_baseline(
    records: list[EntityRecord],
    *,
    field_name: str,
) -> tuple[PredictionRow, ...]:
    candidates: list[tuple[str, str, float]] = []
    for record in records:
        if field_name in record.layer_values:
            value = record.layer_values.get(field_name)
        else:
            value = parse_optional_float(record.metadata.get(field_name))
        if value is None:
            continue
        candidates.append((record.entity_id, record.entity_label, float(value)))
    return _sort_prediction_candidates(candidates)


def _execute_v0_current(
    ranked_rows: list[dict[str, object]],
) -> tuple[PredictionRow, ...]:
    candidates: list[tuple[str, str, float]] = []
    for row in ranked_rows:
        if not row["eligible"]:
            continue
        candidates.append(
            (
                str(row["entity_id"]),
                str(row["entity_label"]),
                float(row["composite_score"]),
            )
        )
    return _sort_prediction_candidates(candidates)


def _aggregate_v1_score(vector: Any) -> float | None:
    available_scores = [
        float(domain_score.score)
        for domain_score in vector.domain_head_scores
        if domain_score.score is not None
    ]
    if not available_scores:
        return None
    return round(sum(available_scores) / len(available_scores), 6)


def _execute_v1_current(
    entities: list[Any],
) -> tuple[PredictionRow, ...]:
    vectors = build_decision_vectors(entities)
    candidates: list[tuple[str, str, float]] = []
    for vector in vectors:
        score = _aggregate_v1_score(vector)
        if score is None:
            continue
        candidates.append((vector.entity_id, vector.entity_label, score))
    return _sort_prediction_candidates(candidates)


def _deterministic_random_score(
    *,
    seed: int,
    snapshot_id: str,
    entity_type: str,
    entity_id: str,
) -> float:
    digest = sha256(
        f"{seed}:{snapshot_id}:{entity_type}:{entity_id}".encode("utf-8")
    ).hexdigest()
    return int(digest[:16], 16) / float(16**16)


def _execute_random_with_coverage(
    *,
    cohort_entities: tuple[tuple[str, str], ...],
    snapshot_id: str,
    entity_type: str,
    seed: int,
) -> tuple[PredictionRow, ...]:
    candidates = [
        (
            entity_id,
            entity_label,
            _deterministic_random_score(
                seed=seed,
                snapshot_id=snapshot_id,
                entity_type=entity_type,
                entity_id=entity_id,
            ),
        )
        for entity_id, entity_label in cohort_entities
    ]
    return _sort_prediction_candidates(candidates)


def _prediction_rows_from_projection_payload(
    payload: dict[str, object],
) -> tuple[PredictionRow, ...]:
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("projection payload rows must be a list")
    candidates: list[tuple[str, str, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not bool(row.get("covered")):
            continue
        score = row.get("projected_score")
        if score in {None, ""}:
            continue
        candidates.append(
            (
                str(row["entity_id"]),
                str(row["entity_label"]),
                float(score),
            )
        )
    return _sort_prediction_candidates(candidates)


def _execute_baseline_predictions(
    context: BenchmarkExecutionContext,
    baseline: BaselineDefinition,
    *,
    random_seed: int,
) -> tuple[
    dict[str, tuple[PredictionRow, ...]],
    list[str],
    dict[str, dict[str, object]],
]:
    notes: list[str] = []
    predictions_by_type: dict[str, tuple[PredictionRow, ...]] = {}
    projection_payloads: dict[str, dict[str, object]] = {}
    if baseline.baseline_id == "pgc_only":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_source_only_baseline(
                context.gene_records,
                field_name="common_variant_support",
            ),
        )
    elif baseline.baseline_id == "schema_only":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_source_only_baseline(
                context.gene_records,
                field_name="rare_variant_support",
            ),
        )
    elif baseline.baseline_id == "opentargets_only":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_source_only_baseline(
                context.gene_records,
                field_name="generic_platform_baseline",
            ),
        )
    elif baseline.baseline_id == "chembl_only":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_source_only_baseline(
                context.gene_records,
                field_name="tractability_compoundability",
            ),
        )
    elif baseline.baseline_id == "v0_current":
        gene_predictions = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_v0_current(context.v0_ranked_rows[GENE_ENTITY_TYPE]),
        )
        module_predictions = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_v0_current(context.v0_ranked_rows[MODULE_ENTITY_TYPE]),
        )
        predictions_by_type[GENE_ENTITY_TYPE] = gene_predictions
        predictions_by_type[MODULE_ENTITY_TYPE] = module_predictions
        if context.intervention_object_bundle_rows:
            projection_payload = build_intervention_object_projection_payload(
                baseline_id=baseline.baseline_id,
                bundle_rows=context.intervention_object_bundle_rows,
                gene_predictions=gene_predictions,
                module_predictions=module_predictions,
            )
            projection_payloads[INTERVENTION_OBJECT_ENTITY_TYPE] = projection_payload
            predictions_by_type[INTERVENTION_OBJECT_ENTITY_TYPE] = _with_entity_type(
                INTERVENTION_OBJECT_ENTITY_TYPE,
                _prediction_rows_from_projection_payload(projection_payload),
            )
    elif baseline.baseline_id == "v1_current":
        gene_predictions = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_v1_current(context.ranked_entities[GENE_ENTITY_TYPE]),
        )
        module_predictions = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_v1_current(context.ranked_entities[MODULE_ENTITY_TYPE]),
        )
        predictions_by_type[GENE_ENTITY_TYPE] = gene_predictions
        predictions_by_type[MODULE_ENTITY_TYPE] = module_predictions
        if context.intervention_object_bundle_rows:
            projection_payload = build_intervention_object_projection_payload(
                baseline_id=baseline.baseline_id,
                bundle_rows=context.intervention_object_bundle_rows,
                gene_predictions=gene_predictions,
                module_predictions=module_predictions,
            )
            projection_payloads[INTERVENTION_OBJECT_ENTITY_TYPE] = projection_payload
            predictions_by_type[INTERVENTION_OBJECT_ENTITY_TYPE] = _with_entity_type(
                INTERVENTION_OBJECT_ENTITY_TYPE,
                _prediction_rows_from_projection_payload(projection_payload),
            )
        notes.append(
            "v1_current benchmark score resolves current additive v1 output via "
            f"{V1_RESOLUTION_METHOD}"
        )
    elif baseline.baseline_id == "random_with_coverage":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_random_with_coverage(
                cohort_entities=context.cohort_entities.get(GENE_ENTITY_TYPE, ()),
                snapshot_id=context.manifest.snapshot_id,
                entity_type=GENE_ENTITY_TYPE,
                seed=random_seed,
            ),
        )
        predictions_by_type[MODULE_ENTITY_TYPE] = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_random_with_coverage(
                cohort_entities=context.cohort_entities.get(MODULE_ENTITY_TYPE, ()),
                snapshot_id=context.manifest.snapshot_id,
                entity_type=MODULE_ENTITY_TYPE,
                seed=random_seed,
            ),
        )
        if INTERVENTION_OBJECT_ENTITY_TYPE in baseline.entity_types:
            predictions_by_type[INTERVENTION_OBJECT_ENTITY_TYPE] = _with_entity_type(
                INTERVENTION_OBJECT_ENTITY_TYPE,
                _execute_random_with_coverage(
                    cohort_entities=context.cohort_entities.get(
                        INTERVENTION_OBJECT_ENTITY_TYPE,
                        (),
                    ),
                    snapshot_id=context.manifest.snapshot_id,
                    entity_type=INTERVENTION_OBJECT_ENTITY_TYPE,
                    seed=random_seed,
                ),
            )
    else:
        raise ValueError(f"unsupported benchmark baseline execution: {baseline.baseline_id}")

    for entity_type, predictions in predictions_by_type.items():
        if entity_type not in baseline.entity_types:
            continue
        if not predictions:
            notes.append(f"{entity_type} slice had no eligible coverage")
    return predictions_by_type, notes, projection_payloads


def _build_run_manifest_path(output_dir: Path, run_id: str) -> Path:
    return output_dir / "run_manifests" / f"{run_id}.json"


def _build_metric_payload_path(
    output_dir: Path,
    *,
    run_id: str,
    entity_type: str,
    horizon: str,
    metric_name: str,
) -> Path:
    return (
        output_dir
        / "metric_payloads"
        / run_id
        / entity_type
        / horizon
        / f"{metric_name}.json"
    )


def _build_interval_payload_path(
    output_dir: Path,
    *,
    run_id: str,
    entity_type: str,
    horizon: str,
    metric_name: str,
) -> Path:
    return (
        output_dir
        / "confidence_interval_payloads"
        / run_id
        / entity_type
        / horizon
        / f"{metric_name}.json"
    )


def _metric_slice_notes(
    *,
    covered_count: int,
    admissible_count: int,
    positive_count: int,
    deterministic_test_mode: bool,
) -> str:
    notes = (
        "relevance=any_positive_outcome;"
        f" positives={positive_count};"
        f" covered_entities={covered_count}/{admissible_count}"
    )
    if deterministic_test_mode:
        notes += "; deterministic_test_mode=true"
    return notes


def _baseline_input_artifacts(
    *,
    baseline_id: str,
    manifest_ref: InputArtifactReference,
    cohort_manifest_ref: InputArtifactReference,
    cohort_members_ref: InputArtifactReference,
    cohort_ref: InputArtifactReference,
    archive_index_ref: InputArtifactReference,
    config_ref: InputArtifactReference,
    source_refs: dict[str, InputArtifactReference],
    additional_refs: tuple[InputArtifactReference, ...] = (),
) -> tuple[InputArtifactReference, ...]:
    refs = [
        manifest_ref,
        cohort_manifest_ref,
        cohort_members_ref,
        cohort_ref,
        archive_index_ref,
    ]
    if baseline_id in {"v0_current", "v1_current"}:
        refs.append(config_ref)
    for source_name in BASELINE_SOURCE_DEPENDENCIES[baseline_id]:
        source_ref = source_refs.get(source_name)
        if source_ref is not None:
            refs.append(source_ref)
    refs.extend(additional_refs)
    refs.sort(
        key=lambda reference: (
            reference.artifact_name,
            reference.source_name,
            reference.artifact_path,
        )
    )
    return tuple(refs)


def _build_run_id(
    *,
    snapshot_id: str,
    baseline_id: str,
    code_version: str,
    parameterization: dict[str, object],
) -> str:
    components = [
        _normalize_component(snapshot_id),
        _normalize_component(baseline_id),
        _normalize_component(code_version[:12]),
        _parameter_digest(parameterization),
    ]
    return "__".join(component for component in components if component)


def _track_b_metric_slice_notes(
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


def _run_track_b_benchmark(
    *,
    manifest: BenchmarkSnapshotManifest,
    manifest_file: Path,
    cohort_labels_file: Path,
    materialized_cohort: Any,
    archive_index_file: Path,
    output_dir: Path,
    config_file: Path,
    code_version: str,
    task_contract: Any,
    bootstrap_iterations: int,
    bootstrap_confidence_level: float,
    random_seed: int,
    deterministic_test_mode: bool,
    execution_timestamp: str | None,
) -> dict[str, object]:
    casebook_path = track_b_casebook_path_for_archive_index_file(archive_index_file)
    if not casebook_path.exists():
        raise ValueError(
            "Track B benchmark requires a checked-in track_b_casebook.csv beside "
            f"the source archive index: {casebook_path}"
        )
    events_path = track_b_events_path_for_archive_index_file(archive_index_file)
    if not events_path.exists():
        raise ValueError(
            "Track B benchmark requires a checked-in events.csv beside the source "
            f"archive index: {events_path}"
        )
    program_universe_path = track_b_program_universe_path_for_archive_index_file(
        archive_index_file
    )
    if not program_universe_path.exists():
        raise ValueError(
            "Track B benchmark requires a checked-in program_universe.csv beside "
            f"the source archive index: {program_universe_path}"
        )
    assets_path = track_b_assets_path_for_archive_index_file(archive_index_file)
    if not assets_path.exists():
        raise ValueError(
            "Track B benchmark requires a checked-in assets.csv beside the source "
            f"archive index: {assets_path}"
        )
    event_provenance_path = track_b_event_provenance_path_for_archive_index_file(
        archive_index_file
    )
    if not event_provenance_path.exists():
        raise ValueError(
            "Track B benchmark requires a checked-in event_provenance.csv beside "
            f"the source archive index: {event_provenance_path}"
        )
    directionality_hypotheses_path = (
        track_b_directionality_hypotheses_path_for_archive_index_file(archive_index_file)
    )
    if not directionality_hypotheses_path.exists():
        raise ValueError(
            "Track B benchmark requires checked-in directionality_hypotheses.csv "
            f"beside the source archive index: {directionality_hypotheses_path}"
        )

    cases = load_track_b_casebook(
        casebook_path,
        as_of_date=manifest.as_of_date,
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    dataset = build_track_b_program_memory_dataset(
        as_of_date=manifest.as_of_date,
        events_path=events_path,
    )

    manifest_ref = _build_artifact_reference(
        artifact_name="benchmark_snapshot_manifest",
        path=manifest_file,
        schema_name="benchmark_snapshot_manifest",
    )
    cohort_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_labels",
        path=cohort_labels_file,
        schema_name="benchmark_cohort_labels",
    )
    cohort_members_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_members",
        path=materialized_cohort.cohort_members_path,
        schema_name="benchmark_cohort_members",
    )
    cohort_manifest_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_manifest",
        path=materialized_cohort.cohort_manifest_path,
        schema_name="benchmark_cohort_manifest",
    )
    cohort_source_input_refs: tuple[InputArtifactReference, ...] = (
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
    )
    archive_index_ref = _build_artifact_reference(
        artifact_name="source_archive_index",
        path=archive_index_file,
        notes="Archived source descriptor index consumed for the Track B snapshot.",
    )
    config_ref = _build_artifact_reference(
        artifact_name="engine_config",
        path=config_file,
        notes=(
            "Retained for CLI parity; Track B v1 baselines do not consume the v0/v1 "
            "engine weight configuration."
        ),
    )
    casebook_ref = _build_artifact_reference(
        artifact_name="track_b_casebook",
        path=casebook_path,
        notes="Frozen Track B casebook with gold analogs and structural replay labels.",
    )
    events_ref = _build_artifact_reference(
        artifact_name="track_b_program_history_events",
        path=events_path,
        notes="Slice-local pinned program-memory event ledger used for Track B cutoff filtering.",
    )
    program_universe_ref = _build_artifact_reference(
        artifact_name="track_b_program_universe",
        path=program_universe_path,
        notes="Slice-local pinned denominator rows used for Track B coverage-at-cutoff labels.",
    )
    program_memory_assets_ref = _build_artifact_reference(
        artifact_name="program_memory_assets",
        path=assets_path,
        notes="Slice-local pinned program-memory asset ledger used for Track B analog expansion.",
    )
    program_memory_provenance_ref = _build_artifact_reference(
        artifact_name="program_memory_event_provenance",
        path=event_provenance_path,
        notes="Slice-local pinned program-memory provenance ledger used for Track B analog references.",
    )
    program_memory_hypotheses_ref = _build_artifact_reference(
        artifact_name="program_memory_directionality_hypotheses",
        path=directionality_hypotheses_path,
        notes="Slice-local pinned directionality hypotheses used by the current structural replay surface.",
    )

    baseline_index = {
        baseline_definition.baseline_id: baseline_definition
        for baseline_definition in task_contract.protocol.baselines
        if baseline_definition.baseline_id in task_contract.supported_baseline_ids
    }
    available_now_baselines = [
        baseline_id
        for baseline_id in task_contract.supported_baseline_ids
        if baseline_index[baseline_id].status == AVAILABLE_NOW_STATUS
    ]
    requested_available_now_baselines = [
        baseline_id
        for baseline_id in manifest.baseline_ids
        if baseline_index[baseline_id].status == AVAILABLE_NOW_STATUS
    ]
    protocol_only_baselines = [
        baseline_id
        for baseline_id in task_contract.supported_baseline_ids
        if baseline_index[baseline_id].status == PROTOCOL_ONLY_STATUS
    ]
    requested_protocol_only_baselines = [
        baseline_id
        for baseline_id in manifest.baseline_ids
        if baseline_index[baseline_id].status == PROTOCOL_ONLY_STATUS
    ]

    run_manifest_files: list[str] = []
    metric_payload_files: list[str] = []
    confidence_interval_files: list[str] = []
    case_output_files: list[str] = []
    confusion_summary_files: list[str] = []
    executed_baselines: list[str] = []

    case_count = len(cases)
    included_coverage_count = sum(
        1 for case in cases if case.coverage_state_at_cutoff == "included"
    )
    replay_supported_case_count = sum(
        1 for case in cases if case.gold_replay_status == "replay_supported"
    )
    analog_evaluable_case_count = sum(
        1 for case in cases if case.gold_analog_event_ids
    )

    for baseline_id in requested_available_now_baselines:
        baseline = baseline_index[baseline_id]
        parameterization: dict[str, object] = {
            "benchmark_question_id": manifest.benchmark_question_id,
            "bootstrap_confidence_level": bootstrap_confidence_level,
            "bootstrap_iterations": bootstrap_iterations,
            "deterministic_test_mode": deterministic_test_mode,
            "interval_method": BOOTSTRAP_INTERVAL_METHOD,
            "random_seed": random_seed,
            "resample_unit": TRACK_B_BOOTSTRAP_RESAMPLE_UNIT,
            "track_b_casebook_sha256": casebook_ref.sha256,
            "track_b_horizon": TRACK_B_HORIZON,
            "track_b_case_count": case_count,
            "track_b_baseline_mode": baseline_id,
        }
        run_id = _build_run_id(
            snapshot_id=manifest.snapshot_id,
            baseline_id=baseline_id,
            code_version=code_version,
            parameterization=parameterization,
        )
        started_at = execution_timestamp or _utc_now()
        slice_random_seed = (
            random_seed
            + int.from_bytes(
                sha256(f"{baseline_id}:{TRACK_B_HORIZON}".encode("utf-8")).digest()[:4],
                "big",
            )
        )
        case_outputs = build_track_b_case_outputs(
            cases=cases,
            dataset=dataset,
            baseline_id=baseline_id,
        )
        metric_values = estimate_track_b_metric_intervals(
            case_outputs,
            iterations=bootstrap_iterations,
            confidence_level=bootstrap_confidence_level,
            random_seed=slice_random_seed,
        )
        metric_cohort_sizes = track_b_metric_cohort_sizes(case_outputs)
        slice_notes = _track_b_metric_slice_notes(
            case_count=case_count,
            included_coverage_count=included_coverage_count,
            replay_supported_case_count=replay_supported_case_count,
            analog_evaluable_case_count=analog_evaluable_case_count,
            deterministic_test_mode=deterministic_test_mode,
        )
        for metric_name in TRACK_B_METRIC_NAMES:
            point_estimate, interval_low, interval_high = metric_values[metric_name]
            metric_payload = BenchmarkMetricOutputPayload(
                run_id=run_id,
                snapshot_id=manifest.snapshot_id,
                baseline_id=baseline_id,
                entity_type=TRACK_B_ENTITY_TYPE,
                horizon=TRACK_B_HORIZON,
                metric_name=metric_name,
                metric_value=point_estimate,
                cohort_size=metric_cohort_sizes[metric_name],
                notes=slice_notes,
            )
            metric_path = _build_metric_payload_path(
                output_dir,
                run_id=run_id,
                entity_type=TRACK_B_ENTITY_TYPE,
                horizon=TRACK_B_HORIZON,
                metric_name=metric_name,
            )
            write_benchmark_metric_output_payload(metric_path, metric_payload)
            metric_payload_files.append(str(metric_path))

            interval_payload = BenchmarkConfidenceIntervalPayload(
                run_id=run_id,
                snapshot_id=manifest.snapshot_id,
                baseline_id=baseline_id,
                entity_type=TRACK_B_ENTITY_TYPE,
                horizon=TRACK_B_HORIZON,
                metric_name=metric_name,
                point_estimate=point_estimate,
                interval_low=interval_low,
                interval_high=interval_high,
                confidence_level=bootstrap_confidence_level,
                bootstrap_iterations=bootstrap_iterations,
                resample_unit=TRACK_B_BOOTSTRAP_RESAMPLE_UNIT,
                random_seed=slice_random_seed,
                notes=f"method={BOOTSTRAP_INTERVAL_METHOD}; {slice_notes}",
            )
            interval_path = _build_interval_payload_path(
                output_dir,
                run_id=run_id,
                entity_type=TRACK_B_ENTITY_TYPE,
                horizon=TRACK_B_HORIZON,
                metric_name=metric_name,
            )
            write_benchmark_confidence_interval_payload(interval_path, interval_payload)
            confidence_interval_files.append(str(interval_path))

        case_output_payload = TrackBCaseOutputPayload(
            run_id=run_id,
            baseline_id=baseline_id,
            snapshot_id=manifest.snapshot_id,
            as_of_date=manifest.as_of_date,
            cases=case_outputs,
        )
        case_output_file = track_b_case_output_path(output_dir, run_id=run_id)
        write_track_b_case_output_payload(case_output_file, case_output_payload)
        case_output_files.append(str(case_output_file))

        confusion_summary = build_track_b_confusion_summary(
            run_id=run_id,
            baseline_id=baseline_id,
            snapshot_id=manifest.snapshot_id,
            case_outputs=case_outputs,
        )
        confusion_summary_file = track_b_confusion_summary_path(
            output_dir,
            run_id=run_id,
        )
        write_track_b_confusion_summary(confusion_summary_file, confusion_summary)
        confusion_summary_files.append(str(confusion_summary_file))

        completed_at = execution_timestamp or _utc_now()
        input_artifacts = [
            manifest_ref,
            cohort_manifest_ref,
            cohort_members_ref,
            cohort_ref,
            archive_index_ref,
            casebook_ref,
            events_ref,
            program_universe_ref,
            program_memory_assets_ref,
            program_memory_provenance_ref,
            program_memory_hypotheses_ref,
            config_ref,
            *cohort_source_input_refs,
        ]
        input_artifacts.sort(
            key=lambda reference: (
                reference.artifact_name,
                reference.source_name,
                reference.artifact_path,
            )
        )
        run_manifest = BenchmarkModelRunManifest(
            run_id=run_id,
            snapshot_id=manifest.snapshot_id,
            baseline_id=baseline_id,
            model_family=baseline.family,
            code_version=code_version,
            benchmark_suite_id=task_contract.suite_id,
            benchmark_task_id=task_contract.task_id,
            parameterization=parameterization,
            input_artifacts=tuple(input_artifacts),
            started_at=started_at,
            completed_at=completed_at,
            notes=(
                "Track B structural replay run using frozen casebook "
                f"{TRACK_B_CASEBOOK_FILE_NAME}"
            ),
        )
        run_manifest_path = _build_run_manifest_path(output_dir, run_id)
        write_benchmark_model_run_manifest(run_manifest_path, run_manifest)
        run_manifest_files.append(str(run_manifest_path))
        executed_baselines.append(baseline_id)

    return {
        "benchmark_suite_id": task_contract.suite_id,
        "benchmark_task_id": task_contract.task_id,
        "benchmark_question_id": manifest.benchmark_question_id,
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_dir": str(output_dir),
        "code_version": code_version,
        "manifest_file": str(manifest_file),
        "cohort_labels_file": str(cohort_labels_file),
        "archive_index_file": str(archive_index_file),
        "executed_baselines": executed_baselines,
        "requested_available_now_baselines": requested_available_now_baselines,
        "available_now_baselines": available_now_baselines,
        "protocol_only_baselines": protocol_only_baselines,
        "requested_protocol_only_baselines": requested_protocol_only_baselines,
        "run_manifest_files": sorted(run_manifest_files),
        "metric_payload_files": sorted(metric_payload_files),
        "confidence_interval_files": sorted(confidence_interval_files),
        "track_b_case_output_files": sorted(case_output_files),
        "track_b_confusion_summary_files": sorted(confusion_summary_files),
        "bootstrap_iterations": bootstrap_iterations,
        "bootstrap_confidence_level": bootstrap_confidence_level,
        "interval_method": BOOTSTRAP_INTERVAL_METHOD,
        "resample_unit": TRACK_B_BOOTSTRAP_RESAMPLE_UNIT,
        "v1_resolution_method": V1_RESOLUTION_METHOD,
    }


def run_benchmark(
    *,
    manifest_file: Path,
    cohort_labels_file: Path,
    archive_index_file: Path,
    output_dir: Path,
    config_file: Path,
    code_version: str,
    bootstrap_iterations: int | None = None,
    bootstrap_confidence_level: float = DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL,
    random_seed: int = DEFAULT_RANDOM_BASELINE_SEED,
    deterministic_test_mode: bool = False,
    execution_timestamp: str | None = None,
) -> dict[str, object]:
    manifest = read_benchmark_snapshot_manifest(manifest_file)
    task_registry_path = _task_registry_path_from_manifest(manifest)
    task_contract = resolve_benchmark_task_contract(
        benchmark_task_id=manifest.benchmark_task_id or None,
        benchmark_question_id=manifest.benchmark_question_id,
        benchmark_suite_id=manifest.benchmark_suite_id or None,
        entity_types=manifest.entity_types,
        baseline_ids=manifest.baseline_ids,
        task_registry_path=task_registry_path,
    )
    protocol = task_contract.protocol
    question = protocol.question
    materialized_cohort = load_materialized_benchmark_cohort_artifacts(
        snapshot_manifest=manifest,
        snapshot_manifest_file=manifest_file,
        cohort_labels_file=cohort_labels_file,
    )
    cohort_labels = materialized_cohort.cohort_labels
    config = load_config(config_file)
    archive_descriptors = load_source_archive_descriptors(archive_index_file)

    resolved_bootstrap_iterations = bootstrap_iterations
    if resolved_bootstrap_iterations is None:
        resolved_bootstrap_iterations = DEFAULT_BOOTSTRAP_ITERATIONS
        if deterministic_test_mode:
            resolved_bootstrap_iterations = DETERMINISTIC_TEST_BOOTSTRAP_ITERATIONS

    if is_track_b_task(task_contract.task_id):
        return _run_track_b_benchmark(
            manifest=manifest,
            manifest_file=manifest_file,
            cohort_labels_file=cohort_labels_file,
            materialized_cohort=materialized_cohort,
            archive_index_file=archive_index_file,
            output_dir=output_dir,
            config_file=config_file,
            code_version=code_version,
            task_contract=task_contract,
            bootstrap_iterations=resolved_bootstrap_iterations,
            bootstrap_confidence_level=bootstrap_confidence_level,
            random_seed=random_seed,
            deterministic_test_mode=deterministic_test_mode,
            execution_timestamp=execution_timestamp,
        )

    context = _build_context(
        manifest,
        cohort_labels,
        archive_descriptors,
        config,
        manifest_file=manifest_file,
    )
    manifest_ref = _build_artifact_reference(
        artifact_name="benchmark_snapshot_manifest",
        path=manifest_file,
        schema_name="benchmark_snapshot_manifest",
    )
    cohort_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_labels",
        path=cohort_labels_file,
        schema_name="benchmark_cohort_labels",
    )
    cohort_members_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_members",
        path=materialized_cohort.cohort_members_path,
        schema_name="benchmark_cohort_members",
    )
    cohort_manifest_ref = _build_artifact_reference(
        artifact_name="benchmark_cohort_manifest",
        path=materialized_cohort.cohort_manifest_path,
        schema_name="benchmark_cohort_manifest",
    )
    cohort_source_input_refs: tuple[InputArtifactReference, ...] = (
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
    )
    archive_index_ref = _build_artifact_reference(
        artifact_name="source_archive_index",
        path=archive_index_file,
        notes="PR9B source archive descriptor index consumed to resolve archived source files",
    )
    config_ref = _build_artifact_reference(
        artifact_name="engine_config",
        path=config_file,
        notes="Current v0/v1 weight configuration used for benchmark execution",
    )

    baseline_index = {
        baseline_definition.baseline_id: baseline_definition
        for baseline_definition in protocol.baselines
        if baseline_definition.baseline_id in task_contract.supported_baseline_ids
    }
    available_now_baselines = [
        baseline_definition.baseline_id
        for baseline_definition in protocol.baselines
        if baseline_definition.baseline_id in task_contract.supported_baseline_ids
        if baseline_definition.status == AVAILABLE_NOW_STATUS
    ]
    protocol_only_baselines = [
        baseline_definition.baseline_id
        for baseline_definition in protocol.baselines
        if baseline_definition.baseline_id in task_contract.supported_baseline_ids
        if baseline_definition.status == PROTOCOL_ONLY_STATUS
    ]
    requested_available_now_baselines = [
        baseline_id
        for baseline_id in manifest.baseline_ids
        if baseline_index[baseline_id].status == AVAILABLE_NOW_STATUS
    ]
    requested_protocol_only_baselines = [
        baseline_id
        for baseline_id in manifest.baseline_ids
        if baseline_index[baseline_id].status == PROTOCOL_ONLY_STATUS
    ]

    run_manifest_files: list[str] = []
    metric_payload_files: list[str] = []
    confidence_interval_files: list[str] = []
    executed_baselines: list[str] = []

    for baseline_id in requested_available_now_baselines:
        baseline = baseline_index[baseline_id]
        predictions_by_type, baseline_notes, projection_payloads = (
            _execute_baseline_predictions(
            context,
            baseline,
            random_seed=random_seed,
        )
        )
        additional_input_refs: list[InputArtifactReference] = []
        if context.intervention_object_bundle_ref is not None:
            additional_input_refs.append(context.intervention_object_bundle_ref)
        if (
            INTERVENTION_OBJECT_ENTITY_TYPE in projection_payloads
            and INTERVENTION_OBJECT_ENTITY_TYPE in baseline.entity_types
        ):
            projection_path = intervention_object_projection_path(
                output_dir=output_dir,
                baseline_id=baseline_id,
            )
            write_intervention_object_projection_payload(
                projection_path,
                projection_payloads[INTERVENTION_OBJECT_ENTITY_TYPE],
            )
            additional_input_refs.append(
                _build_artifact_reference(
                    artifact_name="benchmark_intervention_object_baseline_projection",
                    path=projection_path,
                    schema_name="benchmark_intervention_object_baseline_projection",
                    notes=(
                        "Explicit intervention-object projection artifact built from "
                        "archived current gene/module baseline outputs via the "
                        "checked-in compatibility contract"
                    ),
                )
            )
        parameterization: dict[str, object] = {
            "benchmark_question_id": manifest.benchmark_question_id,
            "bootstrap_confidence_level": bootstrap_confidence_level,
            "bootstrap_iterations": resolved_bootstrap_iterations,
            "deterministic_test_mode": deterministic_test_mode,
            "interval_method": BOOTSTRAP_INTERVAL_METHOD,
            "random_seed": random_seed,
            "resample_unit": BOOTSTRAP_RESAMPLE_UNIT,
            "v1_resolution_method": V1_RESOLUTION_METHOD,
            "config_sha256": config_ref.sha256,
        }
        if INTERVENTION_OBJECT_ENTITY_TYPE in projection_payloads:
            parameterization["intervention_object_projection_aggregation_rule"] = (
                projection_payloads[INTERVENTION_OBJECT_ENTITY_TYPE]["aggregation_rule"]
            )
            parameterization["intervention_object_projection_contract"] = (
                projection_payloads[INTERVENTION_OBJECT_ENTITY_TYPE][
                    "compatibility_projection_contract"
                ]
            )
        run_id = _build_run_id(
            snapshot_id=manifest.snapshot_id,
            baseline_id=baseline_id,
            code_version=code_version,
            parameterization=parameterization,
        )
        started_at = execution_timestamp or _utc_now()

        for entity_type in baseline.entity_types:
            admissible_entities = context.cohort_entities.get(entity_type, ())
            if not admissible_entities:
                continue
            admissible_entity_ids = tuple(
                entity_id for entity_id, _ in admissible_entities
            )
            admissible_entity_id_set = set(admissible_entity_ids)
            predictions = predictions_by_type.get(entity_type, ())
            ranked_entity_ids = tuple(
                prediction.entity_id for prediction in predictions
            )
            covered_count = len(
                {
                    entity_id
                    for entity_id in ranked_entity_ids
                    if entity_id in admissible_entity_id_set
                }
            )
            for horizon in question.evaluation_horizons:
                relevance_index = build_positive_relevance_index(
                    context.cohort_labels,
                    entity_type=entity_type,
                    horizon=horizon,
                )
                ranked_rows = build_ranked_evaluation_rows(
                    admissible_entity_ids,
                    ranked_entity_ids,
                    relevance_index,
                )
                positive_count = count_relevant(ranked_rows)
                slice_notes = _metric_slice_notes(
                    covered_count=covered_count,
                    admissible_count=len(admissible_entity_ids),
                    positive_count=positive_count,
                    deterministic_test_mode=deterministic_test_mode,
                )
                slice_random_seed = (
                    random_seed
                    + int.from_bytes(
                        sha256(
                            f"{baseline_id}:{entity_type}:{horizon}".encode("utf-8")
                        ).digest()[:4],
                        "big",
                    )
                )
                metric_values = estimate_bootstrap_intervals(
                    ranked_rows,
                    iterations=resolved_bootstrap_iterations,
                    confidence_level=bootstrap_confidence_level,
                    random_seed=slice_random_seed,
                )
                for metric_name, (
                    point_estimate,
                    interval_low,
                    interval_high,
                ) in metric_values.items():
                    metric_payload = BenchmarkMetricOutputPayload(
                        run_id=run_id,
                        snapshot_id=manifest.snapshot_id,
                        baseline_id=baseline_id,
                        entity_type=entity_type,
                        horizon=horizon,
                        metric_name=metric_name,
                        metric_value=point_estimate,
                        cohort_size=len(ranked_rows),
                        notes=slice_notes,
                    )
                    metric_path = _build_metric_payload_path(
                        output_dir,
                        run_id=run_id,
                        entity_type=entity_type,
                        horizon=horizon,
                        metric_name=metric_name,
                    )
                    write_benchmark_metric_output_payload(metric_path, metric_payload)
                    metric_payload_files.append(str(metric_path))

                    interval_payload = BenchmarkConfidenceIntervalPayload(
                        run_id=run_id,
                        snapshot_id=manifest.snapshot_id,
                        baseline_id=baseline_id,
                        entity_type=entity_type,
                        horizon=horizon,
                        metric_name=metric_name,
                        point_estimate=point_estimate,
                        interval_low=interval_low,
                        interval_high=interval_high,
                        confidence_level=bootstrap_confidence_level,
                        bootstrap_iterations=resolved_bootstrap_iterations,
                        resample_unit=BOOTSTRAP_RESAMPLE_UNIT,
                        random_seed=slice_random_seed,
                        notes=(
                            f"method={BOOTSTRAP_INTERVAL_METHOD}; "
                            f"{slice_notes}"
                        ),
                    )
                    interval_path = _build_interval_payload_path(
                        output_dir,
                        run_id=run_id,
                        entity_type=entity_type,
                        horizon=horizon,
                        metric_name=metric_name,
                    )
                    write_benchmark_confidence_interval_payload(
                        interval_path,
                        interval_payload,
                    )
                    confidence_interval_files.append(str(interval_path))

        completed_at = execution_timestamp or _utc_now()
        run_manifest = BenchmarkModelRunManifest(
            run_id=run_id,
            snapshot_id=manifest.snapshot_id,
            baseline_id=baseline_id,
            model_family=baseline.family,
            code_version=code_version,
            benchmark_suite_id=task_contract.suite_id,
            benchmark_task_id=task_contract.task_id,
            parameterization=parameterization,
            input_artifacts=_baseline_input_artifacts(
                baseline_id=baseline_id,
                manifest_ref=manifest_ref,
                cohort_manifest_ref=cohort_manifest_ref,
                cohort_members_ref=cohort_members_ref,
                cohort_ref=cohort_ref,
                archive_index_ref=archive_index_ref,
                config_ref=config_ref,
                source_refs=context.included_source_refs,
                additional_refs=cohort_source_input_refs + tuple(additional_input_refs),
            ),
            started_at=started_at,
            completed_at=completed_at,
            notes="; ".join(baseline_notes),
        )
        run_manifest_path = _build_run_manifest_path(output_dir, run_id)
        write_benchmark_model_run_manifest(run_manifest_path, run_manifest)
        run_manifest_files.append(str(run_manifest_path))
        executed_baselines.append(baseline_id)

    return {
        "benchmark_suite_id": task_contract.suite_id,
        "benchmark_task_id": task_contract.task_id,
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_dir": str(output_dir),
        "code_version": code_version,
        "executed_baselines": executed_baselines,
        "requested_available_now_baselines": requested_available_now_baselines,
        "available_now_baselines": available_now_baselines,
        "protocol_only_baselines": protocol_only_baselines,
        "requested_protocol_only_baselines": requested_protocol_only_baselines,
        "run_manifest_files": run_manifest_files,
        "metric_payload_files": metric_payload_files,
        "confidence_interval_files": confidence_interval_files,
        "bootstrap_iterations": resolved_bootstrap_iterations,
        "bootstrap_confidence_level": bootstrap_confidence_level,
        "interval_method": BOOTSTRAP_INTERVAL_METHOD,
        "resample_unit": BOOTSTRAP_RESAMPLE_UNIT,
        "v1_resolution_method": V1_RESOLUTION_METHOD,
    }


def materialize_benchmark_run(
    *,
    manifest_file: Path,
    cohort_labels_file: Path,
    archive_index_file: Path,
    output_dir: Path,
    config_file: Path | None = None,
    code_version: str | None = None,
    bootstrap_iterations: int | None = None,
    bootstrap_confidence_level: float = DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL,
    random_seed: int = DEFAULT_RANDOM_BASELINE_SEED,
    deterministic_test_mode: bool = False,
    execution_timestamp: str | None = None,
) -> dict[str, object]:
    resolved_config_file = (
        config_file.resolve()
        if config_file is not None
        else (Path(__file__).resolve().parents[2] / "config" / "v0.toml").resolve()
    )
    repo_root = Path(__file__).resolve().parents[2]
    resolved_code_version = code_version or _resolve_code_version(repo_root)
    return run_benchmark(
        manifest_file=manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=archive_index_file,
        output_dir=output_dir,
        config_file=resolved_config_file,
        code_version=resolved_code_version,
        bootstrap_iterations=bootstrap_iterations,
        bootstrap_confidence_level=bootstrap_confidence_level,
        random_seed=random_seed,
        deterministic_test_mode=deterministic_test_mode,
        execution_timestamp=execution_timestamp,
    )


__all__ = [
    "BenchmarkModelRunManifest",
    "InputArtifactReference",
    "RUN_MANIFEST_SCHEMA_NAME",
    "RUN_MANIFEST_SCHEMA_VERSION",
    "materialize_benchmark_run",
    "read_benchmark_model_run_manifest",
    "run_benchmark",
    "write_benchmark_model_run_manifest",
]
