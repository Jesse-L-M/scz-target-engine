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
    read_benchmark_cohort_labels,
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
    BENCHMARK_QUESTION_V1,
    FROZEN_BASELINE_MATRIX,
    GENE_ENTITY_TYPE,
    MODULE_ENTITY_TYPE,
    PROTOCOL_ONLY_STATUS,
    BaselineDefinition,
    BenchmarkSnapshotManifest,
)
from scz_target_engine.benchmark_snapshots import (
    SourceArchiveDescriptor,
    load_source_archive_descriptors,
    read_benchmark_snapshot_manifest,
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
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")

    def to_dict(self) -> dict[str, object]:
        return {
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


def _validate_cohort_labels(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
) -> None:
    if not cohort_labels:
        raise ValueError("benchmark cohort labels must contain at least one row")
    snapshot_ids = {label.snapshot_id for label in cohort_labels}
    if snapshot_ids != {manifest.snapshot_id}:
        raise ValueError("benchmark cohort labels must match the manifest snapshot_id")
    cohort_ids = {label.cohort_id for label in cohort_labels}
    if cohort_ids != {manifest.cohort_id}:
        raise ValueError("benchmark cohort labels must match the manifest cohort_id")


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


def _build_context(
    manifest: BenchmarkSnapshotManifest,
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    archive_descriptors: tuple[SourceArchiveDescriptor, ...],
    config: EngineConfig,
) -> BenchmarkExecutionContext:
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


def _execute_baseline_predictions(
    context: BenchmarkExecutionContext,
    baseline: BaselineDefinition,
    *,
    random_seed: int,
) -> tuple[dict[str, tuple[PredictionRow, ...]], list[str]]:
    notes: list[str] = []
    predictions_by_type: dict[str, tuple[PredictionRow, ...]] = {}
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
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_v0_current(context.v0_ranked_rows[GENE_ENTITY_TYPE]),
        )
        predictions_by_type[MODULE_ENTITY_TYPE] = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_v0_current(context.v0_ranked_rows[MODULE_ENTITY_TYPE]),
        )
    elif baseline.baseline_id == "v1_current":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_v1_current(context.ranked_entities[GENE_ENTITY_TYPE]),
        )
        predictions_by_type[MODULE_ENTITY_TYPE] = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_v1_current(context.ranked_entities[MODULE_ENTITY_TYPE]),
        )
        notes.append(
            "v1_current benchmark score resolves current additive v1 output via "
            f"{V1_RESOLUTION_METHOD}"
        )
    elif baseline.baseline_id == "random_with_coverage":
        predictions_by_type[GENE_ENTITY_TYPE] = _with_entity_type(
            GENE_ENTITY_TYPE,
            _execute_random_with_coverage(
                cohort_entities=context.cohort_entities[GENE_ENTITY_TYPE],
                snapshot_id=context.manifest.snapshot_id,
                entity_type=GENE_ENTITY_TYPE,
                seed=random_seed,
            ),
        )
        predictions_by_type[MODULE_ENTITY_TYPE] = _with_entity_type(
            MODULE_ENTITY_TYPE,
            _execute_random_with_coverage(
                cohort_entities=context.cohort_entities[MODULE_ENTITY_TYPE],
                snapshot_id=context.manifest.snapshot_id,
                entity_type=MODULE_ENTITY_TYPE,
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
    return predictions_by_type, notes


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
    cohort_ref: InputArtifactReference,
    archive_index_ref: InputArtifactReference,
    config_ref: InputArtifactReference,
    source_refs: dict[str, InputArtifactReference],
) -> tuple[InputArtifactReference, ...]:
    refs = [manifest_ref, cohort_ref, archive_index_ref]
    if baseline_id in {"v0_current", "v1_current"}:
        refs.append(config_ref)
    for source_name in BASELINE_SOURCE_DEPENDENCIES[baseline_id]:
        source_ref = source_refs.get(source_name)
        if source_ref is not None:
            refs.append(source_ref)
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
    cohort_labels = read_benchmark_cohort_labels(cohort_labels_file)
    _validate_cohort_labels(manifest, cohort_labels)
    config = load_config(config_file)
    archive_descriptors = load_source_archive_descriptors(archive_index_file)

    resolved_bootstrap_iterations = bootstrap_iterations
    if resolved_bootstrap_iterations is None:
        resolved_bootstrap_iterations = DEFAULT_BOOTSTRAP_ITERATIONS
        if deterministic_test_mode:
            resolved_bootstrap_iterations = DETERMINISTIC_TEST_BOOTSTRAP_ITERATIONS

    context = _build_context(
        manifest,
        cohort_labels,
        archive_descriptors,
        config,
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
        for baseline_definition in FROZEN_BASELINE_MATRIX
    }
    available_now_baselines = [
        baseline_definition.baseline_id
        for baseline_definition in FROZEN_BASELINE_MATRIX
        if baseline_definition.status == AVAILABLE_NOW_STATUS
    ]
    protocol_only_baselines = [
        baseline_definition.baseline_id
        for baseline_definition in FROZEN_BASELINE_MATRIX
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
        predictions_by_type, baseline_notes = _execute_baseline_predictions(
            context,
            baseline,
            random_seed=random_seed,
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
            for horizon in BENCHMARK_QUESTION_V1.evaluation_horizons:
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
            parameterization=parameterization,
            input_artifacts=_baseline_input_artifacts(
                baseline_id=baseline_id,
                manifest_ref=manifest_ref,
                cohort_ref=cohort_ref,
                archive_index_ref=archive_index_ref,
                config_ref=config_ref,
                source_refs=context.included_source_refs,
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
