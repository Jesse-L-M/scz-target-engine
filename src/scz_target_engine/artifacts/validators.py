from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
import csv
from pathlib import Path
from typing import Any

from scz_target_engine.artifacts.models import (
    ArtifactSchemaDefinition,
    ValidatedArtifact,
)
from scz_target_engine.artifacts.registry import (
    get_artifact_schema,
    load_artifact_schemas,
)
from scz_target_engine.benchmark_labels import (
    BenchmarkCohortLabel,
    read_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_metrics import (
    BenchmarkConfidenceIntervalPayload,
    BenchmarkMetricOutputPayload,
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_protocol import BenchmarkSnapshotManifest
from scz_target_engine.benchmark_runner import (
    BenchmarkModelRunManifest,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import read_benchmark_snapshot_manifest
from scz_target_engine.decision_vector import (
    AVAILABLE_STATUS,
    DECISION_HEAD_DEFINITIONS,
    DOMAIN_HEAD_DEFINITIONS,
    PARTIAL_STATUS,
)
from scz_target_engine.io import read_json
from scz_target_engine.ledger import TargetLedger
from scz_target_engine.policy.config import REQUIRED_POLICY_ADJUSTMENT_FIELDS
from scz_target_engine.rescue.contracts import (
    RescueTaskContract,
    read_rescue_task_contract,
)
from scz_target_engine.rescue.governance import (
    RescueDatasetCard,
    RescueFreezeManifest,
    RescueRawToFrozenLineage,
    RescueSplitManifest,
    RescueTaskCard,
    read_rescue_dataset_card,
    read_rescue_freeze_manifest,
    read_rescue_raw_to_frozen_lineage,
    read_rescue_split_manifest,
    validate_rescue_governance_bundle,
)


def _load_json_mapping(path: Path) -> dict[str, object]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _read_csv_artifact(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{path} must include a CSV header row")
        return list(reader.fieldnames), list(reader)


def _require_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _require_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_string_list(value: object, field_name: str) -> list[str]:
    values = _require_list(value, field_name)
    return [_require_text(item, f"{field_name}[]") for item in values]


def _require_optional_number(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a number or null")
    return float(value)


def _parse_optional_csv_float(value: str, field_name: str) -> float | None:
    if value == "":
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a float-compatible value") from exc


def _parse_optional_csv_int(value: str, field_name: str) -> int | None:
    if value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer-compatible value") from exc


def _parse_csv_bool(value: str, field_name: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"{field_name} must be True or False")


def _ensure_required_fields(
    schema: ArtifactSchemaDefinition,
    present_fields: set[str],
    *,
    context: str,
) -> None:
    missing = [
        field_name
        for field_name in schema.required_field_names
        if field_name not in present_fields
    ]
    if missing:
        raise ValueError(f"{context} is missing required fields: {', '.join(missing)}")


def _validate_benchmark_snapshot_manifest(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> BenchmarkSnapshotManifest:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return BenchmarkSnapshotManifest.from_dict(payload)


def _validate_benchmark_cohort_labels(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> tuple[BenchmarkCohortLabel, ...]:
    fieldnames, rows = _read_csv_artifact(path)
    _ensure_required_fields(
        schema,
        set(fieldnames),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if not rows:
        raise ValueError("benchmark cohort labels artifact must contain at least one row")
    return read_benchmark_cohort_labels(path)


def _validate_benchmark_model_run_manifest(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> BenchmarkModelRunManifest:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_benchmark_model_run_manifest(path)


def _validate_benchmark_metric_output_payload(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> BenchmarkMetricOutputPayload:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_benchmark_metric_output_payload(path)


def _validate_benchmark_confidence_interval_payload(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> BenchmarkConfidenceIntervalPayload:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_benchmark_confidence_interval_payload(path)


def _validate_rescue_task_contract(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueTaskContract:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_rescue_task_contract(path)


def _validate_rescue_dataset_card(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueDatasetCard:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_rescue_dataset_card(path)


def _validate_rescue_task_card(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueTaskCard:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return validate_rescue_governance_bundle(path).task_card


def _validate_rescue_freeze_manifest(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueFreezeManifest:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_rescue_freeze_manifest(path)


def _validate_rescue_split_manifest(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueSplitManifest:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_rescue_split_manifest(path)


def _validate_rescue_raw_to_frozen_lineage(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> RescueRawToFrozenLineage:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    return read_rescue_raw_to_frozen_lineage(path)


def _validate_structural_failure_history(
    payload: Mapping[str, object],
    *,
    field_name: str,
) -> Mapping[str, object]:
    required_fields = {
        "matched_event_count",
        "failure_event_count",
        "nonfailure_event_count",
        "event_count_by_scope",
        "failure_taxonomy_counts",
        "failure_scopes",
        "failure_taxonomies",
        "events",
    }
    missing = sorted(required_fields.difference(payload))
    if missing:
        raise ValueError(
            f"{field_name} is missing required fields: {', '.join(missing)}"
        )

    events = _require_list(payload["events"], f"{field_name}.events")
    validated_events: list[Mapping[str, object]] = []
    for index, item in enumerate(events):
        event = _require_mapping(item, f"{field_name}.events[{index}]")
        where = _require_mapping(event.get("where"), f"{field_name}.events[{index}].where")
        for required_key in ("domain", "population", "phase", "mono_or_adjunct"):
            _require_string(
                where.get(required_key, ""),
                f"{field_name}.events[{index}].where.{required_key}",
            )
        for required_key in (
            "program_id",
            "event_date",
            "event_type",
            "target",
            "failure_reason_taxonomy",
            "failure_scope",
            "what_failed",
            "evidence_strength",
            "confidence",
            "source_tier",
            "source_url",
        ):
            _require_text(
                event.get(required_key, ""),
                f"{field_name}.events[{index}].{required_key}",
            )
        for optional_key in (
            "molecule",
            "target_class",
            "mechanism",
            "modality",
            "phase",
            "mono_or_adjunct",
            "primary_outcome_result",
            "notes",
        ):
            _require_string(
                event.get(optional_key, ""),
                f"{field_name}.events[{index}].{optional_key}",
            )
        validated_events.append(event)

    scope_counts = Counter(
        _require_text(event["failure_scope"], f"{field_name}.events[].failure_scope")
        for event in validated_events
    )
    taxonomy_counts = Counter(
        _require_text(
            event["failure_reason_taxonomy"],
            f"{field_name}.events[].failure_reason_taxonomy",
        )
        for event in validated_events
    )
    failure_event_count = sum(scope != "nonfailure" for scope in scope_counts.elements())
    nonfailure_event_count = sum(scope == "nonfailure" for scope in scope_counts.elements())
    if _require_int(payload["matched_event_count"], f"{field_name}.matched_event_count") != len(
        validated_events
    ):
        raise ValueError(f"{field_name}.matched_event_count must match the number of events")
    if _require_int(payload["failure_event_count"], f"{field_name}.failure_event_count") != (
        failure_event_count
    ):
        raise ValueError(f"{field_name}.failure_event_count does not match derived events")
    if _require_int(
        payload["nonfailure_event_count"],
        f"{field_name}.nonfailure_event_count",
    ) != nonfailure_event_count:
        raise ValueError(f"{field_name}.nonfailure_event_count does not match derived events")

    event_count_by_scope = _require_mapping(
        payload["event_count_by_scope"],
        f"{field_name}.event_count_by_scope",
    )
    normalized_scope_counts = {
        key: _require_int(value, f"{field_name}.event_count_by_scope.{key}")
        for key, value in event_count_by_scope.items()
    }
    if dict(sorted(scope_counts.items())) != dict(sorted(normalized_scope_counts.items())):
        raise ValueError(f"{field_name}.event_count_by_scope does not match derived events")

    failure_taxonomy_counts = _require_mapping(
        payload["failure_taxonomy_counts"],
        f"{field_name}.failure_taxonomy_counts",
    )
    normalized_taxonomy_counts = {
        key: _require_int(value, f"{field_name}.failure_taxonomy_counts.{key}")
        for key, value in failure_taxonomy_counts.items()
    }
    if dict(sorted(taxonomy_counts.items())) != dict(sorted(normalized_taxonomy_counts.items())):
        raise ValueError(
            f"{field_name}.failure_taxonomy_counts does not match derived events"
        )

    failure_scopes = _require_string_list(payload["failure_scopes"], f"{field_name}.failure_scopes")
    expected_failure_scopes = sorted(
        scope for scope in scope_counts if scope != "nonfailure"
    )
    if failure_scopes != expected_failure_scopes:
        raise ValueError(f"{field_name}.failure_scopes does not match derived events")

    failure_taxonomies = _require_string_list(
        payload["failure_taxonomies"],
        f"{field_name}.failure_taxonomies",
    )
    expected_failure_taxonomies = sorted(
        taxonomy
        for taxonomy in taxonomy_counts
        if taxonomy != "not_applicable_nonfailure"
    )
    if failure_taxonomies != expected_failure_taxonomies:
        raise ValueError(f"{field_name}.failure_taxonomies does not match derived events")

    return payload


def _validate_directionality_hypothesis(
    payload: Mapping[str, object],
    *,
    field_name: str,
) -> Mapping[str, object]:
    required_string_fields = (
        "status",
        "desired_perturbation_direction",
        "modality_hypothesis",
        "confidence",
    )
    optional_string_fields = ("ambiguity", "evidence_basis")
    required_list_fields = (
        "preferred_modalities",
        "supporting_program_ids",
        "contradiction_conditions",
        "falsification_conditions",
        "open_risks",
    )
    for required_key in required_string_fields:
        _require_text(payload.get(required_key, ""), f"{field_name}.{required_key}")
    for optional_key in optional_string_fields:
        _require_string(payload.get(optional_key, ""), f"{field_name}.{optional_key}")
    for required_key in required_list_fields:
        _require_string_list(payload.get(required_key), f"{field_name}.{required_key}")
    return payload


def _validate_gene_target_ledgers(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if payload.get("schema_version") != schema.schema_version:
        raise ValueError(
            f"{schema.artifact_name} schema_version must be {schema.schema_version}"
        )
    _require_bool(payload.get("scoring_neutral"), "gene_target_ledgers.scoring_neutral")
    data_sources = _require_mapping(payload.get("data_sources"), "gene_target_ledgers.data_sources")
    _require_text(
        data_sources.get("program_history", ""),
        "gene_target_ledgers.data_sources.program_history",
    )
    _require_text(
        data_sources.get("directionality_hypotheses", ""),
        "gene_target_ledgers.data_sources.directionality_hypotheses",
    )

    targets = _require_list(payload.get("targets"), "gene_target_ledgers.targets")
    if _require_int(payload.get("target_count"), "gene_target_ledgers.target_count") != len(
        targets
    ):
        raise ValueError("gene_target_ledgers.target_count must match the number of targets")

    validated_targets: list[TargetLedger] = []
    for index, item in enumerate(targets):
        target_payload = _require_mapping(item, f"gene_target_ledgers.targets[{index}]")
        required_fields = (
            "entity_id",
            "entity_label",
            "v0_snapshot",
            "source_primitives",
            "subgroup_domain_relevance",
            "structural_failure_history",
            "directionality_hypothesis",
            "falsification_conditions",
            "open_risks",
        )
        for required_key in required_fields:
            if required_key not in target_payload:
                raise ValueError(
                    f"gene_target_ledgers.targets[{index}] is missing {required_key}"
                )

        subgroup_domain_relevance = _require_mapping(
            target_payload["subgroup_domain_relevance"],
            f"gene_target_ledgers.targets[{index}].subgroup_domain_relevance",
        )
        for list_field in (
            "clinical_domains",
            "clinical_populations",
            "mono_or_adjunct_contexts",
            "psychencode_deg_top_cell_types",
            "psychencode_grn_top_cell_types",
        ):
            _require_list(
                subgroup_domain_relevance.get(list_field),
                (
                    "gene_target_ledgers.targets"
                    f"[{index}].subgroup_domain_relevance.{list_field}"
                ),
            )

        structural_failure_history = _validate_structural_failure_history(
            _require_mapping(
                target_payload["structural_failure_history"],
                f"gene_target_ledgers.targets[{index}].structural_failure_history",
            ),
            field_name=(
                "gene_target_ledgers.targets"
                f"[{index}].structural_failure_history"
            ),
        )
        directionality_hypothesis = _validate_directionality_hypothesis(
            _require_mapping(
                target_payload["directionality_hypothesis"],
                f"gene_target_ledgers.targets[{index}].directionality_hypothesis",
            ),
            field_name=(
                "gene_target_ledgers.targets"
                f"[{index}].directionality_hypothesis"
            ),
        )
        falsification_conditions = _require_string_list(
            target_payload["falsification_conditions"],
            f"gene_target_ledgers.targets[{index}].falsification_conditions",
        )
        open_risks = _require_list(
            target_payload["open_risks"],
            f"gene_target_ledgers.targets[{index}].open_risks",
        )
        for risk_index, risk_item in enumerate(open_risks):
            risk = _require_mapping(
                risk_item,
                f"gene_target_ledgers.targets[{index}].open_risks[{risk_index}]",
            )
            for required_key in ("source", "risk_kind", "severity", "text"):
                _require_text(
                    risk.get(required_key, ""),
                    (
                        "gene_target_ledgers.targets"
                        f"[{index}].open_risks[{risk_index}].{required_key}"
                    ),
                )

        validated_targets.append(
            TargetLedger(
                entity_id=_require_text(
                    target_payload["entity_id"],
                    f"gene_target_ledgers.targets[{index}].entity_id",
                ),
                entity_label=_require_text(
                    target_payload["entity_label"],
                    f"gene_target_ledgers.targets[{index}].entity_label",
                ),
                v0_snapshot=dict(
                    _require_mapping(
                        target_payload["v0_snapshot"],
                        f"gene_target_ledgers.targets[{index}].v0_snapshot",
                    )
                ),
                source_primitives=dict(
                    _require_mapping(
                        target_payload["source_primitives"],
                        f"gene_target_ledgers.targets[{index}].source_primitives",
                    )
                ),
                subgroup_domain_relevance=dict(subgroup_domain_relevance),
                structural_failure_history=dict(structural_failure_history),
                directionality_hypothesis=dict(directionality_hypothesis),
                falsification_conditions=falsification_conditions,
                open_risks=list(open_risks),
            )
        )

    derived_program_history_count = sum(
        1
        for target in validated_targets
        if target.structural_failure_history["matched_event_count"]
    )
    if _require_int(
        payload.get("targets_with_program_history"),
        "gene_target_ledgers.targets_with_program_history",
    ) != derived_program_history_count:
        raise ValueError(
            "gene_target_ledgers.targets_with_program_history does not match targets"
        )

    derived_directionality_count = sum(
        1
        for target in validated_targets
        if target.directionality_hypothesis["status"] == "curated"
    )
    if _require_int(
        payload.get("targets_with_curated_directionality"),
        "gene_target_ledgers.targets_with_curated_directionality",
    ) != derived_directionality_count:
        raise ValueError(
            "gene_target_ledgers.targets_with_curated_directionality does not match targets"
        )

    return payload


def _validate_decision_head_definition_payloads(
    payloads: list[object],
) -> None:
    expected_names = [definition.name for definition in DECISION_HEAD_DEFINITIONS]
    observed_names: list[str] = []
    for index, item in enumerate(payloads):
        payload = _require_mapping(item, f"decision_head_definitions[{index}]")
        observed_names.append(
            _require_text(payload.get("name", ""), f"decision_head_definitions[{index}].name")
        )
        _require_text(
            payload.get("label", ""),
            f"decision_head_definitions[{index}].label",
        )
        _require_text(
            payload.get("semantics", ""),
            f"decision_head_definitions[{index}].semantics",
        )
        _require_mapping(
            payload.get("entity_inputs"),
            f"decision_head_definitions[{index}].entity_inputs",
        )
    if observed_names != expected_names:
        raise ValueError("decision_head_definitions must match the current v1 head order")


def _validate_domain_head_definition_payloads(
    payloads: list[object],
) -> None:
    expected_slugs = [definition.slug for definition in DOMAIN_HEAD_DEFINITIONS]
    observed_slugs: list[str] = []
    for index, item in enumerate(payloads):
        payload = _require_mapping(item, f"domain_head_definitions[{index}]")
        observed_slugs.append(
            _require_text(payload.get("slug", ""), f"domain_head_definitions[{index}].slug")
        )
        _require_text(
            payload.get("label", ""),
            f"domain_head_definitions[{index}].label",
        )
        _require_text(
            payload.get("axis", ""),
            f"domain_head_definitions[{index}].axis",
        )
        _require_text(
            payload.get("semantics", ""),
            f"domain_head_definitions[{index}].semantics",
        )
        _require_list(
            payload.get("decision_head_weights"),
            f"domain_head_definitions[{index}].decision_head_weights",
        )
    if observed_slugs != expected_slugs:
        raise ValueError("domain_head_definitions must match the current v1 domain order")


def _validate_decision_vector_entity(
    payload: Mapping[str, object],
    *,
    field_name: str,
    expected_entity_type: str,
) -> None:
    _require_text(payload.get("entity_type", ""), f"{field_name}.entity_type")
    if payload["entity_type"] != expected_entity_type:
        raise ValueError(f"{field_name}.entity_type must be {expected_entity_type}")
    _require_text(payload.get("entity_id", ""), f"{field_name}.entity_id")
    _require_text(payload.get("entity_label", ""), f"{field_name}.entity_label")
    _require_bool(payload.get("eligible_v0"), f"{field_name}.eligible_v0")
    _require_optional_number(payload.get("heuristic_score_v0"), f"{field_name}.heuristic_score_v0")
    if payload.get("heuristic_rank_v0") is not None:
        _require_int(payload["heuristic_rank_v0"], f"{field_name}.heuristic_rank_v0")
    _require_bool(payload.get("heuristic_stable_v0"), f"{field_name}.heuristic_stable_v0")
    warning_count = _require_int(payload.get("warning_count"), f"{field_name}.warning_count")
    if warning_count < 0:
        raise ValueError(f"{field_name}.warning_count must be non-negative")
    _require_text(payload.get("warning_severity", ""), f"{field_name}.warning_severity")

    expected_head_names = [definition.name for definition in DECISION_HEAD_DEFINITIONS]
    expected_domain_slugs = [definition.slug for definition in DOMAIN_HEAD_DEFINITIONS]

    decision_vector = _require_mapping(payload.get("decision_vector"), f"{field_name}.decision_vector")
    if sorted(decision_vector) != sorted(expected_head_names):
        raise ValueError(f"{field_name}.decision_vector keys do not match current v1 heads")
    for head_name in expected_head_names:
        head_payload = _require_mapping(
            decision_vector[head_name],
            f"{field_name}.decision_vector.{head_name}",
        )
        for required_key in (
            "label",
            "status",
            "semantics",
            "used_inputs",
            "missing_inputs",
            "coverage_weight_fraction",
            "pending_reason",
            "score",
        ):
            if required_key not in head_payload:
                raise ValueError(
                    f"{field_name}.decision_vector.{head_name} is missing {required_key}"
                )
        _require_text(
            head_payload["label"],
            f"{field_name}.decision_vector.{head_name}.label",
        )
        _require_text(
            head_payload["status"],
            f"{field_name}.decision_vector.{head_name}.status",
        )
        _require_text(
            head_payload["semantics"],
            f"{field_name}.decision_vector.{head_name}.semantics",
        )
        _require_string_list(
            head_payload["used_inputs"],
            f"{field_name}.decision_vector.{head_name}.used_inputs",
        )
        _require_string_list(
            head_payload["missing_inputs"],
            f"{field_name}.decision_vector.{head_name}.missing_inputs",
        )
        _require_optional_number(
            head_payload["score"],
            f"{field_name}.decision_vector.{head_name}.score",
        )
        _require_optional_number(
            head_payload["coverage_weight_fraction"],
            f"{field_name}.decision_vector.{head_name}.coverage_weight_fraction",
        )
        pending_reason = head_payload["pending_reason"]
        if pending_reason is not None:
            _require_text(
                pending_reason,
                f"{field_name}.decision_vector.{head_name}.pending_reason",
            )
        _require_text(payload.get(f"{head_name}_status", ""), f"{field_name}.{head_name}_status")
        _require_optional_number(payload.get(head_name), f"{field_name}.{head_name}")

    domain_profiles = _require_mapping(payload.get("domain_profiles"), f"{field_name}.domain_profiles")
    if sorted(domain_profiles) != sorted(expected_domain_slugs):
        raise ValueError(f"{field_name}.domain_profiles keys do not match current v1 domains")
    for domain_slug in expected_domain_slugs:
        domain_payload = _require_mapping(
            domain_profiles[domain_slug],
            f"{field_name}.domain_profiles.{domain_slug}",
        )
        for required_key in (
            "score",
            "label",
            "axis",
            "status",
            "semantics",
            "coverage_weight_fraction",
            "available_head_count",
            "total_head_count",
            "pending_head_names",
            "missing_head_names",
            "projected_head_scores",
        ):
            if required_key not in domain_payload:
                raise ValueError(
                    f"{field_name}.domain_profiles.{domain_slug} is missing {required_key}"
                )
        _require_optional_number(
            domain_payload["score"],
            f"{field_name}.domain_profiles.{domain_slug}.score",
        )
        _require_text(
            domain_payload["label"],
            f"{field_name}.domain_profiles.{domain_slug}.label",
        )
        _require_text(
            domain_payload["axis"],
            f"{field_name}.domain_profiles.{domain_slug}.axis",
        )
        _require_text(
            domain_payload["status"],
            f"{field_name}.domain_profiles.{domain_slug}.status",
        )
        _require_text(
            domain_payload["semantics"],
            f"{field_name}.domain_profiles.{domain_slug}.semantics",
        )
        _require_optional_number(
            domain_payload["coverage_weight_fraction"],
            f"{field_name}.domain_profiles.{domain_slug}.coverage_weight_fraction",
        )
        _require_int(
            domain_payload["available_head_count"],
            f"{field_name}.domain_profiles.{domain_slug}.available_head_count",
        )
        _require_int(
            domain_payload["total_head_count"],
            f"{field_name}.domain_profiles.{domain_slug}.total_head_count",
        )
        _require_string_list(
            domain_payload["pending_head_names"],
            f"{field_name}.domain_profiles.{domain_slug}.pending_head_names",
        )
        _require_string_list(
            domain_payload["missing_head_names"],
            f"{field_name}.domain_profiles.{domain_slug}.missing_head_names",
        )
        projected_head_scores = _require_mapping(
            domain_payload["projected_head_scores"],
            f"{field_name}.domain_profiles.{domain_slug}.projected_head_scores",
        )
        if not projected_head_scores:
            raise ValueError(
                f"{field_name}.domain_profiles.{domain_slug}.projected_head_scores must not be empty"
            )
        for head_name, score_value in projected_head_scores.items():
            if head_name not in expected_head_names:
                raise ValueError(
                    f"{field_name}.domain_profiles.{domain_slug}.projected_head_scores contains unknown head {head_name}"
                )
            _require_optional_number(
                score_value,
                f"{field_name}.domain_profiles.{domain_slug}.projected_head_scores.{head_name}",
            )

    head_scores = _require_list(payload.get("head_scores"), f"{field_name}.head_scores")
    head_score_names: list[str] = []
    for index, item in enumerate(head_scores):
        score_payload = _require_mapping(item, f"{field_name}.head_scores[{index}]")
        head_name = _require_text(
            score_payload.get("head_name", ""),
            f"{field_name}.head_scores[{index}].head_name",
        )
        head_score_names.append(head_name)
        for required_key in (
            "label",
            "status",
            "semantics",
            "used_inputs",
            "missing_inputs",
            "coverage_weight_fraction",
            "pending_reason",
            "score",
        ):
            if required_key not in score_payload:
                raise ValueError(
                    f"{field_name}.head_scores[{index}] is missing {required_key}"
                )
    if head_score_names != expected_head_names:
        raise ValueError(f"{field_name}.head_scores must preserve the current v1 head order")

    domain_head_scores = _require_list(
        payload.get("domain_head_scores"),
        f"{field_name}.domain_head_scores",
    )
    domain_head_slugs: list[str] = []
    for index, item in enumerate(domain_head_scores):
        score_payload = _require_mapping(
            item,
            f"{field_name}.domain_head_scores[{index}]",
        )
        domain_head_slugs.append(
            _require_text(
                score_payload.get("domain_slug", ""),
                f"{field_name}.domain_head_scores[{index}].domain_slug",
            )
        )
    if domain_head_slugs != expected_domain_slugs:
        raise ValueError(
            f"{field_name}.domain_head_scores must preserve the current v1 domain order"
        )


def _validate_decision_vectors(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if payload.get("schema_version") != schema.schema_version:
        raise ValueError(
            f"{schema.artifact_name} schema_version must be {schema.schema_version}"
        )

    decision_head_definitions = _require_list(
        payload.get("decision_head_definitions"),
        "decision_vectors_v1.decision_head_definitions",
    )
    domain_head_definitions = _require_list(
        payload.get("domain_head_definitions"),
        "decision_vectors_v1.domain_head_definitions",
    )
    _validate_decision_head_definition_payloads(decision_head_definitions)
    _validate_domain_head_definition_payloads(domain_head_definitions)

    entities = _require_mapping(payload.get("entities"), "decision_vectors_v1.entities")
    for entity_type in ("gene", "module"):
        entity_payloads = _require_list(
            entities.get(entity_type),
            f"decision_vectors_v1.entities.{entity_type}",
        )
        for index, item in enumerate(entity_payloads):
            _validate_decision_vector_entity(
                _require_mapping(
                    item,
                    f"decision_vectors_v1.entities.{entity_type}[{index}]",
                ),
                field_name=f"decision_vectors_v1.entities.{entity_type}[{index}]",
                expected_entity_type=entity_type,
            )
    return payload


def _validate_policy_definition_payloads(
    payloads: list[object],
) -> tuple[list[str], set[str]]:
    valid_domain_slugs = {definition.slug for definition in DOMAIN_HEAD_DEFINITIONS}
    observed_policy_ids: list[str] = []
    for index, item in enumerate(payloads):
        payload = _require_mapping(item, f"policy_definitions[{index}]")
        policy_id = _require_text(
            payload.get("policy_id", ""),
            f"policy_definitions[{index}].policy_id",
        )
        observed_policy_ids.append(policy_id)
        _require_text(
            payload.get("label", ""),
            f"policy_definitions[{index}].label",
        )
        _require_text(
            payload.get("description", ""),
            f"policy_definitions[{index}].description",
        )
        primary_domain_slug = _require_text(
            payload.get("primary_domain_slug", ""),
            f"policy_definitions[{index}].primary_domain_slug",
        )
        if primary_domain_slug not in valid_domain_slugs:
            raise ValueError(
                f"policy_definitions[{index}].primary_domain_slug must match the current v1 domains"
            )
        _require_text(
            payload.get("source_file", ""),
            f"policy_definitions[{index}].source_file",
        )
        domain_weights = _require_list(
            payload.get("domain_weights"),
            f"policy_definitions[{index}].domain_weights",
        )
        if not domain_weights:
            raise ValueError(f"policy_definitions[{index}].domain_weights must not be empty")
        weight_total = 0.0
        for weight_index, domain_weight in enumerate(domain_weights):
            weight_payload = _require_mapping(
                domain_weight,
                f"policy_definitions[{index}].domain_weights[{weight_index}]",
            )
            domain_slug = _require_text(
                weight_payload.get("domain_slug", ""),
                f"policy_definitions[{index}].domain_weights[{weight_index}].domain_slug",
            )
            if domain_slug not in valid_domain_slugs:
                raise ValueError(
                    "policy_definitions"
                    f"[{index}].domain_weights[{weight_index}].domain_slug must match the current v1 domains"
                )
            weight_total += _require_optional_number(
                weight_payload.get("weight"),
                f"policy_definitions[{index}].domain_weights[{weight_index}].weight",
            ) or 0.0
            if weight_payload.get("weight") is None:
                raise ValueError(
                    f"policy_definitions[{index}].domain_weights[{weight_index}].weight is required"
                )
            if float(weight_payload["weight"]) <= 0:
                raise ValueError(
                    f"policy_definitions[{index}].domain_weights[{weight_index}].weight must be positive"
                )
        if abs(weight_total - 1.0) > 1e-6:
            raise ValueError(f"policy_definitions[{index}].domain_weights must sum to 1.0")
        adjustment_weights = _require_mapping(
            payload.get("adjustment_weights"),
            f"policy_definitions[{index}].adjustment_weights",
        )
        for adjustment_name in REQUIRED_POLICY_ADJUSTMENT_FIELDS:
            if adjustment_name not in adjustment_weights:
                raise ValueError(
                    f"policy_definitions[{index}].adjustment_weights.{adjustment_name} is required"
                )
            _require_optional_number(
                adjustment_weights.get(adjustment_name),
                f"policy_definitions[{index}].adjustment_weights.{adjustment_name}",
            )
    if len(set(observed_policy_ids)) != len(observed_policy_ids):
        raise ValueError("policy_definitions must not repeat policy_id")
    return observed_policy_ids, valid_domain_slugs


def _validate_replay_risk_payload(
    payload: Mapping[str, object],
    *,
    field_name: str,
) -> Mapping[str, object]:
    _require_text(
        payload.get("status", ""),
        f"{field_name}.status",
    )
    _require_text(
        payload.get("summary", ""),
        f"{field_name}.summary",
    )
    proposal = _require_mapping(
        payload.get("proposal"),
        f"{field_name}.proposal",
    )
    for proposal_field in (
        "entity_id",
        "target_symbol",
        "domain",
        "population",
        "mono_or_adjunct",
    ):
        _require_string(
            proposal.get(proposal_field),
            f"{field_name}.proposal.{proposal_field}",
        )
    for count_field in (
        "supporting_reason_count",
        "offsetting_reason_count",
        "uncertainty_reason_count",
        "uncertainty_flag_count",
    ):
        value = _require_int(
            payload.get(count_field),
            f"{field_name}.{count_field}",
        )
        if value < 0:
            raise ValueError(f"{field_name}.{count_field} must be non-negative")
    for list_field in (
        "supporting_reasons",
        "offsetting_reasons",
        "uncertainty_reasons",
    ):
        reasons = _require_list(
            payload.get(list_field),
            f"{field_name}.{list_field}",
        )
        for index, item in enumerate(reasons):
            reason = _require_mapping(
                item,
                f"{field_name}.{list_field}[{index}]",
            )
            for required_field in (
                "relation",
                "event_id",
                "failure_scope",
                "explanation",
            ):
                _require_text(
                    reason.get(required_field, ""),
                    f"{field_name}.{list_field}[{index}].{required_field}",
                )
    uncertainty_flags = _require_list(
        payload.get("uncertainty_flags"),
        f"{field_name}.uncertainty_flags",
    )
    for index, item in enumerate(uncertainty_flags):
        flag = _require_mapping(
            item,
            f"{field_name}.uncertainty_flags[{index}]",
        )
        _require_text(
            flag.get("code", ""),
            f"{field_name}.uncertainty_flags[{index}].code",
        )
        _require_text(
            flag.get("explanation", ""),
            f"{field_name}.uncertainty_flags[{index}].explanation",
        )
    _require_string_list(
        payload.get("falsification_conditions"),
        f"{field_name}.falsification_conditions",
    )
    return payload


def _resolve_artifact_reference(
    artifact_path: Path,
    reference: str,
    *,
    field_name: str,
) -> Path:
    candidate = Path(reference)
    resolved = (
        candidate.resolve()
        if candidate.is_absolute()
        else (artifact_path.parent / candidate).resolve()
    )
    if not resolved.exists():
        raise ValueError(f"{field_name} points to a missing artifact: {reference}")
    return resolved


def _decode_json_pointer_token(token: str) -> str:
    return token.replace("~1", "/").replace("~0", "~")


def _split_json_pointer(
    pointer: str,
    *,
    field_name: str,
) -> list[str]:
    if not pointer.startswith("/"):
        raise ValueError(f"{field_name} must be an absolute JSON pointer")
    return [_decode_json_pointer_token(token) for token in pointer.split("/")[1:]]


def _resolve_json_pointer(
    payload: object,
    pointer: str,
    *,
    field_name: str,
) -> object:
    current = payload
    for raw_token, token in zip(
        pointer.split("/")[1:],
        _split_json_pointer(pointer, field_name=field_name),
        strict=True,
    ):
        if isinstance(current, Mapping):
            if token not in current:
                raise ValueError(f"{field_name} does not resolve within the source artifact")
            current = current[token]
            continue
        if isinstance(current, list):
            if not raw_token.isdigit():
                raise ValueError(f"{field_name} must use numeric indexes for array steps")
            index = int(raw_token)
            if index >= len(current):
                raise ValueError(f"{field_name} does not resolve within the source artifact")
            current = current[index]
            continue
        raise ValueError(f"{field_name} does not resolve to a nested source value")
    return current


def _validate_policy_score_payload(
    payload: Mapping[str, object],
    *,
    field_name: str,
    valid_policy_ids: list[str],
    valid_domain_slugs: set[str],
) -> None:
    policy_id = _require_text(payload.get("policy_id", ""), f"{field_name}.policy_id")
    if policy_id not in valid_policy_ids:
        raise ValueError(f"{field_name}.policy_id must match policy_definitions")
    _require_text(payload.get("label", ""), f"{field_name}.label")
    _require_text(payload.get("description", ""), f"{field_name}.description")
    primary_domain_slug = _require_text(
        payload.get("primary_domain_slug", ""),
        f"{field_name}.primary_domain_slug",
    )
    if primary_domain_slug not in valid_domain_slugs:
        raise ValueError(f"{field_name}.primary_domain_slug must match current v1 domains")
    _require_optional_number(payload.get("score"), f"{field_name}.score")
    _require_optional_number(payload.get("base_score"), f"{field_name}.base_score")
    _require_optional_number(
        payload.get("score_before_clamp"),
        f"{field_name}.score_before_clamp",
    )
    _require_text(payload.get("status", ""), f"{field_name}.status")
    _require_optional_number(
        payload.get("coverage_weight_fraction"),
        f"{field_name}.coverage_weight_fraction",
    )
    _require_optional_number(
        payload.get("uncertainty_adjustment_total"),
        f"{field_name}.uncertainty_adjustment_total",
    )
    domain_contributions = _require_list(
        payload.get("domain_contributions"),
        f"{field_name}.domain_contributions",
    )
    for index, item in enumerate(domain_contributions):
        contribution = _require_mapping(
            item,
            f"{field_name}.domain_contributions[{index}]",
        )
        domain_slug = _require_text(
            contribution.get("domain_slug", ""),
            f"{field_name}.domain_contributions[{index}].domain_slug",
        )
        if domain_slug not in valid_domain_slugs:
            raise ValueError(
                f"{field_name}.domain_contributions[{index}].domain_slug must match current v1 domains"
            )
        _require_text(
            contribution.get("label", ""),
            f"{field_name}.domain_contributions[{index}].label",
        )
        _require_text(
            contribution.get("status", ""),
            f"{field_name}.domain_contributions[{index}].status",
        )
        _require_optional_number(
            contribution.get("weight"),
            f"{field_name}.domain_contributions[{index}].weight",
        )
        _require_optional_number(
            contribution.get("score"),
            f"{field_name}.domain_contributions[{index}].score",
        )
        _require_optional_number(
            contribution.get("domain_coverage_weight_fraction"),
            f"{field_name}.domain_contributions[{index}].domain_coverage_weight_fraction",
        )
        _require_optional_number(
            contribution.get("contribution"),
            f"{field_name}.domain_contributions[{index}].contribution",
        )
    adjustments = _require_list(payload.get("adjustments"), f"{field_name}.adjustments")
    for index, item in enumerate(adjustments):
        adjustment = _require_mapping(item, f"{field_name}.adjustments[{index}]")
        _require_text(
            adjustment.get("adjustment_id", ""),
            f"{field_name}.adjustments[{index}].adjustment_id",
        )
        _require_text(
            adjustment.get("label", ""),
            f"{field_name}.adjustments[{index}].label",
        )
        _require_optional_number(
            adjustment.get("delta"),
            f"{field_name}.adjustments[{index}].delta",
        )
        _require_mapping(
            adjustment.get("evidence"),
            f"{field_name}.adjustments[{index}].evidence",
        )
    uncertainty_context = _require_mapping(
        payload.get("uncertainty_context"),
        f"{field_name}.uncertainty_context",
    )
    _require_int(
        uncertainty_context.get("warning_count"),
        f"{field_name}.uncertainty_context.warning_count",
    )
    _require_text(
        uncertainty_context.get("warning_severity", ""),
        f"{field_name}.uncertainty_context.warning_severity",
    )
    for count_field in (
        "missing_head_count",
        "partial_head_count",
        "not_applicable_head_count",
        "directionality_open_risk_count",
        "directionality_contradiction_count",
        "directionality_falsification_count",
    ):
        value = _require_int(
            uncertainty_context.get(count_field),
            f"{field_name}.uncertainty_context.{count_field}",
        )
        if value < 0:
            raise ValueError(f"{field_name}.uncertainty_context.{count_field} must be non-negative")
    replay_risk = _require_mapping(
        uncertainty_context.get("replay_risk"),
        f"{field_name}.uncertainty_context.replay_risk",
    )
    _validate_replay_risk_payload(
        replay_risk,
        field_name=f"{field_name}.uncertainty_context.replay_risk",
    )


def validate_required_scored_policy_signal(
    payload: Mapping[str, object],
    *,
    field_name: str,
) -> None:
    score = _require_optional_number(payload.get("score"), f"{field_name}.score")
    base_score = _require_optional_number(payload.get("base_score"), f"{field_name}.base_score")
    score_before_clamp = _require_optional_number(
        payload.get("score_before_clamp"),
        f"{field_name}.score_before_clamp",
    )
    if score is None or base_score is None or score_before_clamp is None:
        raise ValueError(
            f"{field_name} must include non-null score, base_score, and score_before_clamp when require_scored_policy_signal is true"
        )
    status = _require_text(payload.get("status", ""), f"{field_name}.status")
    if status not in {AVAILABLE_STATUS, PARTIAL_STATUS}:
        raise ValueError(
            f"{field_name}.status must be available or partial when require_scored_policy_signal is true"
        )


def _validate_policy_decision_vector_entity(
    payload: Mapping[str, object],
    *,
    field_name: str,
    expected_entity_type: str,
    valid_policy_ids: list[str],
    valid_domain_slugs: set[str],
) -> None:
    _require_text(payload.get("entity_type", ""), f"{field_name}.entity_type")
    if payload["entity_type"] != expected_entity_type:
        raise ValueError(f"{field_name}.entity_type must be {expected_entity_type}")
    _require_text(payload.get("entity_id", ""), f"{field_name}.entity_id")
    _require_text(payload.get("entity_label", ""), f"{field_name}.entity_label")
    _require_bool(payload.get("eligible_v0"), f"{field_name}.eligible_v0")
    _require_optional_number(payload.get("heuristic_score_v0"), f"{field_name}.heuristic_score_v0")
    if payload.get("heuristic_rank_v0") is not None:
        _require_int(payload["heuristic_rank_v0"], f"{field_name}.heuristic_rank_v0")
    _require_bool(payload.get("heuristic_stable_v0"), f"{field_name}.heuristic_stable_v0")
    warning_count = _require_int(payload.get("warning_count"), f"{field_name}.warning_count")
    if warning_count < 0:
        raise ValueError(f"{field_name}.warning_count must be non-negative")
    _require_text(payload.get("warning_severity", ""), f"{field_name}.warning_severity")

    policy_vector = _require_mapping(payload.get("policy_vector"), f"{field_name}.policy_vector")
    if list(policy_vector) != valid_policy_ids:
        raise ValueError(f"{field_name}.policy_vector keys must preserve policy_definitions order")
    for policy_id in valid_policy_ids:
        _validate_policy_score_payload(
            _require_mapping(
                policy_vector.get(policy_id),
                f"{field_name}.policy_vector.{policy_id}",
            ),
            field_name=f"{field_name}.policy_vector.{policy_id}",
            valid_policy_ids=valid_policy_ids,
            valid_domain_slugs=valid_domain_slugs,
        )

    policy_scores = _require_list(payload.get("policy_scores"), f"{field_name}.policy_scores")
    observed_policy_ids: list[str] = []
    for index, item in enumerate(policy_scores):
        score_payload = _require_mapping(item, f"{field_name}.policy_scores[{index}]")
        observed_policy_ids.append(
            _require_text(
                score_payload.get("policy_id", ""),
                f"{field_name}.policy_scores[{index}].policy_id",
            )
        )
        _validate_policy_score_payload(
            score_payload,
            field_name=f"{field_name}.policy_scores[{index}]",
            valid_policy_ids=valid_policy_ids,
            valid_domain_slugs=valid_domain_slugs,
        )
    if observed_policy_ids != valid_policy_ids:
        raise ValueError(f"{field_name}.policy_scores must preserve policy_definitions order")


def _validate_policy_decision_vectors(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if payload.get("schema_version") != schema.schema_version:
        raise ValueError(
            f"{schema.artifact_name} schema_version must be {schema.schema_version}"
        )
    _require_string_list(
        payload.get("policy_config_sources"),
        "policy_decision_vectors_v2.policy_config_sources",
    )
    policy_definitions = _require_list(
        payload.get("policy_definitions"),
        "policy_decision_vectors_v2.policy_definitions",
    )
    valid_policy_ids, valid_domain_slugs = _validate_policy_definition_payloads(
        policy_definitions
    )
    entities = _require_mapping(
        payload.get("entities"),
        "policy_decision_vectors_v2.entities",
    )
    for entity_type in ("gene", "module"):
        entity_payloads = _require_list(
            entities.get(entity_type),
            f"policy_decision_vectors_v2.entities.{entity_type}",
        )
        for index, item in enumerate(entity_payloads):
            _validate_policy_decision_vector_entity(
                _require_mapping(
                    item,
                    f"policy_decision_vectors_v2.entities.{entity_type}[{index}]",
                ),
                field_name=f"policy_decision_vectors_v2.entities.{entity_type}[{index}]",
                expected_entity_type=entity_type,
                valid_policy_ids=valid_policy_ids,
                valid_domain_slugs=valid_domain_slugs,
            )
    return payload


def _validate_policy_pareto_fronts(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    payload = _load_json_mapping(path)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if payload.get("schema_version") != schema.schema_version:
        raise ValueError(
            f"{schema.artifact_name} schema_version must be {schema.schema_version}"
        )
    policy_ids = _require_string_list(
        payload.get("policy_ids"),
        "policy_pareto_fronts_v1.policy_ids",
    )
    if len(set(policy_ids)) != len(policy_ids):
        raise ValueError("policy_pareto_fronts_v1.policy_ids must not repeat values")
    entity_types = _require_mapping(
        payload.get("entity_types"),
        "policy_pareto_fronts_v1.entity_types",
    )
    for entity_type in ("gene", "module"):
        rows = _require_list(
            entity_types.get(entity_type),
            f"policy_pareto_fronts_v1.entity_types.{entity_type}",
        )
        for index, item in enumerate(rows):
            row = _require_mapping(
                item,
                f"policy_pareto_fronts_v1.entity_types.{entity_type}[{index}]",
            )
            observed_entity_type = _require_text(
                row.get("entity_type", ""),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].entity_type"
                ),
            )
            if observed_entity_type != entity_type:
                raise ValueError(
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].entity_type must be {entity_type}"
                )
            _require_text(
                row.get("entity_id", ""),
                f"policy_pareto_fronts_v1.entity_types.{entity_type}[{index}].entity_id",
            )
            _require_text(
                row.get("entity_label", ""),
                f"policy_pareto_fronts_v1.entity_types.{entity_type}[{index}].entity_label",
            )
            pareto_front = _require_int(
                row.get("pareto_front"),
                f"policy_pareto_fronts_v1.entity_types.{entity_type}[{index}].pareto_front",
            )
            if pareto_front <= 0:
                raise ValueError(
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].pareto_front must be positive"
                )
            for count_field in ("dominated_by_count", "dominates_count", "warning_count"):
                value = _require_int(
                    row.get(count_field),
                    (
                        "policy_pareto_fronts_v1.entity_types."
                        f"{entity_type}[{index}].{count_field}"
                    ),
                )
                if value < 0:
                    raise ValueError(
                        "policy_pareto_fronts_v1.entity_types."
                        f"{entity_type}[{index}].{count_field} must be non-negative"
                    )
            _require_bool(
                row.get("complete_policy_vector"),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].complete_policy_vector"
                ),
            )
            missing_policy_score_count = _require_int(
                row.get("missing_policy_score_count"),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].missing_policy_score_count"
                ),
            )
            if missing_policy_score_count < 0:
                raise ValueError(
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].missing_policy_score_count must be non-negative"
                )
            _require_optional_number(
                row.get("heuristic_score_v0"),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].heuristic_score_v0"
                ),
            )
            if row.get("heuristic_rank_v0") is not None:
                _require_int(
                    row.get("heuristic_rank_v0"),
                    (
                        "policy_pareto_fronts_v1.entity_types."
                        f"{entity_type}[{index}].heuristic_rank_v0"
                    ),
                )
            _require_bool(
                row.get("heuristic_stable_v0"),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].heuristic_stable_v0"
                ),
            )
            _require_text(
                row.get("warning_severity", ""),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].warning_severity"
                ),
            )
            policy_scores = _require_mapping(
                row.get("policy_scores"),
                (
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].policy_scores"
                ),
            )
            if sorted(policy_scores) != sorted(policy_ids):
                raise ValueError(
                    "policy_pareto_fronts_v1.entity_types."
                    f"{entity_type}[{index}].policy_scores keys must match policy_ids"
                )
            for policy_id in policy_ids:
                _require_optional_number(
                    policy_scores.get(policy_id),
                    (
                        "policy_pareto_fronts_v1.entity_types."
                        f"{entity_type}[{index}].policy_scores.{policy_id}"
                    ),
                )
    return payload


def _validate_hypothesis_packets_payload_mapping(
    payload: Mapping[str, object],
    *,
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    payload = dict(payload)
    _ensure_required_fields(
        schema,
        set(payload),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if payload.get("schema_version") != schema.schema_version:
        raise ValueError(
            f"{schema.artifact_name} schema_version must be {schema.schema_version}"
        )

    source_artifacts = _require_mapping(
        payload.get("source_artifacts"),
        "hypothesis_packets_v1.source_artifacts",
    )
    for artifact_key in (
        "policy_decision_vectors_v2",
        "gene_target_ledgers",
    ):
        _require_text(
            source_artifacts.get(artifact_key, ""),
            f"hypothesis_packets_v1.source_artifacts.{artifact_key}",
        )

    packet_generation_criteria = _require_mapping(
        payload.get("packet_generation_criteria"),
        "hypothesis_packets_v1.packet_generation_criteria",
    )
    if _require_string_list(
        packet_generation_criteria.get("entity_types"),
        "hypothesis_packets_v1.packet_generation_criteria.entity_types",
    ) != ["gene"]:
        raise ValueError(
            "hypothesis_packets_v1.packet_generation_criteria.entity_types must be ['gene']"
        )
    if not _require_bool(
        packet_generation_criteria.get("require_curated_directionality"),
        "hypothesis_packets_v1.packet_generation_criteria.require_curated_directionality",
    ):
        raise ValueError(
            "hypothesis_packets_v1.packet_generation_criteria.require_curated_directionality must be true"
        )
    if not _require_bool(
        packet_generation_criteria.get("require_non_stub_hypothesis"),
        "hypothesis_packets_v1.packet_generation_criteria.require_non_stub_hypothesis",
    ):
        raise ValueError(
            "hypothesis_packets_v1.packet_generation_criteria.require_non_stub_hypothesis must be true"
        )
    if not _require_bool(
        packet_generation_criteria.get("require_scored_policy_signal"),
        "hypothesis_packets_v1.packet_generation_criteria.require_scored_policy_signal",
    ):
        raise ValueError(
            "hypothesis_packets_v1.packet_generation_criteria.require_scored_policy_signal must be true"
        )
    require_scored_policy_signal = True

    policy_source_path = _resolve_artifact_reference(
        path,
        _require_text(
            source_artifacts.get("policy_decision_vectors_v2", ""),
            "hypothesis_packets_v1.source_artifacts.policy_decision_vectors_v2",
        ),
        field_name="hypothesis_packets_v1.source_artifacts.policy_decision_vectors_v2",
    )
    ledger_source_path = _resolve_artifact_reference(
        path,
        _require_text(
            source_artifacts.get("gene_target_ledgers", ""),
            "hypothesis_packets_v1.source_artifacts.gene_target_ledgers",
        ),
        field_name="hypothesis_packets_v1.source_artifacts.gene_target_ledgers",
    )
    policy_source_payload = load_artifact(
        policy_source_path,
        artifact_name="policy_decision_vectors_v2",
    ).payload
    ledger_source_payload = load_artifact(
        ledger_source_path,
        artifact_name="gene_target_ledgers",
    ).payload

    packets = _require_list(payload.get("packets"), "hypothesis_packets_v1.packets")
    if _require_int(payload.get("packet_count"), "hypothesis_packets_v1.packet_count") != len(
        packets
    ):
        raise ValueError("hypothesis_packets_v1.packet_count must match packets")

    valid_domain_slugs = {definition.slug for definition in DOMAIN_HEAD_DEFINITIONS}
    packet_ids: list[str] = []
    for index, item in enumerate(packets):
        packet = _require_mapping(item, f"hypothesis_packets_v1.packets[{index}]")
        packet_id = _require_text(
            packet.get("packet_id", ""),
            f"hypothesis_packets_v1.packets[{index}].packet_id",
        )
        packet_ids.append(packet_id)
        entity_type = _require_text(
            packet.get("entity_type", ""),
            f"hypothesis_packets_v1.packets[{index}].entity_type",
        )
        if entity_type != "gene":
            raise ValueError(f"hypothesis_packets_v1.packets[{index}].entity_type must be gene")
        entity_id = _require_text(
            packet.get("entity_id", ""),
            f"hypothesis_packets_v1.packets[{index}].entity_id",
        )
        entity_label = _require_text(
            packet.get("entity_label", ""),
            f"hypothesis_packets_v1.packets[{index}].entity_label",
        )
        policy_id = _require_text(
            packet.get("policy_id", ""),
            f"hypothesis_packets_v1.packets[{index}].policy_id",
        )
        if packet_id != f"{entity_id}__{policy_id}":
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].packet_id must match entity_id__policy_id"
            )
        _require_text(
            packet.get("policy_label", ""),
            f"hypothesis_packets_v1.packets[{index}].policy_label",
        )
        priority_domain = _require_text(
            packet.get("priority_domain", ""),
            f"hypothesis_packets_v1.packets[{index}].priority_domain",
        )
        if priority_domain not in valid_domain_slugs:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].priority_domain must match current v1 domains"
            )
        decision_focus = _require_mapping(
            packet.get("decision_focus"),
            f"hypothesis_packets_v1.packets[{index}].decision_focus",
        )

        hypothesis = _require_mapping(
            packet.get("hypothesis"),
            f"hypothesis_packets_v1.packets[{index}].hypothesis",
        )
        statement = _require_text(
            hypothesis.get("statement", ""),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.statement",
        )
        if "undetermined" in statement:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].hypothesis.statement must not be a vague stub"
            )
        for field_name in (
            "desired_perturbation_direction",
            "modality_hypothesis",
        ):
            value = _require_text(
                hypothesis.get(field_name, ""),
                f"hypothesis_packets_v1.packets[{index}].hypothesis.{field_name}",
            )
            if value == "undetermined":
                raise ValueError(
                    f"hypothesis_packets_v1.packets[{index}].hypothesis.{field_name} must not be undetermined"
                )
        preferred_modalities = _require_string_list(
            hypothesis.get("preferred_modalities"),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.preferred_modalities",
        )
        if not preferred_modalities:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].hypothesis.preferred_modalities must not be empty"
            )
        if any(value == "undetermined" for value in preferred_modalities):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].hypothesis.preferred_modalities must not contain undetermined"
            )
        _require_text(
            hypothesis.get("confidence", ""),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.confidence",
        )
        _require_string(
            hypothesis.get("ambiguity"),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.ambiguity",
        )
        _require_string(
            hypothesis.get("evidence_basis"),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.evidence_basis",
        )
        supporting_program_ids = _require_string_list(
            hypothesis.get("supporting_program_ids"),
            f"hypothesis_packets_v1.packets[{index}].hypothesis.supporting_program_ids",
        )

        policy_signal = _require_mapping(
            packet.get("policy_signal"),
            f"hypothesis_packets_v1.packets[{index}].policy_signal",
        )
        _validate_policy_score_payload(
            policy_signal,
            field_name=f"hypothesis_packets_v1.packets[{index}].policy_signal",
            valid_policy_ids=[policy_id],
            valid_domain_slugs=valid_domain_slugs,
        )
        if require_scored_policy_signal:
            validate_required_scored_policy_signal(
                policy_signal,
                field_name=f"hypothesis_packets_v1.packets[{index}].policy_signal",
            )
        if policy_signal["label"] != packet["policy_label"]:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].policy_label must match policy_signal.label"
            )
        if policy_signal["primary_domain_slug"] != priority_domain:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].priority_domain must match policy_signal.primary_domain_slug"
            )

        contradiction_handling = _require_mapping(
            packet.get("contradiction_handling"),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling",
        )
        contradiction_status = _require_text(
            contradiction_handling.get("status", ""),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling.status",
        )
        contradiction_conditions = _require_string_list(
            contradiction_handling.get("contradiction_conditions"),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling.contradiction_conditions",
        )
        if contradiction_status not in {"clear", "contradicted"}:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].contradiction_handling.status must be clear or contradicted"
            )
        if bool(contradiction_conditions) != (contradiction_status == "contradicted"):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].contradiction_handling.status does not match contradiction_conditions"
            )
        _require_string_list(
            contradiction_handling.get("directionality_falsification_conditions"),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling.directionality_falsification_conditions",
        )
        open_risks = _require_list(
            contradiction_handling.get("open_risks"),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling.open_risks",
        )
        for risk_index, risk_item in enumerate(open_risks):
            risk = _require_mapping(
                risk_item,
                f"hypothesis_packets_v1.packets[{index}].contradiction_handling.open_risks[{risk_index}]",
            )
            for required_key in ("source", "risk_kind", "severity", "text"):
                _require_text(
                    risk.get(required_key, ""),
                    (
                        "hypothesis_packets_v1.packets"
                        f"[{index}].contradiction_handling.open_risks[{risk_index}].{required_key}"
                    ),
                )

        failure_memory = _require_mapping(
            packet.get("failure_memory"),
            f"hypothesis_packets_v1.packets[{index}].failure_memory",
        )
        structural_failure_history = _validate_structural_failure_history(
            _require_mapping(
                failure_memory.get("structural_failure_history"),
                f"hypothesis_packets_v1.packets[{index}].failure_memory.structural_failure_history",
            ),
            field_name=(
                "hypothesis_packets_v1.packets"
                f"[{index}].failure_memory.structural_failure_history"
            ),
        )
        replay_risk = _validate_replay_risk_payload(
            _require_mapping(
                failure_memory.get("replay_risk"),
                f"hypothesis_packets_v1.packets[{index}].failure_memory.replay_risk",
            ),
            field_name=f"hypothesis_packets_v1.packets[{index}].failure_memory.replay_risk",
        )
        if replay_risk != policy_signal["uncertainty_context"]["replay_risk"]:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].failure_memory.replay_risk must match policy_signal uncertainty_context replay_risk"
            )
        if decision_focus != _build_expected_hypothesis_packet_decision_focus(
            entity_label=entity_label,
            policy_label=_require_text(
                packet.get("policy_label", ""),
                f"hypothesis_packets_v1.packets[{index}].policy_label",
            ),
            priority_domain=priority_domain,
            policy_signal=policy_signal,
            contradiction_handling=contradiction_handling,
            replay_risk=replay_risk,
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].decision_focus must match the derived review ask and packet readout"
            )
        evidence_anchors = [
            _validate_hypothesis_packet_evidence_anchor_mapping(
                anchor_item,
                field_name=(
                    "hypothesis_packets_v1.packets"
                    f"[{index}].evidence_anchors[{anchor_index}]"
                ),
            )
            for anchor_index, anchor_item in enumerate(
                _require_list(
                    packet.get("evidence_anchors"),
                    f"hypothesis_packets_v1.packets[{index}].evidence_anchors",
                )
            )
        ]
        if evidence_anchors != _build_expected_hypothesis_packet_evidence_anchors(
            hypothesis=hypothesis,
            replay_risk=replay_risk,
            structural_failure_history=structural_failure_history,
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].evidence_anchors must stay grounded in packet hypothesis and failure-memory artifacts"
            )
        if _require_text(
            packet.get("evidence_anchor_gap_status", ""),
            f"hypothesis_packets_v1.packets[{index}].evidence_anchor_gap_status",
        ) != _expected_hypothesis_packet_evidence_anchor_gap_status(evidence_anchors):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].evidence_anchor_gap_status must match evidence_anchors coverage"
            )
        if _require_text(
            packet.get("program_history_gap_status", ""),
            f"hypothesis_packets_v1.packets[{index}].program_history_gap_status",
        ) != _expected_hypothesis_packet_program_history_gap_status(
            structural_failure_history
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].program_history_gap_status must match structural_failure_history coverage"
            )
        if _require_string_list(
            packet.get("risk_digest"),
            f"hypothesis_packets_v1.packets[{index}].risk_digest",
        ) != _build_expected_hypothesis_packet_risk_digest(
            contradiction_handling=contradiction_handling,
            replay_risk=replay_risk,
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].risk_digest must match contradiction handling and replay risk"
            )
        if _require_string_list(
            packet.get("evidence_needed_next"),
            f"hypothesis_packets_v1.packets[{index}].evidence_needed_next",
        ) != _build_expected_hypothesis_packet_evidence_needed_next(replay_risk):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].evidence_needed_next must match prioritized replay_risk falsification_conditions"
            )

        failure_escape_logic = _require_mapping(
            packet.get("failure_escape_logic"),
            f"hypothesis_packets_v1.packets[{index}].failure_escape_logic",
        )
        escape_status = _require_text(
            failure_escape_logic.get("status", ""),
            f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.status",
        )
        if escape_status not in {
            "escape_evidence_present",
            "history_insufficient",
            "escape_blocked",
            "escape_unresolved",
        }:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.status is unsupported"
            )
        escape_routes = _require_list(
            failure_escape_logic.get("escape_routes"),
            f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.escape_routes",
        )
        for route_index, route_item in enumerate(escape_routes):
            route = _require_mapping(
                route_item,
                (
                    "hypothesis_packets_v1.packets"
                    f"[{index}].failure_escape_logic.escape_routes[{route_index}]"
                ),
            )
            if (
                _require_text(
                    route.get("route_kind", ""),
                    (
                        "hypothesis_packets_v1.packets"
                        f"[{index}].failure_escape_logic.escape_routes[{route_index}].route_kind"
                    ),
                )
                != "offsetting_reason"
            ):
                raise ValueError(
                    "hypothesis_packets_v1 failure_escape_logic escape_routes currently only support offsetting_reason"
                )
            for required_key in ("event_id", "failure_scope", "explanation"):
                _require_text(
                    route.get(required_key, ""),
                    (
                        "hypothesis_packets_v1.packets"
                        f"[{index}].failure_escape_logic.escape_routes[{route_index}].{required_key}"
                    ),
                )
        if bool(escape_routes) != (escape_status == "escape_evidence_present"):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.status does not match escape_routes"
            )
        next_evidence = _require_string_list(
            failure_escape_logic.get("next_evidence"),
            f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.next_evidence",
        )
        if next_evidence != _require_string_list(
            replay_risk.get("falsification_conditions"),
            f"hypothesis_packets_v1.packets[{index}].failure_memory.replay_risk.falsification_conditions",
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].failure_escape_logic.next_evidence must match replay_risk.falsification_conditions"
            )

        traceability = _require_mapping(
            packet.get("traceability"),
            f"hypothesis_packets_v1.packets[{index}].traceability",
        )
        trace_source_artifacts = _require_mapping(
            traceability.get("source_artifacts"),
            f"hypothesis_packets_v1.packets[{index}].traceability.source_artifacts",
        )
        if dict(trace_source_artifacts) != dict(source_artifacts):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.source_artifacts must match top-level source_artifacts"
            )
        policy_entity_pointer = _require_text(
            traceability.get("policy_entity_pointer", ""),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity_pointer",
        )
        policy_entity_payload = _require_mapping(
            _resolve_json_pointer(
                policy_source_payload,
                policy_entity_pointer,
                field_name=(
                    "hypothesis_packets_v1.packets"
                    f"[{index}].traceability.policy_entity_pointer"
                ),
            ),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity_pointer",
        )
        if (
            _require_text(
                policy_entity_payload.get("entity_id", ""),
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity.entity_id",
            )
            != entity_id
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity_pointer must resolve to the packet entity_id"
            )
        if (
            _require_text(
                policy_entity_payload.get("entity_label", ""),
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity.entity_label",
            )
            != entity_label
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity_pointer must resolve to the packet entity_label"
            )
        if (
            _require_text(
                policy_entity_payload.get("entity_type", ""),
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity.entity_type",
            )
            != entity_type
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity_pointer must resolve to the packet entity_type"
            )
        policy_entity_pointer_tokens = _split_json_pointer(
            policy_entity_pointer,
            field_name=(
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.policy_entity_pointer"
            ),
        )

        policy_score_pointer = _require_text(
            traceability.get("policy_score_pointer", ""),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer",
        )
        policy_score_pointer_tokens = _split_json_pointer(
            policy_score_pointer,
            field_name=(
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.policy_score_pointer"
            ),
        )
        expected_score_pointer_prefix = [
            *policy_entity_pointer_tokens,
            "policy_scores",
        ]
        if (
            len(policy_score_pointer_tokens) != len(expected_score_pointer_prefix) + 1
            or policy_score_pointer_tokens[: len(expected_score_pointer_prefix)]
            != expected_score_pointer_prefix
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must belong to the packet policy_entity_pointer policy_scores list"
            )
        score_index_token = policy_score_pointer_tokens[-1]
        if not score_index_token.isdigit():
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must end in a numeric policy_scores index"
            )
        score_index = int(score_index_token)
        policy_score_payload = _require_mapping(
            _resolve_json_pointer(
                policy_source_payload,
                policy_score_pointer,
                field_name=(
                    "hypothesis_packets_v1.packets"
                    f"[{index}].traceability.policy_score_pointer"
                ),
            ),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer",
        )
        policy_entity_scores = _require_list(
            policy_entity_payload.get("policy_scores"),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity.policy_scores",
        )
        if score_index >= len(policy_entity_scores):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer does not resolve within the packet policy_entity_pointer policy_scores list"
            )
        policy_entity_score_payload = _require_mapping(
            policy_entity_scores[score_index],
            (
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.policy_entity.policy_scores[{score_index}]"
            ),
        )
        if dict(policy_entity_score_payload) != dict(policy_score_payload):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must resolve to the same entity-scoped policy_scores row as policy_entity_pointer"
            )
        if (
            _require_text(
                policy_entity_score_payload.get("policy_id", ""),
                (
                    "hypothesis_packets_v1.packets"
                    f"[{index}].traceability.policy_entity.policy_scores[{score_index}].policy_id"
                ),
            )
            != policy_id
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must resolve to the packet policy_id within the packet policy_entity_pointer context"
            )
        policy_entity_policy_vector = _require_mapping(
            policy_entity_payload.get("policy_vector"),
            f"hypothesis_packets_v1.packets[{index}].traceability.policy_entity.policy_vector",
        )
        policy_vector_score_payload = _require_mapping(
            policy_entity_policy_vector.get(policy_id),
            (
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.policy_entity.policy_vector.{policy_id}"
            ),
        )
        if dict(policy_vector_score_payload) != dict(policy_score_payload):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must resolve to the packet policy_id entry in the packet policy_entity_pointer policy_vector"
            )
        if dict(policy_score_payload) != dict(policy_signal):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.policy_score_pointer must resolve to the packet policy_signal payload"
            )

        ledger_target_pointer = _require_text(
            traceability.get("ledger_target_pointer", ""),
            f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer",
        )
        ledger_target_payload = _require_mapping(
            _resolve_json_pointer(
                ledger_source_payload,
                ledger_target_pointer,
                field_name=(
                    "hypothesis_packets_v1.packets"
                    f"[{index}].traceability.ledger_target_pointer"
                ),
            ),
            f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer",
        )
        if (
            _require_text(
                ledger_target_payload.get("entity_id", ""),
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target.entity_id",
            )
            != entity_id
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet entity_id"
            )
        if (
            _require_text(
                ledger_target_payload.get("entity_label", ""),
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target.entity_label",
            )
            != entity_label
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet entity_label"
            )
        if _require_mapping(
            ledger_target_payload.get("structural_failure_history"),
            (
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.ledger_target.structural_failure_history"
            ),
        ) != dict(structural_failure_history):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet structural_failure_history payload"
            )
        if _require_string_list(
            _require_mapping(
                ledger_target_payload.get("directionality_hypothesis"),
                (
                    "hypothesis_packets_v1.packets"
                    f"[{index}].traceability.ledger_target.directionality_hypothesis"
                ),
            ).get("supporting_program_ids"),
            (
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.ledger_target.directionality_hypothesis.supporting_program_ids"
            ),
        ) != supporting_program_ids:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet supporting_program_ids"
            )
        if _require_string_list(
            ledger_target_payload.get("falsification_conditions"),
            (
                "hypothesis_packets_v1.packets"
                f"[{index}].traceability.ledger_target.falsification_conditions"
            ),
        ) != _require_string_list(
            contradiction_handling.get("directionality_falsification_conditions"),
            f"hypothesis_packets_v1.packets[{index}].contradiction_handling.directionality_falsification_conditions",
        ):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet directionality_falsification_conditions"
            )
        if _require_list(
            ledger_target_payload.get("open_risks"),
            f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target.open_risks",
        ) != list(open_risks):
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.ledger_target_pointer must resolve to the packet open_risks payload"
            )
        if _require_string_list(
            traceability.get("directionality_supporting_program_ids"),
            f"hypothesis_packets_v1.packets[{index}].traceability.directionality_supporting_program_ids",
        ) != supporting_program_ids:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.directionality_supporting_program_ids must match hypothesis.supporting_program_ids"
            )
        if _require_string_list(
            traceability.get("structural_failure_program_ids"),
            f"hypothesis_packets_v1.packets[{index}].traceability.structural_failure_program_ids",
        ) != [
            _require_text(
                event["program_id"],
                (
                    "hypothesis_packets_v1.packets"
                    f"[{index}].failure_memory.structural_failure_history.events[].program_id"
                ),
            )
            for event in structural_failure_history["events"]
        ]:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.structural_failure_program_ids must match structural_failure_history.events"
            )
        replay_reason_event_ids: list[str] = []
        for list_field in (
            "supporting_reasons",
            "offsetting_reasons",
            "uncertainty_reasons",
        ):
            for reason in replay_risk[list_field]:
                event_id_value = _require_text(
                    reason["event_id"],
                    (
                        "hypothesis_packets_v1.packets"
                        f"[{index}].failure_memory.replay_risk.{list_field}[].event_id"
                    ),
                )
                if event_id_value not in replay_reason_event_ids:
                    replay_reason_event_ids.append(event_id_value)
        if _require_string_list(
            traceability.get("replay_reason_event_ids"),
            f"hypothesis_packets_v1.packets[{index}].traceability.replay_reason_event_ids",
        ) != replay_reason_event_ids:
            raise ValueError(
                f"hypothesis_packets_v1.packets[{index}].traceability.replay_reason_event_ids must match failure_memory.replay_risk reason event_ids"
            )

    if len(packet_ids) != len(set(packet_ids)):
        raise ValueError("hypothesis_packets_v1.packets must not repeat packet_id")
    return payload


def _build_expected_hypothesis_packet_decision_focus(
    *,
    entity_label: str,
    policy_label: str,
    priority_domain: str,
    policy_signal: Mapping[str, object],
    contradiction_handling: Mapping[str, object],
    replay_risk: Mapping[str, object],
) -> dict[str, object]:
    return {
        "review_question": (
            f"Should {entity_label} advance, hold, or kill for {policy_label} in "
            f"{priority_domain}?"
        ),
        "decision_options": ["advance", "hold", "kill"],
        "current_readout": (
            f"{policy_label} scored "
            f"{_format_hypothesis_packet_score(policy_signal.get('score'), 'policy_signal.score')} "
            f"({_require_text(policy_signal.get('status'), 'policy_signal.status')}); "
            f"contradiction status "
            f"{_require_text(contradiction_handling.get('status'), 'contradiction_handling.status')}; "
            f"replay status {_require_text(replay_risk.get('status'), 'replay_risk.status')}."
        ),
    }


def _build_expected_hypothesis_packet_evidence_anchors(
    *,
    hypothesis: Mapping[str, object],
    replay_risk: Mapping[str, object],
    structural_failure_history: Mapping[str, object],
) -> list[dict[str, object]]:
    structural_events = {
        _require_text(
            event.get("program_id"),
            "hypothesis_packets_v1.failure_memory.structural_failure_history.events[].program_id",
        ): event
        for event in (
            _require_mapping(
                item,
                "hypothesis_packets_v1.failure_memory.structural_failure_history.events[]",
            )
            for item in _require_list(
                structural_failure_history.get("events"),
                "hypothesis_packets_v1.failure_memory.structural_failure_history.events",
            )
        )
    }
    anchors: list[dict[str, object]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for program_id in _require_string_list(
        hypothesis.get("supporting_program_ids"),
        "hypothesis_packets_v1.hypothesis.supporting_program_ids",
    ):
        role = "supporting_program"
        if (role, program_id) in seen_pairs:
            continue
        anchors.append(
            _build_expected_hypothesis_packet_evidence_anchor(
                role=role,
                event_id=program_id,
                event=structural_events.get(program_id),
                why_it_matters=(
                    _require_text(
                        structural_events[program_id].get("notes"),
                        "hypothesis_packets_v1.failure_memory.structural_failure_history.events[].notes",
                    )
                    if program_id in structural_events
                    else "Program id is referenced in the packet hypothesis."
                ),
            )
        )
        seen_pairs.add((role, program_id))
    for list_field, role in (
        ("supporting_reasons", "supporting_reason"),
        ("offsetting_reasons", "offsetting_reason"),
        ("uncertainty_reasons", "uncertainty_reason"),
    ):
        for index, item in enumerate(
            _require_list(
                replay_risk.get(list_field),
                f"hypothesis_packets_v1.failure_memory.replay_risk.{list_field}",
            )
        ):
            reason = _require_mapping(
                item,
                f"hypothesis_packets_v1.failure_memory.replay_risk.{list_field}[{index}]",
            )
            event_id = _require_text(
                reason.get("event_id"),
                f"hypothesis_packets_v1.failure_memory.replay_risk.{list_field}[{index}].event_id",
            )
            if (role, event_id) in seen_pairs:
                continue
            anchors.append(
                _build_expected_hypothesis_packet_evidence_anchor(
                    role=role,
                    event_id=event_id,
                    event=structural_events.get(event_id),
                    why_it_matters=_require_text(
                        reason.get("explanation"),
                        f"hypothesis_packets_v1.failure_memory.replay_risk.{list_field}[{index}].explanation",
                    ),
                )
            )
            seen_pairs.add((role, event_id))
    return anchors


def _build_expected_hypothesis_packet_evidence_anchor(
    *,
    role: str,
    event_id: str,
    event: Mapping[str, object] | None,
    why_it_matters: str,
) -> dict[str, object]:
    if event is None:
        return {
            "role": role,
            "event_id": event_id,
            "event_type": "referenced_event",
            "outcome": "details_not_recovered_from_program_history",
            "why_it_matters": why_it_matters,
        }
    return {
        "role": role,
        "event_id": event_id,
        "event_type": _require_text(
            event.get("event_type"),
            "hypothesis_packets_v1.failure_memory.structural_failure_history.events[].event_type",
        ),
        "outcome": _require_text(
            event.get("primary_outcome_result"),
            "hypothesis_packets_v1.failure_memory.structural_failure_history.events[].primary_outcome_result",
        ),
        "why_it_matters": why_it_matters,
    }


def _validate_hypothesis_packet_evidence_anchor_mapping(
    payload: object,
    *,
    field_name: str,
) -> dict[str, object]:
    anchor = _require_mapping(payload, field_name)
    return {
        "role": _require_text(anchor.get("role"), f"{field_name}.role"),
        "event_id": _require_text(anchor.get("event_id"), f"{field_name}.event_id"),
        "event_type": _require_text(anchor.get("event_type"), f"{field_name}.event_type"),
        "outcome": _require_text(anchor.get("outcome"), f"{field_name}.outcome"),
        "why_it_matters": _require_text(
            anchor.get("why_it_matters"),
            f"{field_name}.why_it_matters",
        ),
    }


def _expected_hypothesis_packet_evidence_anchor_gap_status(
    evidence_anchors: list[dict[str, object]],
) -> str:
    return "evidence_anchors_present" if evidence_anchors else "no_evidence_anchors"


def _expected_hypothesis_packet_program_history_gap_status(
    structural_failure_history: Mapping[str, object],
) -> str:
    return (
        "program_history_present"
        if _require_list(
            structural_failure_history.get("events"),
            "hypothesis_packets_v1.failure_memory.structural_failure_history.events",
        )
        else "no_direct_program_history"
    )


def _build_expected_hypothesis_packet_risk_digest(
    *,
    contradiction_handling: Mapping[str, object],
    replay_risk: Mapping[str, object],
) -> list[str]:
    contradiction_conditions = _require_string_list(
        contradiction_handling.get("contradiction_conditions"),
        "hypothesis_packets_v1.contradiction_handling.contradiction_conditions",
    )
    digest = [
        (
            "Replay: "
            f"{_require_text(replay_risk.get('summary'), 'hypothesis_packets_v1.failure_memory.replay_risk.summary')}"
        ),
        (
            f"Main contradiction: {contradiction_conditions[0]}"
            if contradiction_conditions
            else "Contradiction status: clear"
        ),
    ]
    for risk in _sort_hypothesis_packet_open_risks(
        _require_list(
            contradiction_handling.get("open_risks"),
            "hypothesis_packets_v1.contradiction_handling.open_risks",
        )
    )[:2]:
        risk_mapping = _require_mapping(
            risk,
            "hypothesis_packets_v1.contradiction_handling.open_risks[]",
        )
        digest.append(
            "Risk: "
            f"{_require_text(risk_mapping.get('severity'), 'hypothesis_packets_v1.contradiction_handling.open_risks[].severity')} | "
            f"{_require_text(risk_mapping.get('text'), 'hypothesis_packets_v1.contradiction_handling.open_risks[].text')}"
        )
    return digest


def _build_expected_hypothesis_packet_evidence_needed_next(
    replay_risk: Mapping[str, object],
) -> list[str]:
    next_evidence: list[str] = []
    for item in _require_string_list(
        replay_risk.get("falsification_conditions"),
        "hypothesis_packets_v1.failure_memory.replay_risk.falsification_conditions",
    ):
        if item not in next_evidence:
            next_evidence.append(item)
    return next_evidence[:2]


def _sort_hypothesis_packet_open_risks(open_risks: list[object]) -> list[object]:
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    return sorted(
        open_risks,
        key=lambda risk: severity_rank.get(
            _require_text(
                _require_mapping(
                    risk,
                    "hypothesis_packets_v1.contradiction_handling.open_risks[]",
                ).get("severity"),
                "hypothesis_packets_v1.contradiction_handling.open_risks[].severity",
            ),
            99,
        ),
    )


def _format_hypothesis_packet_score(value: object, field_name: str) -> str:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    return f"{value:.3f}"


def _validate_hypothesis_packets(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> dict[str, object]:
    return _validate_hypothesis_packets_payload_mapping(
        _load_json_mapping(path),
        path=path,
        schema=schema,
    )


def validate_hypothesis_packets_payload(
    payload: Mapping[str, object],
    *,
    artifact_path: Path,
    schema_dir: Path | None = None,
) -> dict[str, object]:
    schema = get_artifact_schema(
        "hypothesis_packets_v1",
        schema_dir=schema_dir,
    )
    return _validate_hypothesis_packets_payload_mapping(
        payload,
        path=artifact_path.resolve(),
        schema=schema,
    )


def _validate_domain_head_rankings(
    path: Path,
    schema: ArtifactSchemaDefinition,
) -> list[dict[str, str]]:
    fieldnames, rows = _read_csv_artifact(path)
    _ensure_required_fields(
        schema,
        set(fieldnames),
        context=f"{schema.artifact_name} artifact {path}",
    )
    if not rows:
        raise ValueError("domain_head_rankings_v1 artifact must contain at least one row")

    valid_entity_types = {"gene", "module"}
    valid_domain_slugs = {definition.slug for definition in DOMAIN_HEAD_DEFINITIONS}
    for index, row in enumerate(rows):
        row_name = f"domain_head_rankings_v1.rows[{index}]"
        entity_type = _require_text(row.get("entity_type", ""), f"{row_name}.entity_type")
        if entity_type not in valid_entity_types:
            raise ValueError(f"{row_name}.entity_type must be gene or module")
        _require_text(row.get("entity_id", ""), f"{row_name}.entity_id")
        _require_text(row.get("entity_label", ""), f"{row_name}.entity_label")
        domain_slug = _require_text(row.get("domain_slug", ""), f"{row_name}.domain_slug")
        if domain_slug not in valid_domain_slugs:
            raise ValueError(f"{row_name}.domain_slug must match the current ontology")
        _require_text(row.get("domain_label", ""), f"{row_name}.domain_label")
        _require_text(row.get("domain_axis", ""), f"{row_name}.domain_axis")
        _parse_optional_csv_float(row.get("domain_head_score_v1", ""), f"{row_name}.domain_head_score_v1")
        _parse_optional_csv_int(row.get("domain_rank_v1", ""), f"{row_name}.domain_rank_v1")
        _require_text(row.get("domain_score_status", ""), f"{row_name}.domain_score_status")
        _parse_optional_csv_float(
            row.get("domain_coverage_weight_fraction", ""),
            f"{row_name}.domain_coverage_weight_fraction",
        )
        _parse_optional_csv_int(
            row.get("domain_available_head_count", ""),
            f"{row_name}.domain_available_head_count",
        )
        _parse_optional_csv_int(
            row.get("domain_total_head_count", ""),
            f"{row_name}.domain_total_head_count",
        )
        _parse_optional_csv_float(row.get("heuristic_score_v0", ""), f"{row_name}.heuristic_score_v0")
        _parse_optional_csv_int(row.get("heuristic_rank_v0", ""), f"{row_name}.heuristic_rank_v0")
        _parse_csv_bool(row.get("heuristic_stable_v0", ""), f"{row_name}.heuristic_stable_v0")
        _parse_optional_csv_int(row.get("warning_count", ""), f"{row_name}.warning_count")
        _require_text(row.get("warning_severity", ""), f"{row_name}.warning_severity")
        for definition in DECISION_HEAD_DEFINITIONS:
            _parse_optional_csv_float(row.get(definition.name, ""), f"{row_name}.{definition.name}")
            _require_text(
                row.get(f"{definition.name}_status", ""),
                f"{row_name}.{definition.name}_status",
            )
    return rows


def infer_artifact_name(
    path: Path,
    *,
    schema_dir: Path | None = None,
) -> str:
    path = path.resolve()
    registered_names = set(load_artifact_schemas(schema_dir=schema_dir))
    suffix = path.suffix.lower()

    if suffix == ".json":
        payload = _load_json_mapping(path)
        schema_name = payload.get("schema_name")
        if isinstance(schema_name, str) and schema_name in registered_names:
            return schema_name
        if {"schema_version", "scoring_neutral", "targets"}.issubset(payload):
            return "gene_target_ledgers"
        if {
            "schema_version",
            "policy_config_sources",
            "policy_definitions",
            "entities",
        }.issubset(payload):
            return "policy_decision_vectors_v2"
        if {
            "schema_version",
            "decision_head_definitions",
            "domain_head_definitions",
            "entities",
        }.issubset(payload):
            return "decision_vectors_v1"
        if {"schema_version", "policy_ids", "entity_types"}.issubset(payload):
            return "policy_pareto_fronts_v1"
        if {
            "schema_version",
            "source_artifacts",
            "packet_generation_criteria",
            "packets",
        }.issubset(payload):
            return "hypothesis_packets_v1"

    if suffix == ".csv":
        fieldnames, _ = _read_csv_artifact(path)
        fieldname_set = set(fieldnames)
        if {"cohort_id", "snapshot_id", "label_name", "horizon"}.issubset(fieldname_set):
            return "benchmark_cohort_labels"
        if {"domain_slug", "domain_head_score_v1", "domain_rank_v1"}.issubset(fieldname_set):
            return "domain_head_rankings_v1"

    raise ValueError(f"could not infer artifact_name for {path}")


_ARTIFACT_VALIDATORS = {
    "benchmark_snapshot_manifest": _validate_benchmark_snapshot_manifest,
    "benchmark_cohort_labels": _validate_benchmark_cohort_labels,
    "benchmark_model_run_manifest": _validate_benchmark_model_run_manifest,
    "benchmark_metric_output_payload": _validate_benchmark_metric_output_payload,
    "benchmark_confidence_interval_payload": _validate_benchmark_confidence_interval_payload,
    "rescue_dataset_card": _validate_rescue_dataset_card,
    "rescue_freeze_manifest": _validate_rescue_freeze_manifest,
    "rescue_raw_to_frozen_lineage": _validate_rescue_raw_to_frozen_lineage,
    "rescue_split_manifest": _validate_rescue_split_manifest,
    "rescue_task_contract": _validate_rescue_task_contract,
    "rescue_task_card": _validate_rescue_task_card,
    "gene_target_ledgers": _validate_gene_target_ledgers,
    "decision_vectors_v1": _validate_decision_vectors,
    "policy_decision_vectors_v2": _validate_policy_decision_vectors,
    "domain_head_rankings_v1": _validate_domain_head_rankings,
    "policy_pareto_fronts_v1": _validate_policy_pareto_fronts,
    "hypothesis_packets_v1": _validate_hypothesis_packets,
}


def load_artifact(
    path: Path,
    *,
    artifact_name: str | None = None,
    schema_dir: Path | None = None,
) -> ValidatedArtifact:
    resolved_path = path.resolve()
    resolved_artifact_name = artifact_name or infer_artifact_name(
        resolved_path,
        schema_dir=schema_dir,
    )
    schema = get_artifact_schema(
        resolved_artifact_name,
        schema_dir=schema_dir,
    )
    validator = _ARTIFACT_VALIDATORS.get(resolved_artifact_name)
    if validator is None:
        raise ValueError(f"no validator registered for {resolved_artifact_name}")
    payload = validator(resolved_path, schema)
    return ValidatedArtifact(
        artifact_name=resolved_artifact_name,
        schema=schema,
        path=resolved_path,
        payload=payload,
    )


def validate_artifact(
    path: Path,
    *,
    artifact_name: str | None = None,
    schema_dir: Path | None = None,
) -> ValidatedArtifact:
    return load_artifact(
        path,
        artifact_name=artifact_name,
        schema_dir=schema_dir,
    )
