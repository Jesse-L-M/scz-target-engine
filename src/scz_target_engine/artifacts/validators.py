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
    DECISION_HEAD_DEFINITIONS,
    DOMAIN_HEAD_DEFINITIONS,
)
from scz_target_engine.io import read_json
from scz_target_engine.ledger import TargetLedger
from scz_target_engine.rescue.contracts import (
    RescueTaskContract,
    read_rescue_task_contract,
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
            "decision_head_definitions",
            "domain_head_definitions",
            "entities",
        }.issubset(payload):
            return "decision_vectors_v1"

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
    "rescue_task_contract": _validate_rescue_task_contract,
    "gene_target_ledgers": _validate_gene_target_ledgers,
    "decision_vectors_v1": _validate_decision_vectors,
    "domain_head_rankings_v1": _validate_domain_head_rankings,
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
