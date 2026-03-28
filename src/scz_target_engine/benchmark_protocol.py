from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


GENE_ENTITY_TYPE = "gene"
MODULE_ENTITY_TYPE = "module"
VALID_ENTITY_TYPES = (GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE)

STRICT_NO_LEAKAGE_MODE = "strict_no_leakage"
RECORD_TIMESTAMP_CUTOFF = "record_timestamp_lte_as_of"
SOURCE_RELEASE_CUTOFF = "source_release_lte_as_of"
MATERIALIZED_SNAPSHOT_CUTOFF = "materialized_snapshot_lte_as_of"
VALID_CUTOFF_MODES = (
    RECORD_TIMESTAMP_CUTOFF,
    SOURCE_RELEASE_CUTOFF,
    MATERIALIZED_SNAPSHOT_CUTOFF,
)

EXCLUDE_SOURCE_POLICY = "exclude_source"
REJECT_SNAPSHOT_POLICY = "reject_snapshot"
VALID_MISSING_DATE_POLICIES = (
    EXCLUDE_SOURCE_POLICY,
    REJECT_SNAPSHOT_POLICY,
)

AVAILABLE_NOW_STATUS = "available_now"
PROTOCOL_ONLY_STATUS = "protocol_only"
VALID_BASELINE_STATUSES = (
    AVAILABLE_NOW_STATUS,
    PROTOCOL_ONLY_STATUS,
)


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _require_non_empty_tuple(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    for value in values:
        _require_text(value, field_name)


@dataclass(frozen=True)
class BenchmarkQuestion:
    question_id: str
    disease: str
    benchmark_universe: str
    entity_types: tuple[str, ...]
    translational_outcome_labels: tuple[str, ...]
    evaluation_horizons: tuple[str, ...]
    in_scope_evidence: tuple[str, ...]
    future_outcomes: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_text(self.question_id, "question_id")
        _require_text(self.disease, "disease")
        _require_text(self.benchmark_universe, "benchmark_universe")
        _require_non_empty_tuple(self.entity_types, "entity_types")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError(
                "entity_types must only contain supported benchmark entity types"
            )
        _require_non_empty_tuple(
            self.translational_outcome_labels,
            "translational_outcome_labels",
        )
        _require_non_empty_tuple(self.evaluation_horizons, "evaluation_horizons")
        _require_non_empty_tuple(self.in_scope_evidence, "in_scope_evidence")
        _require_non_empty_tuple(self.future_outcomes, "future_outcomes")

    def to_dict(self) -> dict[str, object]:
        return {
            "question_id": self.question_id,
            "disease": self.disease,
            "benchmark_universe": self.benchmark_universe,
            "entity_types": list(self.entity_types),
            "translational_outcome_labels": list(self.translational_outcome_labels),
            "evaluation_horizons": list(self.evaluation_horizons),
            "in_scope_evidence": list(self.in_scope_evidence),
            "future_outcomes": list(self.future_outcomes),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkQuestion:
        return cls(
            question_id=str(payload["question_id"]),
            disease=str(payload["disease"]),
            benchmark_universe=str(payload["benchmark_universe"]),
            entity_types=tuple(str(item) for item in payload["entity_types"]),
            translational_outcome_labels=tuple(
                str(item) for item in payload["translational_outcome_labels"]
            ),
            evaluation_horizons=tuple(
                str(item) for item in payload["evaluation_horizons"]
            ),
            in_scope_evidence=tuple(str(item) for item in payload["in_scope_evidence"]),
            future_outcomes=tuple(str(item) for item in payload["future_outcomes"]),
        )


@dataclass(frozen=True)
class LeakageControls:
    mode: str = STRICT_NO_LEAKAGE_MODE
    require_snapshot_manifest: bool = True
    forbid_future_evidence: bool = True
    forbid_future_outcome_labels_in_inputs: bool = True
    require_precutoff_materialization: bool = True
    undated_source_policy: str = EXCLUDE_SOURCE_POLICY
    missing_cutoff_policy: str = REJECT_SNAPSHOT_POLICY
    internal_state_policy: str = "protocol_only_decoupled_from_head_internals"

    def __post_init__(self) -> None:
        if self.mode != STRICT_NO_LEAKAGE_MODE:
            raise ValueError("mode must remain strict_no_leakage for benchmark snapshots")
        if not self.require_snapshot_manifest:
            raise ValueError("require_snapshot_manifest must remain enabled")
        if not self.forbid_future_evidence:
            raise ValueError("forbid_future_evidence must remain enabled")
        if not self.forbid_future_outcome_labels_in_inputs:
            raise ValueError("forbid_future_outcome_labels_in_inputs must remain enabled")
        if not self.require_precutoff_materialization:
            raise ValueError("require_precutoff_materialization must remain enabled")
        if self.undated_source_policy != EXCLUDE_SOURCE_POLICY:
            raise ValueError("undated_source_policy must remain exclude_source")
        if self.missing_cutoff_policy != REJECT_SNAPSHOT_POLICY:
            raise ValueError("missing_cutoff_policy must remain reject_snapshot")
        if (
            self.internal_state_policy
            != "protocol_only_decoupled_from_head_internals"
        ):
            raise ValueError(
                "internal_state_policy must remain protocol_only_decoupled_from_head_internals"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "require_snapshot_manifest": self.require_snapshot_manifest,
            "forbid_future_evidence": self.forbid_future_evidence,
            "forbid_future_outcome_labels_in_inputs": self.forbid_future_outcome_labels_in_inputs,
            "require_precutoff_materialization": self.require_precutoff_materialization,
            "undated_source_policy": self.undated_source_policy,
            "missing_cutoff_policy": self.missing_cutoff_policy,
            "internal_state_policy": self.internal_state_policy,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> LeakageControls:
        return cls(
            mode=str(payload["mode"]),
            require_snapshot_manifest=bool(payload["require_snapshot_manifest"]),
            forbid_future_evidence=bool(payload["forbid_future_evidence"]),
            forbid_future_outcome_labels_in_inputs=bool(
                payload["forbid_future_outcome_labels_in_inputs"]
            ),
            require_precutoff_materialization=bool(
                payload["require_precutoff_materialization"]
            ),
            undated_source_policy=str(payload["undated_source_policy"]),
            missing_cutoff_policy=str(payload["missing_cutoff_policy"]),
            internal_state_policy=str(payload["internal_state_policy"]),
        )


@dataclass(frozen=True)
class SourceCutoffRule:
    source_name: str
    cutoff_mode: str
    cutoff_reference: str
    evidence_timestamp_field: str | None
    missing_date_policy: str
    future_record_policy: str
    historical_backfill_policy: str
    notes: str

    def __post_init__(self) -> None:
        _require_text(self.source_name, "source_name")
        if self.cutoff_mode not in VALID_CUTOFF_MODES:
            raise ValueError("cutoff_mode must be a supported snapshot cutoff mode")
        _require_text(self.cutoff_reference, "cutoff_reference")
        if (
            self.cutoff_mode == RECORD_TIMESTAMP_CUTOFF
            and not self.evidence_timestamp_field
        ):
            raise ValueError(
                "record_timestamp_lte_as_of requires evidence_timestamp_field"
            )
        if self.missing_date_policy not in VALID_MISSING_DATE_POLICIES:
            raise ValueError("missing_date_policy must be a supported policy")
        if self.future_record_policy != REJECT_SNAPSHOT_POLICY:
            raise ValueError("future_record_policy must remain reject_snapshot")
        _require_text(self.historical_backfill_policy, "historical_backfill_policy")
        _require_text(self.notes, "notes")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "cutoff_mode": self.cutoff_mode,
            "cutoff_reference": self.cutoff_reference,
            "evidence_timestamp_field": self.evidence_timestamp_field,
            "missing_date_policy": self.missing_date_policy,
            "future_record_policy": self.future_record_policy,
            "historical_backfill_policy": self.historical_backfill_policy,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SourceCutoffRule:
        return cls(
            source_name=str(payload["source_name"]),
            cutoff_mode=str(payload["cutoff_mode"]),
            cutoff_reference=str(payload["cutoff_reference"]),
            evidence_timestamp_field=(
                None
                if payload.get("evidence_timestamp_field") in {None, ""}
                else str(payload["evidence_timestamp_field"])
            ),
            missing_date_policy=str(payload["missing_date_policy"]),
            future_record_policy=str(payload["future_record_policy"]),
            historical_backfill_policy=str(payload["historical_backfill_policy"]),
            notes=str(payload["notes"]),
        )


@dataclass(frozen=True)
class SourceSnapshot:
    source_name: str
    source_version: str
    cutoff_mode: str
    allowed_data_through: str
    materialized_at: str
    evidence_timestamp_field: str | None
    missing_date_policy: str
    future_record_policy: str
    included: bool = True
    exclusion_reason: str = ""

    def __post_init__(self) -> None:
        _require_text(self.source_name, "source_name")
        _require_text(self.source_version, "source_version")
        if self.cutoff_mode not in VALID_CUTOFF_MODES:
            raise ValueError("cutoff_mode must be a supported snapshot cutoff mode")
        _parse_iso_date(self.allowed_data_through, "allowed_data_through")
        _parse_iso_date(self.materialized_at, "materialized_at")
        if (
            self.cutoff_mode == RECORD_TIMESTAMP_CUTOFF
            and not self.evidence_timestamp_field
        ):
            raise ValueError(
                "record_timestamp_lte_as_of requires evidence_timestamp_field"
            )
        if self.missing_date_policy not in VALID_MISSING_DATE_POLICIES:
            raise ValueError("missing_date_policy must be a supported policy")
        if self.future_record_policy != REJECT_SNAPSHOT_POLICY:
            raise ValueError("future_record_policy must remain reject_snapshot")
        if not self.included and not self.exclusion_reason:
            raise ValueError("excluded sources must record exclusion_reason")
        if self.included and self.exclusion_reason:
            raise ValueError("included sources cannot set exclusion_reason")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "source_version": self.source_version,
            "cutoff_mode": self.cutoff_mode,
            "allowed_data_through": self.allowed_data_through,
            "materialized_at": self.materialized_at,
            "evidence_timestamp_field": self.evidence_timestamp_field,
            "missing_date_policy": self.missing_date_policy,
            "future_record_policy": self.future_record_policy,
            "included": self.included,
            "exclusion_reason": self.exclusion_reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SourceSnapshot:
        return cls(
            source_name=str(payload["source_name"]),
            source_version=str(payload["source_version"]),
            cutoff_mode=str(payload["cutoff_mode"]),
            allowed_data_through=str(payload["allowed_data_through"]),
            materialized_at=str(payload["materialized_at"]),
            evidence_timestamp_field=(
                None
                if payload.get("evidence_timestamp_field") in {None, ""}
                else str(payload["evidence_timestamp_field"])
            ),
            missing_date_policy=str(payload["missing_date_policy"]),
            future_record_policy=str(payload["future_record_policy"]),
            included=bool(payload.get("included", True)),
            exclusion_reason=str(payload.get("exclusion_reason", "")),
        )


@dataclass(frozen=True)
class BenchmarkSnapshotManifest:
    schema_name: str
    schema_version: str
    snapshot_id: str
    cohort_id: str
    benchmark_question_id: str
    as_of_date: str
    outcome_observation_closed_at: str
    entity_types: tuple[str, ...]
    source_snapshots: tuple[SourceSnapshot, ...]
    leakage_controls: LeakageControls
    baseline_ids: tuple[str, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.cohort_id, "cohort_id")
        _require_text(self.benchmark_question_id, "benchmark_question_id")
        as_of_date = _parse_iso_date(self.as_of_date, "as_of_date")
        outcome_closed_at = _parse_iso_date(
            self.outcome_observation_closed_at,
            "outcome_observation_closed_at",
        )
        if outcome_closed_at < as_of_date:
            raise ValueError(
                "outcome_observation_closed_at must be on or after as_of_date"
            )
        _require_non_empty_tuple(self.entity_types, "entity_types")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError(
                "entity_types must only contain supported benchmark entity types"
            )
        if not self.source_snapshots:
            raise ValueError("source_snapshots must contain at least one source entry")
        if not self.baseline_ids:
            raise ValueError("baseline_ids must contain at least one benchmark baseline")

        seen_sources: set[str] = set()
        known_baseline_ids = set(FROZEN_BASELINE_IDS)
        for baseline_id in self.baseline_ids:
            if baseline_id not in known_baseline_ids:
                raise ValueError(f"unknown baseline_id: {baseline_id}")

        for source_snapshot in self.source_snapshots:
            if source_snapshot.source_name in seen_sources:
                raise ValueError("source_snapshots must not repeat source_name")
            seen_sources.add(source_snapshot.source_name)
            if not source_snapshot.included:
                continue
            allowed_data_through = _parse_iso_date(
                source_snapshot.allowed_data_through,
                f"{source_snapshot.source_name}.allowed_data_through",
            )
            materialized_at = _parse_iso_date(
                source_snapshot.materialized_at,
                f"{source_snapshot.source_name}.materialized_at",
            )
            if allowed_data_through > as_of_date:
                raise ValueError(
                    f"{source_snapshot.source_name} allowed_data_through exceeds as_of_date"
                )
            if (
                self.leakage_controls.require_precutoff_materialization
                and materialized_at > as_of_date
            ):
                raise ValueError(
                    f"{source_snapshot.source_name} materialized_at exceeds as_of_date"
                )
            if (
                source_snapshot.missing_date_policy
                != self.leakage_controls.undated_source_policy
            ):
                raise ValueError(
                    f"{source_snapshot.source_name} missing_date_policy must match leakage controls"
                )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "cohort_id": self.cohort_id,
            "benchmark_question_id": self.benchmark_question_id,
            "as_of_date": self.as_of_date,
            "outcome_observation_closed_at": self.outcome_observation_closed_at,
            "entity_types": list(self.entity_types),
            "source_snapshots": [
                source_snapshot.to_dict() for source_snapshot in self.source_snapshots
            ],
            "leakage_controls": self.leakage_controls.to_dict(),
            "baseline_ids": list(self.baseline_ids),
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkSnapshotManifest:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            snapshot_id=str(payload["snapshot_id"]),
            cohort_id=str(payload["cohort_id"]),
            benchmark_question_id=str(payload["benchmark_question_id"]),
            as_of_date=str(payload["as_of_date"]),
            outcome_observation_closed_at=str(payload["outcome_observation_closed_at"]),
            entity_types=tuple(str(item) for item in payload["entity_types"]),
            source_snapshots=tuple(
                SourceSnapshot.from_dict(item)
                for item in payload["source_snapshots"]
            ),
            leakage_controls=LeakageControls.from_dict(payload["leakage_controls"]),
            baseline_ids=tuple(str(item) for item in payload["baseline_ids"]),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class ArtifactField:
    name: str
    field_type: str
    required: bool
    description: str

    def __post_init__(self) -> None:
        _require_text(self.name, "name")
        _require_text(self.field_type, "field_type")
        _require_text(self.description, "description")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "field_type": self.field_type,
            "required": self.required,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactField:
        return cls(
            name=str(payload["name"]),
            field_type=str(payload["field_type"]),
            required=bool(payload["required"]),
            description=str(payload["description"]),
        )


@dataclass(frozen=True)
class ArtifactSchema:
    artifact_name: str
    schema_version: str
    file_format: str
    description: str
    key_fields: tuple[str, ...]
    fields: tuple[ArtifactField, ...]

    def __post_init__(self) -> None:
        _require_text(self.artifact_name, "artifact_name")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.file_format, "file_format")
        _require_text(self.description, "description")
        _require_non_empty_tuple(self.key_fields, "key_fields")
        if not self.fields:
            raise ValueError("fields must contain at least one field definition")

        field_names = [field.name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("artifact schema field names must be unique")
        missing_key_fields = [name for name in self.key_fields if name not in field_names]
        if missing_key_fields:
            raise ValueError("key_fields must refer to known artifact fields")

    def to_dict(self) -> dict[str, object]:
        required_fields = [field.to_dict() for field in self.fields if field.required]
        optional_fields = [field.to_dict() for field in self.fields if not field.required]
        return {
            "artifact_name": self.artifact_name,
            "schema_version": self.schema_version,
            "file_format": self.file_format,
            "description": self.description,
            "key_fields": list(self.key_fields),
            "required_fields": required_fields,
            "optional_fields": optional_fields,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactSchema:
        fields = tuple(
            ArtifactField.from_dict(item)
            for item in payload["required_fields"] + payload["optional_fields"]
        )
        return cls(
            artifact_name=str(payload["artifact_name"]),
            schema_version=str(payload["schema_version"]),
            file_format=str(payload["file_format"]),
            description=str(payload["description"]),
            key_fields=tuple(str(item) for item in payload["key_fields"]),
            fields=fields,
        )


@dataclass(frozen=True)
class BaselineDefinition:
    baseline_id: str
    label: str
    family: str
    entity_types: tuple[str, ...]
    required_inputs: tuple[str, ...]
    coverage_rule: str
    status: str
    description: str

    def __post_init__(self) -> None:
        _require_text(self.baseline_id, "baseline_id")
        _require_text(self.label, "label")
        _require_text(self.family, "family")
        _require_non_empty_tuple(self.entity_types, "entity_types")
        if any(entity_type not in VALID_ENTITY_TYPES for entity_type in self.entity_types):
            raise ValueError(
                "entity_types must only contain supported benchmark entity types"
            )
        _require_text(self.coverage_rule, "coverage_rule")
        if self.status not in VALID_BASELINE_STATUSES:
            raise ValueError("status must be a supported baseline status")
        _require_text(self.description, "description")

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "label": self.label,
            "family": self.family,
            "entity_types": list(self.entity_types),
            "required_inputs": list(self.required_inputs),
            "coverage_rule": self.coverage_rule,
            "status": self.status,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BaselineDefinition:
        return cls(
            baseline_id=str(payload["baseline_id"]),
            label=str(payload["label"]),
            family=str(payload["family"]),
            entity_types=tuple(str(item) for item in payload["entity_types"]),
            required_inputs=tuple(str(item) for item in payload["required_inputs"]),
            coverage_rule=str(payload["coverage_rule"]),
            status=str(payload["status"]),
            description=str(payload["description"]),
        )


@dataclass(frozen=True)
class BenchmarkProtocol:
    schema_name: str
    schema_version: str
    question: BenchmarkQuestion
    leakage_controls: LeakageControls
    source_cutoff_rules: tuple[SourceCutoffRule, ...]
    baselines: tuple[BaselineDefinition, ...]
    artifact_schemas: tuple[ArtifactSchema, ...]

    def __post_init__(self) -> None:
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")
        if not self.source_cutoff_rules:
            raise ValueError("source_cutoff_rules must contain at least one source")
        if not self.baselines:
            raise ValueError("baselines must contain at least one benchmark baseline")
        if not self.artifact_schemas:
            raise ValueError("artifact_schemas must contain at least one schema")

        cutoff_source_names = [rule.source_name for rule in self.source_cutoff_rules]
        if len(cutoff_source_names) != len(set(cutoff_source_names)):
            raise ValueError("source_cutoff_rules must not repeat source_name")

        baseline_ids = [baseline.baseline_id for baseline in self.baselines]
        if len(baseline_ids) != len(set(baseline_ids)):
            raise ValueError("baselines must not repeat baseline_id")

        artifact_names = [schema.artifact_name for schema in self.artifact_schemas]
        if len(artifact_names) != len(set(artifact_names)):
            raise ValueError("artifact_schemas must not repeat artifact_name")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "question": self.question.to_dict(),
            "leakage_controls": self.leakage_controls.to_dict(),
            "source_cutoff_rules": [
                rule.to_dict() for rule in self.source_cutoff_rules
            ],
            "baselines": [baseline.to_dict() for baseline in self.baselines],
            "artifact_schemas": [
                artifact_schema.to_dict()
                for artifact_schema in self.artifact_schemas
            ],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkProtocol:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            question=BenchmarkQuestion.from_dict(payload["question"]),
            leakage_controls=LeakageControls.from_dict(payload["leakage_controls"]),
            source_cutoff_rules=tuple(
                SourceCutoffRule.from_dict(item)
                for item in payload["source_cutoff_rules"]
            ),
            baselines=tuple(
                BaselineDefinition.from_dict(item) for item in payload["baselines"]
            ),
            artifact_schemas=tuple(
                ArtifactSchema.from_dict(item) for item in payload["artifact_schemas"]
            ),
        )


BENCHMARK_QUESTION_V1 = BenchmarkQuestion(
    question_id="scz_translational_ranking_v1",
    disease="schizophrenia",
    benchmark_universe=(
        "Rank schizophrenia-relevant gene and module entities using only evidence "
        "observable at the snapshot as-of date, then compare those ranks against "
        "later translational outcomes collected on a separate label channel."
    ),
    entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
    translational_outcome_labels=(
        "future_schizophrenia_program_started",
        "future_schizophrenia_program_advanced",
        "future_schizophrenia_positive_signal",
        "future_schizophrenia_negative_signal",
        "no_qualifying_future_outcome",
    ),
    evaluation_horizons=("1y", "3y", "5y"),
    in_scope_evidence=(
        "pre-cutoff schizophrenia genetics",
        "pre-cutoff schizophrenia transcriptomics and regulatory context",
        "pre-cutoff target tractability and platform context",
        "pre-cutoff scoring-neutral failure and directionality ledgers when archived before the snapshot",
    ),
    future_outcomes=(
        "post-cutoff schizophrenia program starts",
        "post-cutoff schizophrenia clinical advancement events",
        "post-cutoff schizophrenia efficacy or failure outcomes",
        "explicit no-outcome-within-horizon labels",
    ),
)

SOURCE_CUTOFF_RULES_V1 = (
    SourceCutoffRule(
        source_name="PGC",
        cutoff_mode=SOURCE_RELEASE_CUTOFF,
        cutoff_reference="pgc_release_date_or_precutoff_archive_materialized_at",
        evidence_timestamp_field=None,
        missing_date_policy=EXCLUDE_SOURCE_POLICY,
        future_record_policy=REJECT_SNAPSHOT_POLICY,
        historical_backfill_policy=(
            "exclude source unless a pre-cutoff archived release is available"
        ),
        notes=(
            "Current prepared PGC support does not expose per-row timestamps. "
            "Historical benchmark snapshots may include PGC only from a release "
            "archived and materialized at or before the as-of date."
        ),
    ),
    SourceCutoffRule(
        source_name="SCHEMA",
        cutoff_mode=SOURCE_RELEASE_CUTOFF,
        cutoff_reference="schema_release_date_or_precutoff_archive_materialized_at",
        evidence_timestamp_field=None,
        missing_date_policy=EXCLUDE_SOURCE_POLICY,
        future_record_policy=REJECT_SNAPSHOT_POLICY,
        historical_backfill_policy=(
            "exclude source unless a pre-cutoff archived release is available"
        ),
        notes=(
            "Current SCHEMA support is release-backed rather than row-dated. "
            "No historical backfill is implied by this protocol."
        ),
    ),
    SourceCutoffRule(
        source_name="PsychENCODE",
        cutoff_mode=SOURCE_RELEASE_CUTOFF,
        cutoff_reference=(
            "psychencode_release_date_or_precutoff_archive_materialized_at"
        ),
        evidence_timestamp_field=None,
        missing_date_policy=EXCLUDE_SOURCE_POLICY,
        future_record_policy=REJECT_SNAPSHOT_POLICY,
        historical_backfill_policy=(
            "exclude source unless a pre-cutoff archived release is available"
        ),
        notes=(
            "Prepared PsychENCODE support and module derivations are treated as "
            "release-scoped evidence. If a pre-cutoff archive is missing, the "
            "source must be excluded from that snapshot."
        ),
    ),
    SourceCutoffRule(
        source_name="Open Targets",
        cutoff_mode=SOURCE_RELEASE_CUTOFF,
        cutoff_reference=(
            "opentargets_release_date_or_precutoff_archive_materialized_at"
        ),
        evidence_timestamp_field=None,
        missing_date_policy=EXCLUDE_SOURCE_POLICY,
        future_record_policy=REJECT_SNAPSHOT_POLICY,
        historical_backfill_policy=(
            "exclude source unless a pre-cutoff archived pull is available"
        ),
        notes=(
            "The current Open Targets baseline path is a live GraphQL pull. "
            "Historical benchmark snapshots must rely on an archived pre-cutoff "
            "extract or leave the source out."
        ),
    ),
    SourceCutoffRule(
        source_name="ChEMBL",
        cutoff_mode=SOURCE_RELEASE_CUTOFF,
        cutoff_reference="chembl_release_date_or_precutoff_archive_materialized_at",
        evidence_timestamp_field=None,
        missing_date_policy=EXCLUDE_SOURCE_POLICY,
        future_record_policy=REJECT_SNAPSHOT_POLICY,
        historical_backfill_policy=(
            "exclude source unless a pre-cutoff archived release is available"
        ),
        notes=(
            "Current ChEMBL context is release-scoped. The benchmark protocol does "
            "not allow current-release tractability context to leak into older snapshots."
        ),
    ),
)

FROZEN_BASELINE_MATRIX = (
    BaselineDefinition(
        baseline_id="pgc_only",
        label="PGC only",
        family="source_only",
        entity_types=(GENE_ENTITY_TYPE,),
        required_inputs=("common_variant_support",),
        coverage_rule=(
            "Score only genes with non-null PGC common-variant support at the snapshot cutoff."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Common-variant-only schizophrenia gene ranking baseline.",
    ),
    BaselineDefinition(
        baseline_id="schema_only",
        label="SCHEMA only",
        family="source_only",
        entity_types=(GENE_ENTITY_TYPE,),
        required_inputs=("rare_variant_support",),
        coverage_rule=(
            "Score only genes with non-null SCHEMA rare-variant support at the snapshot cutoff."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Rare-variant-only schizophrenia gene ranking baseline.",
    ),
    BaselineDefinition(
        baseline_id="opentargets_only",
        label="Open Targets baseline only",
        family="source_only",
        entity_types=(GENE_ENTITY_TYPE,),
        required_inputs=("generic_platform_baseline",),
        coverage_rule=(
            "Score only genes with non-null Open Targets baseline context from the same snapshot."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Generic-platform baseline comparator sourced from Open Targets only.",
    ),
    BaselineDefinition(
        baseline_id="v0_current",
        label="Current v0",
        family="engine_output",
        entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
        required_inputs=("v0_ranked_outputs",),
        coverage_rule=(
            "Evaluate the currently shipped v0 composite ranking on the admissible snapshot cohort."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Primary benchmark comparator for the current transparent heuristic stack.",
    ),
    BaselineDefinition(
        baseline_id="v1_current",
        label="Current v1",
        family="engine_output",
        entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
        required_inputs=("decision_vectors_v1",),
        coverage_rule=(
            "Evaluate the currently shipped additive v1 output without mutating v0 semantics."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Current additive v1 comparator resolved against the emitted v1 head contract.",
    ),
    BaselineDefinition(
        baseline_id="v1_pre_numeric_pr7_heads",
        label="V1 pre-numeric PR7 heads",
        family="engine_output",
        entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
        required_inputs=("decision_vectors_v1", "pr7_heads_structural_only"),
        coverage_rule=(
            "Protocol-only comparison mode for v1 artifacts where PR7-backed heads remain structural and unscored."
        ),
        status=PROTOCOL_ONLY_STATUS,
        description=(
            "Frozen comparison label for pre-PR8.1 style v1 outputs, independent of current implementation internals."
        ),
    ),
    BaselineDefinition(
        baseline_id="v1_post_numeric_pr7_heads",
        label="V1 post-numeric PR7 heads",
        family="engine_output",
        entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
        required_inputs=("decision_vectors_v1", "pr7_heads_numeric"),
        coverage_rule=(
            "Protocol-only comparison mode for future v1 artifacts once PR7-backed numeric heads land."
        ),
        status=PROTOCOL_ONLY_STATUS,
        description=(
            "Frozen comparison label for the future post-PR8.1 v1 contract without requiring this PR to implement it."
        ),
    ),
    BaselineDefinition(
        baseline_id="chembl_only",
        label="ChEMBL only",
        family="source_only",
        entity_types=(GENE_ENTITY_TYPE,),
        required_inputs=("tractability_compoundability",),
        coverage_rule=(
            "Score only genes with tractability_compoundability present; modules are out of scope."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Target tractability-only comparator where applicable.",
    ),
    BaselineDefinition(
        baseline_id="random_with_coverage",
        label="Random with coverage",
        family="random",
        entity_types=(GENE_ENTITY_TYPE, MODULE_ENTITY_TYPE),
        required_inputs=(),
        coverage_rule=(
            "Draw random rankings within entity type while matching admissible cohort size and coverage masks."
        ),
        status=AVAILABLE_NOW_STATUS,
        description="Coverage-matched random baseline for sanity-checking ranking lift.",
    ),
)

FROZEN_BASELINE_IDS = tuple(
    baseline_definition.baseline_id for baseline_definition in FROZEN_BASELINE_MATRIX
)

BENCHMARK_ARTIFACT_SCHEMAS_V1 = (
    ArtifactSchema(
        artifact_name="benchmark_snapshot_manifest",
        schema_version="v1",
        file_format="json",
        description=(
            "Manifest describing the evidence snapshot boundary, admissible sources, and leakage controls."
        ),
        key_fields=("snapshot_id",),
        fields=(
            ArtifactField(
                name="schema_name",
                field_type="string",
                required=True,
                description="Artifact schema identifier.",
            ),
            ArtifactField(
                name="schema_version",
                field_type="string",
                required=True,
                description="Version of the snapshot manifest schema.",
            ),
            ArtifactField(
                name="snapshot_id",
                field_type="string",
                required=True,
                description="Stable identifier for the benchmark snapshot.",
            ),
            ArtifactField(
                name="cohort_id",
                field_type="string",
                required=True,
                description="Identifier for the benchmark cohort evaluated under the snapshot.",
            ),
            ArtifactField(
                name="benchmark_question_id",
                field_type="string",
                required=True,
                description="Frozen benchmark question identifier.",
            ),
            ArtifactField(
                name="as_of_date",
                field_type="date",
                required=True,
                description="Evidence cutoff date for the benchmark snapshot.",
            ),
            ArtifactField(
                name="outcome_observation_closed_at",
                field_type="date",
                required=True,
                description="Date through which future outcome labels were observed.",
            ),
            ArtifactField(
                name="entity_types",
                field_type="string[]",
                required=True,
                description="Entity types covered by this benchmark snapshot.",
            ),
            ArtifactField(
                name="source_snapshots",
                field_type="object[]",
                required=True,
                description="Per-source cutoff and materialization entries.",
            ),
            ArtifactField(
                name="leakage_controls",
                field_type="object",
                required=True,
                description="Strict no-leakage control block enforced for this snapshot.",
            ),
            ArtifactField(
                name="baseline_ids",
                field_type="string[]",
                required=True,
                description="Frozen baseline identifiers that runner outputs must cover.",
            ),
            ArtifactField(
                name="notes",
                field_type="string",
                required=False,
                description="Optional provenance or exclusion notes for this snapshot.",
            ),
        ),
    ),
    ArtifactSchema(
        artifact_name="benchmark_cohort_labels",
        schema_version="v1",
        file_format="csv",
        description=(
            "Row-level benchmark cohort and translational label table consumed by later evaluation runs."
        ),
        key_fields=("cohort_id", "entity_type", "entity_id", "label_name", "horizon"),
        fields=(
            ArtifactField(
                name="cohort_id",
                field_type="string",
                required=True,
                description="Benchmark cohort identifier matching the snapshot manifest.",
            ),
            ArtifactField(
                name="snapshot_id",
                field_type="string",
                required=True,
                description="Snapshot identifier used to produce the cohort.",
            ),
            ArtifactField(
                name="entity_type",
                field_type="string",
                required=True,
                description="Entity type under evaluation.",
            ),
            ArtifactField(
                name="entity_id",
                field_type="string",
                required=True,
                description="Stable entity identifier.",
            ),
            ArtifactField(
                name="entity_label",
                field_type="string",
                required=True,
                description="Human-readable entity label used in reports.",
            ),
            ArtifactField(
                name="label_name",
                field_type="string",
                required=True,
                description="Translational outcome label name from the frozen question contract.",
            ),
            ArtifactField(
                name="label_value",
                field_type="string",
                required=True,
                description="Observed label state for the entity and horizon.",
            ),
            ArtifactField(
                name="horizon",
                field_type="string",
                required=True,
                description="Evaluation horizon such as 1y, 3y, or 5y.",
            ),
            ArtifactField(
                name="outcome_date",
                field_type="date",
                required=False,
                description="Observed date for the future outcome when one exists.",
            ),
            ArtifactField(
                name="label_source",
                field_type="string",
                required=True,
                description="Source of the future outcome label.",
            ),
            ArtifactField(
                name="label_notes",
                field_type="string",
                required=False,
                description="Optional free-text context for label adjudication.",
            ),
        ),
    ),
    ArtifactSchema(
        artifact_name="benchmark_model_run_manifest",
        schema_version="v1",
        file_format="json",
        description=(
            "Runner-produced manifest for a single baseline or model evaluation pass."
        ),
        key_fields=("run_id",),
        fields=(
            ArtifactField(
                name="schema_name",
                field_type="string",
                required=True,
                description="Artifact schema identifier.",
            ),
            ArtifactField(
                name="schema_version",
                field_type="string",
                required=True,
                description="Version of the model run manifest schema.",
            ),
            ArtifactField(
                name="run_id",
                field_type="string",
                required=True,
                description="Stable identifier for the evaluation run.",
            ),
            ArtifactField(
                name="snapshot_id",
                field_type="string",
                required=True,
                description="Snapshot manifest consumed by the run.",
            ),
            ArtifactField(
                name="baseline_id",
                field_type="string",
                required=True,
                description="Frozen baseline identifier evaluated in the run.",
            ),
            ArtifactField(
                name="model_family",
                field_type="string",
                required=True,
                description="Logical model or baseline family for grouping runs.",
            ),
            ArtifactField(
                name="code_version",
                field_type="string",
                required=True,
                description="Code revision or release identifier used for the run.",
            ),
            ArtifactField(
                name="parameterization",
                field_type="object",
                required=False,
                description="Opaque parameter block for runner-specific settings.",
            ),
            ArtifactField(
                name="input_artifacts",
                field_type="object[]",
                required=True,
                description="Resolved input artifact references used by the run.",
            ),
            ArtifactField(
                name="started_at",
                field_type="datetime",
                required=True,
                description="Run start timestamp.",
            ),
            ArtifactField(
                name="completed_at",
                field_type="datetime",
                required=False,
                description="Run completion timestamp when available.",
            ),
            ArtifactField(
                name="notes",
                field_type="string",
                required=False,
                description="Optional execution notes.",
            ),
        ),
    ),
    ArtifactSchema(
        artifact_name="benchmark_metric_output_payload",
        schema_version="v1",
        file_format="json",
        description=(
            "Runner-produced metric payload for a single baseline, entity type, and horizon."
        ),
        key_fields=("run_id", "baseline_id", "entity_type", "metric_name", "horizon"),
        fields=(
            ArtifactField(
                name="schema_name",
                field_type="string",
                required=True,
                description="Artifact schema identifier.",
            ),
            ArtifactField(
                name="schema_version",
                field_type="string",
                required=True,
                description="Version of the metric payload schema.",
            ),
            ArtifactField(
                name="run_id",
                field_type="string",
                required=True,
                description="Run identifier matching the model run manifest.",
            ),
            ArtifactField(
                name="snapshot_id",
                field_type="string",
                required=True,
                description="Snapshot manifest identifier.",
            ),
            ArtifactField(
                name="baseline_id",
                field_type="string",
                required=True,
                description="Frozen baseline identifier evaluated for the metric.",
            ),
            ArtifactField(
                name="entity_type",
                field_type="string",
                required=True,
                description="Entity type used for the metric slice.",
            ),
            ArtifactField(
                name="horizon",
                field_type="string",
                required=True,
                description="Evaluation horizon used for the metric slice.",
            ),
            ArtifactField(
                name="metric_name",
                field_type="string",
                required=True,
                description="Metric identifier such as AUROC or top-k precision.",
            ),
            ArtifactField(
                name="metric_value",
                field_type="number",
                required=True,
                description="Observed metric value.",
            ),
            ArtifactField(
                name="metric_unit",
                field_type="string",
                required=False,
                description="Optional unit or denominator label for the metric.",
            ),
            ArtifactField(
                name="cohort_size",
                field_type="integer",
                required=True,
                description="Number of evaluated rows contributing to the metric.",
            ),
            ArtifactField(
                name="notes",
                field_type="string",
                required=False,
                description="Optional notes or caveats for the metric slice.",
            ),
        ),
    ),
    ArtifactSchema(
        artifact_name="benchmark_confidence_interval_payload",
        schema_version="v1",
        file_format="json",
        description=(
            "Runner-produced confidence interval or bootstrap payload for benchmark metrics."
        ),
        key_fields=("run_id", "baseline_id", "entity_type", "metric_name", "horizon"),
        fields=(
            ArtifactField(
                name="schema_name",
                field_type="string",
                required=True,
                description="Artifact schema identifier.",
            ),
            ArtifactField(
                name="schema_version",
                field_type="string",
                required=True,
                description="Version of the interval payload schema.",
            ),
            ArtifactField(
                name="run_id",
                field_type="string",
                required=True,
                description="Run identifier matching the model run manifest.",
            ),
            ArtifactField(
                name="snapshot_id",
                field_type="string",
                required=True,
                description="Snapshot manifest identifier.",
            ),
            ArtifactField(
                name="baseline_id",
                field_type="string",
                required=True,
                description="Frozen baseline identifier evaluated for the interval.",
            ),
            ArtifactField(
                name="entity_type",
                field_type="string",
                required=True,
                description="Entity type used for the interval slice.",
            ),
            ArtifactField(
                name="horizon",
                field_type="string",
                required=True,
                description="Evaluation horizon used for the interval slice.",
            ),
            ArtifactField(
                name="metric_name",
                field_type="string",
                required=True,
                description="Metric identifier matching the primary metric payload.",
            ),
            ArtifactField(
                name="point_estimate",
                field_type="number",
                required=True,
                description="Point estimate associated with the interval.",
            ),
            ArtifactField(
                name="interval_low",
                field_type="number",
                required=True,
                description="Lower bound for the confidence interval.",
            ),
            ArtifactField(
                name="interval_high",
                field_type="number",
                required=True,
                description="Upper bound for the confidence interval.",
            ),
            ArtifactField(
                name="confidence_level",
                field_type="number",
                required=True,
                description="Confidence level such as 0.95.",
            ),
            ArtifactField(
                name="bootstrap_iterations",
                field_type="integer",
                required=True,
                description="Number of bootstrap resamples used to estimate the interval.",
            ),
            ArtifactField(
                name="resample_unit",
                field_type="string",
                required=True,
                description="Sampling unit used for interval estimation.",
            ),
            ArtifactField(
                name="random_seed",
                field_type="integer",
                required=False,
                description="Optional random seed for deterministic bootstrap reruns.",
            ),
            ArtifactField(
                name="notes",
                field_type="string",
                required=False,
                description="Optional notes about the interval estimation procedure.",
            ),
        ),
    ),
)

FROZEN_BENCHMARK_PROTOCOL = BenchmarkProtocol(
    schema_name="benchmark_protocol",
    schema_version="v1",
    question=BENCHMARK_QUESTION_V1,
    leakage_controls=LeakageControls(),
    source_cutoff_rules=SOURCE_CUTOFF_RULES_V1,
    baselines=FROZEN_BASELINE_MATRIX,
    artifact_schemas=BENCHMARK_ARTIFACT_SCHEMAS_V1,
)


__all__ = [
    "AVAILABLE_NOW_STATUS",
    "BENCHMARK_ARTIFACT_SCHEMAS_V1",
    "BENCHMARK_QUESTION_V1",
    "BenchmarkProtocol",
    "BenchmarkQuestion",
    "BenchmarkSnapshotManifest",
    "BaselineDefinition",
    "EXCLUDE_SOURCE_POLICY",
    "FROZEN_BASELINE_IDS",
    "FROZEN_BASELINE_MATRIX",
    "FROZEN_BENCHMARK_PROTOCOL",
    "GENE_ENTITY_TYPE",
    "LeakageControls",
    "MATERIALIZED_SNAPSHOT_CUTOFF",
    "MODULE_ENTITY_TYPE",
    "PROTOCOL_ONLY_STATUS",
    "RECORD_TIMESTAMP_CUTOFF",
    "REJECT_SNAPSHOT_POLICY",
    "SOURCE_CUTOFF_RULES_V1",
    "SOURCE_RELEASE_CUTOFF",
    "STRICT_NO_LEAKAGE_MODE",
    "SourceCutoffRule",
    "SourceSnapshot",
]
