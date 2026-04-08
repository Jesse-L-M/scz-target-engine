from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import json
from pathlib import Path
import re
from typing import Any

from pyarrow import Table
from pyarrow import parquet as pq

from scz_target_engine.io import read_csv_rows, read_json, write_json


REPO_ROOT = Path(__file__).resolve().parents[2]
PROGRAM_UNIVERSE_PATH = (
    REPO_ROOT / "data" / "curated" / "program_history" / "v2" / "program_universe.csv"
)
PROGRAM_HISTORY_EVENTS_PATH = (
    REPO_ROOT / "data" / "curated" / "program_history" / "v2" / "events.csv"
)

INTERVENTION_OBJECT_ENTITY_TYPE = "intervention_object"
INTERVENTION_OBJECT_BUNDLE_FILE_NAME = "intervention_object_feature_bundle.parquet"
INTERVENTION_OBJECT_BUNDLE_SCHEMA_NAME = "intervention_object_feature_bundle"
INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION = "v2"
INTERVENTION_OBJECT_PROJECTION_SCHEMA_NAME = (
    "benchmark_intervention_object_baseline_projection"
)
INTERVENTION_OBJECT_PROJECTION_SCHEMA_VERSION = "v1"
INTERVENTION_OBJECT_PROJECTION_DIR_NAME = "baseline_projections"
INTERVENTION_OBJECT_PROJECTION_AGGREGATION_RULE = (
    "mean_available_legacy_consumer_score"
)
INTERVENTION_OBJECT_PROJECTION_CONTRACT = "intervention_object_compatibility_v1"
INTERVENTION_OBJECT_BUNDLE_SOURCE_PROVENANCE_METADATA_FIELD = (
    "source_snapshot_provenance_json"
)
INTERVENTION_OBJECT_BUNDLE_REQUIRED_COLUMNS = (
    "entity_type",
    "entity_id",
    "intervention_object_id",
    "entity_label",
    "source_program_universe_id",
    "asset_id",
    "asset_name",
    "asset_lineage_id",
    "target",
    "target_symbols_json",
    "target_class",
    "target_class_lineage_id",
    "mechanism",
    "modality",
    "domain",
    "population",
    "regimen",
    "stage_bucket",
    "coverage_state",
    "coverage_reason",
    "common_variant_support",
    "rare_variant_support",
    "cell_state_support",
    "developmental_regulatory_support",
    "tractability_compoundability",
    "generic_platform_baseline",
    "source_present_pgc",
    "source_present_schema",
    "source_present_psychencode",
    "source_present_chembl",
    "source_present_opentargets",
    "matched_gene_entity_ids_json",
    "matched_gene_symbols_json",
    "matched_module_entity_ids_json",
    "included_sources_json",
    "excluded_sources_json",
    "compatibility_projection_contract",
)

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
KNOWN_METADATA_FIELDS = ("generic_platform_baseline",)

IN_SCOPE_COVERAGE_STATES = frozenset({"included", "unresolved"})
APPROVAL_EVENT_TYPES = frozenset({"regulatory_approval"})
NEGATIVE_RESULT_PREFIXES = (
    "did_not_",
    "terminated_",
    "failed_",
)
POSITIVE_RESULT_PREFIXES = (
    "approved_",
    "met_",
    "positive_",
)
MODULE_MEMBER_GENES_FIELD = "psychencode_module_member_genes_json"
STAGE_BUCKET_RANKS = {
    "phase_2": 1,
    "phase_3_or_registration": 2,
    "approved": 3,
    "postapproval_supporting": 4,
}
SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _slugify(value: object) -> str:
    return SLUG_PATTERN.sub("-", _clean_text(value).lower()).strip("-")


def _normalize_gene_symbol(value: str) -> str:
    return value.strip().upper()


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise ValueError(f"expected JSON list, got {value!r}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"expected JSON list, got {value!r}")
    return [str(item).strip() for item in payload if str(item).strip()]


def _float_or_none(value: object) -> float | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    return float(cleaned)


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _domain_label(value: str) -> str:
    return value.replace("_", " ")


def _render_intervention_object_label(
    row: dict[str, str],
    *,
    stage_bucket: str | None = None,
) -> str:
    asset_name = _clean_text(row.get("asset_name")) or _clean_text(row.get("asset_id"))
    domain = _domain_label(_clean_text(row.get("domain")))
    resolved_stage_bucket = _clean_text(stage_bucket)
    if not resolved_stage_bucket:
        resolved_stage_bucket = _clean_text(row.get("stage_bucket"))
    return " | ".join(
        part for part in (asset_name, domain, resolved_stage_bucket) if part
    )


def _intervention_object_identity_components(
    row: dict[str, str],
    *,
    stage_bucket: str,
) -> tuple[str, ...]:
    return (
        _clean_text(row.get("asset_lineage_id"))
        or _clean_text(row.get("asset_id"))
        or _clean_text(row.get("asset_name")),
        _clean_text(row.get("target_class_lineage_id"))
        or _clean_text(row.get("target_class"))
        or _clean_text(row.get("target")),
        _clean_text(row.get("modality")),
        _clean_text(row.get("domain")),
        _clean_text(row.get("population")),
        _clean_text(row.get("regimen")),
        _clean_text(stage_bucket),
    )


def _describe_intervention_object_identity(
    row: dict[str, str],
    *,
    stage_bucket: str,
) -> str:
    labels = (
        "asset_lineage_id",
        "target_class_lineage_id",
        "modality",
        "domain",
        "population",
        "regimen",
        "stage_bucket",
    )
    components = _intervention_object_identity_components(
        row,
        stage_bucket=stage_bucket,
    )
    fields = [
        f"{label}={value or '<missing>'}"
        for label, value in zip(labels, components, strict=True)
    ]
    source_program_universe_id = _clean_text(row.get("source_program_universe_id")) or _clean_text(
        row.get("program_universe_id")
    )
    if source_program_universe_id:
        fields.append(f"source_program_universe_id={source_program_universe_id}")
    return "; ".join(fields)


def _register_intervention_object_entity_id(
    seen_entity_ids: dict[str, str],
    *,
    entity_id: str,
    row: dict[str, str],
    stage_bucket: str,
    context: str,
) -> None:
    if entity_id not in seen_entity_ids:
        seen_entity_ids[entity_id] = _describe_intervention_object_identity(
            row,
            stage_bucket=stage_bucket,
        )
        return
    raise ValueError(
        f"{context} produced duplicate replay entity_id {entity_id}: "
        f"{seen_entity_ids[entity_id]} vs "
        f"{_describe_intervention_object_identity(row, stage_bucket=stage_bucket)}"
    )


def _load_archive_rows(descriptor: object) -> tuple[dict[str, str], ...]:
    archive_path = Path(_clean_text(getattr(descriptor, "archive_file"))).resolve()
    archive_format = _clean_text(getattr(descriptor, "archive_format"))
    if archive_format == "csv":
        rows = read_csv_rows(archive_path)
        if any("entity_type" in row for row in rows):
            return tuple(rows)
        return tuple({**row, "entity_type": "gene"} for row in rows)
    if archive_format == "json":
        payload = read_json(archive_path)
        if isinstance(payload, list):
            return tuple(
                {
                    **item,
                    "entity_type": _clean_text(item.get("entity_type")) or "gene",
                }
                for item in payload
                if isinstance(item, dict)
            )
        if not isinstance(payload, dict):
            raise ValueError(f"unsupported JSON archive payload for {archive_path.name}")
        rows: list[dict[str, str]] = []
        for key, entity_type in (("genes", "gene"), ("modules", "module")):
            entity_rows = payload.get(key, [])
            if not isinstance(entity_rows, list):
                continue
            for entity_row in entity_rows:
                if not isinstance(entity_row, dict):
                    continue
                rows.append(
                    {
                        **entity_row,
                        "entity_type": _clean_text(entity_row.get("entity_type"))
                        or entity_type,
                    }
                )
        return tuple(rows)
    raise ValueError(f"unsupported archive format: {archive_format}")


def _descriptor_index(
    archive_descriptors: tuple[object, ...],
) -> dict[tuple[str, str], object]:
    return {
        (
            _clean_text(getattr(descriptor, "source_name")),
            _clean_text(getattr(descriptor, "source_version")),
        ): descriptor
        for descriptor in archive_descriptors
    }


def build_intervention_object_bundle_source_snapshot_provenance(
    source_snapshots: tuple[object, ...],
    archive_descriptors: tuple[object, ...],
) -> str:
    descriptors_by_key = _descriptor_index(archive_descriptors)
    provenance_records: list[dict[str, object]] = []
    for source_snapshot in sorted(
        source_snapshots,
        key=lambda item: (
            _clean_text(getattr(item, "source_name")),
            _clean_text(getattr(item, "source_version")),
        ),
    ):
        source_name = _clean_text(getattr(source_snapshot, "source_name"))
        source_version = _clean_text(getattr(source_snapshot, "source_version"))
        included = bool(getattr(source_snapshot, "included", False))
        archive_sha256 = ""
        if included:
            descriptor = descriptors_by_key.get((source_name, source_version))
            if descriptor is None:
                raise ValueError(
                    "intervention-object feature bundle requires an archive descriptor "
                    "for included source snapshot "
                    f"{source_name}/{source_version}"
                )
            archive_sha256 = _clean_text(getattr(descriptor, "sha256"))
        provenance_records.append(
            {
                "allowed_data_through": _clean_text(
                    getattr(source_snapshot, "allowed_data_through")
                ),
                "archive_sha256": archive_sha256,
                "cutoff_mode": _clean_text(getattr(source_snapshot, "cutoff_mode")),
                "evidence_frozen_at": _clean_text(
                    getattr(source_snapshot, "evidence_frozen_at")
                ),
                "evidence_timestamp_field": _clean_text(
                    getattr(source_snapshot, "evidence_timestamp_field")
                ),
                "exclusion_reason": _clean_text(
                    getattr(source_snapshot, "exclusion_reason")
                ),
                "future_record_policy": _clean_text(
                    getattr(source_snapshot, "future_record_policy")
                ),
                "included": included,
                "materialized_at": _clean_text(getattr(source_snapshot, "materialized_at")),
                "missing_date_policy": _clean_text(
                    getattr(source_snapshot, "missing_date_policy")
                ),
                "source_name": source_name,
                "source_version": source_version,
            }
        )
    return json.dumps(
        provenance_records,
        sort_keys=True,
        separators=(",", ":"),
    )


def _collect_included_archive_rows(
    *,
    source_snapshots: tuple[object, ...],
    archive_descriptors: tuple[object, ...],
) -> tuple[tuple[dict[str, str], ...], tuple[dict[str, str], ...]]:
    descriptors_by_key = _descriptor_index(archive_descriptors)
    gene_rows: list[dict[str, str]] = []
    module_rows: list[dict[str, str]] = []
    for source_snapshot in source_snapshots:
        if not bool(getattr(source_snapshot, "included", False)):
            continue
        key = (
            _clean_text(getattr(source_snapshot, "source_name")),
            _clean_text(getattr(source_snapshot, "source_version")),
        )
        descriptor = descriptors_by_key.get(key)
        if descriptor is None:
            continue
        for row in _load_archive_rows(descriptor):
            entity_type = _clean_text(row.get("entity_type"))
            if entity_type == "gene":
                gene_rows.append(row)
            elif entity_type == "module":
                module_rows.append(row)
    return tuple(gene_rows), tuple(module_rows)


def _merge_entity_rows(
    rows: tuple[dict[str, str], ...],
    *,
    entity_type: str,
) -> dict[str, dict[str, object]]:
    field_names = (
        KNOWN_GENE_LAYER_FIELDS + KNOWN_METADATA_FIELDS
        if entity_type == "gene"
        else KNOWN_MODULE_LAYER_FIELDS
    )
    merged: dict[str, dict[str, object]] = {}
    for row in rows:
        entity_id = _clean_text(row.get("entity_id"))
        entity_label = _clean_text(row.get("entity_label"))
        if not entity_id or not entity_label:
            continue
        state = merged.setdefault(
            entity_id,
            {
                "entity_id": entity_id,
                "entity_label": entity_label,
                "entity_type": entity_type,
                **{field_name: None for field_name in field_names},
            },
        )
        for field_name in field_names:
            if field_name not in row:
                continue
            cleaned = _clean_text(row.get(field_name))
            if not cleaned:
                continue
            if field_name in KNOWN_METADATA_FIELDS:
                if state[field_name] in {None, ""}:
                    state[field_name] = cleaned
                continue
            parsed = float(cleaned)
            existing = state[field_name]
            if existing is None:
                state[field_name] = parsed
            elif abs(float(existing) - parsed) > 1e-9:
                state[field_name] = max(float(existing), parsed)
        if MODULE_MEMBER_GENES_FIELD in row and _clean_text(row.get(MODULE_MEMBER_GENES_FIELD)):
            state[MODULE_MEMBER_GENES_FIELD] = _clean_text(row.get(MODULE_MEMBER_GENES_FIELD))
    return merged


def _load_program_history_events(
    path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> dict[str, dict[str, str]]:
    return {
        _clean_text(row.get("event_id")): row
        for row in read_csv_rows(path)
        if _clean_text(row.get("event_id"))
    }


def _load_program_universe_rows(
    path: Path = PROGRAM_UNIVERSE_PATH,
) -> tuple[dict[str, str], ...]:
    return tuple(read_csv_rows(path))


def _mapped_events_for_row(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
) -> tuple[dict[str, str], ...]:
    mapped_event_ids = _json_list(row.get("mapped_event_ids_json", "[]"))
    return tuple(
        events_by_id[event_id]
        for event_id in mapped_event_ids
        if event_id in events_by_id
    )


def _mapped_event_stage_bucket(event: dict[str, str]) -> str:
    phase = _clean_text(event.get("phase"))
    return {
        "phase_3": "phase_3_or_registration",
        "registration": "phase_3_or_registration",
    }.get(phase, phase)


def _stage_bucket_rank(stage_bucket: str) -> int:
    return STAGE_BUCKET_RANKS.get(_clean_text(stage_bucket), 0)


def _mapped_stage_buckets_by_cutoff(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> tuple[set[str], set[str]]:
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    pre_cutoff_stage_buckets: set[str] = set()
    future_stage_buckets: set[str] = set()
    for event in _mapped_events_for_row(row, events_by_id=events_by_id):
        stage_bucket = _mapped_event_stage_bucket(event)
        event_date = _parse_iso_date(_clean_text(event.get("event_date")), "event_date")
        if event_date <= cutoff:
            pre_cutoff_stage_buckets.add(stage_bucket)
        else:
            future_stage_buckets.add(stage_bucket)
    return pre_cutoff_stage_buckets, future_stage_buckets


def _event_sort_key(event: dict[str, str]) -> tuple[date, int, str]:
    sort_order_text = _clean_text(event.get("sort_order"))
    try:
        sort_order = int(sort_order_text) if sort_order_text else 0
    except ValueError:
        sort_order = 0
    return (
        _parse_iso_date(_clean_text(event.get("event_date")), "event_date"),
        sort_order,
        _clean_text(event.get("event_id")),
    )


def _latest_pre_cutoff_event(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> dict[str, str] | None:
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    pre_cutoff_events = [
        event
        for event in _mapped_events_for_row(row, events_by_id=events_by_id)
        if _parse_iso_date(_clean_text(event.get("event_date")), "event_date") <= cutoff
    ]
    if not pre_cutoff_events:
        return None
    return max(pre_cutoff_events, key=_event_sort_key)


def _render_intervention_object_entity_id(
    row: dict[str, str],
    *,
    stage_bucket: str,
) -> str:
    components = tuple(
        _slugify(component)
        for component in _intervention_object_identity_components(
            row,
            stage_bucket=_clean_text(stage_bucket),
        )
        if _slugify(component)
    )
    entity_id = "-".join(components)
    if not entity_id:
        raise ValueError(
            "intervention-object replay identity requires at least one non-empty "
            "identity component"
        )
    return entity_id


def _stage_bucket_as_of_snapshot(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> str:
    current_stage_bucket = _clean_text(row.get("stage_bucket"))
    pre_cutoff_stage_buckets, future_stage_buckets = _mapped_stage_buckets_by_cutoff(
        row,
        events_by_id=events_by_id,
        as_of_date=as_of_date,
    )

    if current_stage_bucket in pre_cutoff_stage_buckets:
        return current_stage_bucket

    if current_stage_bucket in future_stage_buckets and pre_cutoff_stage_buckets:
        return max(pre_cutoff_stage_buckets, key=_stage_bucket_rank)

    if pre_cutoff_stage_buckets:
        return max(pre_cutoff_stage_buckets, key=_stage_bucket_rank)

    future_events = sorted(
        (
            event
            for event in _mapped_events_for_row(row, events_by_id=events_by_id)
            if _parse_iso_date(_clean_text(event.get("event_date")), "event_date")
            > _parse_iso_date(as_of_date, "as_of_date")
        ),
        key=_event_sort_key,
    )
    if future_events:
        first_future_event = future_events[0]
        if (
            _clean_text(first_future_event.get("event_type")) in APPROVAL_EVENT_TYPES
            and current_stage_bucket == "approved"
        ):
            return "phase_3_or_registration"
        inferred_stage_bucket = _mapped_event_stage_bucket(first_future_event)
        if inferred_stage_bucket:
            return inferred_stage_bucket

    return current_stage_bucket


def _program_row_as_of_snapshot(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> dict[str, str]:
    latest_pre_cutoff_event = _latest_pre_cutoff_event(
        row,
        events_by_id=events_by_id,
        as_of_date=as_of_date,
    )
    stage_bucket = _stage_bucket_as_of_snapshot(
        row,
        events_by_id=events_by_id,
        as_of_date=as_of_date,
    )
    domain = (
        _clean_text(latest_pre_cutoff_event.get("domain"))
        if latest_pre_cutoff_event is not None
        else ""
    ) or _clean_text(row.get("domain"))
    population = (
        _clean_text(latest_pre_cutoff_event.get("population"))
        if latest_pre_cutoff_event is not None
        else ""
    ) or _clean_text(row.get("population"))
    regimen = (
        _clean_text(latest_pre_cutoff_event.get("mono_or_adjunct"))
        if latest_pre_cutoff_event is not None
        else ""
    ) or _clean_text(row.get("regimen"))
    as_of_row = {
        **row,
        "domain": domain,
        "population": population,
        "regimen": regimen,
    }
    return {
        **as_of_row,
        "source_program_universe_id": _clean_text(row.get("program_universe_id")),
        "program_universe_id": _render_intervention_object_entity_id(
            as_of_row,
            stage_bucket=stage_bucket,
        ),
        "stage_bucket": stage_bucket,
    }


def _has_pre_cutoff_approval(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> bool:
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    for event in _mapped_events_for_row(row, events_by_id=events_by_id):
        event_type = _clean_text(event.get("event_type"))
        if event_type not in APPROVAL_EVENT_TYPES:
            continue
        event_date = _parse_iso_date(_clean_text(event.get("event_date")), "event_date")
        if event_date <= cutoff:
            return True
    return False


def _has_pre_cutoff_stage_state(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> bool:
    pre_cutoff_stage_buckets, _future_stage_buckets = _mapped_stage_buckets_by_cutoff(
        row,
        events_by_id=events_by_id,
        as_of_date=as_of_date,
    )
    return bool(pre_cutoff_stage_buckets)


def _has_future_only_included_visibility(
    row: dict[str, str],
    *,
    events_by_id: dict[str, dict[str, str]],
    as_of_date: str,
) -> bool:
    if _clean_text(row.get("coverage_state")) != "included":
        return False
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    return any(
        _parse_iso_date(_clean_text(event.get("event_date")), "event_date") > cutoff
        for event in _mapped_events_for_row(row, events_by_id=events_by_id)
    )


def _iter_admissible_program_rows(
    *,
    as_of_date: str,
    program_universe_path: Path = PROGRAM_UNIVERSE_PATH,
    events_path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> tuple[dict[str, str], ...]:
    events_by_id = _load_program_history_events(events_path)
    rows = _load_program_universe_rows(program_universe_path)
    admissible: list[dict[str, str]] = []
    for row in rows:
        coverage_state = _clean_text(row.get("coverage_state"))
        if coverage_state not in IN_SCOPE_COVERAGE_STATES:
            continue
        if _clean_text(row.get("duplicate_of_program_universe_id")):
            continue
        if not (
            _has_pre_cutoff_stage_state(
                row,
                events_by_id=events_by_id,
                as_of_date=as_of_date,
            )
            or _has_future_only_included_visibility(
                row,
                events_by_id=events_by_id,
                as_of_date=as_of_date,
            )
        ):
            continue
        if _has_pre_cutoff_approval(row, events_by_id=events_by_id, as_of_date=as_of_date):
            continue
        admissible.append(row)
    return tuple(admissible)


def build_intervention_object_candidate_cutoff_dates(
    *,
    as_of_date: str,
    minimum_cutoff_date: str,
    program_universe_path: Path = PROGRAM_UNIVERSE_PATH,
    events_path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> tuple[str, ...]:
    minimum_cutoff = _parse_iso_date(minimum_cutoff_date, "minimum_cutoff_date")
    maximum_cutoff = _parse_iso_date(as_of_date, "as_of_date")
    if minimum_cutoff > maximum_cutoff:
        raise ValueError("minimum_cutoff_date must be on or before as_of_date")

    events_by_id = _load_program_history_events(events_path)
    candidate_dates: set[str] = {maximum_cutoff.isoformat()}
    for row in _load_program_universe_rows(program_universe_path):
        coverage_state = _clean_text(row.get("coverage_state"))
        if coverage_state not in IN_SCOPE_COVERAGE_STATES:
            continue
        if _clean_text(row.get("duplicate_of_program_universe_id")):
            continue
        for event in _mapped_events_for_row(row, events_by_id=events_by_id):
            event_date = _parse_iso_date(
                _clean_text(event.get("event_date")),
                "event_date",
            )
            if minimum_cutoff <= event_date <= maximum_cutoff:
                candidate_dates.add(event_date.isoformat())
            prior_date = event_date - timedelta(days=1)
            if minimum_cutoff <= prior_date <= maximum_cutoff:
                candidate_dates.add(prior_date.isoformat())
    return tuple(sorted(candidate_dates))


def build_intervention_object_public_slice_rows(
    *,
    as_of_date: str,
    outcome_observation_closed_at: str,
    program_universe_path: Path = PROGRAM_UNIVERSE_PATH,
    events_path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    outcome_closed_at = _parse_iso_date(
        outcome_observation_closed_at,
        "outcome_observation_closed_at",
    )
    events_by_id = _load_program_history_events(events_path)
    cohort_rows: list[dict[str, str]] = []
    future_outcome_rows: list[dict[str, str]] = []
    seen_entity_ids: dict[str, str] = {}
    for row in _iter_admissible_program_rows(
        as_of_date=as_of_date,
        program_universe_path=program_universe_path,
        events_path=events_path,
    ):
        as_of_row = _program_row_as_of_snapshot(
            row,
            events_by_id=events_by_id,
            as_of_date=as_of_date,
        )
        entity_id = _clean_text(as_of_row.get("program_universe_id"))
        _register_intervention_object_entity_id(
            seen_entity_ids,
            entity_id=entity_id,
            row=as_of_row,
            stage_bucket=_clean_text(as_of_row.get("stage_bucket")),
            context="intervention-object public slice materialization",
        )
        entity_label = _render_intervention_object_label(as_of_row)
        as_of_stage_rank = _stage_bucket_rank(_clean_text(as_of_row.get("stage_bucket")))
        cohort_rows.append(
            {
                "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                "entity_id": entity_id,
                "entity_label": entity_label,
            }
        )
        for event in _mapped_events_for_row(row, events_by_id=events_by_id):
            event_date = _parse_iso_date(_clean_text(event.get("event_date")), "event_date")
            if event_date <= cutoff or event_date > outcome_closed_at:
                continue
            event_type = _clean_text(event.get("event_type"))
            primary_outcome_result = _clean_text(event.get("primary_outcome_result"))
            event_id = _clean_text(event.get("event_id"))
            future_stage_rank = _stage_bucket_rank(_mapped_event_stage_bucket(event))
            if future_stage_rank > as_of_stage_rank:
                future_outcome_rows.append(
                    {
                        "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                        "entity_id": entity_id,
                        "outcome_label": "future_schizophrenia_program_advanced",
                        "outcome_date": event.get("event_date", ""),
                        "label_source": "program_history_v2",
                        "label_notes": (
                            f"event_id={event_id}; stage_bucket={_mapped_event_stage_bucket(event)}"
                        ),
                    }
                )
            if event_type in APPROVAL_EVENT_TYPES:
                future_outcome_rows.append(
                    {
                        "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                        "entity_id": entity_id,
                        "outcome_label": "future_schizophrenia_positive_signal",
                        "outcome_date": event.get("event_date", ""),
                        "label_source": "program_history_v2",
                        "label_notes": (
                            f"event_id={event_id}; mapped from regulatory approval"
                        ),
                    }
                )
                continue
            if primary_outcome_result.startswith(NEGATIVE_RESULT_PREFIXES):
                future_outcome_rows.append(
                    {
                        "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                        "entity_id": entity_id,
                        "outcome_label": "future_schizophrenia_negative_signal",
                        "outcome_date": event.get("event_date", ""),
                        "label_source": "program_history_v2",
                        "label_notes": f"event_id={event_id}; result={primary_outcome_result}",
                    }
                )
                continue
            if primary_outcome_result.startswith(POSITIVE_RESULT_PREFIXES):
                future_outcome_rows.append(
                    {
                        "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                        "entity_id": entity_id,
                        "outcome_label": "future_schizophrenia_positive_signal",
                        "outcome_date": event.get("event_date", ""),
                        "label_source": "program_history_v2",
                        "label_notes": f"event_id={event_id}; result={primary_outcome_result}",
                    }
                )
    cohort_rows.sort(key=lambda row: (row["entity_label"].lower(), row["entity_id"]))
    future_outcome_rows.sort(
        key=lambda row: (
            row["outcome_date"],
            row["entity_id"],
            row["outcome_label"],
        )
    )
    return cohort_rows, future_outcome_rows


def build_intervention_object_bundle_rows(
    *,
    as_of_date: str,
    source_snapshots: tuple[object, ...],
    archive_descriptors: tuple[object, ...],
    program_universe_path: Path = PROGRAM_UNIVERSE_PATH,
    events_path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> list[dict[str, object]]:
    events_by_id = _load_program_history_events(events_path)
    gene_archive_rows, module_archive_rows = _collect_included_archive_rows(
        source_snapshots=source_snapshots,
        archive_descriptors=archive_descriptors,
    )
    gene_index = _merge_entity_rows(gene_archive_rows, entity_type="gene")
    module_index = _merge_entity_rows(module_archive_rows, entity_type="module")
    module_membership_index: dict[str, set[str]] = {}
    for module_id, module_row in module_index.items():
        member_genes = {
            _normalize_gene_symbol(symbol)
            for symbol in _json_list(module_row.get(MODULE_MEMBER_GENES_FIELD, "[]"))
        }
        module_membership_index[module_id] = member_genes

    included_source_names = sorted(
        _clean_text(getattr(source_snapshot, "source_name"))
        for source_snapshot in source_snapshots
        if bool(getattr(source_snapshot, "included", False))
    )
    excluded_source_names = sorted(
        _clean_text(getattr(source_snapshot, "source_name"))
        for source_snapshot in source_snapshots
        if not bool(getattr(source_snapshot, "included", False))
    )

    bundle_rows: list[dict[str, object]] = []
    seen_entity_ids: dict[str, str] = {}
    for row in _iter_admissible_program_rows(
        as_of_date=as_of_date,
        program_universe_path=program_universe_path,
        events_path=events_path,
    ):
        as_of_row = _program_row_as_of_snapshot(
            row,
            events_by_id=events_by_id,
            as_of_date=as_of_date,
        )
        intervention_object_id = _clean_text(as_of_row.get("program_universe_id"))
        _register_intervention_object_entity_id(
            seen_entity_ids,
            entity_id=intervention_object_id,
            row=as_of_row,
            stage_bucket=_clean_text(as_of_row.get("stage_bucket")),
            context="intervention-object feature-bundle materialization",
        )
        target_symbols = [
            _normalize_gene_symbol(symbol)
            for symbol in _json_list(as_of_row.get("target_symbols_json", "[]"))
        ]
        matched_gene_rows = [
            gene_row
            for gene_row in gene_index.values()
            if _normalize_gene_symbol(_clean_text(gene_row.get("entity_label"))) in target_symbols
        ]
        matched_module_rows = [
            module_row
            for module_id, module_row in module_index.items()
            if module_membership_index.get(module_id, set()).intersection(target_symbols)
        ]
        bundle_rows.append(
            {
                "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                "entity_id": intervention_object_id,
                "intervention_object_id": intervention_object_id,
                "entity_label": _render_intervention_object_label(as_of_row),
                "source_program_universe_id": _clean_text(
                    as_of_row.get("source_program_universe_id")
                ),
                "asset_id": _clean_text(as_of_row.get("asset_id")),
                "asset_name": _clean_text(as_of_row.get("asset_name")),
                "asset_lineage_id": _clean_text(as_of_row.get("asset_lineage_id")),
                "target": _clean_text(as_of_row.get("target")),
                "target_symbols_json": json.dumps(target_symbols, sort_keys=True),
                "target_class": _clean_text(as_of_row.get("target_class")),
                "target_class_lineage_id": _clean_text(
                    as_of_row.get("target_class_lineage_id")
                ),
                "mechanism": _clean_text(as_of_row.get("mechanism")),
                "modality": _clean_text(as_of_row.get("modality")),
                "domain": _clean_text(as_of_row.get("domain")),
                "population": _clean_text(as_of_row.get("population")),
                "regimen": _clean_text(as_of_row.get("regimen")),
                "stage_bucket": _clean_text(as_of_row.get("stage_bucket")),
                "coverage_state": _clean_text(as_of_row.get("coverage_state")),
                "coverage_reason": _clean_text(as_of_row.get("coverage_reason")),
                "common_variant_support": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("common_variant_support"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "rare_variant_support": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("rare_variant_support"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "cell_state_support": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("cell_state_support"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "developmental_regulatory_support": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("developmental_regulatory_support"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "tractability_compoundability": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("tractability_compoundability"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "generic_platform_baseline": _mean(
                    [
                        float(value)
                        for value in (
                            _float_or_none(gene_row.get("generic_platform_baseline"))
                            for gene_row in matched_gene_rows
                        )
                        if value is not None
                    ]
                ),
                "source_present_pgc": _bool_text(
                    any(
                        _float_or_none(gene_row.get("common_variant_support")) is not None
                        for gene_row in matched_gene_rows
                    )
                ),
                "source_present_schema": _bool_text(
                    any(
                        _float_or_none(gene_row.get("rare_variant_support")) is not None
                        for gene_row in matched_gene_rows
                    )
                ),
                "source_present_psychencode": _bool_text(
                    any(
                        _float_or_none(gene_row.get("cell_state_support")) is not None
                        or _float_or_none(gene_row.get("developmental_regulatory_support"))
                        is not None
                        for gene_row in matched_gene_rows
                    )
                    or bool(matched_module_rows)
                ),
                "source_present_chembl": _bool_text(
                    any(
                        _float_or_none(gene_row.get("tractability_compoundability"))
                        is not None
                        for gene_row in matched_gene_rows
                    )
                ),
                "source_present_opentargets": _bool_text(
                    any(
                        _float_or_none(gene_row.get("generic_platform_baseline"))
                        is not None
                        for gene_row in matched_gene_rows
                    )
                ),
                "matched_gene_entity_ids_json": json.dumps(
                    sorted(
                        _clean_text(gene_row.get("entity_id"))
                        for gene_row in matched_gene_rows
                    ),
                    sort_keys=True,
                ),
                "matched_gene_symbols_json": json.dumps(
                    sorted(
                        {
                            _normalize_gene_symbol(
                                _clean_text(gene_row.get("entity_label"))
                            )
                            for gene_row in matched_gene_rows
                        }
                    ),
                    sort_keys=True,
                ),
                "matched_module_entity_ids_json": json.dumps(
                    sorted(
                        _clean_text(module_row.get("entity_id"))
                        for module_row in matched_module_rows
                    ),
                    sort_keys=True,
                ),
                "included_sources_json": json.dumps(included_source_names, sort_keys=True),
                "excluded_sources_json": json.dumps(excluded_source_names, sort_keys=True),
                "compatibility_projection_contract": INTERVENTION_OBJECT_PROJECTION_CONTRACT,
            }
        )
    bundle_rows.sort(key=lambda row: (str(row["entity_label"]).lower(), str(row["entity_id"])))
    return bundle_rows


def materialize_intervention_object_feature_bundle(
    *,
    output_file: Path,
    as_of_date: str,
    source_snapshots: tuple[object, ...],
    archive_descriptors: tuple[object, ...],
    program_universe_path: Path = PROGRAM_UNIVERSE_PATH,
    events_path: Path = PROGRAM_HISTORY_EVENTS_PATH,
) -> dict[str, object]:
    rows = build_intervention_object_bundle_rows(
        as_of_date=as_of_date,
        source_snapshots=source_snapshots,
        archive_descriptors=archive_descriptors,
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    table = Table.from_pylist(rows)
    source_snapshot_provenance_json = build_intervention_object_bundle_source_snapshot_provenance(
        source_snapshots,
        archive_descriptors,
    )
    table = table.replace_schema_metadata(
        {
            b"schema_name": INTERVENTION_OBJECT_BUNDLE_SCHEMA_NAME.encode("utf-8"),
            b"schema_version": INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION.encode("utf-8"),
            b"as_of_date": as_of_date.encode("utf-8"),
            INTERVENTION_OBJECT_BUNDLE_SOURCE_PROVENANCE_METADATA_FIELD.encode(
                "utf-8"
            ): source_snapshot_provenance_json.encode("utf-8"),
        }
    )
    pq.write_table(table, output_file)
    return {
        "output_file": str(output_file),
        "row_count": len(rows),
        "schema_name": INTERVENTION_OBJECT_BUNDLE_SCHEMA_NAME,
        "schema_version": INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION,
    }


def read_intervention_object_feature_bundle(
    path: Path,
    *,
    expected_as_of_date: str | None = None,
    expected_entities: dict[str, str] | None = None,
    expected_included_sources: tuple[str, ...] | None = None,
    expected_excluded_sources: tuple[str, ...] | None = None,
    expected_source_snapshot_provenance_json: str | None = None,
) -> list[dict[str, object]]:
    table = pq.read_table(path)
    metadata = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in (table.schema.metadata or {}).items()
    }
    schema_name = _clean_text(metadata.get("schema_name"))
    if schema_name != INTERVENTION_OBJECT_BUNDLE_SCHEMA_NAME:
        raise ValueError(
            "intervention-object feature bundle schema_name mismatch: "
            f"expected {INTERVENTION_OBJECT_BUNDLE_SCHEMA_NAME}, found {schema_name or '<missing>'}"
        )
    schema_version = _clean_text(metadata.get("schema_version"))
    if schema_version != INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION:
        raise ValueError(
            "intervention-object feature bundle schema_version mismatch: "
            f"expected {INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION}, found {schema_version or '<missing>'}"
        )
    bundle_as_of_date = _clean_text(metadata.get("as_of_date"))
    _parse_iso_date(bundle_as_of_date, "intervention-object feature bundle as_of_date")
    if expected_as_of_date is not None and bundle_as_of_date != expected_as_of_date:
        raise ValueError(
            "intervention-object feature bundle as_of_date does not match the "
            f"snapshot manifest: {bundle_as_of_date} != {expected_as_of_date}"
        )
    source_snapshot_provenance_json = _clean_text(
        metadata.get(INTERVENTION_OBJECT_BUNDLE_SOURCE_PROVENANCE_METADATA_FIELD)
    )
    if not source_snapshot_provenance_json:
        raise ValueError(
            "intervention-object feature bundle metadata is missing "
            f"{INTERVENTION_OBJECT_BUNDLE_SOURCE_PROVENANCE_METADATA_FIELD}"
        )
    try:
        source_snapshot_provenance = json.loads(source_snapshot_provenance_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "intervention-object feature bundle source snapshot provenance metadata "
            "must be valid JSON"
        ) from exc
    if not isinstance(source_snapshot_provenance, list):
        raise ValueError(
            "intervention-object feature bundle source snapshot provenance metadata "
            "must be a JSON list"
        )
    canonical_source_snapshot_provenance_json = json.dumps(
        source_snapshot_provenance,
        sort_keys=True,
        separators=(",", ":"),
    )
    if (
        expected_source_snapshot_provenance_json is not None
        and canonical_source_snapshot_provenance_json
        != expected_source_snapshot_provenance_json
    ):
        raise ValueError(
            "intervention-object feature bundle source snapshot provenance does not "
            "match the snapshot manifest and archive index"
        )
    missing_columns = [
        column_name
        for column_name in INTERVENTION_OBJECT_BUNDLE_REQUIRED_COLUMNS
        if column_name not in table.column_names
    ]
    if missing_columns:
        raise ValueError(
            "intervention-object feature bundle is missing required columns: "
            + ", ".join(missing_columns)
        )
    rows = table.to_pylist()
    bundle_entities: dict[str, str] = {}
    for index, row in enumerate(rows):
        entity_type = _clean_text(row.get("entity_type"))
        if entity_type != INTERVENTION_OBJECT_ENTITY_TYPE:
            raise ValueError(
                "intervention-object feature bundle rows must all have "
                f"entity_type={INTERVENTION_OBJECT_ENTITY_TYPE}: row {index}"
            )
        entity_id = _clean_text(row.get("entity_id"))
        if not entity_id:
            raise ValueError(
                "intervention-object feature bundle rows must include entity_id: "
                f"row {index}"
            )
        intervention_object_id = _clean_text(row.get("intervention_object_id"))
        if intervention_object_id != entity_id:
            raise ValueError(
                "intervention-object feature bundle rows must align "
                "intervention_object_id with entity_id"
            )
        entity_label = _clean_text(row.get("entity_label"))
        if entity_id in bundle_entities:
            raise ValueError(
                "intervention-object feature bundle repeated entity_id: "
                f"{entity_id}"
            )
        bundle_entities[entity_id] = entity_label
        if _clean_text(row.get("compatibility_projection_contract")) != (
            INTERVENTION_OBJECT_PROJECTION_CONTRACT
        ):
            raise ValueError(
                "intervention-object feature bundle compatibility_projection_contract "
                "does not match the frozen replay contract"
            )
        if expected_included_sources is not None:
            included_sources = tuple(
                sorted(_json_list(row.get("included_sources_json", "[]")))
            )
            if included_sources != tuple(sorted(expected_included_sources)):
                raise ValueError(
                    "intervention-object feature bundle included_sources_json does "
                    "not match the snapshot manifest"
                )
        if expected_excluded_sources is not None:
            excluded_sources = tuple(
                sorted(_json_list(row.get("excluded_sources_json", "[]")))
            )
            if excluded_sources != tuple(sorted(expected_excluded_sources)):
                raise ValueError(
                    "intervention-object feature bundle excluded_sources_json does "
                    "not match the snapshot manifest"
                )
    if expected_entities is not None and bundle_entities != expected_entities:
        missing_entities = sorted(set(expected_entities).difference(bundle_entities))
        extra_entities = sorted(set(bundle_entities).difference(expected_entities))
        mismatched_labels = sorted(
            entity_id
            for entity_id, entity_label in bundle_entities.items()
            if entity_id in expected_entities and expected_entities[entity_id] != entity_label
        )
        details: list[str] = []
        if missing_entities:
            details.append("missing=" + ", ".join(missing_entities[:5]))
        if extra_entities:
            details.append("extra=" + ", ".join(extra_entities[:5]))
        if mismatched_labels:
            details.append("label_mismatch=" + ", ".join(mismatched_labels[:5]))
        raise ValueError(
            "intervention-object feature bundle does not align with the replay cohort: "
            + "; ".join(details)
        )
    return rows


def intervention_object_bundle_path_for_manifest_file(manifest_file: Path) -> Path:
    return manifest_file.resolve().parent / INTERVENTION_OBJECT_BUNDLE_FILE_NAME


def intervention_object_projection_path(
    *,
    output_dir: Path,
    baseline_id: str,
) -> Path:
    return (
        output_dir.resolve()
        / INTERVENTION_OBJECT_PROJECTION_DIR_NAME
        / f"{baseline_id}__{INTERVENTION_OBJECT_ENTITY_TYPE}.json"
    )


def build_intervention_object_projection_payload(
    *,
    baseline_id: str,
    bundle_rows: list[dict[str, object]],
    gene_predictions: tuple[object, ...] = (),
    module_predictions: tuple[object, ...] = (),
) -> dict[str, object]:
    gene_index = {
        _clean_text(getattr(prediction, "entity_id")): prediction
        for prediction in gene_predictions
    }
    module_index = {
        _clean_text(getattr(prediction, "entity_id")): prediction
        for prediction in module_predictions
    }
    rows: list[dict[str, object]] = []
    for bundle_row in bundle_rows:
        matched_gene_ids = _json_list(bundle_row.get("matched_gene_entity_ids_json", "[]"))
        matched_module_ids = _json_list(
            bundle_row.get("matched_module_entity_ids_json", "[]")
        )
        contributors: list[dict[str, object]] = []
        for entity_id in matched_gene_ids:
            prediction = gene_index.get(entity_id)
            if prediction is None:
                continue
            contributors.append(
                {
                    "entity_type": "gene",
                    "entity_id": entity_id,
                    "entity_label": getattr(prediction, "entity_label"),
                    "rank": getattr(prediction, "rank"),
                    "score": getattr(prediction, "score"),
                }
            )
        for entity_id in matched_module_ids:
            prediction = module_index.get(entity_id)
            if prediction is None:
                continue
            contributors.append(
                {
                    "entity_type": "module",
                    "entity_id": entity_id,
                    "entity_label": getattr(prediction, "entity_label"),
                    "rank": getattr(prediction, "rank"),
                    "score": getattr(prediction, "score"),
                }
            )
        scores = [float(contributor["score"]) for contributor in contributors]
        rows.append(
            {
                "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
                "entity_id": _clean_text(bundle_row.get("entity_id")),
                "entity_label": _clean_text(bundle_row.get("entity_label")),
                "baseline_id": baseline_id,
                "aggregation_rule": INTERVENTION_OBJECT_PROJECTION_AGGREGATION_RULE,
                "compatibility_projection_contract": INTERVENTION_OBJECT_PROJECTION_CONTRACT,
                "covered": bool(scores),
                "projected_score": (
                    round(sum(scores) / len(scores), 6)
                    if scores
                    else None
                ),
                "rank": None,
                "matched_gene_entity_ids_json": json.dumps(matched_gene_ids, sort_keys=True),
                "matched_module_entity_ids_json": json.dumps(
                    matched_module_ids,
                    sort_keys=True,
                ),
                "contributing_legacy_entities": contributors,
            }
        )

    covered_rows = [row for row in rows if row["projected_score"] is not None]
    covered_rows.sort(
        key=lambda row: (
            -float(row["projected_score"]),
            str(row["entity_label"]).lower(),
            str(row["entity_id"]),
        )
    )
    for index, row in enumerate(covered_rows, start=1):
        row["rank"] = index
    row_index = {(_clean_text(row["entity_id"])): row for row in covered_rows}
    ordered_rows = [
        row_index.get(_clean_text(row["entity_id"]), row)
        for row in rows
    ]
    return {
        "schema_name": INTERVENTION_OBJECT_PROJECTION_SCHEMA_NAME,
        "schema_version": INTERVENTION_OBJECT_PROJECTION_SCHEMA_VERSION,
        "baseline_id": baseline_id,
        "entity_type": INTERVENTION_OBJECT_ENTITY_TYPE,
        "aggregation_rule": INTERVENTION_OBJECT_PROJECTION_AGGREGATION_RULE,
        "compatibility_projection_contract": INTERVENTION_OBJECT_PROJECTION_CONTRACT,
        "rows": ordered_rows,
    }


def write_intervention_object_projection_payload(
    path: Path,
    payload: dict[str, object],
) -> None:
    write_json(path, payload)


def read_intervention_object_projection_payload(path: Path) -> dict[str, object]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("intervention-object projection payload must be a JSON object")
    return payload
