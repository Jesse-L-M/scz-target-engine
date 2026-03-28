from __future__ import annotations

from dataclasses import dataclass
import json


GENE_IDENTITY_SOURCES = ("seed", "pgc", "schema", "psychencode", "opentargets", "chembl")

SOURCE_MATCH_STATUS_FIELDS = {
    "schema": "schema_match_status",
    "psychencode": "psychencode_match_status",
    "chembl": "chembl_match_status",
}


@dataclass(frozen=True)
class SourceIdentityMatch:
    source: str
    matched: bool
    entity_id: str
    entity_label: str
    match_key: str
    match_status: str


def _clean(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def build_seed_identity_match(seed_row: dict[str, str]) -> SourceIdentityMatch:
    seed_entity_id = _clean(seed_row.get("entity_id"))
    seed_entity_label = _clean(seed_row.get("entity_label"))
    match_key = "seed_entity_id" if seed_entity_id else "seed_entity_label"
    return SourceIdentityMatch(
        source="seed",
        matched=bool(seed_entity_id or seed_entity_label),
        entity_id=seed_entity_id,
        entity_label=seed_entity_label,
        match_key=match_key if (seed_entity_id or seed_entity_label) else "",
        match_status="seed",
    )


def build_source_identity_match(
    source: str,
    source_row: dict[str, str] | None,
    match_key: str,
) -> SourceIdentityMatch:
    if source_row is None:
        return SourceIdentityMatch(
            source=source,
            matched=False,
            entity_id="",
            entity_label="",
            match_key="",
            match_status="",
        )
    match_status_field = SOURCE_MATCH_STATUS_FIELDS.get(source, "")
    match_status = _clean(source_row.get(match_status_field)) if match_status_field else "matched"
    return SourceIdentityMatch(
        source=source,
        matched=True,
        entity_id=_clean(source_row.get("entity_id")),
        entity_label=_clean(source_row.get("entity_label")),
        match_key=_clean(match_key),
        match_status=match_status or "matched",
    )


def resolve_primary_gene_id(
    seed_row: dict[str, str],
    source_matches: dict[str, SourceIdentityMatch],
) -> str:
    seed_entity_id = _clean(seed_row.get("entity_id"))
    if seed_entity_id:
        return seed_entity_id
    for source_name in GENE_IDENTITY_SOURCES[1:]:
        source_match = source_matches[source_name]
        if source_match.entity_id:
            return source_match.entity_id
    return ""


def derive_match_confidence(
    primary_gene_id: str,
    source_matches: dict[str, SourceIdentityMatch],
) -> str:
    matched_sources = [
        source_matches[source_name]
        for source_name in GENE_IDENTITY_SOURCES[1:]
        if source_matches[source_name].matched
    ]
    if not matched_sources:
        return "seed_only"

    conflicting_ids = {
        source_match.entity_id
        for source_match in matched_sources
        if source_match.entity_id and source_match.entity_id != primary_gene_id
    }
    if conflicting_ids:
        return "source_conflict"

    if any(
        source_match.entity_id == primary_gene_id and source_match.match_key == "entity_id"
        for source_match in matched_sources
    ):
        return "id_confirmed"

    if any(source_match.entity_id == primary_gene_id for source_match in matched_sources):
        return "source_confirmed"

    return "source_matched"


def _json_value(value: str) -> str | None:
    return value or None


def serialize_source_entity_ids(
    source_matches: dict[str, SourceIdentityMatch],
) -> str:
    payload = {
        source_name: _json_value(source_matches[source_name].entity_id)
        for source_name in GENE_IDENTITY_SOURCES
    }
    return json.dumps(payload)


def serialize_match_provenance(
    source_matches: dict[str, SourceIdentityMatch],
) -> str:
    payload = [
        {
            "source": source_name,
            "matched": source_matches[source_name].matched,
            "entity_id": _json_value(source_matches[source_name].entity_id),
            "entity_label": _json_value(source_matches[source_name].entity_label),
            "match_key": _json_value(source_matches[source_name].match_key),
            "match_status": _json_value(source_matches[source_name].match_status),
        }
        for source_name in GENE_IDENTITY_SOURCES
    ]
    return json.dumps(payload)


def serialize_provenance_sources(
    source_matches: dict[str, SourceIdentityMatch],
) -> str:
    payload = [
        source_name
        for source_name in GENE_IDENTITY_SOURCES
        if source_matches[source_name].matched
    ]
    return json.dumps(payload)


def build_gene_identity_fields(
    seed_row: dict[str, str],
    source_matches: dict[str, SourceIdentityMatch],
    *,
    keep_canonical_alias: bool,
) -> dict[str, str]:
    primary_gene_id = resolve_primary_gene_id(seed_row, source_matches)
    identity_fields = {
        "primary_gene_id": primary_gene_id,
        "seed_entity_id": source_matches["seed"].entity_id,
        "source_entity_ids_json": serialize_source_entity_ids(source_matches),
        "match_confidence": derive_match_confidence(primary_gene_id, source_matches),
        "match_provenance_json": serialize_match_provenance(source_matches),
        "provenance_sources_json": serialize_provenance_sources(source_matches),
    }
    if keep_canonical_alias:
        identity_fields["canonical_entity_id"] = primary_gene_id
    return identity_fields
