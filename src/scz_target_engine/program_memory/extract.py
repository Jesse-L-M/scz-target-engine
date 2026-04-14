from __future__ import annotations

from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from scz_target_engine.io import read_csv_rows
from scz_target_engine.program_memory._helpers import (
    clean_text,
    default_asset_lineage_id,
    default_target_class_lineage_id,
    parse_int,
    parse_string_list,
    slugify,
    split_target_symbols,
)
from scz_target_engine.program_memory.models import (
    ProgramMemoryAsset,
    ProgramMemoryDirectionalityHypothesis,
    ProgramMemoryEvent,
    ProgramMemoryProvenance,
)


PROGRAM_MEMORY_EVENT_SUGGESTION = "event"
PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION = "directionality_hypothesis"
PROGRAM_MEMORY_SUGGESTION_KINDS = {
    PROGRAM_MEMORY_EVENT_SUGGESTION,
    PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION,
}
CHECKED_IN_PROGRAM_MEMORY_ASSETS_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "curated"
    / "program_history"
    / "v2"
    / "assets.csv"
)
CHECKED_IN_PROGRAM_MEMORY_UNIVERSE_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "curated"
    / "program_history"
    / "v2"
    / "program_universe.csv"
)


@dataclass(frozen=True)
class _CanonicalProgramMemoryAsset:
    asset_id: str
    molecule: str
    target: str
    target_symbols: tuple[str, ...]
    target_class: str
    mechanism: str
    modality: str
    asset_lineage_id: str
    asset_aliases: tuple[str, ...]
    target_class_lineage_id: str
    target_class_aliases: tuple[str, ...]


def _clean_value(value: Any) -> str:
    return clean_text("" if value is None else str(value))


def _require_text(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    context: str,
) -> str:
    value = _clean_value(payload.get(field_name))
    if not value:
        raise ValueError(f"{context} requires {field_name}")
    return value


def _parse_string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(_clean_value(item) for item in value if _clean_value(item))
    return parse_string_list(_clean_value(value))


def _parse_sort_order(value: Any, *, default: int) -> int:
    if isinstance(value, int):
        return value
    return parse_int(_clean_value(value), default=default)


def _normalize_identity_key(value: str) -> str:
    return slugify(value)


def _merge_alias_values(*values: object) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, str):
            candidates = (value,)
        elif isinstance(value, (list, tuple)):
            candidates = tuple(str(item) for item in value)
        else:
            continue
        for candidate in candidates:
            cleaned = clean_text(candidate)
            if not cleaned:
                continue
            normalized = _normalize_identity_key(cleaned)
            if normalized in seen:
                continue
            merged.append(cleaned)
            seen.add(normalized)
    return tuple(merged)


def _filter_alias_values(
    aliases: tuple[str, ...],
    *,
    canonical_values: tuple[str, ...],
) -> tuple[str, ...]:
    canonical_keys = {
        _normalize_identity_key(value)
        for value in canonical_values
        if _normalize_identity_key(value)
    }
    return tuple(
        alias
        for alias in aliases
        if _normalize_identity_key(alias) not in canonical_keys
    )


def _catalog_source_revision(path: Path) -> tuple[str, int, int]:
    try:
        stats = path.stat()
    except FileNotFoundError:
        return (path.as_posix(), -1, -1)
    return (path.as_posix(), stats.st_mtime_ns, stats.st_size)


def _checked_in_program_memory_identity_catalog_revision() -> tuple[
    tuple[str, int, int],
    tuple[str, int, int],
]:
    return (
        _catalog_source_revision(CHECKED_IN_PROGRAM_MEMORY_ASSETS_PATH),
        _catalog_source_revision(CHECKED_IN_PROGRAM_MEMORY_UNIVERSE_PATH),
    )


@lru_cache(maxsize=4)
def _load_checked_in_program_memory_identity_catalog(
    _revision: tuple[tuple[str, int, int], tuple[str, int, int]],
) -> tuple[
    dict[str, _CanonicalProgramMemoryAsset],
    dict[str, _CanonicalProgramMemoryAsset],
]:
    asset_index: dict[str, _CanonicalProgramMemoryAsset] = {}
    target_class_index: dict[str, _CanonicalProgramMemoryAsset] = {}
    if CHECKED_IN_PROGRAM_MEMORY_ASSETS_PATH.exists():
        for row in read_csv_rows(CHECKED_IN_PROGRAM_MEMORY_ASSETS_PATH):
            _register_canonical_program_memory_asset_identity(
                asset_index,
                target_class_index,
                _build_canonical_program_memory_asset(
                    asset_id=clean_text(row.get("asset_id")),
                    molecule=clean_text(row.get("molecule")),
                    target=clean_text(row.get("target")),
                    target_symbols=parse_string_list(row.get("target_symbols_json")),
                    target_class=clean_text(row.get("target_class")),
                    mechanism=clean_text(row.get("mechanism")),
                    modality=clean_text(row.get("modality")),
                    asset_lineage_id=clean_text(row.get("asset_lineage_id")),
                    asset_aliases=parse_string_list(row.get("asset_aliases_json")),
                    target_class_lineage_id=clean_text(
                        row.get("target_class_lineage_id")
                    ),
                    target_class_aliases=parse_string_list(
                        row.get("target_class_aliases_json")
                    ),
                ),
            )

    if CHECKED_IN_PROGRAM_MEMORY_UNIVERSE_PATH.exists():
        universe_rows = read_csv_rows(CHECKED_IN_PROGRAM_MEMORY_UNIVERSE_PATH)
        universe_rows_by_id = {
            clean_text(row.get("program_universe_id")): row for row in universe_rows
        }
        for row in universe_rows:
            canonical_row = row
            duplicate_of_program_universe_id = clean_text(
                row.get("duplicate_of_program_universe_id")
            )
            if duplicate_of_program_universe_id:
                canonical_row = universe_rows_by_id.get(
                    duplicate_of_program_universe_id,
                    row,
                )
            _register_canonical_program_memory_asset_identity(
                asset_index,
                target_class_index,
                _build_canonical_program_memory_asset(
                    asset_id=clean_text(canonical_row.get("asset_id")),
                    molecule=clean_text(canonical_row.get("asset_name")),
                    target=clean_text(canonical_row.get("target")),
                    target_symbols=parse_string_list(
                        canonical_row.get("target_symbols_json")
                    ),
                    target_class=clean_text(canonical_row.get("target_class")),
                    mechanism=clean_text(canonical_row.get("mechanism")),
                    modality=clean_text(canonical_row.get("modality")),
                    asset_lineage_id=clean_text(canonical_row.get("asset_lineage_id")),
                    asset_aliases=_merge_alias_values(
                        parse_string_list(canonical_row.get("asset_aliases_json")),
                        clean_text(row.get("asset_id")),
                        clean_text(row.get("asset_name")),
                        parse_string_list(row.get("asset_aliases_json")),
                    ),
                    target_class_lineage_id=clean_text(
                        canonical_row.get("target_class_lineage_id")
                    ),
                    target_class_aliases=_merge_alias_values(
                        parse_string_list(
                            canonical_row.get("target_class_aliases_json")
                        ),
                        parse_string_list(row.get("target_class_aliases_json")),
                    ),
                ),
            )
    return asset_index, target_class_index


def _get_checked_in_program_memory_identity_catalog() -> tuple[
    dict[str, _CanonicalProgramMemoryAsset],
    dict[str, _CanonicalProgramMemoryAsset],
]:
    return _load_checked_in_program_memory_identity_catalog(
        _checked_in_program_memory_identity_catalog_revision()
    )


def _build_canonical_program_memory_asset(
    *,
    asset_id: str,
    molecule: str,
    target: str,
    target_symbols: tuple[str, ...],
    target_class: str,
    mechanism: str,
    modality: str,
    asset_lineage_id: str,
    asset_aliases: tuple[str, ...],
    target_class_lineage_id: str,
    target_class_aliases: tuple[str, ...],
) -> _CanonicalProgramMemoryAsset:
    cleaned_target = clean_text(target)
    cleaned_asset_id = clean_text(asset_id)
    cleaned_molecule = clean_text(molecule)
    cleaned_target_class = clean_text(target_class)
    return _CanonicalProgramMemoryAsset(
        asset_id=cleaned_asset_id,
        molecule=cleaned_molecule or cleaned_asset_id,
        target=cleaned_target,
        target_symbols=target_symbols or split_target_symbols(cleaned_target),
        target_class=cleaned_target_class,
        mechanism=clean_text(mechanism),
        modality=clean_text(modality),
        asset_lineage_id=clean_text(asset_lineage_id)
        or default_asset_lineage_id(cleaned_asset_id, cleaned_molecule),
        asset_aliases=asset_aliases,
        target_class_lineage_id=clean_text(target_class_lineage_id)
        or default_target_class_lineage_id(cleaned_target_class),
        target_class_aliases=target_class_aliases,
    )


def _register_canonical_program_memory_asset_identity(
    asset_index: dict[str, _CanonicalProgramMemoryAsset],
    target_class_index: dict[str, _CanonicalProgramMemoryAsset],
    asset: _CanonicalProgramMemoryAsset,
) -> None:
    for candidate in (
        asset.asset_id,
        asset.molecule,
        asset.asset_lineage_id,
        *asset.asset_aliases,
    ):
        key = _normalize_identity_key(candidate)
        if key:
            asset_index.setdefault(key, asset)
    for candidate in (
        asset.target_class,
        asset.target_class_lineage_id,
        *asset.target_class_aliases,
    ):
        key = _normalize_identity_key(candidate)
        if key:
            target_class_index.setdefault(key, asset)


def canonicalize_program_memory_asset_identity(
    asset: ProgramMemoryAsset,
) -> ProgramMemoryAsset:
    asset_index, target_class_index = _get_checked_in_program_memory_identity_catalog()
    asset_match: _CanonicalProgramMemoryAsset | None = None
    for candidate in (
        asset.asset_lineage_id,
        asset.asset_id,
        asset.molecule,
        *asset.asset_aliases,
    ):
        key = _normalize_identity_key(candidate)
        if not key:
            continue
        asset_match = asset_index.get(key)
        if asset_match is not None:
            break

    if asset_match is not None:
        asset_aliases = _filter_alias_values(
            _merge_alias_values(
                asset_match.asset_aliases,
                asset.asset_aliases,
                asset.asset_id,
                asset.molecule,
            ),
            canonical_values=(
                asset_match.asset_id,
                asset_match.molecule,
                asset_match.asset_lineage_id,
            ),
        )
        target_class_aliases = _filter_alias_values(
            _merge_alias_values(
                asset_match.target_class_aliases,
                asset.target_class_aliases,
                asset.target_class,
            ),
            canonical_values=(
                asset_match.target_class,
                asset_match.target_class_lineage_id,
            ),
        )
        return ProgramMemoryAsset(
            asset_id=asset_match.asset_id,
            molecule=asset_match.molecule,
            target=asset_match.target,
            target_symbols=asset_match.target_symbols,
            target_class=asset_match.target_class,
            mechanism=asset_match.mechanism,
            modality=asset_match.modality,
            asset_lineage_id=asset_match.asset_lineage_id,
            asset_aliases=asset_aliases,
            target_class_lineage_id=asset_match.target_class_lineage_id,
            target_class_aliases=target_class_aliases,
        )

    target_class_match: _CanonicalProgramMemoryAsset | None = None
    for candidate in (
        asset.target_class_lineage_id,
        asset.target_class,
        *asset.target_class_aliases,
    ):
        key = _normalize_identity_key(candidate)
        if not key:
            continue
        target_class_match = target_class_index.get(key)
        if target_class_match is not None:
            break

    if target_class_match is None:
        return asset

    target_class_aliases = _filter_alias_values(
        _merge_alias_values(
            target_class_match.target_class_aliases,
            asset.target_class_aliases,
            asset.target_class,
        ),
        canonical_values=(
            target_class_match.target_class,
            target_class_match.target_class_lineage_id,
        ),
    )
    return replace(
        asset,
        target_class=target_class_match.target_class,
        target_class_lineage_id=target_class_match.target_class_lineage_id,
        target_class_aliases=target_class_aliases,
    )


def resolve_program_memory_asset_identifier(value: str) -> str | None:
    normalized = _normalize_identity_key(value)
    if not normalized:
        return None
    asset_index, _ = _get_checked_in_program_memory_identity_catalog()
    matched_asset = asset_index.get(normalized)
    if matched_asset is None:
        return None
    return matched_asset.asset_id


def canonicalize_program_memory_event_identity(
    asset: ProgramMemoryAsset,
    event: ProgramMemoryEvent,
) -> tuple[ProgramMemoryAsset, ProgramMemoryEvent]:
    canonical_asset = canonicalize_program_memory_asset_identity(asset)
    if event.asset_id == canonical_asset.asset_id:
        return canonical_asset, event
    return canonical_asset, replace(event, asset_id=canonical_asset.asset_id)


def parse_program_memory_asset(payload: Mapping[str, Any]) -> ProgramMemoryAsset:
    target = _require_text(payload, "target", context="program memory asset")
    target_symbols = _parse_string_tuple(
        payload.get("target_symbols", payload.get("target_symbols_json"))
    ) or split_target_symbols(target)
    asset = ProgramMemoryAsset(
        asset_id=_require_text(payload, "asset_id", context="program memory asset"),
        molecule=_require_text(payload, "molecule", context="program memory asset"),
        target=target,
        target_symbols=target_symbols,
        target_class=_require_text(
            payload,
            "target_class",
            context="program memory asset",
        ),
        mechanism=_require_text(payload, "mechanism", context="program memory asset"),
        modality=_require_text(payload, "modality", context="program memory asset"),
        asset_lineage_id=_clean_value(payload.get("asset_lineage_id"))
        or default_asset_lineage_id(
            _clean_value(payload.get("asset_id")),
            _clean_value(payload.get("molecule")),
        ),
        asset_aliases=_parse_string_tuple(
            payload.get("asset_aliases", payload.get("asset_aliases_json"))
        ),
        target_class_lineage_id=_clean_value(payload.get("target_class_lineage_id"))
        or default_target_class_lineage_id(
            _clean_value(payload.get("target_class"))
        ),
        target_class_aliases=_parse_string_tuple(
            payload.get(
                "target_class_aliases",
                payload.get("target_class_aliases_json"),
            )
        ),
    )
    return canonicalize_program_memory_asset_identity(asset)


def parse_program_memory_event(payload: Mapping[str, Any]) -> ProgramMemoryEvent:
    return ProgramMemoryEvent(
        event_id=_require_text(payload, "event_id", context="program memory event"),
        asset_id=_require_text(payload, "asset_id", context="program memory event"),
        sponsor=_require_text(payload, "sponsor", context="program memory event"),
        population=_require_text(payload, "population", context="program memory event"),
        domain=_require_text(payload, "domain", context="program memory event"),
        mono_or_adjunct=_require_text(
            payload,
            "mono_or_adjunct",
            context="program memory event",
        ),
        phase=_require_text(payload, "phase", context="program memory event"),
        event_type=_require_text(payload, "event_type", context="program memory event"),
        event_date=_require_text(payload, "event_date", context="program memory event"),
        primary_outcome_result=_require_text(
            payload,
            "primary_outcome_result",
            context="program memory event",
        ),
        failure_reason_taxonomy=_require_text(
            payload,
            "failure_reason_taxonomy",
            context="program memory event",
        ),
        confidence=_require_text(payload, "confidence", context="program memory event")
        .lower(),
        notes=_clean_value(payload.get("notes")),
        sort_order=_parse_sort_order(payload.get("sort_order"), default=1),
    )


def parse_program_memory_provenance(
    payload: Mapping[str, Any],
) -> ProgramMemoryProvenance:
    return ProgramMemoryProvenance(
        event_id=_require_text(
            payload,
            "event_id",
            context="program memory provenance",
        ),
        source_tier=_require_text(
            payload,
            "source_tier",
            context="program memory provenance",
        ),
        source_url=_require_text(
            payload,
            "source_url",
            context="program memory provenance",
        ),
    )


def parse_program_memory_directionality_hypothesis(
    payload: Mapping[str, Any],
) -> ProgramMemoryDirectionalityHypothesis:
    return ProgramMemoryDirectionalityHypothesis(
        hypothesis_id=_require_text(
            payload,
            "hypothesis_id",
            context="program memory directionality hypothesis",
        ),
        entity_id=_clean_value(payload.get("entity_id")),
        entity_label=_require_text(
            payload,
            "entity_label",
            context="program memory directionality hypothesis",
        ),
        desired_perturbation_direction=_require_text(
            payload,
            "desired_perturbation_direction",
            context="program memory directionality hypothesis",
        ),
        modality_hypothesis=_require_text(
            payload,
            "modality_hypothesis",
            context="program memory directionality hypothesis",
        ),
        preferred_modalities=_parse_string_tuple(
            payload.get("preferred_modalities", payload.get("preferred_modalities_json"))
        ),
        confidence=_require_text(
            payload,
            "confidence",
            context="program memory directionality hypothesis",
        ).lower(),
        ambiguity=_clean_value(payload.get("ambiguity")),
        evidence_basis=_clean_value(payload.get("evidence_basis")),
        supporting_event_ids=_parse_string_tuple(
            payload.get("supporting_event_ids", payload.get("supporting_event_ids_json"))
            or payload.get("supporting_program_ids_json")
        ),
        contradiction_conditions=_parse_string_tuple(
            payload.get(
                "contradiction_conditions",
                payload.get("contradiction_conditions_json"),
            )
        ),
        falsification_conditions=_parse_string_tuple(
            payload.get(
                "falsification_conditions",
                payload.get("falsification_conditions_json"),
            )
        ),
        open_risks=_parse_string_tuple(
            payload.get("open_risks", payload.get("open_risks_json"))
        ),
        sort_order=_parse_sort_order(payload.get("sort_order"), default=1),
    )


def _asset_to_dict(asset: ProgramMemoryAsset) -> dict[str, object]:
    return {
        "asset_id": asset.asset_id,
        "molecule": asset.molecule,
        "target": asset.target,
        "target_symbols": list(asset.target_symbols),
        "target_class": asset.target_class,
        "mechanism": asset.mechanism,
        "modality": asset.modality,
        "asset_lineage_id": asset.asset_lineage_id,
        "asset_aliases": list(asset.asset_aliases),
        "target_class_lineage_id": asset.target_class_lineage_id,
        "target_class_aliases": list(asset.target_class_aliases),
    }


def _event_to_dict(event: ProgramMemoryEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "asset_id": event.asset_id,
        "sponsor": event.sponsor,
        "population": event.population,
        "domain": event.domain,
        "mono_or_adjunct": event.mono_or_adjunct,
        "phase": event.phase,
        "event_type": event.event_type,
        "event_date": event.event_date,
        "primary_outcome_result": event.primary_outcome_result,
        "failure_reason_taxonomy": event.failure_reason_taxonomy,
        "confidence": event.confidence,
        "notes": event.notes,
        "sort_order": event.sort_order,
    }


def _provenance_to_dict(provenance: ProgramMemoryProvenance) -> dict[str, str]:
    return {
        "event_id": provenance.event_id,
        "source_tier": provenance.source_tier,
        "source_url": provenance.source_url,
    }


def _directionality_hypothesis_to_dict(
    hypothesis: ProgramMemoryDirectionalityHypothesis,
) -> dict[str, object]:
    return {
        "hypothesis_id": hypothesis.hypothesis_id,
        "entity_id": hypothesis.entity_id,
        "entity_label": hypothesis.entity_label,
        "desired_perturbation_direction": hypothesis.desired_perturbation_direction,
        "modality_hypothesis": hypothesis.modality_hypothesis,
        "preferred_modalities": list(hypothesis.preferred_modalities),
        "confidence": hypothesis.confidence,
        "ambiguity": hypothesis.ambiguity,
        "evidence_basis": hypothesis.evidence_basis,
        "supporting_event_ids": list(hypothesis.supporting_event_ids),
        "contradiction_conditions": list(hypothesis.contradiction_conditions),
        "falsification_conditions": list(hypothesis.falsification_conditions),
        "open_risks": list(hypothesis.open_risks),
        "sort_order": hypothesis.sort_order,
    }


@dataclass(frozen=True)
class ProgramMemorySourceDocument:
    source_document_id: str
    title: str
    source_tier: str
    source_url: str
    publisher: str
    published_at: str
    evidence_excerpt: str
    notes: str

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> ProgramMemorySourceDocument:
        return cls(
            source_document_id=_require_text(
                payload,
                "source_document_id",
                context="program memory source document",
            ),
            title=_clean_value(payload.get("title")),
            source_tier=_require_text(
                payload,
                "source_tier",
                context="program memory source document",
            ),
            source_url=_require_text(
                payload,
                "source_url",
                context="program memory source document",
            ),
            publisher=_clean_value(payload.get("publisher")),
            published_at=_clean_value(payload.get("published_at")),
            evidence_excerpt=_clean_value(payload.get("evidence_excerpt")),
            notes=_clean_value(payload.get("notes")),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "source_document_id": self.source_document_id,
            "title": self.title,
            "source_tier": self.source_tier,
            "source_url": self.source_url,
            "publisher": self.publisher,
            "published_at": self.published_at,
            "evidence_excerpt": self.evidence_excerpt,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ProgramMemorySuggestion:
    suggestion_id: str
    suggestion_kind: str
    source_document_id: str
    extractor_name: str
    extractor_version: str
    machine_confidence: str
    rationale: str
    evidence_excerpt: str
    asset: ProgramMemoryAsset | None = None
    event: ProgramMemoryEvent | None = None
    provenance: ProgramMemoryProvenance | None = None
    directionality_hypothesis: ProgramMemoryDirectionalityHypothesis | None = None

    def __post_init__(self) -> None:
        if self.suggestion_kind not in PROGRAM_MEMORY_SUGGESTION_KINDS:
            raise ValueError(
                f"unsupported program memory suggestion_kind {self.suggestion_kind!r}"
            )
        if not self.suggestion_id:
            raise ValueError("program memory suggestions require suggestion_id")
        if not self.source_document_id:
            raise ValueError("program memory suggestions require source_document_id")
        if not self.extractor_name:
            raise ValueError("program memory suggestions require extractor_name")
        if self.suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION:
            if self.asset is None or self.event is None or self.provenance is None:
                raise ValueError(
                    "event suggestions require asset, event, and provenance payloads"
                )
            if self.directionality_hypothesis is not None:
                raise ValueError(
                    "event suggestions cannot carry directionality hypothesis payloads"
                )
            if self.asset.asset_id != self.event.asset_id:
                raise ValueError(
                    "event suggestions require matching asset.asset_id and event.asset_id"
                )
            if self.event.event_id != self.provenance.event_id:
                raise ValueError(
                    "event suggestions require matching event.event_id and provenance.event_id"
                )
            return
        if self.directionality_hypothesis is None:
            raise ValueError(
                "directionality suggestions require directionality_hypothesis payloads"
            )
        if any(item is not None for item in (self.asset, self.event, self.provenance)):
            raise ValueError(
                "directionality suggestions cannot carry asset, event, or provenance payloads"
            )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ProgramMemorySuggestion:
        suggestion_kind = _require_text(
            payload,
            "suggestion_kind",
            context="program memory suggestion",
        )
        asset = None
        event = None
        provenance = None
        directionality_hypothesis = None
        if suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION:
            asset = parse_program_memory_asset(
                _require_mapping(
                    payload.get("asset"),
                    field_name="asset",
                    context="event suggestion",
                )
            )
            event = parse_program_memory_event(
                _require_mapping(
                    payload.get("event"),
                    field_name="event",
                    context="event suggestion",
                )
            )
            provenance = parse_program_memory_provenance(
                _require_mapping(
                    payload.get("provenance"),
                    field_name="provenance",
                    context="event suggestion",
                )
            )
            asset, event = canonicalize_program_memory_event_identity(asset, event)
        elif suggestion_kind == PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION:
            directionality_hypothesis = parse_program_memory_directionality_hypothesis(
                _require_mapping(
                    payload.get("directionality_hypothesis"),
                    field_name="directionality_hypothesis",
                    context="directionality suggestion",
                )
            )
        return cls(
            suggestion_id=_require_text(
                payload,
                "suggestion_id",
                context="program memory suggestion",
            ),
            suggestion_kind=suggestion_kind,
            source_document_id=_require_text(
                payload,
                "source_document_id",
                context="program memory suggestion",
            ),
            extractor_name=_require_text(
                payload,
                "extractor_name",
                context="program memory suggestion",
            ),
            extractor_version=_clean_value(payload.get("extractor_version")),
            machine_confidence=(
                _clean_value(payload.get("machine_confidence")).lower() or "unscored"
            ),
            rationale=_clean_value(payload.get("rationale")),
            evidence_excerpt=_clean_value(payload.get("evidence_excerpt")),
            asset=asset,
            event=event,
            provenance=provenance,
            directionality_hypothesis=directionality_hypothesis,
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "suggestion_id": self.suggestion_id,
            "suggestion_kind": self.suggestion_kind,
            "source_document_id": self.source_document_id,
            "extractor_name": self.extractor_name,
            "extractor_version": self.extractor_version,
            "machine_confidence": self.machine_confidence,
            "rationale": self.rationale,
            "evidence_excerpt": self.evidence_excerpt,
        }
        if self.asset is not None:
            payload["asset"] = _asset_to_dict(self.asset)
        if self.event is not None:
            payload["event"] = _event_to_dict(self.event)
        if self.provenance is not None:
            payload["provenance"] = _provenance_to_dict(self.provenance)
        if self.directionality_hypothesis is not None:
            payload["directionality_hypothesis"] = _directionality_hypothesis_to_dict(
                self.directionality_hypothesis
            )
        return payload

    @property
    def candidate_identifier(self) -> str:
        if self.event is not None:
            return self.event.event_id
        if self.directionality_hypothesis is not None:
            return self.directionality_hypothesis.hypothesis_id
        raise ValueError("program memory suggestion is missing a candidate identifier")


def _require_mapping(
    value: Any,
    *,
    field_name: str,
    context: str,
) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError(f"{context} requires mapping payload {field_name}")


def parse_source_documents(
    payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
) -> tuple[ProgramMemorySourceDocument, ...]:
    return tuple(ProgramMemorySourceDocument.from_dict(payload) for payload in payloads)


def parse_program_memory_suggestions(
    payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
) -> tuple[ProgramMemorySuggestion, ...]:
    return tuple(ProgramMemorySuggestion.from_dict(payload) for payload in payloads)
