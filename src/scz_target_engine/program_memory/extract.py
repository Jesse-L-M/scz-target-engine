from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from scz_target_engine.program_memory._helpers import (
    clean_text,
    parse_int,
    parse_string_list,
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


def parse_program_memory_asset(payload: Mapping[str, Any]) -> ProgramMemoryAsset:
    target = _require_text(payload, "target", context="program memory asset")
    target_symbols = _parse_string_tuple(
        payload.get("target_symbols", payload.get("target_symbols_json"))
    ) or split_target_symbols(target)
    return ProgramMemoryAsset(
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
    )


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
