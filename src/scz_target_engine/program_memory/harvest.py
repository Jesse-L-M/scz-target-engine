from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from scz_target_engine.io import read_json, write_json
from scz_target_engine.program_memory._helpers import clean_text
from scz_target_engine.program_memory.extract import (
    PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION,
    PROGRAM_MEMORY_EVENT_SUGGESTION,
    ProgramMemorySourceDocument,
    ProgramMemorySuggestion,
    parse_program_memory_suggestions,
    parse_source_documents,
)


PROGRAM_MEMORY_HARVEST_SCHEMA_VERSION = "program-memory-harvest-v1"


@dataclass(frozen=True)
class ProgramMemoryHarvestBatch:
    schema_version: str
    harvest_id: str
    created_at: str
    harvester: str
    source_documents: tuple[ProgramMemorySourceDocument, ...]
    suggestions: tuple[ProgramMemorySuggestion, ...]

    def __post_init__(self) -> None:
        if self.schema_version != PROGRAM_MEMORY_HARVEST_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported program memory harvest schema_version {self.schema_version!r}"
            )
        if not self.harvest_id:
            raise ValueError("program memory harvest batches require harvest_id")
        if not self.harvester:
            raise ValueError("program memory harvest batches require harvester")

        source_document_ids: set[str] = set()
        for source_document in self.source_documents:
            if source_document.source_document_id in source_document_ids:
                raise ValueError(
                    "duplicate program memory source_document_id "
                    f"{source_document.source_document_id!r}"
                )
            source_document_ids.add(source_document.source_document_id)

        suggestion_ids: set[str] = set()
        for suggestion in self.suggestions:
            if suggestion.suggestion_id in suggestion_ids:
                raise ValueError(
                    "duplicate program memory suggestion_id "
                    f"{suggestion.suggestion_id!r}"
                )
            if suggestion.source_document_id not in source_document_ids:
                raise ValueError(
                    "program memory suggestion "
                    f"{suggestion.suggestion_id!r} references unknown source_document_id "
                    f"{suggestion.source_document_id!r}"
                )
            suggestion_ids.add(suggestion.suggestion_id)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ProgramMemoryHarvestBatch:
        source_document_payloads = _read_payload_list(
            payload,
            "source_documents",
            context="program memory harvest batch",
        )
        suggestion_payloads = _read_payload_list(
            payload,
            "suggestions",
            context="program memory harvest batch",
        )
        return cls(
            schema_version=clean_text(str(payload.get("schema_version") or "")),
            harvest_id=_require_text(
                payload,
                "harvest_id",
                context="program memory harvest batch",
            ),
            created_at=clean_text(str(payload.get("created_at") or "")),
            harvester=_require_text(
                payload,
                "harvester",
                context="program memory harvest batch",
            ),
            source_documents=parse_source_documents(source_document_payloads),
            suggestions=parse_program_memory_suggestions(suggestion_payloads),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "harvest_id": self.harvest_id,
            "created_at": self.created_at,
            "harvester": self.harvester,
            "source_documents": [
                source_document.to_dict() for source_document in self.source_documents
            ],
            "suggestions": [suggestion.to_dict() for suggestion in self.suggestions],
        }


def _require_text(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    context: str,
) -> str:
    value = clean_text(str(payload.get(field_name) or ""))
    if not value:
        raise ValueError(f"{context} requires {field_name}")
    return value


def _read_payload_list(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    context: str,
) -> list[Mapping[str, Any]]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise ValueError(f"{context} requires {field_name} list payload")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{context} requires {field_name} items to be objects")
    return list(value)


def build_program_memory_harvest_batch(
    *,
    harvest_id: str,
    harvester: str,
    source_document_payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    suggestion_payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    created_at: str = "",
) -> ProgramMemoryHarvestBatch:
    return ProgramMemoryHarvestBatch(
        schema_version=PROGRAM_MEMORY_HARVEST_SCHEMA_VERSION,
        harvest_id=clean_text(harvest_id),
        created_at=clean_text(created_at),
        harvester=clean_text(harvester),
        source_documents=parse_source_documents(source_document_payloads),
        suggestions=parse_program_memory_suggestions(suggestion_payloads),
    )


def load_program_memory_harvest_batch(path: Path) -> ProgramMemoryHarvestBatch:
    payload = read_json(path)
    if not isinstance(payload, Mapping):
        raise ValueError(f"program memory harvest batch must be a JSON object: {path}")
    return ProgramMemoryHarvestBatch.from_dict(payload)


def write_program_memory_harvest_batch(
    path: Path,
    harvest: ProgramMemoryHarvestBatch,
) -> None:
    write_json(path, harvest.to_dict())


def build_program_memory_harvest_review_rows(
    harvest: ProgramMemoryHarvestBatch,
) -> list[dict[str, str]]:
    source_documents = {
        source_document.source_document_id: source_document
        for source_document in harvest.source_documents
    }
    rows: list[dict[str, str]] = []
    for suggestion in harvest.suggestions:
        source_document = source_documents[suggestion.source_document_id]
        rows.append(
            {
                "suggestion_id": suggestion.suggestion_id,
                "suggestion_kind": suggestion.suggestion_kind,
                "candidate_identifier": suggestion.candidate_identifier,
                "machine_confidence": suggestion.machine_confidence,
                "extractor_name": suggestion.extractor_name,
                "source_document_id": source_document.source_document_id,
                "source_tier": source_document.source_tier,
                "source_url": source_document.source_url,
                "needs_human_adjudication": "true",
                "proposed_record_type": (
                    "event"
                    if suggestion.suggestion_kind == PROGRAM_MEMORY_EVENT_SUGGESTION
                    else PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION
                ),
            }
        )
    return rows
