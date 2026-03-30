from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
class ArtifactFieldDefinition:
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
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactFieldDefinition:
        return cls(
            name=str(payload["name"]),
            field_type=str(payload["field_type"]),
            required=bool(payload["required"]),
            description=str(payload["description"]),
        )


@dataclass(frozen=True)
class ArtifactSchemaDefinition:
    artifact_name: str
    schema_version: str
    file_format: str
    description: str
    key_fields: tuple[str, ...]
    fields: tuple[ArtifactFieldDefinition, ...]

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

    @property
    def required_field_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields if field.required)

    @property
    def optional_field_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields if not field.required)

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
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactSchemaDefinition:
        fields = tuple(
            ArtifactFieldDefinition.from_dict(item)
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
class ValidatedArtifact:
    artifact_name: str
    schema: ArtifactSchemaDefinition
    path: Path
    payload: object

    @property
    def schema_version(self) -> str:
        return self.schema.schema_version
