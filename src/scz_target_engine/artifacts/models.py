from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _require_explicit_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be an explicit boolean")
    return value


def _require_non_empty_tuple(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must contain at least one value")
    for value in values:
        _require_text(value, field_name)


def _require_relative_path(value: str, field_name: str) -> str:
    path_value = _require_text(value, field_name)
    if Path(path_value).is_absolute():
        raise ValueError(f"{field_name} must be a relative path")
    return path_value


def _require_sha256(value: str, field_name: str) -> str:
    digest = _require_text(value, field_name)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{field_name} must be a lowercase hexadecimal SHA256 digest")
    return digest


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
            required=_require_explicit_bool(payload["required"], "required"),
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
    schema_dir: Path | None = None

    def __post_init__(self) -> None:
        _require_text(self.artifact_name, "artifact_name")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.file_format, "file_format")
        _require_text(self.description, "description")
        _require_non_empty_tuple(self.key_fields, "key_fields")
        if not self.fields:
            raise ValueError("fields must contain at least one field definition")
        if self.schema_dir is not None and not isinstance(self.schema_dir, Path):
            raise ValueError("schema_dir must be a pathlib.Path or None")

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
class ReleaseManifestFileEntry:
    artifact_id: str
    path: str
    sha256: str
    artifact_name: str = ""
    expected_schema_version: str = ""
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.artifact_id, "artifact_id")
        _require_relative_path(self.path, "path")
        _require_sha256(self.sha256, "sha256")
        if not isinstance(self.notes, str):
            raise ValueError("notes must be a string")
        if bool(self.artifact_name) != bool(self.expected_schema_version):
            raise ValueError(
                "artifact_name and expected_schema_version must be provided together"
            )
        if self.artifact_name:
            _require_text(self.artifact_name, "artifact_name")
            _require_text(self.expected_schema_version, "expected_schema_version")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "artifact_id": self.artifact_id,
            "path": self.path,
            "sha256": self.sha256,
        }
        if self.artifact_name:
            payload["artifact_name"] = self.artifact_name
            payload["expected_schema_version"] = self.expected_schema_version
        if self.notes:
            payload["notes"] = self.notes
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ReleaseManifestFileEntry:
        return cls(
            artifact_id=str(payload["artifact_id"]),
            path=str(payload["path"]),
            sha256=str(payload["sha256"]),
            artifact_name=str(payload.get("artifact_name", "")),
            expected_schema_version=str(payload.get("expected_schema_version", "")),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class ReleaseManifest:
    schema_name: str
    schema_version: str
    release_id: str
    release_family: str
    release_version: str
    materialized_at: str
    compatibility_phase: str
    files: tuple[ReleaseManifestFileEntry, ...]
    notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")
        _require_text(self.release_id, "release_id")
        _require_text(self.release_family, "release_family")
        _require_text(self.release_version, "release_version")
        _require_text(self.materialized_at, "materialized_at")
        _require_text(self.compatibility_phase, "compatibility_phase")
        if not self.files:
            raise ValueError("files must contain at least one release manifest entry")
        if not isinstance(self.notes, str):
            raise ValueError("notes must be a string")
        artifact_ids = [entry.artifact_id for entry in self.files]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("release manifest artifact_id values must be unique")
        relative_paths = [entry.path for entry in self.files]
        if len(relative_paths) != len(set(relative_paths)):
            raise ValueError("release manifest file paths must be unique")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "release_family": self.release_family,
            "release_version": self.release_version,
            "materialized_at": self.materialized_at,
            "compatibility_phase": self.compatibility_phase,
            "files": [entry.to_dict() for entry in self.files],
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ReleaseManifest:
        files_payload = payload["files"]
        if not isinstance(files_payload, list):
            raise ValueError("files must be a list")
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            release_id=str(payload["release_id"]),
            release_family=str(payload["release_family"]),
            release_version=str(payload["release_version"]),
            materialized_at=str(payload["materialized_at"]),
            compatibility_phase=str(payload["compatibility_phase"]),
            files=tuple(
                ReleaseManifestFileEntry.from_dict(item) for item in files_payload
            ),
            notes=str(payload.get("notes", "")),
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
