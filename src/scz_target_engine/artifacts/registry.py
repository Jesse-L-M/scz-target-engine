from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from scz_target_engine.artifacts.models import ArtifactSchemaDefinition
from scz_target_engine.io import read_json


DEFAULT_SCHEMA_DIR = (
    Path(__file__).resolve().parents[3] / "schemas" / "artifact_schemas"
)


def load_artifact_schema(path: Path) -> ArtifactSchemaDefinition:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a single artifact schema object")
    return ArtifactSchemaDefinition.from_dict(payload)


def _load_artifact_schemas_from_dir(
    schema_dir: Path,
) -> dict[str, ArtifactSchemaDefinition]:
    if not schema_dir.exists():
        raise FileNotFoundError(f"artifact schema directory does not exist: {schema_dir}")

    schema_files = sorted(schema_dir.glob("*.json"))
    if not schema_files:
        raise ValueError(f"no artifact schema files found in {schema_dir}")

    schemas: dict[str, ArtifactSchemaDefinition] = {}
    for schema_file in schema_files:
        schema = load_artifact_schema(schema_file)
        if schema.artifact_name in schemas:
            raise ValueError(
                "artifact schema directory must not repeat artifact_name "
                f"{schema.artifact_name}"
            )
        schemas[schema.artifact_name] = schema
    return schemas


@lru_cache(maxsize=1)
def _load_default_artifact_schemas() -> dict[str, ArtifactSchemaDefinition]:
    return _load_artifact_schemas_from_dir(DEFAULT_SCHEMA_DIR)


def load_artifact_schemas(
    schema_dir: Path | None = None,
) -> dict[str, ArtifactSchemaDefinition]:
    resolved_dir = DEFAULT_SCHEMA_DIR if schema_dir is None else schema_dir.resolve()
    if resolved_dir == DEFAULT_SCHEMA_DIR:
        return dict(_load_default_artifact_schemas())
    return _load_artifact_schemas_from_dir(resolved_dir)


def get_artifact_schema(
    artifact_name: str,
    *,
    schema_dir: Path | None = None,
) -> ArtifactSchemaDefinition:
    schemas = load_artifact_schemas(schema_dir=schema_dir)
    try:
        return schemas[artifact_name]
    except KeyError as exc:
        known = ", ".join(sorted(schemas))
        raise ValueError(
            f"unknown artifact_name {artifact_name!r}; registered values: {known}"
        ) from exc


def list_artifact_schemas(
    *,
    schema_dir: Path | None = None,
) -> tuple[ArtifactSchemaDefinition, ...]:
    schemas = load_artifact_schemas(schema_dir=schema_dir)
    return tuple(schemas[name] for name in sorted(schemas))
