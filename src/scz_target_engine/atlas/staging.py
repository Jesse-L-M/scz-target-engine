from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from scz_target_engine.atlas.contracts import (
    ATLAS_SOURCE_CONTRACT_VERSION,
    AtlasSourceContract,
)
from scz_target_engine.io import write_json


_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    slug = _SLUG_PATTERN.sub("-", value.strip().lower()).strip("-")
    return slug or "default"


def resolve_materialized_at(materialized_at: str | None) -> str:
    if materialized_at:
        return materialized_at
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


@dataclass(frozen=True)
class StagedRawArtifact:
    artifact_name: str
    path: str
    relative_path: str
    media_type: str
    sha256: str
    size_bytes: int
    extra_metadata: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class RawArtifactRecorder:
    contract: AtlasSourceContract
    raw_root: Path
    dataset_slug: str
    materialized_at: str | None = None
    artifacts: list[StagedRawArtifact] = field(default_factory=list)
    stage_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        resolved_root = self.raw_root.resolve()
        resolved_materialized_at = resolve_materialized_at(self.materialized_at)
        object.__setattr__(self, "raw_root", resolved_root)
        object.__setattr__(self, "materialized_at", resolved_materialized_at)
        object.__setattr__(
            self,
            "stage_dir",
            (
                resolved_root
                / self.contract.source_name
                / slugify(self.dataset_slug)
                / slugify(resolved_materialized_at)
            ).resolve(),
        )

    def _record_artifact(
        self,
        artifact_name: str,
        payload: bytes,
        media_type: str,
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        artifact_path = self.stage_dir / artifact_name
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(payload)
        artifact = StagedRawArtifact(
            artifact_name=artifact_name,
            path=str(artifact_path),
            relative_path=str(artifact_path.relative_to(self.raw_root)),
            media_type=media_type,
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            extra_metadata=extra_metadata or {},
        )
        self.artifacts.append(artifact)
        return artifact.to_dict()

    def stage_bytes(
        self,
        artifact_name: str,
        payload: bytes,
        media_type: str,
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self._record_artifact(
            artifact_name=artifact_name,
            payload=payload,
            media_type=media_type,
            extra_metadata=extra_metadata,
        )

    def stage_text(
        self,
        artifact_name: str,
        payload: str,
        media_type: str = "text/plain; charset=utf-8",
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self._record_artifact(
            artifact_name=artifact_name,
            payload=payload.encode("utf-8"),
            media_type=media_type,
            extra_metadata=extra_metadata,
        )

    def stage_json(
        self,
        artifact_name: str,
        payload: object,
        extra_metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.stage_text(
            artifact_name=artifact_name,
            payload=json.dumps(payload, indent=2, sort_keys=True) + "\n",
            media_type="application/json",
            extra_metadata=extra_metadata,
        )

    def write_manifest(
        self,
        *,
        request_metadata: dict[str, object],
        processed_artifacts: list[Path],
        status: str,
        upstream_metadata: dict[str, object] | None = None,
        error: str | None = None,
    ) -> Path:
        manifest_file = self.stage_dir / "manifest.json"
        write_json(
            manifest_file,
            {
                "contract_version": ATLAS_SOURCE_CONTRACT_VERSION,
                "source_contract": self.contract.to_dict(),
                "dataset_slug": self.dataset_slug,
                "materialized_at": self.materialized_at,
                "raw_stage_dir": str(self.stage_dir),
                "raw_artifact_count": len(self.artifacts),
                "status": status,
                "request_metadata": request_metadata,
                "processed_artifacts": [
                    {
                        "path": str(path),
                        "exists": path.exists(),
                    }
                    for path in processed_artifacts
                ],
                "artifacts": [artifact.to_dict() for artifact in self.artifacts],
                "upstream_metadata": upstream_metadata,
                "error": error,
            },
        )
        return manifest_file
