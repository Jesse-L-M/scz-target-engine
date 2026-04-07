"""Discover and load observatory artifacts from the data directory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.benchmark_leaderboard import (
    BenchmarkLeaderboardPayload,
    BenchmarkReportCardPayload,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.io import read_csv_rows, read_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DEFAULT_PUBLIC_SLICES_CATALOG = (
    DEFAULT_DATA_DIR / "benchmark" / "public_slices" / "catalog.json"
)
DEFAULT_GENERATED_DIR = DEFAULT_DATA_DIR / "benchmark" / "generated"
DEFAULT_HYPOTHESIS_PACKETS_FILE = (
    REPO_ROOT / "examples" / "v0" / "output" / "hypothesis_packets_v1.json"
)
DEFAULT_RESCUE_TASK_REGISTRY = (
    DEFAULT_DATA_DIR / "curated" / "rescue_tasks" / "rescue_task_registry.csv"
)


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return value


def _require_list(value: object, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a JSON array")
    return value


@dataclass(frozen=True)
class PublicSliceSummary:
    slice_id: str
    as_of_date: str
    included_sources: tuple[str, ...]
    excluded_source_names: tuple[str, ...]
    slice_dir: str
    notes: str = ""


@dataclass(frozen=True)
class PublicSliceCatalog:
    benchmark_suite_id: str
    benchmark_task_id: str
    slices: tuple[PublicSliceSummary, ...]
    catalog_path: str


@dataclass(frozen=True)
class GeneratedPayloadIndex:
    report_card_files: tuple[Path, ...]
    leaderboard_files: tuple[Path, ...]
    snapshot_manifest_files: tuple[Path, ...]
    source_dir: Path


def load_public_slice_catalog(
    catalog_path: Path | None = None,
) -> PublicSliceCatalog | None:
    resolved = (catalog_path or DEFAULT_PUBLIC_SLICES_CATALOG).resolve()
    if not resolved.exists():
        return None
    payload = _require_mapping(read_json(resolved), "public slice catalog")
    slices: list[PublicSliceSummary] = []
    for index, entry_value in enumerate(_require_list(payload.get("slices", []), "slices")):
        entry = _require_mapping(entry_value, f"slices[{index}]")
        excluded_names = tuple(
            _require_text(
                _require_mapping(exc, f"slices[{index}].excluded_sources[{exc_index}]").get(
                    "source_name"
                ),
                f"slices[{index}].excluded_sources[{exc_index}].source_name",
            )
            for exc_index, exc in enumerate(
                _require_list(
                    entry.get("excluded_sources", []),
                    f"slices[{index}].excluded_sources",
                )
            )
        )
        included_sources = tuple(
            _require_text(
                source_name,
                f"slices[{index}].included_sources[{source_index}]",
            )
            for source_index, source_name in enumerate(
                _require_list(
                    entry.get("included_sources", []),
                    f"slices[{index}].included_sources",
                )
            )
        )
        slices.append(
            PublicSliceSummary(
                slice_id=_require_text(entry.get("slice_id"), f"slices[{index}].slice_id"),
                as_of_date=_require_text(
                    entry.get("as_of_date"),
                    f"slices[{index}].as_of_date",
                ),
                included_sources=included_sources,
                excluded_source_names=excluded_names,
                slice_dir=_require_text(entry.get("slice_dir"), f"slices[{index}].slice_dir"),
                notes=str(entry.get("notes", "")),
            )
        )
    return PublicSliceCatalog(
        benchmark_suite_id=_require_text(
            payload.get("benchmark_suite_id"),
            "benchmark_suite_id",
        ),
        benchmark_task_id=_require_text(
            payload.get("benchmark_task_id"),
            "benchmark_task_id",
        ),
        slices=tuple(slices),
        catalog_path=str(resolved),
    )


def discover_generated_payloads(
    generated_dir: Path | None = None,
) -> GeneratedPayloadIndex:
    resolved = (generated_dir or DEFAULT_GENERATED_DIR).resolve()
    if not resolved.exists():
        return GeneratedPayloadIndex(
            report_card_files=(),
            leaderboard_files=(),
            snapshot_manifest_files=(),
            source_dir=resolved,
        )
    report_cards = tuple(sorted(resolved.rglob("report_cards/**/*.json")))
    leaderboards = tuple(sorted(resolved.rglob("leaderboards/**/*.json")))
    manifests = tuple(sorted(resolved.rglob("snapshot_manifest.json")))
    return GeneratedPayloadIndex(
        report_card_files=report_cards,
        leaderboard_files=leaderboards,
        snapshot_manifest_files=manifests,
        source_dir=resolved,
    )


def load_report_cards(
    report_card_files: tuple[Path, ...],
) -> tuple[BenchmarkReportCardPayload, ...]:
    return tuple(
        read_benchmark_report_card_payload(path) for path in report_card_files
    )


def load_leaderboards(
    leaderboard_files: tuple[Path, ...],
) -> tuple[BenchmarkLeaderboardPayload, ...]:
    return tuple(
        read_benchmark_leaderboard_payload(path) for path in leaderboard_files
    )


def load_hypothesis_packets(
    packets_file: Path | None = None,
) -> dict[str, object] | None:
    """Load a hypothesis packets payload from a JSON file.

    Falls back to the default checked-in example if no path is given.
    """
    resolved = (packets_file or DEFAULT_HYPOTHESIS_PACKETS_FILE).resolve()
    if not resolved.exists():
        return None
    payload = read_json(resolved)
    if not isinstance(payload, dict):
        return None
    return payload


def load_rescue_augmented_packets(
    packets_file: Path | None = None,
) -> dict[str, object] | None:
    """Load a rescue-augmented hypothesis packets payload.

    Returns None if the file does not exist or does not contain the
    rescue_augmentation top-level key.  This prevents accidentally
    treating a plain hypothesis packets file as rescue-augmented.
    """
    if packets_file is not None:
        resolved = packets_file.resolve()
        if not resolved.exists():
            return None
        payload = read_json(resolved)
        if not isinstance(payload, dict):
            return None
        if "rescue_augmentation" not in payload:
            return None
        return payload
    # No default path for rescue-augmented packets; must be explicit.
    return None


def load_rescue_task_registry(
    registry_path: Path | None = None,
) -> list[dict[str, str]]:
    """Load the rescue task registry CSV."""
    resolved = (registry_path or DEFAULT_RESCUE_TASK_REGISTRY).resolve()
    if not resolved.exists():
        return []
    return read_csv_rows(resolved)


__all__ = [
    "DEFAULT_DATA_DIR",
    "DEFAULT_GENERATED_DIR",
    "DEFAULT_HYPOTHESIS_PACKETS_FILE",
    "DEFAULT_PUBLIC_SLICES_CATALOG",
    "DEFAULT_RESCUE_TASK_REGISTRY",
    "GeneratedPayloadIndex",
    "PublicSliceCatalog",
    "PublicSliceSummary",
    "REPO_ROOT",
    "discover_generated_payloads",
    "load_hypothesis_packets",
    "load_leaderboards",
    "load_public_slice_catalog",
    "load_report_cards",
    "load_rescue_augmented_packets",
    "load_rescue_task_registry",
]
