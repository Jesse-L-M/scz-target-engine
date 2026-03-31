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
from scz_target_engine.io import read_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DEFAULT_PUBLIC_SLICES_CATALOG = (
    DEFAULT_DATA_DIR / "benchmark" / "public_slices" / "catalog.json"
)
DEFAULT_GENERATED_DIR = DEFAULT_DATA_DIR / "benchmark" / "generated"


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
    payload = read_json(resolved)
    if not isinstance(payload, dict):
        return None
    slices: list[PublicSliceSummary] = []
    for entry in payload.get("slices", []):
        excluded_names = tuple(
            str(exc.get("source_name", ""))
            for exc in entry.get("excluded_sources", [])
        )
        slices.append(
            PublicSliceSummary(
                slice_id=str(entry["slice_id"]),
                as_of_date=str(entry["as_of_date"]),
                included_sources=tuple(
                    str(s) for s in entry.get("included_sources", [])
                ),
                excluded_source_names=excluded_names,
                slice_dir=str(entry.get("slice_dir", "")),
                notes=str(entry.get("notes", "")),
            )
        )
    return PublicSliceCatalog(
        benchmark_suite_id=str(payload.get("benchmark_suite_id", "")),
        benchmark_task_id=str(payload.get("benchmark_task_id", "")),
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


__all__ = [
    "DEFAULT_DATA_DIR",
    "DEFAULT_GENERATED_DIR",
    "DEFAULT_PUBLIC_SLICES_CATALOG",
    "GeneratedPayloadIndex",
    "PublicSliceCatalog",
    "PublicSliceSummary",
    "discover_generated_payloads",
    "load_leaderboards",
    "load_public_slice_catalog",
    "load_report_cards",
]
