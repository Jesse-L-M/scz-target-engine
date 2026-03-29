from pathlib import Path

import pytest

from scz_target_engine.benchmark_snapshots import (
    SourceArchiveDescriptor,
    SnapshotBuildRequest,
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)


def test_build_benchmark_snapshot_manifest_materializes_fixture_sources(
    tmp_path: Path,
) -> None:
    manifest = build_benchmark_snapshot_manifest(
        load_snapshot_build_request(FIXTURE_DIR / "snapshot_request.json"),
        load_source_archive_descriptors(FIXTURE_DIR / "source_archives.json"),
        materialized_at="2026-03-28",
    )

    output_file = tmp_path / "snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=output_file,
        materialized_at="2026-03-28",
    )
    restored = read_benchmark_snapshot_manifest(output_file)

    assert restored == manifest
    included_sources = {
        source_snapshot.source_name
        for source_snapshot in manifest.source_snapshots
        if source_snapshot.included
    }
    assert included_sources == {"PGC", "Open Targets", "PsychENCODE"}

    exclusions = {
        source_snapshot.source_name: source_snapshot.exclusion_reason
        for source_snapshot in manifest.source_snapshots
        if not source_snapshot.included
    }
    assert "no archived release descriptor available" in exclusions["SCHEMA"]
    assert "latest archived release 2024-07-15 is after requested cutoff 2024-06-30" in exclusions["ChEMBL"]


def test_snapshot_builder_excludes_missing_archive_file_explicitly(tmp_path: Path) -> None:
    broken_index = tmp_path / "broken_source_archives.json"
    broken_index.write_text(
        (
            "{\n"
            '  "archives": [\n'
            "    {\n"
            '      "source_name": "PGC",\n'
            '      "source_version": "scz2022_fixture",\n'
            '      "archive_file": "missing.csv",\n'
            '      "archive_format": "csv",\n'
            '      "allowed_data_through": "2024-06-15",\n'
            '      "evidence_frozen_at": "2024-06-15",\n'
            '      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"\n'
            "    }\n"
            "  ]\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    manifest = build_benchmark_snapshot_manifest(
        load_snapshot_build_request(FIXTURE_DIR / "snapshot_request.json"),
        load_source_archive_descriptors(broken_index),
        materialized_at="2026-03-28",
    )

    pgc_snapshot = next(
        source_snapshot
        for source_snapshot in manifest.source_snapshots
        if source_snapshot.source_name == "PGC"
    )
    assert pgc_snapshot.included is False
    assert "archive file missing" in pgc_snapshot.exclusion_reason


def test_snapshot_builder_rejects_ambiguous_same_date_descriptors() -> None:
    pgc_archive = (
        FIXTURE_DIR / "archives" / "pgc" / "scz2022_fixture.csv"
    ).resolve()
    manifest_request = SnapshotBuildRequest(
        snapshot_id="scz_fixture_2024_06_30",
        cohort_id="scz_fixture_small",
        benchmark_question_id="scz_translational_ranking_v1",
        as_of_date="2024-06-30",
        outcome_observation_closed_at="2029-06-30",
        entity_types=("gene",),
        baseline_ids=("pgc_only",),
    )

    with pytest.raises(
        ValueError,
        match="PGC has multiple eligible archive descriptors",
    ):
        build_benchmark_snapshot_manifest(
            manifest_request,
            (
                SourceArchiveDescriptor(
                    source_name="PGC",
                    source_version="v9",
                    archive_file=str(pgc_archive),
                    archive_format="csv",
                    allowed_data_through="2024-06-15",
                    evidence_frozen_at="2024-06-15",
                    sha256="6f471ddec2b00b0ce76c3a5b547c022f4c5ae39b77be6ff437d5b2b1c26d9403",
                ),
                SourceArchiveDescriptor(
                    source_name="PGC",
                    source_version="v10",
                    archive_file=str(pgc_archive),
                    archive_format="csv",
                    allowed_data_through="2024-06-15",
                    evidence_frozen_at="2024-06-15",
                    sha256="6f471ddec2b00b0ce76c3a5b547c022f4c5ae39b77be6ff437d5b2b1c26d9403",
                ),
            ),
            materialized_at="2026-03-28",
        )
