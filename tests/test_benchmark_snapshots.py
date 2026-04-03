from pathlib import Path

import pytest
from pyarrow import parquet as pq

from scz_target_engine.benchmark_intervention_objects import (
    INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION,
    build_intervention_object_bundle_rows,
    build_intervention_object_public_slice_rows,
    read_intervention_object_feature_bundle,
)
from scz_target_engine.benchmark_snapshots import (
    SourceArchiveDescriptor,
    SnapshotBuildRequest,
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from tests.benchmark_test_support import write_intervention_object_slice_fixture


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
    assert manifest.benchmark_suite_id == "scz_translational_suite"
    assert manifest.benchmark_task_id == "scz_translational_task"
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
    result = materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=tmp_path / "snapshot_manifest_2.json",
        materialized_at="2026-03-28",
    )
    assert result["benchmark_suite_id"] == "scz_translational_suite"
    assert result["benchmark_task_id"] == "scz_translational_task"


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


def test_intervention_object_snapshot_request_requires_pinned_program_history() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "intervention_object snapshot requests must provide "
            "program_universe_file and program_history_events_file"
        ),
    ):
        SnapshotBuildRequest(
            snapshot_id="scz_fixture_2024_06_20",
            cohort_id="scz_fixture_intervention_object",
            benchmark_question_id="scz_translational_ranking_v1",
            benchmark_suite_id="scz_translational_suite",
            benchmark_task_id="scz_translational_task",
            as_of_date="2024-06-20",
            outcome_observation_closed_at="2025-06-30",
            entity_types=("intervention_object",),
            baseline_ids=("v0_current",),
        )


def test_materialize_benchmark_snapshot_manifest_emits_intervention_object_bundle(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    output_file = tmp_path / "snapshot_manifest.json"
    result = materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=output_file,
        materialized_at="2026-04-02",
    )

    assert output_file.exists()
    assert result["intervention_object_feature_bundle"].endswith(
        "intervention_object_feature_bundle.parquet"
    )
    bundle_path = tmp_path / "intervention_object_feature_bundle.parquet"
    assert bundle_path.exists()
    bundle_table = pq.read_table(bundle_path)
    assert (
        bundle_table.schema.metadata[b"schema_version"].decode("utf-8")
        == INTERVENTION_OBJECT_BUNDLE_SCHEMA_VERSION
    )

    bundle_rows = read_intervention_object_feature_bundle(bundle_path)
    bundle_entity_ids = {str(row["entity_id"]) for row in bundle_rows}
    expected_entity_ids = {
        line.split(",")[1]
        for line in public_slice.cohort_members_file.read_text(encoding="utf-8").splitlines()[1:]
        if line
    }
    assert bundle_entity_ids == expected_entity_ids


def test_intervention_object_replay_rewinds_future_stage_only_when_supported(
    tmp_path: Path,
) -> None:
    program_universe_path = tmp_path / "program_universe.csv"
    events_path = tmp_path / "events.csv"
    program_universe_path.write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-phase-progression-phase-3-or-registration,example-asset,Example Asset,asset:example-asset,[],'
            'GENE1,"[""GENE1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_3_or_registration,included,checked_in_event_history,high,'
            '"[""example-phase-2-2024"", ""example-phase-3-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,Example progression row.\n"
        ),
        encoding="utf-8",
    )
    events_path.write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-phase-2-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-01-15,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Phase 2 signal,1\n"
            "example-phase-3-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_3,topline_readout,2024-10-01,"
            "did_not_meet_primary_endpoint,unresolved,medium,Phase 3 miss,2\n"
        ),
        encoding="utf-8",
    )

    cohort_rows, future_outcome_rows = build_intervention_object_public_slice_rows(
        as_of_date="2024-06-20",
        outcome_observation_closed_at="2025-06-30",
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    assert cohort_rows == [
        {
            "entity_type": "intervention_object",
            "entity_id": (
                "asset-example-asset-target-class-example-class-small-molecule-"
                "acute-positive-symptoms-adults-with-schizophrenia-monotherapy-phase-2"
            ),
            "entity_label": "Example Asset | acute positive symptoms | phase_2",
        }
    ]
    assert future_outcome_rows == [
        {
            "entity_type": "intervention_object",
            "entity_id": (
                "asset-example-asset-target-class-example-class-small-molecule-"
                "acute-positive-symptoms-adults-with-schizophrenia-monotherapy-phase-2"
            ),
            "outcome_label": "future_schizophrenia_negative_signal",
            "outcome_date": "2024-10-01",
            "label_source": "program_history_v2",
            "label_notes": "event_id=example-phase-3-2024; result=did_not_meet_primary_endpoint",
        },
        {
            "entity_type": "intervention_object",
            "entity_id": (
                "asset-example-asset-target-class-example-class-small-molecule-"
                "acute-positive-symptoms-adults-with-schizophrenia-monotherapy-phase-2"
            ),
            "outcome_label": "future_schizophrenia_program_advanced",
            "outcome_date": "2024-10-01",
            "label_source": "program_history_v2",
            "label_notes": "event_id=example-phase-3-2024; stage_bucket=phase_3_or_registration",
        },
    ]

    bundle_rows = build_intervention_object_bundle_rows(
        as_of_date="2024-06-20",
        source_snapshots=(),
        archive_descriptors=(),
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    assert bundle_rows[0]["entity_id"] == (
        "asset-example-asset-target-class-example-class-small-molecule-"
        "acute-positive-symptoms-adults-with-schizophrenia-monotherapy-phase-2"
    )
    assert bundle_rows[0]["stage_bucket"] == "phase_2"


def test_intervention_object_replay_rewinds_domain_population_and_regimen(
    tmp_path: Path,
) -> None:
    program_universe_path = tmp_path / "program_universe.csv"
    events_path = tmp_path / "events.csv"
    program_universe_path.write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-cognition-adjunct-phase-3-or-registration,example-asset,Example Asset,asset:example-asset,[],'
            'GENE1,"[""GENE1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,cognition,adults with cognitive impairment on stable antipsychotics,"
            'adjunct,phase_3_or_registration,included,checked_in_event_history,high,'
            '"[""example-phase-2-2024"", ""example-phase-3-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,Example identity-drift row.\n"
        ),
        encoding="utf-8",
    )
    events_path.write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-phase-2-2024,example-asset,Example Sponsor,acutely psychotic adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-01-15,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Phase 2 signal,1\n"
            "example-phase-3-2024,example-asset,Example Sponsor,adults with cognitive impairment on stable antipsychotics,"
            "cognition,adjunct,phase_3,topline_readout,2024-10-01,"
            "did_not_meet_primary_endpoint,unresolved,medium,Phase 3 miss,2\n"
        ),
        encoding="utf-8",
    )

    cohort_rows, future_outcome_rows = build_intervention_object_public_slice_rows(
        as_of_date="2024-06-20",
        outcome_observation_closed_at="2025-06-30",
        program_universe_path=program_universe_path,
        events_path=events_path,
    )

    assert cohort_rows == [
        {
            "entity_type": "intervention_object",
            "entity_id": (
                "asset-example-asset-target-class-example-class-small-molecule-"
                "acute-positive-symptoms-acutely-psychotic-adults-with-schizophrenia-"
                "monotherapy-phase-2"
            ),
            "entity_label": "Example Asset | acute positive symptoms | phase_2",
        }
    ]
    assert {row["entity_id"] for row in future_outcome_rows} == {
        (
            "asset-example-asset-target-class-example-class-small-molecule-"
            "acute-positive-symptoms-acutely-psychotic-adults-with-schizophrenia-"
            "monotherapy-phase-2"
        )
    }

    bundle_rows = build_intervention_object_bundle_rows(
        as_of_date="2024-06-20",
        source_snapshots=(),
        archive_descriptors=(),
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    assert bundle_rows[0]["entity_id"] == (
        "asset-example-asset-target-class-example-class-small-molecule-"
        "acute-positive-symptoms-acutely-psychotic-adults-with-schizophrenia-"
        "monotherapy-phase-2"
    )
    assert bundle_rows[0]["domain"] == "acute_positive_symptoms"
    assert (
        bundle_rows[0]["population"]
        == "acutely psychotic adults with schizophrenia"
    )
    assert bundle_rows[0]["regimen"] == "monotherapy"


def test_intervention_object_replay_excludes_future_only_program_rows(
    tmp_path: Path,
) -> None:
    program_universe_path = tmp_path / "program_universe.csv"
    events_path = tmp_path / "events.csv"
    program_universe_path.write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-future-only-approved,example-asset,Example Asset,asset:example-asset,[],'
            'GENE1,"[""GENE1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,approved,included,checked_in_event_history,high,'
            '"[""example-approval-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,Future-only row.\n"
        ),
        encoding="utf-8",
    )
    events_path.write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-approval-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,approved,regulatory_approval,2024-10-01,"
            "approved_for_adults_with_schizophrenia,not_applicable_nonfailure,high,"
            "Future approval only,1\n"
        ),
        encoding="utf-8",
    )

    cohort_rows, future_outcome_rows = build_intervention_object_public_slice_rows(
        as_of_date="2024-06-20",
        outcome_observation_closed_at="2025-06-30",
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    assert cohort_rows == []
    assert future_outcome_rows == []

    bundle_rows = build_intervention_object_bundle_rows(
        as_of_date="2024-06-20",
        source_snapshots=(),
        archive_descriptors=(),
        program_universe_path=program_universe_path,
        events_path=events_path,
    )
    assert bundle_rows == []


def test_intervention_object_replay_rejects_duplicate_replay_entity_ids(
    tmp_path: Path,
) -> None:
    program_universe_path = tmp_path / "program_universe.csv"
    events_path = tmp_path / "events.csv"
    program_universe_path.write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'duplicate-a,example-asset,Example Asset,asset:example-asset,[],'
            'GENE1,"[""GENE1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_2,included,checked_in_event_history,high,"[""event-a""]",,'
            "clinicaltrials_gov,NCT00000000,https://example.test/a,First replay row.\n"
            'duplicate-b,example-asset-v2,Example Asset v2,asset:example-asset,[],'
            'GENE1,"[""GENE1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_2,included,checked_in_event_history,high,"[""event-b""]",,'
            "clinicaltrials_gov,NCT00000001,https://example.test/b,Second replay row.\n"
        ),
        encoding="utf-8",
    )
    events_path.write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "event-a,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-01-15,"
            "met_primary_endpoint,not_applicable_nonfailure,high,First row,1\n"
            "event-b,example-asset-v2,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-01-16,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Second row,2\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate replay entity_id"):
        build_intervention_object_public_slice_rows(
            as_of_date="2024-06-20",
            outcome_observation_closed_at="2025-06-30",
            program_universe_path=program_universe_path,
            events_path=events_path,
        )

    with pytest.raises(ValueError, match="duplicate replay entity_id"):
        build_intervention_object_bundle_rows(
            as_of_date="2024-06-20",
            source_snapshots=(),
            archive_descriptors=(),
            program_universe_path=program_universe_path,
            events_path=events_path,
        )
