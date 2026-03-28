import pytest

from scz_target_engine.benchmark_protocol import (
    EXCLUDE_SOURCE_POLICY,
    FROZEN_BASELINE_IDS,
    FROZEN_BASELINE_MATRIX,
    FROZEN_BENCHMARK_PROTOCOL,
    LeakageControls,
    RECORD_TIMESTAMP_CUTOFF,
    REJECT_SNAPSHOT_POLICY,
    SOURCE_RELEASE_CUTOFF,
    BenchmarkSnapshotManifest,
    BaselineDefinition,
    SourceSnapshot,
)


def test_snapshot_manifest_round_trips_cleanly() -> None:
    manifest = BenchmarkSnapshotManifest(
        schema_name="benchmark_snapshot_manifest",
        schema_version="v1",
        snapshot_id="scz_2024_06_30",
        cohort_id="gene_module_eval",
        benchmark_question_id="scz_translational_ranking_v1",
        as_of_date="2024-06-30",
        outcome_observation_closed_at="2026-06-30",
        entity_types=("gene", "module"),
        source_snapshots=(
            SourceSnapshot(
                source_name="PGC",
                source_version="scz2022",
                cutoff_mode=SOURCE_RELEASE_CUTOFF,
                allowed_data_through="2024-06-30",
                materialized_at="2024-06-30",
                evidence_timestamp_field=None,
                missing_date_policy=EXCLUDE_SOURCE_POLICY,
                future_record_policy=REJECT_SNAPSHOT_POLICY,
            ),
            SourceSnapshot(
                source_name="Open Targets",
                source_version="24.03",
                cutoff_mode=SOURCE_RELEASE_CUTOFF,
                allowed_data_through="2024-06-30",
                materialized_at="2024-06-30",
                evidence_timestamp_field=None,
                missing_date_policy=EXCLUDE_SOURCE_POLICY,
                future_record_policy=REJECT_SNAPSHOT_POLICY,
            ),
        ),
        leakage_controls=LeakageControls(),
        baseline_ids=("pgc_only", "v0_current", "random_with_coverage"),
        notes="Protocol-only fixture manifest.",
    )

    payload = manifest.to_dict()
    restored = BenchmarkSnapshotManifest.from_dict(payload)

    assert restored == manifest
    assert payload["baseline_ids"] == ["pgc_only", "v0_current", "random_with_coverage"]


def test_invalid_cutoff_definition_fails_clearly() -> None:
    with pytest.raises(
        ValueError,
        match="record_timestamp_lte_as_of requires evidence_timestamp_field",
    ):
        SourceSnapshot(
            source_name="PGC",
            source_version="scz2022",
            cutoff_mode=RECORD_TIMESTAMP_CUTOFF,
            allowed_data_through="2024-06-30",
            materialized_at="2024-06-30",
            evidence_timestamp_field=None,
            missing_date_policy=EXCLUDE_SOURCE_POLICY,
            future_record_policy=REJECT_SNAPSHOT_POLICY,
        )


def test_leakage_controls_reject_loose_configuration() -> None:
    with pytest.raises(
        ValueError,
        match="forbid_future_outcome_labels_in_inputs must remain enabled",
    ):
        LeakageControls(forbid_future_outcome_labels_in_inputs=False)


def test_snapshot_rejects_post_cutoff_materialization() -> None:
    with pytest.raises(ValueError, match="materialized_at exceeds as_of_date"):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_only_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=(
                SourceSnapshot(
                    source_name="SCHEMA",
                    source_version="1.0",
                    cutoff_mode=SOURCE_RELEASE_CUTOFF,
                    allowed_data_through="2024-06-30",
                    materialized_at="2024-07-01",
                    evidence_timestamp_field=None,
                    missing_date_policy=EXCLUDE_SOURCE_POLICY,
                    future_record_policy=REJECT_SNAPSHOT_POLICY,
                ),
            ),
            leakage_controls=LeakageControls(),
            baseline_ids=("schema_only",),
        )


def test_baseline_definitions_serialize_deterministically() -> None:
    serialized = [baseline.to_dict() for baseline in FROZEN_BASELINE_MATRIX]
    restored = [
        BaselineDefinition.from_dict(payload).to_dict() for payload in serialized
    ]

    assert serialized == restored
    assert [payload["baseline_id"] for payload in serialized] == list(FROZEN_BASELINE_IDS)
    assert FROZEN_BENCHMARK_PROTOCOL.to_dict()["baselines"] == serialized
