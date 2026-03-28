import pytest

from scz_target_engine.benchmark_protocol import (
    EXCLUDE_SOURCE_POLICY,
    FROZEN_BASELINE_IDS,
    FROZEN_BASELINE_MATRIX,
    FROZEN_BENCHMARK_PROTOCOL,
    LeakageControls,
    RECORD_TIMESTAMP_CUTOFF,
    REJECT_SNAPSHOT_POLICY,
    SOURCE_CUTOFF_RULES_V1,
    SOURCE_RELEASE_CUTOFF,
    BenchmarkSnapshotManifest,
    BaselineDefinition,
    SourceSnapshot,
)


def build_source_snapshot(
    source_name: str,
    *,
    source_version: str = "1.0",
    cutoff_mode: str | None = None,
    allowed_data_through: str = "2024-06-30",
    evidence_frozen_at: str | None = "2024-06-01",
    materialized_at: str = "2024-06-30",
    evidence_timestamp_field: str | None = None,
    missing_date_policy: str | None = None,
    future_record_policy: str | None = None,
    included: bool = True,
    exclusion_reason: str = "",
) -> SourceSnapshot:
    rule = next(rule for rule in SOURCE_CUTOFF_RULES_V1 if rule.source_name == source_name)
    return SourceSnapshot(
        source_name=source_name,
        source_version=source_version,
        cutoff_mode=rule.cutoff_mode if cutoff_mode is None else cutoff_mode,
        allowed_data_through=allowed_data_through,
        evidence_frozen_at=evidence_frozen_at,
        materialized_at=materialized_at,
        evidence_timestamp_field=(
            rule.evidence_timestamp_field
            if evidence_timestamp_field is None
            else evidence_timestamp_field
        ),
        missing_date_policy=(
            rule.missing_date_policy
            if missing_date_policy is None
            else missing_date_policy
        ),
        future_record_policy=(
            rule.future_record_policy
            if future_record_policy is None
            else future_record_policy
        ),
        included=included,
        exclusion_reason=exclusion_reason,
    )


def build_full_source_snapshots(
    overrides: dict[str, SourceSnapshot] | None = None,
    excluded_sources: dict[str, str] | None = None,
) -> tuple[SourceSnapshot, ...]:
    resolved_overrides = overrides or {}
    resolved_exclusions = excluded_sources or {}
    snapshots: list[SourceSnapshot] = []
    for rule in SOURCE_CUTOFF_RULES_V1:
        if rule.source_name in resolved_overrides:
            snapshots.append(resolved_overrides[rule.source_name])
            continue
        if rule.source_name in resolved_exclusions:
            snapshots.append(
                build_source_snapshot(
                    rule.source_name,
                    included=False,
                    exclusion_reason=resolved_exclusions[rule.source_name],
                )
            )
            continue
        snapshots.append(build_source_snapshot(rule.source_name))
    return tuple(snapshots)


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
        source_snapshots=build_full_source_snapshots(
            overrides={
                "PGC": build_source_snapshot(
                    "PGC",
                    source_version="scz2022",
                    evidence_frozen_at="2024-06-15",
                ),
                "Open Targets": build_source_snapshot(
                    "Open Targets",
                    source_version="24.03",
                    evidence_frozen_at="2024-06-20",
                ),
            }
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
            evidence_frozen_at="2024-06-30",
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


def test_snapshot_allows_post_cutoff_materialization_from_precutoff_archive() -> None:
    manifest = BenchmarkSnapshotManifest(
        schema_name="benchmark_snapshot_manifest",
        schema_version="v1",
        snapshot_id="scz_2024_06_30",
        cohort_id="gene_only_eval",
        benchmark_question_id="scz_translational_ranking_v1",
        as_of_date="2024-06-30",
        outcome_observation_closed_at="2026-06-30",
        entity_types=("gene",),
        source_snapshots=build_full_source_snapshots(
            overrides={
                "SCHEMA": build_source_snapshot(
                    "SCHEMA",
                    materialized_at="2026-07-01",
                )
            }
        ),
        leakage_controls=LeakageControls(),
        baseline_ids=("schema_only",),
    )

    assert manifest.snapshot_id == "scz_2024_06_30"


def test_snapshot_rejects_post_cutoff_evidence_freeze() -> None:
    with pytest.raises(ValueError, match="evidence_frozen_at exceeds as_of_date"):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_only_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=build_full_source_snapshots(
                overrides={
                    "SCHEMA": build_source_snapshot(
                        "SCHEMA",
                        evidence_frozen_at="2024-07-01",
                        materialized_at="2026-07-01",
                    )
                }
            ),
            leakage_controls=LeakageControls(),
            baseline_ids=("schema_only",),
        )


def test_snapshot_rejects_unknown_source_without_frozen_rule() -> None:
    with pytest.raises(ValueError, match="missing a frozen cutoff rule"):
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
                    source_name="MadeUpSource",
                    source_version="1.0",
                    cutoff_mode=SOURCE_RELEASE_CUTOFF,
                    allowed_data_through="2024-06-30",
                    evidence_frozen_at="2024-06-01",
                    materialized_at="2024-06-01",
                    evidence_timestamp_field=None,
                    missing_date_policy=EXCLUDE_SOURCE_POLICY,
                    future_record_policy=REJECT_SNAPSHOT_POLICY,
                ),
                *tuple(
                    build_source_snapshot(
                        source_name,
                        included=False,
                        exclusion_reason="not part of this fixture",
                    )
                    for source_name in (
                        "PGC",
                        "SCHEMA",
                        "PsychENCODE",
                        "Open Targets",
                        "ChEMBL",
                    )
                ),
            ),
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_snapshot_rejects_source_cutoff_mode_mismatch() -> None:
    with pytest.raises(
        ValueError,
        match="PGC cutoff_mode does not match the frozen cutoff rule",
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_only_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=build_full_source_snapshots(
                overrides={
                    "PGC": build_source_snapshot(
                        "PGC",
                        source_version="scz2022",
                        cutoff_mode=RECORD_TIMESTAMP_CUTOFF,
                        evidence_timestamp_field="published_at",
                        materialized_at="2024-06-01",
                    )
                }
            ),
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_snapshot_rejects_baseline_without_entity_type_overlap() -> None:
    with pytest.raises(
        ValueError,
        match="baseline_id pgc_only does not apply to snapshot entity_types",
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="module_only_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("module",),
            source_snapshots=build_full_source_snapshots(),
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_snapshot_rejects_non_frozen_benchmark_question_id() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "benchmark_question_id must match the frozen benchmark question id "
            "scz_translational_ranking_v1"
        ),
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_only_eval",
            benchmark_question_id="different_question_id",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=build_full_source_snapshots(),
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_snapshot_rejects_omitted_frozen_source_entry() -> None:
    partial_snapshots = tuple(
        snapshot
        for snapshot in build_full_source_snapshots()
        if snapshot.source_name != "ChEMBL"
    )

    with pytest.raises(
        ValueError,
        match="source_snapshots must account for every frozen source, missing: ChEMBL",
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_only_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=partial_snapshots,
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_baseline_definitions_serialize_deterministically() -> None:
    serialized = [baseline.to_dict() for baseline in FROZEN_BASELINE_MATRIX]
    restored = [
        BaselineDefinition.from_dict(payload).to_dict() for payload in serialized
    ]

    assert serialized == restored
    assert [payload["baseline_id"] for payload in serialized] == list(FROZEN_BASELINE_IDS)
    assert FROZEN_BENCHMARK_PROTOCOL.to_dict()["baselines"] == serialized
