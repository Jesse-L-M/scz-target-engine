import json
from pathlib import Path

import pytest

from scz_target_engine.benchmark_protocol import (
    EXCLUDE_SOURCE_POLICY,
    FROZEN_BASELINE_IDS,
    FROZEN_BASELINE_MATRIX,
    FROZEN_BENCHMARK_PROTOCOL,
    INTERVENTION_OBJECT_ENTITY_TYPE,
    LeakageControls,
    RECORD_TIMESTAMP_CUTOFF,
    REJECT_SNAPSHOT_POLICY,
    SOURCE_CUTOFF_RULES_V1,
    SOURCE_RELEASE_CUTOFF,
    TRACK_B_BENCHMARK_PROTOCOL,
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


def test_snapshot_manifest_reports_invalid_entity_types_before_registry_lookup() -> None:
    with pytest.raises(
        ValueError,
        match="entity_types must only contain supported benchmark entity types",
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_module_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("not_a_real_entity_type",),
            source_snapshots=build_full_source_snapshots(),
            leakage_controls=LeakageControls(),
            baseline_ids=("pgc_only",),
        )


def test_snapshot_manifest_reports_invalid_baseline_ids_before_registry_lookup() -> None:
    with pytest.raises(
        ValueError,
        match="baseline_ids must only contain supported benchmark baselines",
    ):
        BenchmarkSnapshotManifest(
            schema_name="benchmark_snapshot_manifest",
            schema_version="v1",
            snapshot_id="scz_2024_06_30",
            cohort_id="gene_module_eval",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2026-06-30",
            entity_types=("gene",),
            source_snapshots=build_full_source_snapshots(),
            leakage_controls=LeakageControls(),
            baseline_ids=("not_a_real_baseline",),
        )


def test_frozen_protocol_declares_intervention_object_track_a_support() -> None:
    assert INTERVENTION_OBJECT_ENTITY_TYPE in FROZEN_BENCHMARK_PROTOCOL.question.entity_types
    available_now_baselines = {
        baseline.baseline_id: baseline
        for baseline in FROZEN_BASELINE_MATRIX
    }
    assert INTERVENTION_OBJECT_ENTITY_TYPE in available_now_baselines["v0_current"].entity_types
    assert INTERVENTION_OBJECT_ENTITY_TYPE in available_now_baselines["v1_current"].entity_types
    assert (
        INTERVENTION_OBJECT_ENTITY_TYPE
        in available_now_baselines["random_with_coverage"].entity_types
    )


def test_track_b_protocol_uses_explicit_structural_replay_question() -> None:
    assert TRACK_B_BENCHMARK_PROTOCOL.question.question_id == "scz_failure_memory_track_b_v1"
    assert TRACK_B_BENCHMARK_PROTOCOL.question.evaluation_horizons == (
        "structural_replay",
    )
    assert TRACK_B_BENCHMARK_PROTOCOL.question.translational_outcome_labels == (
        "replay_supported",
        "replay_not_supported",
        "replay_inconclusive",
        "insufficient_history",
    )


def test_scz_small_fixture_remains_minimal_gene_module_regression_surface() -> None:
    fixture_dir = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "benchmark"
        / "fixtures"
        / "scz_small"
    )
    source_archives = json.loads(
        (fixture_dir / "source_archives.json").read_text(encoding="utf-8")
    )
    archive_index = {
        archive["source_name"]: archive["sha256"]
        for archive in source_archives["archives"]
    }
    assert archive_index["PGC"] == (
        "6f471ddec2b00b0ce76c3a5b547c022f4c5ae39b77be6ff437d5b2b1c26d9403"
    )
    assert archive_index["Open Targets"] == (
        "08bd5918a2a573f86d6d7cc8cf804d97ab1e47f1153f8027f5cd0fc66436e815"
    )
    assert archive_index["PsychENCODE"] == (
        "f030adfdb6aa996d9aa7de873528ab052cd512c696e622fd878a18d62f7ebe2d"
    )

    assert (
        (fixture_dir / "archives" / "pgc" / "scz2022_fixture.csv")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    ) == [
        "entity_id,entity_label,common_variant_support",
        "ENSG00000162946,DISC1,0.92",
        "ENSG00000151067,CACNA1C,0.81",
    ]
    assert (
        (fixture_dir / "archives" / "opentargets" / "24_03_fixture.csv")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    ) == [
        "entity_id,entity_label,generic_platform_baseline",
        "ENSG00000162946,DISC1,0.74",
        "ENSG00000151067,CACNA1C,0.88",
    ]
    psychencode_fixture = json.loads(
        (
            fixture_dir
            / "archives"
            / "psychencode"
            / "brainscope_fixture.json"
        ).read_text(encoding="utf-8")
    )
    assert psychencode_fixture == {
        "genes": [
            {
                "cell_state_support": 0.77,
                "entity_id": "ENSG00000162946",
                "entity_label": "DISC1",
            },
            {
                "cell_state_support": 0.69,
                "entity_id": "ENSG00000151067",
                "entity_label": "CACNA1C",
            },
        ],
        "modules": [
            {
                "developmental_regulatory_relevance": 0.83,
                "entity_id": "MOD_DLPFC_GRN",
                "entity_label": "Deep Layer Cortical GRN",
            }
        ],
    }


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
        match="benchmark_question_id must match a supported benchmark question id",
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
    assert (
        FROZEN_BENCHMARK_PROTOCOL.to_dict()["baselines"]
        + TRACK_B_BENCHMARK_PROTOCOL.to_dict()["baselines"]
        == serialized
    )
