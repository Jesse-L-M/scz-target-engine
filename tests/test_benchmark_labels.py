from pathlib import Path

from scz_target_engine.benchmark_labels import (
    FutureOutcomeRecord,
    OBSERVED_LABEL_VALUE,
    build_benchmark_cohort_labels,
    load_cohort_members,
    load_future_outcomes,
    materialize_benchmark_cohort_labels,
    read_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_snapshots import (
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
)
from tests.benchmark_test_support import write_intervention_object_slice_fixture


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)
def build_fixture_manifest():
    return build_benchmark_snapshot_manifest(
        load_snapshot_build_request(FIXTURE_DIR / "snapshot_request.json"),
        load_source_archive_descriptors(FIXTURE_DIR / "source_archives.json"),
        materialized_at="2026-03-28",
    )


def test_build_benchmark_cohort_labels_generates_deterministic_fixture_rows() -> None:
    labels = build_benchmark_cohort_labels(
        build_fixture_manifest(),
        load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
        load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
    )

    assert len(labels) == 45

    row_map = {
        (label.entity_id, label.horizon, label.label_name): label
        for label in labels
    }
    assert row_map[
        (
            "ENSG00000162946",
            "1y",
            "future_schizophrenia_program_started",
        )
    ].label_value == OBSERVED_LABEL_VALUE
    assert row_map[
        (
            "ENSG00000162946",
            "3y",
            "future_schizophrenia_positive_signal",
        )
    ].label_value == OBSERVED_LABEL_VALUE
    assert row_map[
        (
            "ENSG00000151067",
            "1y",
            "no_qualifying_future_outcome",
        )
    ].label_value == OBSERVED_LABEL_VALUE
    assert row_map[
        (
            "ENSG00000151067",
            "3y",
            "future_schizophrenia_negative_signal",
        )
    ].label_value == OBSERVED_LABEL_VALUE
    assert row_map[
        (
            "MOD_DLPFC_GRN",
            "3y",
            "future_schizophrenia_program_advanced",
        )
    ].label_value == OBSERVED_LABEL_VALUE


def test_materialize_benchmark_cohort_labels_round_trips_fixture_flow(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "cohort_labels.csv"
    manifest = build_fixture_manifest()

    materialize_benchmark_cohort_labels(
        manifest=manifest,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    restored = read_benchmark_cohort_labels(output_file)

    assert restored == build_benchmark_cohort_labels(
        manifest,
        load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
        load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
    )


def test_build_benchmark_cohort_labels_rejects_precutoff_outcomes() -> None:
    manifest = build_fixture_manifest()

    try:
        build_benchmark_cohort_labels(
            manifest,
            load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
            (
                FutureOutcomeRecord(
                    entity_type="gene",
                    entity_id="ENSG00000151067",
                    outcome_label="future_schizophrenia_program_started",
                    outcome_date="2024-06-01",
                    label_source="fixture_program_history",
                ),
            ),
        )
    except ValueError as exc:
        assert "must be after as_of_date" in str(exc)
    else:
        raise AssertionError("expected pre-cutoff future outcome to be rejected")


def test_build_benchmark_cohort_labels_rejects_post_observation_outcomes() -> None:
    manifest = build_fixture_manifest()

    try:
        build_benchmark_cohort_labels(
            manifest,
            load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
            (
                FutureOutcomeRecord(
                    entity_type="gene",
                    entity_id="ENSG00000151067",
                    outcome_label="future_schizophrenia_program_started",
                    outcome_date="2030-01-01",
                    label_source="fixture_program_history",
                ),
            ),
        )
    except ValueError as exc:
        assert "exceeds outcome_observation_closed_at" in str(exc)
    else:
        raise AssertionError("expected post-observation future outcome to be rejected")


def test_build_benchmark_cohort_labels_supports_intervention_object_public_slice(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    manifest = build_benchmark_snapshot_manifest(
        load_snapshot_build_request(public_slice.snapshot_request_file),
        load_source_archive_descriptors(public_slice.source_archives_file),
        materialized_at="2026-04-02",
    )
    labels = build_benchmark_cohort_labels(
        manifest,
        load_cohort_members(public_slice.cohort_members_file),
        load_future_outcomes(public_slice.future_outcomes_file),
    )

    assert len(labels) == 30
    assert {label.entity_id for label in labels} == {
        "pimavanserin-negative-symptoms-adjunct-phase-3-or-registration",
        "ulotaront-acute-positive-symptoms-monotherapy-phase-3-or-registration",
    }
    row_map = {
        (label.entity_id, label.horizon, label.label_name): label
        for label in labels
    }
    assert row_map[
        (
            "ulotaront-acute-positive-symptoms-monotherapy-phase-3-or-registration",
            "1y",
            "future_schizophrenia_positive_signal",
        )
    ].label_value == OBSERVED_LABEL_VALUE
