import json
import shutil
from pathlib import Path

import pytest

from scz_target_engine.benchmark_labels import (
    FutureOutcomeRecord,
    OBSERVED_LABEL_VALUE,
    benchmark_cohort_manifest_path_for_labels_file,
    benchmark_cohort_members_path_for_labels_file,
    benchmark_source_cohort_members_path_for_labels_file,
    benchmark_source_future_outcomes_path_for_labels_file,
    build_benchmark_cohort_labels,
    load_materialized_benchmark_cohort_artifacts,
    load_cohort_members,
    load_future_outcomes,
    materialize_benchmark_cohort_labels,
    read_benchmark_cohort_manifest,
    read_benchmark_cohort_members,
    read_benchmark_cohort_labels,
    validate_benchmark_cohort_labels_against_manifest,
)
from scz_target_engine.benchmark_snapshots import (
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
TASK_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "curated"
    / "rescue_tasks"
    / "task_registry.csv"
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
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)

    result = materialize_benchmark_cohort_labels(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    restored = read_benchmark_cohort_labels(output_file)
    restored_members = read_benchmark_cohort_members(
        benchmark_cohort_members_path_for_labels_file(output_file)
    )
    restored_cohort_manifest = read_benchmark_cohort_manifest(
        benchmark_cohort_manifest_path_for_labels_file(output_file)
    )

    assert restored == build_benchmark_cohort_labels(
        manifest,
        load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
        load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
    )
    assert sorted(
        restored_members,
        key=lambda item: (item.entity_type, item.entity_id, item.entity_label.lower()),
    ) == sorted(
        load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
        key=lambda item: (item.entity_type, item.entity_id, item.entity_label.lower()),
    )
    assert restored_cohort_manifest.snapshot_id == manifest.snapshot_id
    assert restored_cohort_manifest.cohort_id == manifest.cohort_id
    assert restored_cohort_manifest.schema_version == "v3"
    assert restored_cohort_manifest.snapshot_manifest_artifact_path == "snapshot_manifest.json"
    assert restored_cohort_manifest.cohort_members_artifact_path == "benchmark_cohort_members.csv"
    assert restored_cohort_manifest.cohort_labels_artifact_path == "cohort_labels.csv"
    assert restored_cohort_manifest.source_cohort_members_path == "source_cohort_members.csv"
    assert restored_cohort_manifest.source_future_outcomes_path == "source_future_outcomes.csv"
    assert restored_cohort_manifest.entity_count == len(restored_members)
    assert restored_cohort_manifest.label_row_count == len(restored)
    assert result["benchmark_cohort_members_file"].endswith("benchmark_cohort_members.csv")
    assert result["benchmark_cohort_manifest_file"].endswith("benchmark_cohort_manifest.json")
    assert result["source_cohort_members_file"].endswith("source_cohort_members.csv")
    assert result["source_future_outcomes_file"].endswith("source_future_outcomes.csv")


def test_materialize_benchmark_cohort_labels_accepts_header_only_future_outcomes(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    future_outcomes_file = tmp_path / "future_outcomes.csv"
    future_outcomes_file.write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n",
        encoding="utf-8",
    )
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-03",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)

    result = materialize_benchmark_cohort_labels(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=future_outcomes_file,
        output_file=output_file,
    )
    restored = read_benchmark_cohort_labels(output_file)
    restored_members = read_benchmark_cohort_members(
        benchmark_cohort_members_path_for_labels_file(output_file)
    )
    restored_cohort_manifest = read_benchmark_cohort_manifest(
        benchmark_cohort_manifest_path_for_labels_file(output_file)
    )
    source_future_outcomes_path = benchmark_source_future_outcomes_path_for_labels_file(
        output_file
    )

    assert load_future_outcomes(source_future_outcomes_path) == ()
    assert result["entity_count"] == len(restored_members) == 3
    assert result["row_count"] == len(restored) == 45
    assert result["observed_label_rows"] == 9
    assert restored_cohort_manifest.label_row_count == 45
    assert restored_cohort_manifest.observed_label_row_count == 9
    assert source_future_outcomes_path.read_text(encoding="utf-8") == (
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
    )
    assert all(
        label.label_name == "no_qualifying_future_outcome"
        if label.label_value == OBSERVED_LABEL_VALUE
        else label.label_name != "no_qualifying_future_outcome"
        for label in restored
    )


def test_read_benchmark_cohort_manifest_rejects_non_integer_counts(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-03",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    materialize_benchmark_cohort_labels(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(output_file)
    payload = json.loads(cohort_manifest_file.read_text(encoding="utf-8"))
    payload["entity_count"] = "3"
    cohort_manifest_file.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="entity_count must be an integer"):
        read_benchmark_cohort_manifest(cohort_manifest_file)


def test_future_outcome_record_from_dict_rejects_non_string_entity_type() -> None:
    with pytest.raises(ValueError, match="entity_type must be a string"):
        FutureOutcomeRecord.from_dict(
            {
                "entity_type": False,
                "entity_id": "ENSG00000162946",
                "outcome_label": "future_schizophrenia_program_started",
                "outcome_date": "2025-01-01",
                "label_source": "manual",
                "label_notes": "",
            }
        )


def test_materialize_benchmark_cohort_labels_requires_manifest_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError):
        materialize_benchmark_cohort_labels(
            manifest=build_fixture_manifest(),
            cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
            future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
            output_file=tmp_path / "cohort_labels.csv",
        )


def test_materialize_benchmark_cohort_labels_rejects_none_manifest_file(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "cohort_labels.csv"

    with pytest.raises(ValueError, match="manifest_file is required"):
        materialize_benchmark_cohort_labels(
            manifest=build_fixture_manifest(),
            manifest_file=None,
            cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
            future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
            output_file=output_file,
        )

    assert not output_file.exists()
    assert not benchmark_cohort_members_path_for_labels_file(output_file).exists()
    assert not benchmark_cohort_manifest_path_for_labels_file(output_file).exists()
    assert not benchmark_source_cohort_members_path_for_labels_file(output_file).exists()
    assert not benchmark_source_future_outcomes_path_for_labels_file(output_file).exists()


def test_materialize_benchmark_cohort_labels_rejects_mismatched_manifest_file(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    mismatched_manifest_file = tmp_path / "snapshot_manifest_mismatched.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    mismatched_payload = json.loads(snapshot_manifest_file.read_text())
    mismatched_payload["snapshot_id"] = "bogus_snapshot_id"
    mismatched_manifest_file.write_text(
        json.dumps(mismatched_payload, indent=2, sort_keys=True) + "\n"
    )

    with pytest.raises(
        ValueError,
        match="manifest_file must contain the same benchmark snapshot manifest",
    ):
        materialize_benchmark_cohort_labels(
            manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            manifest_file=mismatched_manifest_file,
            cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
            future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
            output_file=output_file,
        )

    assert not output_file.exists()
    assert not benchmark_cohort_members_path_for_labels_file(output_file).exists()
    assert not benchmark_cohort_manifest_path_for_labels_file(output_file).exists()
    assert not benchmark_source_cohort_members_path_for_labels_file(output_file).exists()
    assert not benchmark_source_future_outcomes_path_for_labels_file(output_file).exists()


def test_materialize_benchmark_cohort_labels_accepts_task_registry_override(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )

    result = materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(
            snapshot_manifest_file,
            task_registry_path=TASK_REGISTRY_PATH,
        ),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )

    assert result["row_count"] == 45
    assert benchmark_cohort_manifest_path_for_labels_file(output_file).exists()


def test_load_materialized_benchmark_cohort_artifacts_supports_relocated_bundle(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    relocated_dir = tmp_path / "relocated"
    source_dir.mkdir()
    relocated_dir.mkdir()
    source_snapshot_manifest_file = source_dir / "snapshot_manifest.json"
    source_output_file = source_dir / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=source_snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(source_snapshot_manifest_file),
        manifest_file=source_snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=source_output_file,
    )
    for artifact_name in (
        "snapshot_manifest.json",
        "cohort_labels.csv",
        "benchmark_cohort_members.csv",
        "benchmark_cohort_manifest.json",
        "source_cohort_members.csv",
        "source_future_outcomes.csv",
    ):
        shutil.copy2(source_dir / artifact_name, relocated_dir / artifact_name)

    materialized = load_materialized_benchmark_cohort_artifacts(
        snapshot_manifest=read_benchmark_snapshot_manifest(
            relocated_dir / "snapshot_manifest.json"
        ),
        snapshot_manifest_file=relocated_dir / "snapshot_manifest.json",
        cohort_labels_file=relocated_dir / "cohort_labels.csv",
    )

    assert materialized.cohort_manifest.snapshot_id == "scz_fixture_2024_06_30"
    assert len(materialized.cohort_members) == 3
    assert len(materialized.cohort_labels) == 45


def test_load_materialized_benchmark_cohort_artifacts_requires_source_bundle_provenance_fields(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(output_file)
    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text())
    for field_name in (
        "source_cohort_members_path",
        "source_cohort_members_sha256",
        "source_future_outcomes_path",
        "source_future_outcomes_sha256",
    ):
        cohort_manifest_payload.pop(field_name)
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n"
    )

    with pytest.raises(
        ValueError,
        match="benchmark cohort manifest is missing required field: source_cohort_members_path",
    ):
        load_materialized_benchmark_cohort_artifacts(
            snapshot_manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            snapshot_manifest_file=snapshot_manifest_file,
            cohort_labels_file=output_file,
        )


@pytest.mark.parametrize(
    ("field_name", "path_value", "expected_message"),
    (
        (
            "snapshot_manifest_artifact_path",
            "wrong/snapshot_manifest.json",
            "benchmark cohort manifest does not point to the supplied "
            "benchmark_snapshot_manifest artifact",
        ),
        (
            "cohort_labels_artifact_path",
            "wrong/cohort_labels.csv",
            "benchmark cohort manifest does not point to the supplied "
            "benchmark_cohort_labels artifact",
        ),
    ),
)
def test_load_materialized_benchmark_cohort_artifacts_rejects_mismatched_path_refs(
    tmp_path: Path,
    field_name: str,
    path_value: str,
    expected_message: str,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(output_file)
    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text())
    cohort_manifest_payload[field_name] = path_value
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n"
    )

    with pytest.raises(ValueError, match=expected_message):
        load_materialized_benchmark_cohort_artifacts(
            snapshot_manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            snapshot_manifest_file=snapshot_manifest_file,
            cohort_labels_file=output_file,
        )


@pytest.mark.parametrize(
    ("field_name", "field_value", "expected_message"),
    (
        (
            "source_cohort_members_path",
            "wrong/source_cohort_members.csv",
            "benchmark cohort manifest must point to the canonical benchmark_source_cohort_members artifact beside cohort labels",
        ),
        (
            "source_future_outcomes_path",
            "wrong/source_future_outcomes.csv",
            "benchmark cohort manifest must point to the canonical benchmark_source_future_outcomes artifact beside cohort labels",
        ),
        (
            "source_cohort_members_sha256",
            "0" * 64,
            "benchmark cohort source cohort members sha256 does not match benchmark_cohort_manifest",
        ),
        (
            "source_future_outcomes_sha256",
            "1" * 64,
            "benchmark cohort source future outcomes sha256 does not match benchmark_cohort_manifest",
        ),
    ),
)
def test_load_materialized_benchmark_cohort_artifacts_rejects_mismatched_source_provenance(
    tmp_path: Path,
    field_name: str,
    field_value: str,
    expected_message: str,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(output_file)
    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text())
    cohort_manifest_payload[field_name] = field_value
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n"
    )

    with pytest.raises(ValueError, match=expected_message):
        load_materialized_benchmark_cohort_artifacts(
            snapshot_manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            snapshot_manifest_file=snapshot_manifest_file,
            cohort_labels_file=output_file,
        )


@pytest.mark.parametrize(
    ("field_name", "artifact_file_name", "expected_message"),
    (
        (
            "cohort_members_artifact_path",
            "external/benchmark_cohort_members.csv",
            "benchmark cohort manifest must point to the canonical benchmark_cohort_members artifact beside cohort labels",
        ),
        (
            "source_cohort_members_path",
            "external/source_cohort_members.csv",
            "benchmark cohort manifest must point to the canonical benchmark_source_cohort_members artifact beside cohort labels",
        ),
        (
            "source_future_outcomes_path",
            "external/source_future_outcomes.csv",
            "benchmark cohort manifest must point to the canonical benchmark_source_future_outcomes artifact beside cohort labels",
        ),
    ),
)
def test_load_materialized_benchmark_cohort_artifacts_rejects_external_bundle_refs(
    tmp_path: Path,
    field_name: str,
    artifact_file_name: str,
    expected_message: str,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    output_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=output_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(output_file)
    external_artifact_path = tmp_path / artifact_file_name
    external_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    source_artifact_path = {
        "cohort_members_artifact_path": benchmark_cohort_members_path_for_labels_file(
            output_file
        ),
        "source_cohort_members_path": benchmark_source_cohort_members_path_for_labels_file(
            output_file
        ),
        "source_future_outcomes_path": benchmark_source_future_outcomes_path_for_labels_file(
            output_file
        ),
    }[field_name]
    shutil.copy2(source_artifact_path, external_artifact_path)
    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text())
    cohort_manifest_payload[field_name] = str(external_artifact_path)
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n"
    )

    with pytest.raises(ValueError, match=expected_message):
        load_materialized_benchmark_cohort_artifacts(
            snapshot_manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            snapshot_manifest_file=snapshot_manifest_file,
            cohort_labels_file=output_file,
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
    cohort_members = load_cohort_members(public_slice.cohort_members_file)
    ulotaront_entity_id = next(
        member.entity_id
        for member in cohort_members
        if member.entity_label.startswith("ulotaront | ")
    )

    label_matrix = {(label.horizon, label.label_name) for label in labels}
    assert len(label_matrix) > 0
    assert len(labels) == len(cohort_members) * len(label_matrix)
    assert {label.entity_id for label in labels} == {
        member.entity_id for member in cohort_members
    }
    row_map = {
        (label.entity_id, label.horizon, label.label_name): label
        for label in labels
    }
    assert row_map[
        (
            ulotaront_entity_id,
            "1y",
            "future_schizophrenia_positive_signal",
        )
    ].label_value == OBSERVED_LABEL_VALUE


def test_validate_benchmark_cohort_labels_rejects_missing_protocol_matrix_rows() -> None:
    manifest = build_fixture_manifest()
    cohort_members = load_cohort_members(FIXTURE_DIR / "cohort_members.csv")
    labels = build_benchmark_cohort_labels(
        manifest,
        cohort_members,
        load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
    )
    incomplete_labels = tuple(
        label
        for label in labels
        if not (
            label.entity_type == "gene"
            and label.entity_id == "ENSG00000162946"
            and label.horizon == "3y"
        )
    )

    try:
        validate_benchmark_cohort_labels_against_manifest(
            manifest,
            incomplete_labels,
            cohort_members=cohort_members,
        )
    except ValueError as exc:
        assert "full protocol label matrix" in str(exc)
    else:
        raise AssertionError("expected missing protocol matrix rows to be rejected")


def test_validate_benchmark_cohort_labels_rejects_missing_cohort_entity() -> None:
    manifest = build_fixture_manifest()
    cohort_members = load_cohort_members(FIXTURE_DIR / "cohort_members.csv")
    labels = tuple(
        label
        for label in build_benchmark_cohort_labels(
            manifest,
            cohort_members,
            load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
        )
        if label.entity_id != "ENSG00000162946"
    )

    try:
        validate_benchmark_cohort_labels_against_manifest(
            manifest,
            labels,
            cohort_members=cohort_members,
        )
    except ValueError as exc:
        assert "benchmark_cohort_members artifact" in str(exc)
    else:
        raise AssertionError("expected missing cohort entity labels to be rejected")
