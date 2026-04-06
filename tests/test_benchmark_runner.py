from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

import pytest
from pyarrow import Table
from pyarrow import parquet as pq

from scz_target_engine.benchmark_labels import (
    BenchmarkCohortLabel,
    build_benchmark_cohort_manifest,
    benchmark_cohort_manifest_path_for_labels_file,
    benchmark_cohort_members_path_for_labels_file,
    benchmark_source_cohort_members_path_for_labels_file,
    benchmark_source_future_outcomes_path_for_labels_file,
    CohortMember,
    FutureOutcomeRecord,
    build_benchmark_cohort_labels,
    load_cohort_members,
    load_future_outcomes,
    materialize_benchmark_cohort_labels,
    write_benchmark_cohort_manifest,
    write_benchmark_cohort_members,
    write_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_metrics import (
    build_positive_relevance_index,
    build_ranked_evaluation_rows,
    calculate_metric_values,
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_runner import (
    _deterministic_random_score,
    derive_track_b_slice_random_seed,
    materialize_benchmark_run,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import (
    SnapshotBuildRequest,
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
    write_benchmark_snapshot_manifest,
)
from scz_target_engine.io import write_csv
from tests.benchmark_test_support import write_intervention_object_slice_fixture


FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_small"
)
AVAILABLE_NOW_GENE_BASELINES = [
    "pgc_only",
    "schema_only",
    "opentargets_only",
    "v0_current",
    "v1_current",
    "chembl_only",
    "random_with_coverage",
]


def _load_metric_payload_index(
    metric_payload_files: list[str],
) -> dict[tuple[str, str, str, str], object]:
    payloads = {}
    for path in metric_payload_files:
        payload = read_benchmark_metric_output_payload(Path(path))
        payloads[
            (
                payload.baseline_id,
                payload.entity_type,
                payload.horizon,
                payload.metric_name,
            )
        ] = payload
    return payloads


def _write_materialized_cohort_artifacts(
    *,
    manifest: object,
    manifest_file: Path,
    cohort_members: tuple[CohortMember, ...],
    cohort_labels: tuple[BenchmarkCohortLabel, ...],
    source_cohort_members_file: Path,
    source_future_outcomes_file: Path,
    cohort_labels_file: Path,
) -> None:
    write_benchmark_cohort_labels(cohort_labels_file, cohort_labels)
    cohort_members_path = benchmark_cohort_members_path_for_labels_file(cohort_labels_file)
    write_benchmark_cohort_members(cohort_members_path, cohort_members)
    source_cohort_members_bundle_path = benchmark_source_cohort_members_path_for_labels_file(
        cohort_labels_file
    )
    source_future_outcomes_bundle_path = (
        benchmark_source_future_outcomes_path_for_labels_file(cohort_labels_file)
    )
    if source_cohort_members_file.resolve() != source_cohort_members_bundle_path.resolve():
        source_cohort_members_bundle_path.write_bytes(
            source_cohort_members_file.read_bytes()
        )
    if (
        source_future_outcomes_file.resolve()
        != source_future_outcomes_bundle_path.resolve()
    ):
        source_future_outcomes_bundle_path.write_bytes(
            source_future_outcomes_file.read_bytes()
        )
    cohort_manifest_path = benchmark_cohort_manifest_path_for_labels_file(
        cohort_labels_file
    )
    cohort_manifest = build_benchmark_cohort_manifest(
        snapshot_manifest=manifest,
        snapshot_manifest_file=manifest_file,
        cohort_members=cohort_members,
        cohort_labels=cohort_labels,
        cohort_manifest_artifact_file=cohort_manifest_path,
        cohort_members_artifact_file=cohort_members_path,
        cohort_labels_artifact_file=cohort_labels_file,
        source_cohort_members_file=source_cohort_members_bundle_path,
        source_future_outcomes_file=source_future_outcomes_bundle_path,
    )
    write_benchmark_cohort_manifest(
        cohort_manifest_path,
        cohort_manifest,
    )


def _write_source_cohort_inputs(
    *,
    cohort_members: tuple[CohortMember, ...],
    future_outcomes: tuple[FutureOutcomeRecord, ...],
    cohort_members_file: Path,
    future_outcomes_file: Path,
) -> None:
    write_csv(
        cohort_members_file,
        [
            {
                "entity_type": member.entity_type,
                "entity_id": member.entity_id,
                "entity_label": member.entity_label,
            }
            for member in cohort_members
        ],
        ["entity_type", "entity_id", "entity_label"],
    )
    write_csv(
        future_outcomes_file,
        [
            {
                "entity_type": outcome.entity_type,
                "entity_id": outcome.entity_id,
                "outcome_label": outcome.outcome_label,
                "outcome_date": outcome.outcome_date,
                "label_source": outcome.label_source,
                "label_notes": outcome.label_notes,
            }
            for outcome in future_outcomes
        ],
        [
            "entity_type",
            "entity_id",
            "outcome_label",
            "outcome_date",
            "label_source",
            "label_notes",
        ],
    )


def test_derive_track_b_slice_random_seed_is_deterministic() -> None:
    structural_seed = derive_track_b_slice_random_seed(
        base_random_seed=17,
        baseline_id="track_b_structural_current",
    )
    nearest_history_seed = derive_track_b_slice_random_seed(
        base_random_seed=17,
        baseline_id="track_b_nearest_history",
    )

    assert structural_seed == derive_track_b_slice_random_seed(
        base_random_seed=17,
        baseline_id="track_b_structural_current",
    )
    assert structural_seed != nearest_history_seed


def test_materialize_benchmark_run_executes_fixture_baselines_and_emits_artifacts(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-28",
    )
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )

    result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_dir=tmp_path / "runner_outputs",
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )

    assert result["benchmark_suite_id"] == "scz_translational_suite"
    assert result["benchmark_task_id"] == "scz_translational_task"
    assert result["executed_baselines"] == [
        "pgc_only",
        "opentargets_only",
        "v0_current",
        "v1_current",
        "random_with_coverage",
    ]
    assert result["protocol_only_baselines"] == [
        "v1_pre_numeric_pr7_heads",
        "v1_post_numeric_pr7_heads",
    ]
    assert len(result["run_manifest_files"]) == 5
    assert result["metric_payload_files"]
    assert result["confidence_interval_files"]

    v1_manifest_path = next(
        Path(path)
        for path in result["run_manifest_files"]
        if "v1_current" in path
    )
    v1_manifest = read_benchmark_model_run_manifest(v1_manifest_path)
    assert v1_manifest.schema_name == "benchmark_model_run_manifest"
    assert v1_manifest.benchmark_suite_id == "scz_translational_suite"
    assert v1_manifest.benchmark_task_id == "scz_translational_task"
    assert "mean_available_domain_head_score" in v1_manifest.notes
    assert {
        artifact.artifact_name for artifact in v1_manifest.input_artifacts
    } >= {
        "benchmark_snapshot_manifest",
        "benchmark_cohort_manifest",
        "benchmark_cohort_members",
        "benchmark_source_cohort_members",
        "benchmark_source_future_outcomes",
        "benchmark_cohort_labels",
        "source_archive_index",
        "engine_config",
    }

    metric_payload = read_benchmark_metric_output_payload(
        Path(result["metric_payload_files"][0])
    )
    interval_payload = read_benchmark_confidence_interval_payload(
        Path(result["confidence_interval_files"][0])
    )
    assert metric_payload.schema_name == "benchmark_metric_output_payload"
    assert interval_payload.schema_name == "benchmark_confidence_interval_payload"
    assert interval_payload.point_estimate == metric_payload.metric_value
    assert interval_payload.notes.startswith("method=percentile_bootstrap;")


def test_materialize_benchmark_run_projects_current_baselines_onto_intervention_objects(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    manifest = load_snapshot_build_request(public_slice.snapshot_request_file)
    assert manifest.entity_types == ("intervention_object",)
    assert (tmp_path / "intervention_object_feature_bundle.parquet").exists()

    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )
    result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=public_slice.source_archives_file,
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-02T00:00:00Z",
    )

    assert result["executed_baselines"] == [
        "v0_current",
        "v1_current",
        "random_with_coverage",
    ]
    v0_manifest = read_benchmark_model_run_manifest(
        next(
            Path(path)
            for path in result["run_manifest_files"]
            if "v0_current" in path
        )
    )
    assert {
        artifact.artifact_name for artifact in v0_manifest.input_artifacts
    } >= {
        "benchmark_cohort_manifest",
        "benchmark_cohort_members",
        "benchmark_source_cohort_members",
        "benchmark_source_future_outcomes",
        "intervention_object_feature_bundle",
        "benchmark_intervention_object_baseline_projection",
    }
    metric_payload_index = _load_metric_payload_index(result["metric_payload_files"])
    assert (
        "v0_current",
        "intervention_object",
        "3y",
        "average_precision_any_positive_outcome",
    ) in metric_payload_index


def test_materialize_benchmark_run_rejects_mixed_entity_type_labels_for_intervention_objects(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    labels = list(
        build_benchmark_cohort_labels(
            manifest,
            load_cohort_members(public_slice.cohort_members_file),
            load_future_outcomes(public_slice.future_outcomes_file),
        )
    )
    labels.append(
        BenchmarkCohortLabel(
            cohort_id=manifest.cohort_id,
            snapshot_id=manifest.snapshot_id,
            entity_type="gene",
            entity_id="ENSG00000162946",
            entity_label="DISC1",
            label_name="no_qualifying_future_outcome",
            label_value="true",
            horizon="1y",
            outcome_date="",
            label_source="stale_gene_fixture",
        )
    )
    _write_materialized_cohort_artifacts(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members=load_cohort_members(public_slice.cohort_members_file),
        cohort_labels=tuple(labels),
        source_cohort_members_file=public_slice.cohort_members_file,
        source_future_outcomes_file=public_slice.future_outcomes_file,
        cohort_labels_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="benchmark cohort labels contain an entity outside the benchmark cohort members artifact",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_incomplete_label_matrix_for_intervention_objects(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    labels = tuple(
        label
        for label in build_benchmark_cohort_labels(
            manifest,
            load_cohort_members(public_slice.cohort_members_file),
            load_future_outcomes(public_slice.future_outcomes_file),
        )
        if not (
            label.entity_label.startswith("ulotaront | ")
            and label.horizon == "3y"
        )
    )
    _write_materialized_cohort_artifacts(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members=load_cohort_members(public_slice.cohort_members_file),
        cohort_labels=labels,
        source_cohort_members_file=public_slice.cohort_members_file,
        source_future_outcomes_file=public_slice.future_outcomes_file,
        cohort_labels_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="full protocol label matrix",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_missing_cohort_entity_labels(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"

    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
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
    _write_materialized_cohort_artifacts(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members=cohort_members,
        cohort_labels=labels,
        source_cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        source_future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        cohort_labels_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="benchmark_cohort_members artifact",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=FIXTURE_DIR / "source_archives.json",
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_requires_source_bundle_provenance_fields(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"

    materialize_benchmark_snapshot_manifest(
        request_file=FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(
        cohort_labels_file
    )
    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text())
    for field_name in (
        "source_cohort_members_path",
        "source_cohort_members_sha256",
        "source_future_outcomes_path",
        "source_future_outcomes_sha256",
    ):
        cohort_manifest_payload.pop(field_name)
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="benchmark cohort manifest is missing required field: source_cohort_members_path",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=FIXTURE_DIR / "source_archives.json",
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_stale_intervention_object_bundle_date(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    bundle_path = tmp_path / "intervention_object_feature_bundle.parquet"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    table = pq.read_table(bundle_path)
    bad_metadata = dict(table.schema.metadata or {})
    bad_metadata[b"as_of_date"] = b"2024-06-19"
    pq.write_table(
        table.replace_schema_metadata(bad_metadata),
        bundle_path,
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="feature bundle as_of_date does not match the snapshot manifest",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_stale_intervention_object_bundle_provenance(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    pgc_archive_path = (
        public_slice.source_archives_file.parent
        / "archives"
        / "pgc"
        / "scz2022_fixture.csv"
    )

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )

    pgc_archive_lines = pgc_archive_path.read_text(encoding="utf-8").splitlines()
    pgc_archive_lines[1] = "ENSG00000162946,DISC1,0.11"
    pgc_archive_path.write_text("\n".join(pgc_archive_lines) + "\n", encoding="utf-8")
    archive_index_payload = json.loads(
        public_slice.source_archives_file.read_text(encoding="utf-8")
    )
    for archive_payload in archive_index_payload["archives"]:
        if archive_payload["source_name"] != "PGC":
            continue
        archive_payload["sha256"] = sha256(pgc_archive_path.read_bytes()).hexdigest()
        break
    public_slice.source_archives_file.write_text(
        json.dumps(archive_index_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="source snapshot provenance does not match",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_malformed_intervention_object_bundle(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    bundle_path = tmp_path / "intervention_object_feature_bundle.parquet"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    table = pq.read_table(bundle_path)
    bundle_rows = table.to_pylist()
    for row in bundle_rows:
        row.pop("matched_module_entity_ids_json", None)
    pq.write_table(
        Table.from_pylist(bundle_rows).replace_schema_metadata(table.schema.metadata),
        bundle_path,
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="feature bundle is missing required columns",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_rejects_bundle_cohort_misalignment(
    tmp_path: Path,
) -> None:
    public_slice = write_intervention_object_slice_fixture(tmp_path)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    bundle_path = tmp_path / "intervention_object_feature_bundle.parquet"

    materialize_benchmark_snapshot_manifest(
        request_file=public_slice.snapshot_request_file,
        archive_index_file=public_slice.source_archives_file,
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-02",
    )
    table = pq.read_table(bundle_path)
    bundle_rows = table.to_pylist()
    bundle_rows[0]["entity_id"] = "stale-bundle-entity"
    bundle_rows[0]["intervention_object_id"] = "stale-bundle-entity"
    pq.write_table(
        Table.from_pylist(bundle_rows).replace_schema_metadata(table.schema.metadata),
        bundle_path,
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=public_slice.cohort_members_file,
        future_outcomes_file=public_slice.future_outcomes_file,
        output_file=cohort_labels_file,
    )

    with pytest.raises(
        ValueError,
        match="feature bundle does not align with the replay cohort",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=public_slice.source_archives_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-02T00:00:00Z",
        )


def test_materialize_benchmark_run_supports_all_available_now_gene_baselines(
    tmp_path: Path,
) -> None:
    archives_dir = tmp_path / "archives"
    archives_dir.mkdir()

    pgc_path = archives_dir / "pgc.csv"
    pgc_path.write_text(
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.90\n"
        "GENE_B,Gene B,0.40\n",
        encoding="utf-8",
    )
    schema_path = archives_dir / "schema.csv"
    schema_path.write_text(
        "entity_id,entity_label,rare_variant_support\n"
        "GENE_A,Gene A,0.85\n"
        "GENE_B,Gene B,0.10\n",
        encoding="utf-8",
    )
    opentargets_path = archives_dir / "opentargets.csv"
    opentargets_path.write_text(
        "entity_id,entity_label,generic_platform_baseline\n"
        "GENE_A,Gene A,0.70\n"
        "GENE_B,Gene B,0.30\n",
        encoding="utf-8",
    )
    psychencode_path = archives_dir / "psychencode.json"
    psychencode_path.write_text(
        json.dumps(
            {
                "genes": [
                    {
                        "entity_id": "GENE_A",
                        "entity_label": "Gene A",
                        "cell_state_support": 0.80,
                    },
                    {
                        "entity_id": "GENE_B",
                        "entity_label": "Gene B",
                        "cell_state_support": 0.20,
                    },
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    chembl_path = archives_dir / "chembl.json"
    chembl_path.write_text(
        json.dumps(
            {
                "genes": [
                    {
                        "entity_id": "GENE_A",
                        "entity_label": "Gene A",
                        "tractability_compoundability": 0.65,
                    },
                    {
                        "entity_id": "GENE_B",
                        "entity_label": "Gene B",
                        "tractability_compoundability": 0.15,
                    },
                ],
                "release_name": "chembl_fixture",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    archive_index_path = tmp_path / "source_archives.json"
    archive_index_path.write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": str(pgc_path),
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(pgc_path.read_bytes()).hexdigest(),
                    },
                    {
                        "source_name": "SCHEMA",
                        "source_version": "schema_fixture",
                        "archive_file": str(schema_path),
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(schema_path.read_bytes()).hexdigest(),
                    },
                    {
                        "source_name": "Open Targets",
                        "source_version": "opentargets_fixture",
                        "archive_file": str(opentargets_path),
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(opentargets_path.read_bytes()).hexdigest(),
                    },
                    {
                        "source_name": "PsychENCODE",
                        "source_version": "psychencode_fixture",
                        "archive_file": str(psychencode_path),
                        "archive_format": "json",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(psychencode_path.read_bytes()).hexdigest(),
                    },
                    {
                        "source_name": "ChEMBL",
                        "source_version": "chembl_fixture",
                        "archive_file": str(chembl_path),
                        "archive_format": "json",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(chembl_path.read_bytes()).hexdigest(),
                    },
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = build_benchmark_snapshot_manifest(
        SnapshotBuildRequest(
            snapshot_id="scz_fixture_gene_only",
            cohort_id="scz_gene_fixture_cohort",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2029-06-30",
            entity_types=("gene",),
            baseline_ids=tuple(AVAILABLE_NOW_GENE_BASELINES),
            notes="Synthetic gene-only runner fixture",
        ),
        load_source_archive_descriptors(archive_index_path),
        materialized_at="2026-03-28",
    )
    manifest_path = tmp_path / "snapshot_manifest.json"
    write_benchmark_snapshot_manifest(manifest_path, manifest)

    cohort_members = (
        CohortMember("gene", "GENE_A", "Gene A"),
        CohortMember("gene", "GENE_B", "Gene B"),
    )
    future_outcomes = (
        FutureOutcomeRecord(
            entity_type="gene",
            entity_id="GENE_A",
            outcome_label="future_schizophrenia_program_started",
            outcome_date="2024-12-01",
            label_source="synthetic_history",
        ),
    )
    cohort_labels = build_benchmark_cohort_labels(
        manifest,
        cohort_members,
        future_outcomes,
    )
    cohort_labels_path = tmp_path / "cohort_labels.csv"
    source_cohort_members_file = tmp_path / "source_cohort_members.csv"
    source_future_outcomes_file = tmp_path / "source_future_outcomes.csv"
    _write_source_cohort_inputs(
        cohort_members=cohort_members,
        future_outcomes=future_outcomes,
        cohort_members_file=source_cohort_members_file,
        future_outcomes_file=source_future_outcomes_file,
    )
    _write_materialized_cohort_artifacts(
        manifest=manifest,
        manifest_file=manifest_path,
        cohort_members=cohort_members,
        cohort_labels=cohort_labels,
        source_cohort_members_file=source_cohort_members_file,
        source_future_outcomes_file=source_future_outcomes_file,
        cohort_labels_file=cohort_labels_path,
    )

    result = materialize_benchmark_run(
        manifest_file=manifest_path,
        cohort_labels_file=cohort_labels_path,
        archive_index_file=archive_index_path,
        output_dir=tmp_path / "runner_outputs",
        bootstrap_iterations=25,
        code_version="synthetic-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )

    assert result["benchmark_task_id"] == "scz_translational_task"
    assert result["executed_baselines"] == AVAILABLE_NOW_GENE_BASELINES

    schema_manifest = read_benchmark_model_run_manifest(
        next(
            Path(path)
            for path in result["run_manifest_files"]
            if "schema_only" in path
        )
    )
    chembl_manifest = read_benchmark_model_run_manifest(
        next(
            Path(path)
            for path in result["run_manifest_files"]
            if "chembl_only" in path
        )
    )
    assert any(
        artifact.source_name == "SCHEMA"
        for artifact in schema_manifest.input_artifacts
    )
    assert any(
        artifact.source_name == "ChEMBL"
        for artifact in chembl_manifest.input_artifacts
    )


def test_materialize_benchmark_run_scores_full_admissible_cohort_and_aligns_random_baseline(
    tmp_path: Path,
) -> None:
    archives_dir = tmp_path / "archives"
    archives_dir.mkdir()

    pgc_path = archives_dir / "pgc.csv"
    pgc_path.write_text(
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.90\n"
        "GENE_C,Gene C,0.40\n",
        encoding="utf-8",
    )

    archive_index_path = tmp_path / "source_archives.json"
    archive_index_path.write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": str(pgc_path),
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": sha256(pgc_path.read_bytes()).hexdigest(),
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = build_benchmark_snapshot_manifest(
        SnapshotBuildRequest(
            snapshot_id="scz_fixture_sparse_gene",
            cohort_id="scz_sparse_gene_cohort",
            benchmark_question_id="scz_translational_ranking_v1",
            as_of_date="2024-06-30",
            outcome_observation_closed_at="2029-06-30",
            entity_types=("gene",),
            baseline_ids=("pgc_only", "random_with_coverage"),
            notes="Synthetic sparse-coverage runner fixture",
        ),
        load_source_archive_descriptors(archive_index_path),
        materialized_at="2026-03-28",
    )
    manifest_path = tmp_path / "snapshot_manifest.json"
    write_benchmark_snapshot_manifest(manifest_path, manifest)

    cohort_members = (
        CohortMember("gene", "GENE_A", "Gene A"),
        CohortMember("gene", "GENE_B", "Gene B"),
        CohortMember("gene", "GENE_C", "Gene C"),
        CohortMember("gene", "GENE_D", "Gene D"),
    )
    cohort_labels = build_benchmark_cohort_labels(
        manifest,
        cohort_members,
        future_outcomes := (
            FutureOutcomeRecord(
                entity_type="gene",
                entity_id="GENE_A",
                outcome_label="future_schizophrenia_program_started",
                outcome_date="2024-12-01",
                label_source="synthetic_history",
            ),
            FutureOutcomeRecord(
                entity_type="gene",
                entity_id="GENE_B",
                outcome_label="future_schizophrenia_program_started",
                outcome_date="2025-02-01",
                label_source="synthetic_history",
            ),
        ),
    )
    cohort_labels_path = tmp_path / "cohort_labels.csv"
    source_cohort_members_file = tmp_path / "source_cohort_members.csv"
    source_future_outcomes_file = tmp_path / "source_future_outcomes.csv"
    _write_source_cohort_inputs(
        cohort_members=cohort_members,
        future_outcomes=future_outcomes,
        cohort_members_file=source_cohort_members_file,
        future_outcomes_file=source_future_outcomes_file,
    )
    _write_materialized_cohort_artifacts(
        manifest=manifest,
        manifest_file=manifest_path,
        cohort_members=cohort_members,
        cohort_labels=cohort_labels,
        source_cohort_members_file=source_cohort_members_file,
        source_future_outcomes_file=source_future_outcomes_file,
        cohort_labels_file=cohort_labels_path,
    )

    result = materialize_benchmark_run(
        manifest_file=manifest_path,
        cohort_labels_file=cohort_labels_path,
        archive_index_file=archive_index_path,
        output_dir=tmp_path / "runner_outputs",
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="synthetic-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )

    metric_payloads = _load_metric_payload_index(result["metric_payload_files"])
    pgc_ap = metric_payloads[
        (
            "pgc_only",
            "gene",
            "1y",
            "average_precision_any_positive_outcome",
        )
    ]
    pgc_recall_at_1 = metric_payloads[
        ("pgc_only", "gene", "1y", "recall_at_1_any_positive_outcome")
    ]
    assert pgc_ap.metric_value == 0.833333
    assert pgc_recall_at_1.metric_value == 0.5
    assert pgc_ap.cohort_size == 4
    assert pgc_ap.notes == (
        "relevance=any_positive_outcome;"
        " positives=2;"
        " covered_entities=2/4;"
        " deterministic_test_mode=true"
    )

    admissible_entities = (
        ("GENE_A", "Gene A"),
        ("GENE_B", "Gene B"),
        ("GENE_C", "Gene C"),
        ("GENE_D", "Gene D"),
    )
    admissible_entity_ids = tuple(entity_id for entity_id, _ in admissible_entities)
    relevance_index = build_positive_relevance_index(
        cohort_labels,
        entity_type="gene",
        horizon="1y",
    )
    random_ranked_entity_ids = tuple(
        entity_id
        for entity_id, entity_label in sorted(
            admissible_entities,
            key=lambda item: (
                -_deterministic_random_score(
                    seed=17,
                    snapshot_id=manifest.snapshot_id,
                    entity_type="gene",
                    entity_id=item[0],
                ),
                item[1].lower(),
                item[0],
            ),
        )
    )
    expected_random_metrics = calculate_metric_values(
        build_ranked_evaluation_rows(
            admissible_entity_ids,
            random_ranked_entity_ids,
            relevance_index,
        )
    )

    random_ap = metric_payloads[
        (
            "random_with_coverage",
            "gene",
            "1y",
            "average_precision_any_positive_outcome",
        )
    ]
    random_recall_at_3 = metric_payloads[
        ("random_with_coverage", "gene", "1y", "recall_at_3_any_positive_outcome")
    ]
    assert random_ap.metric_value == expected_random_metrics[
        "average_precision_any_positive_outcome"
    ]
    assert random_recall_at_3.metric_value == expected_random_metrics[
        "recall_at_3_any_positive_outcome"
    ]
    assert random_ap.cohort_size == len(admissible_entity_ids)
    assert random_ap.notes == (
        "relevance=any_positive_outcome;"
        " positives=2;"
        " covered_entities=4/4;"
        " deterministic_test_mode=true"
    )
