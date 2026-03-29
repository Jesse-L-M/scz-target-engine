from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

from scz_target_engine.benchmark_labels import (
    CohortMember,
    FutureOutcomeRecord,
    build_benchmark_cohort_labels,
    load_cohort_members,
    load_future_outcomes,
    write_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_metrics import (
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_runner import (
    materialize_benchmark_run,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import (
    SnapshotBuildRequest,
    build_benchmark_snapshot_manifest,
    load_snapshot_build_request,
    load_source_archive_descriptors,
    materialize_benchmark_snapshot_manifest,
    write_benchmark_snapshot_manifest,
)


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
    manifest = build_benchmark_snapshot_manifest(
        load_snapshot_build_request(FIXTURE_DIR / "snapshot_request.json"),
        load_source_archive_descriptors(FIXTURE_DIR / "source_archives.json"),
        materialized_at="2026-03-28",
    )
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    write_benchmark_cohort_labels(
        cohort_labels_file,
        build_benchmark_cohort_labels(
            manifest,
            load_cohort_members(FIXTURE_DIR / "cohort_members.csv"),
            load_future_outcomes(FIXTURE_DIR / "future_outcomes.csv"),
        ),
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
    assert "mean_available_domain_head_score" in v1_manifest.notes
    assert {
        artifact.artifact_name for artifact in v1_manifest.input_artifacts
    } >= {
        "benchmark_snapshot_manifest",
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

    cohort_labels = build_benchmark_cohort_labels(
        manifest,
        (
            CohortMember("gene", "GENE_A", "Gene A"),
            CohortMember("gene", "GENE_B", "Gene B"),
        ),
        (
            FutureOutcomeRecord(
                entity_type="gene",
                entity_id="GENE_A",
                outcome_label="future_schizophrenia_program_started",
                outcome_date="2024-12-01",
                label_source="synthetic_history",
            ),
        ),
    )
    cohort_labels_path = tmp_path / "cohort_labels.csv"
    write_benchmark_cohort_labels(cohort_labels_path, cohort_labels)

    result = materialize_benchmark_run(
        manifest_file=manifest_path,
        cohort_labels_file=cohort_labels_path,
        archive_index_file=archive_index_path,
        output_dir=tmp_path / "runner_outputs",
        bootstrap_iterations=25,
        code_version="synthetic-sha",
        execution_timestamp="2026-03-28T00:00:00Z",
    )

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
