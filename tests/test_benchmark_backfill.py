from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import shutil

from scz_target_engine.benchmark_backfill import (
    _build_public_slice_specs,
    _coverage_limitation,
    materialize_public_benchmark_slices,
    plan_public_benchmark_slices,
)
from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_leaderboard import materialize_benchmark_reporting
from scz_target_engine.benchmark_protocol import FROZEN_BENCHMARK_PROTOCOL
from scz_target_engine.benchmark_registry import (
    DEFAULT_TASK_REGISTRY_PATH,
    BenchmarkFixturePaths,
    BenchmarkTaskContract,
)
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
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


def _sha256_for_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _write_sparse_custom_registry_fixture(
    tmp_path: Path,
    *,
    task_id: str = "sparse_fixture_task",
    entity_types: str = "gene",
    supported_baseline_ids: str = "pgc_only|random_with_coverage",
    request_entity_types: list[str] | None = None,
    request_baseline_ids: list[str] | None = None,
) -> tuple[Path, Path]:
    fixture_dir = tmp_path / "fixture"
    archives_dir = fixture_dir / "archives" / "pgc"
    archives_dir.mkdir(parents=True)

    archive_contents = (
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.9\n"
    )
    archive_file = archives_dir / "pgc_fixture.csv"
    archive_file.write_text(archive_contents, encoding="utf-8")

    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(
            {
                "snapshot_id": "scz_fixture_2024_06_20",
                "cohort_id": "scz_fixture_small",
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": task_id,
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2029-06-30",
                "entity_types": request_entity_types or ["gene"],
                "baseline_ids": request_baseline_ids or ["pgc_only", "random_with_coverage"],
                "notes": "Sparse archive fixture",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        "entity_type,entity_id,entity_label\n"
        "gene,GENE_A,Gene A\n",
        encoding="utf-8",
    )
    (fixture_dir / "future_outcomes.csv").write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
        "gene,GENE_A,future_schizophrenia_program_started,2024-12-01,fixture,Fixture outcome\n",
        encoding="utf-8",
    )
    (fixture_dir / "source_archives.json").write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": "archives/pgc/pgc_fixture.csv",
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": _sha256_for_text(archive_contents),
                        "notes": "Sparse archive fixture.",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    task_registry_path = tmp_path / "task_registry.csv"
    task_registry_path.write_text(
        (
            "suite_id,suite_label,task_id,task_label,protocol_id,benchmark_question_id,"
            "entity_types,supported_baseline_ids,emitted_artifact_names,"
            "fixture_snapshot_request_file,fixture_cohort_members_file,"
            "fixture_future_outcomes_file,fixture_archive_index_file,notes\n"
            "scz_translational_suite,Schizophrenia Translational Benchmark Suite,"
            f"{task_id},Sparse fixture task,frozen_benchmark_protocol_v1,"
            f"scz_translational_ranking_v1,{entity_types},{supported_baseline_ids},"
            "benchmark_snapshot_manifest|benchmark_cohort_members|"
            "benchmark_source_cohort_members|benchmark_source_future_outcomes|"
            "benchmark_cohort_manifest|benchmark_cohort_labels|"
            "benchmark_model_run_manifest|benchmark_metric_output_payload|"
            "benchmark_confidence_interval_payload,"
            f"{fixture_dir / 'snapshot_request.json'},"
            f"{fixture_dir / 'cohort_members.csv'},"
            f"{fixture_dir / 'future_outcomes.csv'},"
            f"{fixture_dir / 'source_archives.json'},"
            "Sparse public-slice coverage fixture.\n"
        ),
        encoding="utf-8",
    )
    return task_registry_path, fixture_dir


def _write_custom_intervention_object_registry_fixture(
    tmp_path: Path,
    *,
    task_id: str = "fixture_intervention_object_task",
) -> tuple[Path, Path]:
    fixture = write_intervention_object_slice_fixture(tmp_path / task_id)
    snapshot_payload = json.loads(
        fixture.snapshot_request_file.read_text(encoding="utf-8")
    )
    snapshot_payload["benchmark_task_id"] = task_id
    fixture.snapshot_request_file.write_text(
        json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    task_registry_path = tmp_path / f"{task_id}_registry.csv"
    task_registry_path.write_text(
        (
            "suite_id,suite_label,task_id,task_label,protocol_id,benchmark_question_id,"
            "entity_types,supported_baseline_ids,emitted_artifact_names,"
            "fixture_snapshot_request_file,fixture_cohort_members_file,"
            "fixture_future_outcomes_file,fixture_archive_index_file,notes\n"
            "scz_translational_suite,Schizophrenia Translational Benchmark Suite,"
            f"{task_id},Fixture intervention-object task,frozen_benchmark_protocol_v1,"
            "scz_translational_ranking_v1,intervention_object,"
            "v0_current|v1_current|random_with_coverage,"
            "benchmark_snapshot_manifest|benchmark_cohort_members|"
            "benchmark_source_cohort_members|benchmark_source_future_outcomes|"
            "benchmark_cohort_manifest|benchmark_cohort_labels|"
            "benchmark_model_run_manifest|benchmark_metric_output_payload|"
            "benchmark_confidence_interval_payload,"
            f"{fixture.snapshot_request_file},"
            f"{fixture.cohort_members_file},"
            f"{fixture.future_outcomes_file},"
            f"{fixture.source_archives_file},"
            "Custom intervention-object fixture task.\n"
        ),
        encoding="utf-8",
    )
    return task_registry_path, fixture.snapshot_request_file.parent


def _default_track_a_contract(fixture_dir: Path) -> BenchmarkTaskContract:
    return BenchmarkTaskContract(
        suite_id="scz_translational_suite",
        suite_label="Schizophrenia Translational Benchmark Suite",
        task_id="scz_translational_task",
        task_label="Schizophrenia translational ranking task",
        protocol_id="frozen_benchmark_protocol_v1",
        benchmark_question_id="scz_translational_ranking_v1",
        entity_types=("gene", "module", "intervention_object"),
        supported_baseline_ids=(
            "pgc_only",
            "schema_only",
            "opentargets_only",
            "v0_current",
            "v1_current",
            "v1_pre_numeric_pr7_heads",
            "v1_post_numeric_pr7_heads",
            "chembl_only",
            "random_with_coverage",
        ),
        emitted_artifact_names=(
            "benchmark_snapshot_manifest",
            "benchmark_cohort_members",
            "benchmark_source_cohort_members",
            "benchmark_source_future_outcomes",
            "benchmark_cohort_manifest",
            "benchmark_cohort_labels",
            "benchmark_model_run_manifest",
            "benchmark_metric_output_payload",
            "benchmark_confidence_interval_payload",
        ),
        fixture_paths=BenchmarkFixturePaths(
            snapshot_request_file=fixture_dir / "snapshot_request.json",
            cohort_members_file=fixture_dir / "cohort_members.csv",
            future_outcomes_file=fixture_dir / "future_outcomes.csv",
            archive_index_file=fixture_dir / "source_archives.json",
        ),
        protocol=FROZEN_BENCHMARK_PROTOCOL,
        notes="Default Track A regression fixture.",
    )


def test_plan_public_benchmark_slices_discovers_honest_fixture_cutoffs() -> None:
    plan = plan_public_benchmark_slices(
        benchmark_task_id="scz_translational_task",
        current_date="2026-04-08",
    )

    assert plan.benchmark_suite_id == "scz_translational_suite"
    assert plan.benchmark_task_id == "scz_translational_task"
    assert [slice_spec.slice_id for slice_spec in plan.slices] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
        "scz_translational_2024_07_15",
        "scz_translational_2024_09_25",
        "scz_translational_2024_09_26",
        "scz_translational_2024_11_10",
        "scz_translational_2024_11_11",
        "scz_translational_2025_01_15",
        "scz_translational_2025_01_16",
    ]
    assert [slice_spec.principal_positive_entity_count for slice_spec in plan.slices] == [
        1,
        1,
        1,
        1,
        1,
        0,
        0,
        0,
        0,
        0,
    ]
    assert [slice_spec.principal_current_baseline_compatible_entity_count for slice_spec in plan.slices] == [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ]
    assert "none are honestly comparable for v0_current/v1_current" in plan.coverage_limitation


def test_early_public_slice_excludes_post_cutoff_archive_entries_and_files(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "public_slices"
    stale_slice_dir = output_dir / "scz_translational_2024_06_15"
    (stale_slice_dir / "snapshot_request.json").parent.mkdir(
        parents=True, exist_ok=True
    )
    (stale_slice_dir / "snapshot_request.json").write_text(
        "{}\n", encoding="utf-8"
    )
    stale_opentargets_file = (
        stale_slice_dir / "archives" / "opentargets" / "24_03_fixture.csv"
    )
    stale_chembl_file = (
        stale_slice_dir / "archives" / "chembl" / "chembl35_fixture.json"
    )
    stale_opentargets_file.parent.mkdir(parents=True, exist_ok=True)
    stale_chembl_file.parent.mkdir(parents=True, exist_ok=True)
    stale_opentargets_file.write_text("stale fixture\n", encoding="utf-8")
    stale_chembl_file.write_text('{"stale": true}\n', encoding="utf-8")
    result = materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="scz_translational_task",
        current_date="2026-04-08",
    )

    assert result["public_slice_ids"] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
        "scz_translational_2024_07_15",
        "scz_translational_2024_09_25",
        "scz_translational_2024_09_26",
        "scz_translational_2024_11_10",
        "scz_translational_2024_11_11",
        "scz_translational_2025_01_15",
        "scz_translational_2025_01_16",
    ]
    assert "none are honestly comparable for v0_current/v1_current" in result["coverage_limitation"]
    catalog_file = output_dir / "catalog.json"
    assert catalog_file.exists()
    catalog_payload = json.loads(catalog_file.read_text(encoding="utf-8"))
    assert catalog_payload["public_slice_ids"] == result["public_slice_ids"]

    early_slice_dir = output_dir / "scz_translational_2024_06_15"
    early_archive_payload = json.loads(
        (early_slice_dir / "source_archives.json").read_text(encoding="utf-8")
    )
    assert [archive["source_name"] for archive in early_archive_payload["archives"]] == [
        "PGC"
    ]
    snapshot_request_payload = json.loads(
        (early_slice_dir / "snapshot_request.json").read_text(encoding="utf-8")
    )
    assert snapshot_request_payload["program_universe_file"] == "program_universe.csv"
    assert snapshot_request_payload["program_history_events_file"] == "events.csv"
    assert (early_slice_dir / "program_universe.csv").exists()
    assert (early_slice_dir / "events.csv").exists()
    assert not stale_opentargets_file.exists()
    assert not stale_chembl_file.exists()
    assert not (early_slice_dir / "archives" / "opentargets").exists()
    assert not (early_slice_dir / "archives" / "psychencode").exists()
    assert not (early_slice_dir / "archives" / "chembl").exists()


def test_public_slice_builder_reports_sparse_archive_coverage_limitation(
    tmp_path: Path,
) -> None:
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(tmp_path)

    result = materialize_public_benchmark_slices(
        output_dir=tmp_path / "public_slices",
        benchmark_task_id="sparse_fixture_task",
        task_registry_path=task_registry_path,
    )

    assert result["public_slice_ids"] == ["sparse_fixture_2024_06_15"]
    assert (
        "Archived descriptor coverage is too sparse for multiple public slices"
        in result["coverage_limitation"]
    )


def test_regenerating_with_smaller_plan_prunes_obsolete_sibling_slice_dirs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "public_slices"
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(tmp_path)
    materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="sparse_fixture_task",
        task_registry_path=task_registry_path,
    )
    assert (output_dir / "sparse_fixture_2024_06_15").exists()
    result = materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="scz_translational_task",
        current_date="2026-04-08",
    )

    assert result["public_slice_ids"] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
        "scz_translational_2024_07_15",
        "scz_translational_2024_09_25",
        "scz_translational_2024_09_26",
        "scz_translational_2024_11_10",
        "scz_translational_2024_11_11",
        "scz_translational_2025_01_15",
        "scz_translational_2025_01_16",
    ]
    assert "none are honestly comparable for v0_current/v1_current" in result["coverage_limitation"]
    assert not (output_dir / "sparse_fixture_2024_06_15").exists()


def test_default_track_a_planner_considers_program_history_cutoffs_between_archive_releases(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "default_track_a_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(
            {
                "snapshot_id": "scz_fixture_2024_06_20",
                "cohort_id": "scz_fixture_small",
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "scz_translational_task",
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2025-06-30",
                "entity_types": ["gene"],
                "baseline_ids": ["pgc_only"],
                "notes": "Track A cutoff regression fixture",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        "entity_type,entity_id,entity_label\n"
        "gene,GENE_A,Gene A\n",
        encoding="utf-8",
    )
    (fixture_dir / "future_outcomes.csv").write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
        "gene,GENE_A,future_schizophrenia_program_started,2024-12-01,fixture,Fixture outcome\n",
        encoding="utf-8",
    )
    archive_contents = (
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.9\n"
    )
    archive_file = fixture_dir / "archives" / "pgc" / "pgc_fixture.csv"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text(archive_contents, encoding="utf-8")
    (fixture_dir / "source_archives.json").write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": "archives/pgc/pgc_fixture.csv",
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": _sha256_for_text(archive_contents),
                        "notes": "Track A cutoff regression fixture.",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "program_universe.csv").write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-future-stage-phase-3-or-registration,example-asset,Example Asset,asset:example-asset,[],'
            'GENE_A,"[""GENE_A""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_3_or_registration,included,checked_in_event_history,high,'
            '"[""example-phase-2-2024"", ""example-approval-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,Default Track A cutoff regression row.\n"
        ),
        encoding="utf-8",
    )
    (fixture_dir / "events.csv").write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-phase-2-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-06-16,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Phase 2 signal,1\n"
            "example-approval-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,approved,regulatory_approval,2024-09-01,"
            "approved_for_adults_with_schizophrenia,not_applicable_nonfailure,high,Approval,2\n"
        ),
        encoding="utf-8",
    )
    snapshot_payload = json.loads(
        (fixture_dir / "snapshot_request.json").read_text(encoding="utf-8")
    )
    snapshot_payload["program_universe_file"] = "program_universe.csv"
    snapshot_payload["program_history_events_file"] = "events.csv"
    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    slice_specs = _build_public_slice_specs(_default_track_a_contract(fixture_dir))

    assert [slice_spec.slice_id for slice_spec in slice_specs] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_16",
        "scz_translational_2024_06_20",
    ]
    assert [slice_spec.principal_positive_entity_count for slice_spec in slice_specs] == [
        1,
        1,
        1,
    ]


def test_custom_registry_with_default_task_id_preserves_registry_contract(
    tmp_path: Path,
) -> None:
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(
        tmp_path,
        task_id="scz_translational_task",
    )

    plan = plan_public_benchmark_slices(
        benchmark_task_id="scz_translational_task",
        task_registry_path=task_registry_path,
    )

    assert [slice_spec.slice_id for slice_spec in plan.slices] == ["scz_translational_2024_06_15"]
    assert plan.slices[0].snapshot_request.entity_types == ("gene",)
    assert plan.slices[0].snapshot_request.baseline_ids == (
        "pgc_only",
        "random_with_coverage",
    )


def test_explicit_default_registry_path_preserves_track_a_replay() -> None:
    plan = plan_public_benchmark_slices(
        benchmark_task_id="scz_translational_task",
        task_registry_path=DEFAULT_TASK_REGISTRY_PATH,
        current_date="2026-04-08",
    )

    assert [slice_spec.slice_id for slice_spec in plan.slices] == [
        "scz_translational_2024_06_15",
        "scz_translational_2024_06_18",
        "scz_translational_2024_06_20",
        "scz_translational_2024_07_15",
        "scz_translational_2024_09_25",
        "scz_translational_2024_09_26",
        "scz_translational_2024_11_10",
        "scz_translational_2024_11_11",
        "scz_translational_2025_01_15",
        "scz_translational_2025_01_16",
    ]
    assert plan.slices[0].snapshot_request.entity_types == ("intervention_object",)
    assert plan.slices[0].snapshot_request.baseline_ids == (
        "v0_current",
        "v1_current",
        "random_with_coverage",
    )
    assert plan.slices[0].snapshot_request.program_universe_file == "program_universe.csv"
    assert plan.slices[0].snapshot_request.program_history_events_file == "events.csv"
    assert plan.slices[-1].as_of_date == "2025-01-16"


def test_custom_intervention_object_task_uses_fixture_rows_not_repo_replay(
    tmp_path: Path,
) -> None:
    task_registry_path, fixture_dir = _write_custom_intervention_object_registry_fixture(
        tmp_path
    )
    output_dir = tmp_path / "public_slices"

    result = materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="fixture_intervention_object_task",
        task_registry_path=task_registry_path,
    )

    assert result["public_slice_ids"] == [
        "fixture_intervention_object_2024_06_15",
        "fixture_intervention_object_2024_06_18",
        "fixture_intervention_object_2024_06_20",
    ]
    materialized_slice_dir = output_dir / "fixture_intervention_object_2024_06_20"
    assert (
        materialized_slice_dir / "cohort_members.csv"
    ).read_text(encoding="utf-8") == (
        fixture_dir / "cohort_members.csv"
    ).read_text(encoding="utf-8")
    assert (
        materialized_slice_dir / "future_outcomes.csv"
    ).read_text(encoding="utf-8") == (
        fixture_dir / "future_outcomes.csv"
    ).read_text(encoding="utf-8")
    snapshot_request_payload = json.loads(
        (materialized_slice_dir / "snapshot_request.json").read_text(encoding="utf-8")
    )
    assert snapshot_request_payload["program_universe_file"] == "program_universe.csv"
    assert snapshot_request_payload["program_history_events_file"] == "events.csv"
    assert (materialized_slice_dir / "program_universe.csv").exists()
    assert (materialized_slice_dir / "events.csv").exists()


def test_track_a_coverage_limitation_clears_when_current_baseline_compatibility_exists(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "default_track_a_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(
            {
                "snapshot_id": "scz_fixture_2024_06_20",
                "cohort_id": "scz_fixture_small",
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "scz_translational_task",
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2025-06-30",
                "entity_types": ["gene"],
                "baseline_ids": ["pgc_only"],
                "notes": "Track A coverage regression fixture",
                "program_universe_file": "program_universe.csv",
                "program_history_events_file": "events.csv",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        "entity_type,entity_id,entity_label\n"
        "gene,DISC1,DISC1\n",
        encoding="utf-8",
    )
    (fixture_dir / "future_outcomes.csv").write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
        "gene,DISC1,future_schizophrenia_program_started,2024-12-01,fixture,Fixture outcome\n",
        encoding="utf-8",
    )
    archive_contents = (
        "entity_id,entity_label,common_variant_support\n"
        "DISC1,DISC1,0.9\n"
    )
    archive_file = fixture_dir / "archives" / "pgc" / "pgc_fixture.csv"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text(archive_contents, encoding="utf-8")
    (fixture_dir / "source_archives.json").write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": "archives/pgc/pgc_fixture.csv",
                        "archive_format": "csv",
                        "allowed_data_through": "2024-06-15",
                        "evidence_frozen_at": "2024-06-15",
                        "sha256": _sha256_for_text(archive_contents),
                        "notes": "Track A coverage regression fixture.",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "program_universe.csv").write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-future-stage-phase-3-or-registration,example-asset,Example Asset,asset:example-asset,[],'
            'DISC1,"[""DISC1""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_3_or_registration,included,checked_in_event_history,high,'
            '"[""example-phase-2-2024"", ""example-approval-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,Track A coverage regression row.\n"
        ),
        encoding="utf-8",
    )
    (fixture_dir / "events.csv").write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-phase-2-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-06-16,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Phase 2 signal,1\n"
            "example-approval-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,approved,regulatory_approval,2024-09-01,"
            "approved_for_adults_with_schizophrenia,not_applicable_nonfailure,high,Approval,2\n"
        ),
        encoding="utf-8",
    )

    slice_specs = _build_public_slice_specs(_default_track_a_contract(fixture_dir))

    assert [slice_spec.principal_positive_entity_count for slice_spec in slice_specs] == [
        1,
        1,
        1,
    ]
    assert [
        slice_spec.principal_current_baseline_compatible_entity_count
        for slice_spec in slice_specs
    ] == [1, 1, 1]
    assert _coverage_limitation(
        slice_specs=slice_specs,
        as_of_date="2024-06-20",
        benchmark_task_id="scz_translational_task",
    ) == ""


def test_track_a_replay_requires_at_least_one_eligible_archive_descriptor(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "empty_archive_track_a_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    (fixture_dir / "snapshot_request.json").write_text(
        json.dumps(
            {
                "snapshot_id": "scz_fixture_2024_06_20",
                "cohort_id": "scz_fixture_small",
                "benchmark_suite_id": "scz_translational_suite",
                "benchmark_task_id": "scz_translational_task",
                "benchmark_question_id": "scz_translational_ranking_v1",
                "as_of_date": "2024-06-20",
                "outcome_observation_closed_at": "2025-06-30",
                "entity_types": ["gene"],
                "baseline_ids": ["pgc_only"],
                "notes": "No-archive Track A regression fixture",
                "program_universe_file": "program_universe.csv",
                "program_history_events_file": "events.csv",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        "entity_type,entity_id,entity_label\n"
        "gene,GENE_A,Gene A\n",
        encoding="utf-8",
    )
    (fixture_dir / "future_outcomes.csv").write_text(
        "entity_type,entity_id,outcome_label,outcome_date,label_source,label_notes\n"
        "gene,GENE_A,future_schizophrenia_program_started,2024-12-01,fixture,Fixture outcome\n",
        encoding="utf-8",
    )
    archive_contents = (
        "entity_id,entity_label,common_variant_support\n"
        "GENE_A,Gene A,0.9\n"
    )
    archive_file = fixture_dir / "archives" / "pgc" / "pgc_fixture.csv"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text(archive_contents, encoding="utf-8")
    (fixture_dir / "source_archives.json").write_text(
        json.dumps(
            {
                "archives": [
                    {
                        "source_name": "PGC",
                        "source_version": "pgc_fixture",
                        "archive_file": "archives/pgc/pgc_fixture.csv",
                        "archive_format": "csv",
                        "allowed_data_through": "2024-07-15",
                        "evidence_frozen_at": "2024-07-15",
                        "sha256": _sha256_for_text(archive_contents),
                        "notes": "No eligible pre-cutoff archive descriptor.",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (fixture_dir / "program_universe.csv").write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,"
            "target,target_symbols_json,target_class,target_class_lineage_id,"
            "target_class_aliases_json,mechanism,modality,domain,population,regimen,"
            "stage_bucket,coverage_state,coverage_reason,coverage_confidence,"
            "mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,"
            "discovery_source_id,source_candidate_url,notes\n"
            'example-future-stage-phase-2,example-asset,Example Asset,asset:example-asset,[],'
            'GENE_A,"[""GENE_A""]",example class,target-class:example-class,[],'
            "example mechanism,small_molecule,acute_positive_symptoms,adults with schizophrenia,"
            'monotherapy,phase_2,included,checked_in_event_history,high,'
            '"[""example-phase-2-2024""]",,clinicaltrials_gov,'
            "NCT00000000,https://example.test/study,No-archive Track A regression row.\n"
        ),
        encoding="utf-8",
    )
    (fixture_dir / "events.csv").write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,"
            "event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "example-phase-2-2024,example-asset,Example Sponsor,adults with schizophrenia,"
            "acute_positive_symptoms,monotherapy,phase_2,topline_readout,2024-06-16,"
            "met_primary_endpoint,not_applicable_nonfailure,high,Phase 2 signal,1\n"
        ),
        encoding="utf-8",
    )

    assert _build_public_slice_specs(_default_track_a_contract(fixture_dir)) == ()


def test_custom_registry_slice_round_trips_through_emitted_slice_artifacts(
    tmp_path: Path,
) -> None:
    task_registry_path, _fixture_dir = _write_sparse_custom_registry_fixture(tmp_path)
    output_dir = tmp_path / "public_slices"
    materialize_public_benchmark_slices(
        output_dir=output_dir,
        benchmark_task_id="sparse_fixture_task",
        task_registry_path=task_registry_path,
    )

    slice_dir = output_dir / "sparse_fixture_2024_06_15"
    snapshot_request_payload = json.loads(
        (slice_dir / "snapshot_request.json").read_text(encoding="utf-8")
    )
    assert snapshot_request_payload["task_registry_path"] == "task_registry.csv"
    assert (slice_dir / "task_registry.csv").exists()

    generated_dir = tmp_path / "generated"
    snapshot_manifest_file = generated_dir / "snapshot_manifest.json"
    cohort_labels_file = generated_dir / "cohort_labels.csv"
    runner_output_dir = generated_dir / "runner_outputs"
    reporting_output_dir = generated_dir / "public_payloads"

    materialize_benchmark_snapshot_manifest(
        request_file=slice_dir / "snapshot_request.json",
        archive_index_file=slice_dir / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-03-30",
    )
    snapshot_manifest_payload = json.loads(
        snapshot_manifest_file.read_text(encoding="utf-8")
    )
    assert snapshot_manifest_payload["task_registry_path"] == str(
        (slice_dir / "task_registry.csv").resolve()
    )

    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=slice_dir / "cohort_members.csv",
        future_outcomes_file=slice_dir / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    run_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=slice_dir / "source_archives.json",
        output_dir=runner_output_dir,
        config_file=Path("config/v0.toml").resolve(),
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-03-30T00:00:00Z",
    )
    assert run_result["benchmark_task_id"] == "sparse_fixture_task"
    assert list((runner_output_dir / "run_manifests").glob("*.json"))
    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_output_dir,
        generated_at="2026-03-30T12:00:00Z",
    )
    assert reporting_result["benchmark_task_id"] == "sparse_fixture_task"
    assert reporting_result["report_card_files"]
    assert reporting_result["leaderboard_payload_files"]

    relocated_slice_dir = tmp_path / "relocated_slice"
    shutil.copytree(slice_dir, relocated_slice_dir)
    task_registry_path.unlink()
    relocated_manifest_file = generated_dir / "relocated_snapshot_manifest.json"
    materialize_benchmark_snapshot_manifest(
        request_file=relocated_slice_dir / "snapshot_request.json",
        archive_index_file=relocated_slice_dir / "source_archives.json",
        output_file=relocated_manifest_file,
        materialized_at="2026-03-31",
    )
    relocated_manifest_payload = json.loads(
        relocated_manifest_file.read_text(encoding="utf-8")
    )
    assert relocated_manifest_payload["task_registry_path"] == str(
        (relocated_slice_dir / "task_registry.csv").resolve()
    )
