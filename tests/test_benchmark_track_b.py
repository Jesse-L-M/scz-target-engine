from hashlib import sha256
from pathlib import Path
import shutil

from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_leaderboard import (
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_metrics import (
    read_benchmark_confidence_interval_payload,
)
from scz_target_engine.benchmark_runner import (
    materialize_benchmark_run,
    read_benchmark_model_run_manifest,
)
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.benchmark_track_b import (
    TRACK_B_HORIZON,
    TRACK_B_METRIC_NAMES,
    build_track_b_case_outputs,
    build_track_b_program_memory_dataset,
    load_track_b_casebook,
    read_track_b_case_output_payload,
    read_track_b_confusion_summary,
    track_b_case_output_path,
    track_b_casebook_path_for_archive_index_file,
    track_b_confusion_summary_path,
    track_b_events_path_for_archive_index_file,
    track_b_program_universe_path_for_archive_index_file,
)


TRACK_B_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_failure_memory_2025_02_01"
)


def test_load_track_b_casebook_from_checked_in_fixture() -> None:
    archive_index_file = TRACK_B_FIXTURE_DIR / "source_archives.json"
    cases = load_track_b_casebook(
        track_b_casebook_path_for_archive_index_file(archive_index_file),
        as_of_date="2025-02-01",
        program_universe_path=track_b_program_universe_path_for_archive_index_file(
            archive_index_file
        ),
        events_path=track_b_events_path_for_archive_index_file(archive_index_file),
    )

    assert [case.case_id for case in cases] == [
        "brilaroxazine_acute_phase3",
        "emraclidine_acute_phase2",
        "iclepertin_cognition_phase3",
        "pimavanserin_negative_symptoms_phase3",
        "roluperidone_negative_symptoms_phase3",
        "ulotaront_acute_phase3",
    ]
    assert cases[1].gold_analog_event_ids == (
        "emraclidine-empower-acute-scz-topline-2024",
        "cobenfy-xanomeline-trospium-approval-us-2024",
    )
    assert cases[0].coverage_state_at_cutoff == "unresolved"
    assert cases[4].gold_replay_status == "insufficient_history"


def test_track_b_current_structural_baseline_keeps_sparse_history_explicit() -> None:
    archive_index_file = TRACK_B_FIXTURE_DIR / "source_archives.json"
    cases = load_track_b_casebook(
        track_b_casebook_path_for_archive_index_file(archive_index_file),
        as_of_date="2025-02-01",
        program_universe_path=track_b_program_universe_path_for_archive_index_file(
            archive_index_file
        ),
        events_path=track_b_events_path_for_archive_index_file(archive_index_file),
    )
    dataset = build_track_b_program_memory_dataset(
        as_of_date="2025-02-01",
        events_path=track_b_events_path_for_archive_index_file(archive_index_file),
    )

    case_outputs = build_track_b_case_outputs(
        cases=cases,
        dataset=dataset,
        baseline_id="track_b_structural_current",
    )
    output_index = {case_output.case_id: case_output for case_output in case_outputs}

    assert (
        output_index["roluperidone_negative_symptoms_phase3"].predicted_replay_status
        == "insufficient_history"
    )
    assert (
        output_index["brilaroxazine_acute_phase3"].predicted_replay_status
        == "insufficient_history"
    )
    assert (
        output_index["emraclidine_acute_phase2"].predicted_replay_status
        == "replay_not_supported"
    )
    assert output_index["iclepertin_cognition_phase3"].predicted_required_differences == (
        "failure_scope_resolution",
    )


def test_build_track_b_program_memory_dataset_uses_local_dataset_dir(
    tmp_path: Path,
) -> None:
    dataset_dir = tmp_path / "track_b_dataset"
    dataset_dir.mkdir()
    (dataset_dir / "assets.csv").write_text(
        (
            "asset_id,molecule,target,target_symbols_json,target_class,mechanism,modality\n"
            'local-asset,localmol,GENE1,"[""GENE1""]",local class,local mechanism,small_molecule\n'
        ),
        encoding="utf-8",
    )
    (dataset_dir / "events.csv").write_text(
        (
            "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n"
            "local-event,local-asset,Local Sponsor,adults,acute_positive_symptoms,monotherapy,phase_2,topline,2025-01-15,negative,unresolved,medium,Local note,1\n"
        ),
        encoding="utf-8",
    )
    (dataset_dir / "event_provenance.csv").write_text(
        (
            "event_id,source_tier,source_url\n"
            "local-event,press_release,https://example.com/local-event\n"
        ),
        encoding="utf-8",
    )
    (dataset_dir / "directionality_hypotheses.csv").write_text(
        (
            "hypothesis_id,entity_id,entity_label,desired_perturbation_direction,modality_hypothesis,preferred_modalities_json,confidence,ambiguity,evidence_basis,supporting_event_ids_json,contradiction_conditions_json,falsification_conditions_json,open_risks_json,sort_order\n"
            'local-hypothesis,GENE1,Gene 1,increase_activity,small_molecule,"[""small_molecule""]",medium,,Local basis,"[""local-event""]","[]","[]","[]",1\n'
        ),
        encoding="utf-8",
    )
    (dataset_dir / "program_universe.csv").write_text(
        (
            "program_universe_id,asset_id,asset_name,asset_lineage_id,asset_aliases_json,target,target_symbols_json,target_class,target_class_lineage_id,target_class_aliases_json,mechanism,modality,domain,population,regimen,stage_bucket,coverage_state,coverage_reason,coverage_confidence,mapped_event_ids_json,duplicate_of_program_universe_id,discovery_source_type,discovery_source_id,source_candidate_url,notes\n"
            'local-program,local-asset,localmol,asset:local-asset,[],GENE1,"[""GENE1""]",local class,target-class:local-class,[],local mechanism,small_molecule,acute_positive_symptoms,adults,monotherapy,phase_2,included,checked_in_event_history,high,"[""local-event""]",,clinicaltrials_gov,NCT00000000,https://clinicaltrials.gov/study/NCT00000000,Local program row\n'
        ),
        encoding="utf-8",
    )

    dataset = build_track_b_program_memory_dataset(
        as_of_date="2025-02-01",
        events_path=dataset_dir / "events.csv",
    )

    assert tuple(asset.asset_id for asset in dataset.assets) == ("local-asset",)
    assert tuple(event.event_id for event in dataset.events) == ("local-event",)
    assert tuple(provenance.event_id for provenance in dataset.provenances) == (
        "local-event",
    )
    assert tuple(row.program_universe_id for row in dataset.program_universe_rows) == (
        "local-program",
    )


def test_track_b_fixture_runs_snapshot_to_reporting(tmp_path: Path) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_dir = tmp_path / "public_payloads"
    materialize_benchmark_snapshot_manifest(
        request_file=TRACK_B_FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=TRACK_B_FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=TRACK_B_FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )

    run_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )

    assert run_result["benchmark_task_id"] == "scz_failure_memory_track_b_task"
    assert run_result["executed_baselines"] == [
        "track_b_exact_target",
        "track_b_target_class",
        "track_b_nearest_history",
        "track_b_structural_current",
    ]
    assert len(run_result["run_manifest_files"]) == 4
    assert len(run_result["metric_payload_files"]) == 16
    assert len(run_result["confidence_interval_files"]) == 16
    assert len(run_result["track_b_case_output_files"]) == 4
    assert len(run_result["track_b_confusion_summary_files"]) == 4

    structural_manifest_path = next(
        Path(path)
        for path in run_result["run_manifest_files"]
        if "track_b_structural_current" in path
    )
    structural_manifest = read_benchmark_model_run_manifest(structural_manifest_path)
    assert structural_manifest.benchmark_task_id == "scz_failure_memory_track_b_task"
    assert {
        artifact.artifact_name for artifact in structural_manifest.input_artifacts
    } >= {
        "track_b_casebook",
        "track_b_program_history_events",
        "track_b_program_universe",
        "program_memory_assets",
        "program_memory_event_provenance",
        "program_memory_directionality_hypotheses",
    }

    structural_run_id = structural_manifest.run_id
    case_output_payload = read_track_b_case_output_payload(
        track_b_case_output_path(runner_output_dir, run_id=structural_run_id)
    )
    confusion_summary = read_track_b_confusion_summary(
        track_b_confusion_summary_path(runner_output_dir, run_id=structural_run_id)
    )
    assert len(case_output_payload.cases) == 6
    assert confusion_summary.case_count == 6
    assert confusion_summary.analog_evaluable_case_count == 4

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_dir,
        generated_at="2026-04-05T00:00:00Z",
    )

    assert reporting_result["benchmark_task_id"] == "scz_failure_memory_track_b_task"
    assert len(reporting_result["report_card_files"]) == 4
    assert len(reporting_result["leaderboard_payload_files"]) == 4
    assert len(reporting_result["error_analysis_files"]) == 8

    report_card = read_benchmark_report_card_payload(
        Path(reporting_result["report_card_files"][0])
    )
    assert report_card.benchmark_task_id == "scz_failure_memory_track_b_task"
    assert len(report_card.slices) == 1
    assert report_card.slices[0].entity_type == "intervention_object"
    assert report_card.slices[0].horizon == TRACK_B_HORIZON
    assert report_card.slices[0].admissible_entity_count == 6
    assert report_card.slices[0].positive_entity_count == 1
    assert report_card.slices[0].covered_entity_count == 4
    assert {
        metric.metric_name for metric in report_card.slices[0].metrics
    } == set(TRACK_B_METRIC_NAMES)

    leaderboard = read_benchmark_leaderboard_payload(
        Path(reporting_result["leaderboard_payload_files"][0])
    )
    assert leaderboard.benchmark_task_id == "scz_failure_memory_track_b_task"
    assert leaderboard.entity_type == "intervention_object"
    assert leaderboard.horizon == TRACK_B_HORIZON
    assert leaderboard.metric_name in TRACK_B_METRIC_NAMES
    assert len(leaderboard.entries) == 4

    interval_payload = read_benchmark_confidence_interval_payload(
        next(
            Path(path)
            for path in run_result["confidence_interval_files"]
            if structural_run_id in path and "analog_recall_at_3.json" in path
        )
    )
    expected_seed = 17 + int.from_bytes(
        sha256(
            f"track_b_structural_current:{TRACK_B_HORIZON}".encode("utf-8")
        ).digest()[:4],
        "big",
    )
    assert interval_payload.random_seed == expected_seed


def test_track_b_reporting_uses_runner_emitted_case_outputs(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "fixture"
    shutil.copytree(TRACK_B_FIXTURE_DIR, fixture_dir)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    reporting_dir = tmp_path / "public_payloads"

    materialize_benchmark_snapshot_manifest(
        request_file=fixture_dir / "snapshot_request.json",
        archive_index_file=fixture_dir / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    materialize_benchmark_cohort_labels(
        manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
        manifest_file=snapshot_manifest_file,
        cohort_members_file=fixture_dir / "cohort_members.csv",
        future_outcomes_file=fixture_dir / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    run_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=fixture_dir / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )

    mutated_casebook = (fixture_dir / "track_b_casebook.csv").read_text(encoding="utf-8")
    (fixture_dir / "track_b_casebook.csv").write_text(
        mutated_casebook.replace("replay_supported", "replay_not_supported", 1),
        encoding="utf-8",
    )

    reporting_result = materialize_benchmark_reporting(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        runner_output_dir=runner_output_dir,
        output_dir=reporting_dir,
        generated_at="2026-04-05T00:00:00Z",
    )

    report_card = read_benchmark_report_card_payload(
        Path(reporting_result["report_card_files"][0])
    )
    case_output_payload = read_track_b_case_output_payload(
        track_b_case_output_path(
            runner_output_dir,
            run_id=next(
                read_benchmark_model_run_manifest(Path(path)).run_id
                for path in run_result["run_manifest_files"]
                if "track_b_structural_current" in path
            ),
        )
    )
    expected_positive_count = sum(
        1
        for case_output in case_output_payload.cases
        if case_output.gold_replay_status == "replay_supported"
    )
    assert report_card.slices[0].positive_entity_count == expected_positive_count
