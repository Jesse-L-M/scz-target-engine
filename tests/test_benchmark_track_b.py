from hashlib import sha256
import json
from pathlib import Path
import shutil

import pytest

from scz_target_engine.benchmark_labels import (
    benchmark_cohort_manifest_path_for_labels_file,
    benchmark_track_b_auxiliary_source_artifact_path_for_labels_file,
    benchmark_track_b_source_archives_dir_for_labels_file,
    materialize_benchmark_cohort_labels,
    read_benchmark_cohort_manifest,
    read_benchmark_cohort_labels,
)
from scz_target_engine.benchmark_leaderboard import (
    materialize_benchmark_reporting,
    read_benchmark_leaderboard_payload,
    read_benchmark_report_card_payload,
)
from scz_target_engine.benchmark_metrics import (
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
)
from scz_target_engine.benchmark_runner import (
    derive_track_b_slice_random_seed,
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
    TRACK_B_RUN_PARAMETERIZATION_FIELDS,
    TrackBAnalogCandidate,
    TrackBCaseOutput,
    TrackBCaseOutputPayload,
    build_track_b_case_outputs,
    build_track_b_confusion_summary,
    build_track_b_error_analysis_markdown,
    build_track_b_program_memory_dataset,
    estimate_track_b_metric_intervals,
    load_track_b_casebook,
    read_track_b_case_output_payload,
    read_track_b_confusion_summary,
    track_b_case_output_path,
    track_b_casebook_path_for_archive_index_file,
    track_b_confusion_summary_path,
    track_b_events_path_for_archive_index_file,
    track_b_program_universe_path_for_archive_index_file,
    validate_track_b_case_output_payload,
    validate_track_b_case_output_payload_against_expected_cases,
)


TRACK_B_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "benchmark"
    / "fixtures"
    / "scz_failure_memory_2025_02_01"
)


def _build_track_b_analog_candidate(event_id: str) -> TrackBAnalogCandidate:
    return TrackBAnalogCandidate(
        event_id=event_id,
        asset_id=f"asset-{event_id}",
        molecule="synthetic molecule",
        target="GENE1",
        target_class="synthetic class",
        domain="acute_positive_symptoms",
        population="adults",
        mono_or_adjunct="monotherapy",
        phase="phase_2",
        event_date="2024-01-01",
        primary_outcome_result="did_not_meet_primary_endpoint",
        failure_reason_taxonomy="unresolved",
        failure_scope="unresolved",
        match_tier="nearest_history",
        biological_anchor=False,
        source_tier="company_press_release",
        source_url=f"https://example.com/{event_id}",
        match_dimensions=("domain",),
    )


def _build_track_b_case_output(
    *,
    case_id: str,
    analog_recall_at_3: float | None,
    failure_scope_exact_match: bool = True,
    replay_status_exact_match: bool = True,
    checklist_f1: float = 1.0,
) -> TrackBCaseOutput:
    gold_required_differences = ("failure_scope_resolution",)
    predicted_required_differences = (
        gold_required_differences
        if checklist_f1 == 1.0
        else ()
    )
    return TrackBCaseOutput(
        case_id=case_id,
        baseline_id="track_b_structural_current",
        proposal_entity_id=case_id,
        proposal_entity_label=case_id.replace("-", " "),
        source_program_universe_id=f"source-{case_id}",
        coverage_state_at_cutoff="included",
        coverage_reason_at_cutoff="checked_in_event_history",
        gold_analog_event_ids=("gold-analog",) if analog_recall_at_3 is not None else (),
        retrieved_analog_event_ids=(
            ("gold-analog",)
            if analog_recall_at_3 == 1.0
            else ("different-analog",)
            if analog_recall_at_3 is not None
            else ()
        ),
        retrieved_analogs=(),
        gold_failure_scope="unresolved",
        predicted_failure_scope=(
            "unresolved"
            if failure_scope_exact_match
            else "population"
        ),
        gold_replay_status="replay_supported",
        predicted_replay_status=(
            "replay_supported"
            if replay_status_exact_match
            else "replay_not_supported"
        ),
        gold_required_differences=gold_required_differences,
        predicted_required_differences=predicted_required_differences,
        analog_recall_at_3=analog_recall_at_3,
        checklist_f1=checklist_f1,
        replay_status_exact_match=replay_status_exact_match,
        failure_scope_exact_match=failure_scope_exact_match,
        reasoning_summary="synthetic track b case output",
    )


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload["cases"][0].__setitem__(
                "proposal_entity_label",
                False,
            ),
            "proposal_entity_label must be a string",
        ),
        (
            lambda payload: payload["cases"][0].__setitem__(
                "analog_recall_at_3",
                "1.0",
            ),
            "analog_recall_at_3 must be a float",
        ),
        (
            lambda payload: payload["cases"][0].__setitem__(
                "replay_status_exact_match",
                "true",
            ),
            "replay_status_exact_match must be a boolean",
        ),
    ),
)
def test_read_track_b_case_output_payload_rejects_malformed_json_types(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    payload = TrackBCaseOutputPayload(
        run_id="fixture_run",
        baseline_id="track_b_structural_current",
        snapshot_id="fixture_snapshot",
        as_of_date="2025-02-01",
        cases=(
            _build_track_b_case_output(
                case_id="fixture_case",
                analog_recall_at_3=1.0,
            ),
        ),
    ).to_dict()
    mutator(payload)
    payload_path = tmp_path / "track_b_case_output.json"
    payload_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_track_b_case_output_payload(payload_path)


@pytest.mark.parametrize(
    ("mutator", "error_fragment"),
    (
        (
            lambda payload: payload.__setitem__("case_count", 1.5),
            "case_count must be an integer",
        ),
        (
            lambda payload: payload["metric_values"].__setitem__(
                "analog_recall_at_3",
                "0.5",
            ),
            r"metric_values\['analog_recall_at_3'\] must be a float",
        ),
    ),
)
def test_read_track_b_confusion_summary_rejects_malformed_json_types(
    tmp_path: Path,
    mutator: object,
    error_fragment: str,
) -> None:
    case_outputs = (
        _build_track_b_case_output(
            case_id="fixture_case",
            analog_recall_at_3=1.0,
        ),
    )
    payload = build_track_b_confusion_summary(
        run_id="fixture_run",
        baseline_id="track_b_structural_current",
        snapshot_id="fixture_snapshot",
        case_outputs=case_outputs,
    ).to_dict()
    mutator(payload)
    payload_path = tmp_path / "track_b_confusion_summary.json"
    payload_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=error_fragment):
        read_track_b_confusion_summary(payload_path)


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


def test_track_b_casebook_alignment_is_enforced_during_cohort_materialization(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "fixture"
    shutil.copytree(TRACK_B_FIXTURE_DIR, fixture_dir)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"

    materialize_benchmark_snapshot_manifest(
        request_file=fixture_dir / "snapshot_request.json",
        archive_index_file=fixture_dir / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    (fixture_dir / "cohort_members.csv").write_text(
        (
            "entity_type,entity_id,entity_label\n"
            "intervention_object,emraclidine-acute-positive-symptoms-monotherapy-phase-2,"
            "emraclidine | acute positive symptoms | phase_2\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B cohort members must match track_b_casebook.csv",
    ):
        materialize_benchmark_cohort_labels(
            manifest=read_benchmark_snapshot_manifest(snapshot_manifest_file),
            manifest_file=snapshot_manifest_file,
            cohort_members_file=fixture_dir / "cohort_members.csv",
            future_outcomes_file=fixture_dir / "future_outcomes.csv",
            output_file=tmp_path / "cohort_labels.csv",
        )


def test_track_b_cohort_labels_use_casebook_entity_surface(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    materialize_benchmark_snapshot_manifest(
        request_file=TRACK_B_FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    materialize_benchmark_cohort_labels(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members_file=TRACK_B_FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=TRACK_B_FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )

    labels = read_benchmark_cohort_labels(cohort_labels_file)
    observed_status_by_entity = {
        label.entity_id: label.label_name
        for label in labels
        if label.label_value == "true"
    }
    assert manifest.benchmark_question_id == "scz_failure_memory_track_b_v1"
    assert {label.horizon for label in labels} == {TRACK_B_HORIZON}
    assert set(observed_status_by_entity) == {
        "brilaroxazine-acute-positive-symptoms-monotherapy-phase-3-or-registration",
        "emraclidine-acute-positive-symptoms-monotherapy-phase-2",
        "iclepertin-cognition-adjunct-phase-3-or-registration",
        "pimavanserin-negative-symptoms-adjunct-phase-3-or-registration",
        "roluperidone-negative-symptoms-monotherapy-phase-3-or-registration",
        "ulotaront-acute-positive-symptoms-monotherapy-phase-3-or-registration",
    }
    assert observed_status_by_entity == {
        "brilaroxazine-acute-positive-symptoms-monotherapy-phase-3-or-registration": "insufficient_history",
        "emraclidine-acute-positive-symptoms-monotherapy-phase-2": "replay_not_supported",
        "iclepertin-cognition-adjunct-phase-3-or-registration": "replay_inconclusive",
        "pimavanserin-negative-symptoms-adjunct-phase-3-or-registration": "replay_inconclusive",
        "roluperidone-negative-symptoms-monotherapy-phase-3-or-registration": "insufficient_history",
        "ulotaront-acute-positive-symptoms-monotherapy-phase-3-or-registration": "replay_supported",
    }
    cohort_manifest = read_benchmark_cohort_manifest(
        benchmark_cohort_manifest_path_for_labels_file(cohort_labels_file)
    )
    assert {
        artifact.artifact_name
        for artifact in cohort_manifest.auxiliary_source_artifacts
    } == {
        "source_archive_index",
        "track_b_casebook",
        "track_b_program_universe",
        "track_b_program_history_events",
        "program_memory_assets",
        "program_memory_event_provenance",
        "program_memory_directionality_hypotheses",
    }
    assert {
        artifact.artifact_name: (
            benchmark_cohort_manifest_path_for_labels_file(cohort_labels_file).parent
            / artifact.artifact_path
        ).resolve()
        for artifact in cohort_manifest.auxiliary_source_artifacts
    } == {
        artifact_name: benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
            cohort_labels_file,
            artifact_name=artifact_name,
        )
        for artifact_name in {
            "source_archive_index",
            "track_b_casebook",
            "track_b_program_universe",
            "track_b_program_history_events",
            "program_memory_assets",
            "program_memory_event_provenance",
            "program_memory_directionality_hypotheses",
        }
    }
    assert benchmark_track_b_source_archives_dir_for_labels_file(
        cohort_labels_file
    ).exists()


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
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)
    assert manifest.benchmark_question_id == "scz_failure_memory_track_b_v1"
    materialize_benchmark_cohort_labels(
        manifest=manifest,
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
    assert {
        artifact.artifact_name for artifact in structural_manifest.input_artifacts
    }.isdisjoint({"engine_config"})

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
    assert {
        artifact.artifact_name for artifact in report_card.evaluation_input_artifacts
    } == {
        "benchmark_snapshot_manifest",
        "benchmark_cohort_manifest",
        "benchmark_cohort_members",
        "benchmark_source_cohort_members",
        "benchmark_source_future_outcomes",
        "benchmark_cohort_labels",
        "source_archive_index",
        "track_b_casebook",
        "track_b_program_universe",
        "track_b_program_history_events",
        "program_memory_assets",
        "program_memory_event_provenance",
        "program_memory_directionality_hypotheses",
    }
    track_b_casebook_input = next(
        artifact
        for artifact in report_card.evaluation_input_artifacts
        if artifact.artifact_name == "track_b_casebook"
    )
    assert not Path(track_b_casebook_input.artifact_path).is_absolute()
    public_casebook_path = (
        Path(reporting_result["report_card_files"][0]).parent
        / Path(track_b_casebook_input.artifact_path)
    ).resolve()
    bundled_casebook_path = benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
        cohort_labels_file,
        artifact_name="track_b_casebook",
    )
    assert public_casebook_path != bundled_casebook_path
    assert public_casebook_path.name == "track_b_casebook.csv"
    assert {"validated_track_b_runner_bundle", "inputs"}.issubset(
        set(public_casebook_path.parts)
    )
    assert public_casebook_path.read_bytes() == bundled_casebook_path.read_bytes()
    assert len(report_card.slices) == 1
    assert report_card.run_parameterization is not None
    assert set(report_card.run_parameterization) == set(
        TRACK_B_RUN_PARAMETERIZATION_FIELDS
    )
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
    expected_seed = derive_track_b_slice_random_seed(
        base_random_seed=17,
        baseline_id="track_b_structural_current",
    )
    assert interval_payload.random_seed == expected_seed

    run_id_by_baseline = {
        read_benchmark_model_run_manifest(Path(path)).baseline_id: (
            read_benchmark_model_run_manifest(Path(path)).run_id
        )
        for path in run_result["run_manifest_files"]
    }
    report_card_by_baseline = {
        read_benchmark_report_card_payload(Path(path)).baseline_id: (
            read_benchmark_report_card_payload(Path(path))
        )
        for path in reporting_result["report_card_files"]
    }
    replay_status_leaderboard = read_benchmark_leaderboard_payload(
        next(
            Path(path)
            for path in reporting_result["leaderboard_payload_files"]
            if path.endswith("replay_status_exact_match.json")
        )
    )
    leaderboard_entry_by_baseline = {
        entry.baseline_id: entry for entry in replay_status_leaderboard.entries
    }

    def _runner_metric_and_interval(
        baseline_id: str,
    ) -> tuple[float, tuple[float, float]]:
        run_id = run_id_by_baseline[baseline_id]
        metric_payload = read_benchmark_metric_output_payload(
            next(
                Path(path)
                for path in run_result["metric_payload_files"]
                if run_id in path and path.endswith("replay_status_exact_match.json")
            )
        )
        interval_payload = read_benchmark_confidence_interval_payload(
            next(
                Path(path)
                for path in run_result["confidence_interval_files"]
                if run_id in path and path.endswith("replay_status_exact_match.json")
            )
        )
        return metric_payload.metric_value, (
            interval_payload.interval_low,
            interval_payload.interval_high,
        )

    nearest_history_value, nearest_history_interval = _runner_metric_and_interval(
        "track_b_nearest_history"
    )
    structural_current_value, structural_current_interval = _runner_metric_and_interval(
        "track_b_structural_current"
    )
    assert nearest_history_value < structural_current_value

    nearest_history_metric = next(
        metric
        for metric in report_card_by_baseline["track_b_nearest_history"].slices[0].metrics
        if metric.metric_name == "replay_status_exact_match"
    )
    structural_current_metric = next(
        metric
        for metric in report_card_by_baseline["track_b_structural_current"].slices[0].metrics
        if metric.metric_name == "replay_status_exact_match"
    )
    assert nearest_history_metric.metric_value == nearest_history_value
    assert (
        nearest_history_metric.interval_low,
        nearest_history_metric.interval_high,
    ) == nearest_history_interval
    assert structural_current_metric.metric_value == structural_current_value
    assert (
        structural_current_metric.interval_low,
        structural_current_metric.interval_high,
    ) == structural_current_interval
    assert (
        report_card_by_baseline["track_b_nearest_history"].run_id
        != run_id_by_baseline["track_b_nearest_history"]
    )
    assert (
        leaderboard_entry_by_baseline["track_b_nearest_history"].run_id
        == report_card_by_baseline["track_b_nearest_history"].run_id
    )

    assert (
        leaderboard_entry_by_baseline["track_b_nearest_history"].metric_value
        == nearest_history_value
    )
    assert (
        leaderboard_entry_by_baseline["track_b_nearest_history"].interval_low,
        leaderboard_entry_by_baseline["track_b_nearest_history"].interval_high,
    ) == nearest_history_interval
    assert (
        leaderboard_entry_by_baseline["track_b_structural_current"].metric_value
        == structural_current_value
    )
    assert (
        leaderboard_entry_by_baseline["track_b_structural_current"].interval_low,
        leaderboard_entry_by_baseline["track_b_structural_current"].interval_high,
    ) == structural_current_interval

    nearest_history_error_analysis = next(
        Path(path)
        for path in reporting_result["error_analysis_files"]
        if path.endswith(".md")
        and report_card_by_baseline["track_b_nearest_history"].report_card_id in path
    ).read_text(encoding="utf-8")
    assert (
        f"- `replay_status_exact_match`: {nearest_history_value:.3f} "
        f"[{nearest_history_interval[0]:.3f}, {nearest_history_interval[1]:.3f}]"
    ) in nearest_history_error_analysis
    assert run_id_by_baseline["track_b_nearest_history"] not in nearest_history_error_analysis


def test_track_b_cohort_materialization_preserves_auxiliary_sources_but_runner_requires_task_id(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

    materialize_benchmark_snapshot_manifest(
        request_file=TRACK_B_FIXTURE_DIR / "snapshot_request.json",
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_file=snapshot_manifest_file,
        materialized_at="2026-04-05",
    )
    snapshot_payload = json.loads(snapshot_manifest_file.read_text(encoding="utf-8"))
    del snapshot_payload["benchmark_task_id"]
    snapshot_manifest_file.write_text(
        json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = read_benchmark_snapshot_manifest(snapshot_manifest_file)

    materialize_benchmark_cohort_labels(
        manifest=manifest,
        manifest_file=snapshot_manifest_file,
        cohort_members_file=TRACK_B_FIXTURE_DIR / "cohort_members.csv",
        future_outcomes_file=TRACK_B_FIXTURE_DIR / "future_outcomes.csv",
        output_file=cohort_labels_file,
    )
    cohort_manifest = read_benchmark_cohort_manifest(
        benchmark_cohort_manifest_path_for_labels_file(cohort_labels_file)
    )
    assert {
        artifact.artifact_name
        for artifact in cohort_manifest.auxiliary_source_artifacts
    } == {
        "source_archive_index",
        "track_b_casebook",
        "track_b_program_universe",
        "track_b_program_history_events",
        "program_memory_assets",
        "program_memory_event_provenance",
        "program_memory_directionality_hypotheses",
    }

    with pytest.raises(
        ValueError,
        match="Track B snapshot manifests must pin benchmark_suite_id and benchmark_task_id",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
            output_dir=runner_output_dir,
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-05T00:00:00Z",
        )


def test_track_b_reporting_ignores_mutated_original_source_fixture(
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
    assert len(reporting_result["report_card_files"]) == 4


def test_track_b_reporting_rejects_mutated_bundled_source_fixture(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

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
    materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=TRACK_B_FIXTURE_DIR / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )

    bundled_casebook = benchmark_track_b_auxiliary_source_artifact_path_for_labels_file(
        cohort_labels_file,
        artifact_name="track_b_casebook",
    )
    bundled_casebook.write_text(
        bundled_casebook.read_text(encoding="utf-8").replace(
            "replay_supported",
            "replay_not_supported",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="benchmark cohort auxiliary source artifact sha256 does not match",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=tmp_path / "public_payloads",
            generated_at="2026-04-05T00:00:00Z",
        )


def test_track_b_runner_uses_cohort_pinned_source_fixture(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "fixture"
    alt_fixture_dir = tmp_path / "alt-fixture"
    shutil.copytree(TRACK_B_FIXTURE_DIR, fixture_dir)
    shutil.copytree(TRACK_B_FIXTURE_DIR, alt_fixture_dir)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

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

    mutated_casebook = (alt_fixture_dir / "track_b_casebook.csv").read_text(
        encoding="utf-8"
    )
    (alt_fixture_dir / "track_b_casebook.csv").write_text(
        mutated_casebook.replace("replay_supported", "replay_not_supported", 1),
        encoding="utf-8",
    )

    run_result = materialize_benchmark_run(
        manifest_file=snapshot_manifest_file,
        cohort_labels_file=cohort_labels_file,
        archive_index_file=alt_fixture_dir / "source_archives.json",
        output_dir=runner_output_dir,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )

    structural_manifest = read_benchmark_model_run_manifest(
        next(
            Path(path)
            for path in run_result["run_manifest_files"]
            if "track_b_structural_current" in path
        )
    )
    track_b_casebook_artifact = next(
        artifact
        for artifact in structural_manifest.input_artifacts
        if artifact.artifact_name == "track_b_casebook"
    )
    assert Path(track_b_casebook_artifact.artifact_path) == (
        cohort_labels_file.parent / "track_b_casebook.csv"
    ).resolve()
    structural_case_output_payload = read_track_b_case_output_payload(
        track_b_case_output_path(runner_output_dir, run_id=structural_manifest.run_id)
    )
    assert sum(
        1
        for case_output in structural_case_output_payload.cases
        if case_output.gold_replay_status == "replay_supported"
    ) == 1


def test_track_b_run_rejects_tampered_cohort_source_archive_index_path(
    tmp_path: Path,
) -> None:
    fixture_dir = tmp_path / "fixture"
    alt_fixture_dir = tmp_path / "alt-fixture"
    shutil.copytree(TRACK_B_FIXTURE_DIR, fixture_dir)
    shutil.copytree(TRACK_B_FIXTURE_DIR, alt_fixture_dir)
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    cohort_manifest_file = benchmark_cohort_manifest_path_for_labels_file(
        cohort_labels_file
    )

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

    cohort_manifest_payload = json.loads(cohort_manifest_file.read_text(encoding="utf-8"))
    for artifact in cohort_manifest_payload["auxiliary_source_artifacts"]:
        if artifact["artifact_name"] == "source_archive_index":
            artifact["artifact_path"] = str(alt_fixture_dir / "source_archives.json")
    cohort_manifest_file.write_text(
        json.dumps(cohort_manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="canonical source_archive_index artifact beside cohort labels",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=fixture_dir / "source_archives.json",
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-05T00:00:00Z",
        )


def test_track_b_run_rejects_invalid_archive_index(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    invalid_archive_index_file = tmp_path / "invalid_source_archives.json"

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
    invalid_archive_index_file.write_text("{}", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="source archive index must define an archives list",
    ):
        materialize_benchmark_run(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=invalid_archive_index_file,
            output_dir=tmp_path / "runner_outputs",
            bootstrap_iterations=25,
            deterministic_test_mode=True,
            code_version="fixture-sha",
            execution_timestamp="2026-04-05T00:00:00Z",
        )


def test_track_b_reporting_rejects_tampered_metric_payload(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"

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

    metric_payload_path = next(
        Path(path)
        for path in run_result["metric_payload_files"]
        if "track_b_structural_current" in path
        and path.endswith("analog_recall_at_3.json")
    )
    metric_payload = json.loads(metric_payload_path.read_text(encoding="utf-8"))
    metric_payload["metric_value"] = 999.0
    metric_payload_path.write_text(
        json.dumps(metric_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Track B metric payload does not match runner case outputs",
    ):
        materialize_benchmark_reporting(
            manifest_file=snapshot_manifest_file,
            cohort_labels_file=cohort_labels_file,
            runner_output_dir=runner_output_dir,
            output_dir=tmp_path / "public_payloads",
            generated_at="2026-04-05T00:00:00Z",
        )


def test_track_b_intervals_skip_nonevaluable_analog_resamples() -> None:
    case_outputs = (
        _build_track_b_case_output(case_id="evaluable", analog_recall_at_3=1.0),
        _build_track_b_case_output(case_id="nonevaluable", analog_recall_at_3=None),
    )

    metric_intervals = estimate_track_b_metric_intervals(
        case_outputs,
        iterations=64,
        confidence_level=0.95,
        random_seed=11,
    )

    assert metric_intervals["analog_recall_at_3"] == (1.0, 1.0, 1.0)


def test_track_b_error_analysis_surfaces_analog_only_mismatches() -> None:
    case_outputs = (
        _build_track_b_case_output(case_id="analog-only", analog_recall_at_3=0.0),
    )
    confusion_summary = build_track_b_confusion_summary(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        case_outputs=case_outputs,
    )
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="2025-02-01",
        cases=case_outputs,
    )
    markdown = build_track_b_error_analysis_markdown(
        payload=payload,
        confusion_summary=confusion_summary,
        metric_intervals={
            metric_name: (0.0, 0.0, 0.0)
            for metric_name in TRACK_B_METRIC_NAMES
        },
    )

    assert confusion_summary.mismatched_case_ids == ("analog-only",)
    assert "- analog recall@3: 0.000" in markdown


def test_validate_track_b_case_output_payload_rejects_stale_analog_recall() -> None:
    case_payload = _build_track_b_case_output(
        case_id="tampered-analog",
        analog_recall_at_3=1.0,
    ).to_dict()
    case_payload["retrieved_analog_event_ids"] = ["fake_event_1"]
    case_payload["retrieved_analogs"] = [
        _build_track_b_analog_candidate("fake_event_1").to_dict()
    ]
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="2025-02-01",
        cases=(TrackBCaseOutput.from_dict(case_payload),),
    )

    with pytest.raises(
        ValueError,
        match="Track B case output analog_recall_at_3 does not match analog event ids",
    ):
        validate_track_b_case_output_payload(
            payload,
            expected_as_of_date="2025-02-01",
        )


def test_validate_track_b_case_output_payload_rejects_non_evaluable_retrieved_ids() -> None:
    case_payload = _build_track_b_case_output(
        case_id="tampered-non-evaluable",
        analog_recall_at_3=None,
    ).to_dict()
    case_payload["retrieved_analog_event_ids"] = ["fake_event_x", "fake_event_y"]
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="2025-02-01",
        cases=(TrackBCaseOutput.from_dict(case_payload),),
    )

    with pytest.raises(
        ValueError,
        match="Track B case output retrieved_analog_event_ids do not match retrieved_analogs",
    ):
        validate_track_b_case_output_payload(
            payload,
            expected_as_of_date="2025-02-01",
        )


def test_validate_track_b_case_output_payload_rejects_excess_retrieved_analogs() -> None:
    case_payload = _build_track_b_case_output(
        case_id="tampered-too-many-analogs",
        analog_recall_at_3=None,
    ).to_dict()
    event_ids = ("event-1", "event-2", "event-3", "event-4")
    case_payload["retrieved_analog_event_ids"] = list(event_ids)
    case_payload["retrieved_analogs"] = [
        _build_track_b_analog_candidate(event_id).to_dict()
        for event_id in event_ids
    ]
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="2025-02-01",
        cases=(TrackBCaseOutput.from_dict(case_payload),),
    )

    with pytest.raises(
        ValueError,
        match="Track B case output retrieved_analogs exceed the Track B retrieval limit",
    ):
        validate_track_b_case_output_payload(
            payload,
            expected_as_of_date="2025-02-01",
        )


def test_validate_track_b_case_output_payload_against_expected_cases_rejects_fabricated_retrieved_analogs() -> None:
    expected_case_payload = _build_track_b_case_output(
        case_id="tampered-fake-analogs",
        analog_recall_at_3=None,
    ).to_dict()
    expected_event_ids = ("event-1", "event-2", "event-3")
    expected_case_payload["retrieved_analog_event_ids"] = list(expected_event_ids)
    expected_case_payload["retrieved_analogs"] = [
        _build_track_b_analog_candidate(event_id).to_dict()
        for event_id in expected_event_ids
    ]
    expected_case = TrackBCaseOutput.from_dict(expected_case_payload)

    tampered_case_payload = expected_case.to_dict()
    tampered_event_ids = ("fake_event_a", "fake_event_b", "fake_event_c")
    tampered_case_payload["retrieved_analog_event_ids"] = list(tampered_event_ids)
    tampered_case_payload["retrieved_analogs"] = [
        _build_track_b_analog_candidate(event_id).to_dict()
        for event_id in tampered_event_ids
    ]
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="2025-02-01",
        cases=(TrackBCaseOutput.from_dict(tampered_case_payload),),
    )

    with pytest.raises(
        ValueError,
        match="Track B case output does not match the pinned source artifacts",
    ):
        validate_track_b_case_output_payload_against_expected_cases(
            payload,
            expected_cases=(expected_case,),
        )


def test_validate_track_b_case_output_payload_rejects_as_of_date_mismatch() -> None:
    payload = TrackBCaseOutputPayload(
        run_id="run-1",
        baseline_id="track_b_structural_current",
        snapshot_id="scz_failure_memory_2025_02_01",
        as_of_date="1999-01-01",
        cases=(
            _build_track_b_case_output(
                case_id="date-mismatch",
                analog_recall_at_3=1.0,
            ),
        ),
    )

    with pytest.raises(
        ValueError,
        match="Track B case output payload as_of_date does not match snapshot manifest",
    ):
        validate_track_b_case_output_payload(
            payload,
            expected_as_of_date="2025-02-01",
        )


def test_track_b_run_does_not_require_parsing_engine_config(
    tmp_path: Path,
) -> None:
    snapshot_manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    malformed_config_file = tmp_path / "bad-config.toml"
    malformed_config_file.write_text("weights =", encoding="utf-8")

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
        config_file=malformed_config_file,
        bootstrap_iterations=25,
        deterministic_test_mode=True,
        code_version="fixture-sha",
        execution_timestamp="2026-04-05T00:00:00Z",
    )

    structural_manifest = read_benchmark_model_run_manifest(
        next(
            Path(path)
            for path in run_result["run_manifest_files"]
            if "track_b_structural_current" in path
        )
    )
    assert {
        artifact.artifact_name for artifact in structural_manifest.input_artifacts
    }.isdisjoint({"engine_config"})
