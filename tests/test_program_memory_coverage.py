import csv
import json
from pathlib import Path
from shutil import copytree

import pytest

from scz_target_engine.cli import main
from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.program_memory import (
    PROGRAM_MEMORY_COVERAGE_DENOMINATOR_GAP_FIELDNAMES,
    PROGRAM_MEMORY_COVERAGE_DENOMINATOR_SUMMARY_FIELDNAMES,
    PROGRAM_MEMORY_COVERAGE_GAP_FIELDNAMES,
    PROGRAM_MEMORY_COVERAGE_SCOPE_GAP_FIELDNAMES,
    PROGRAM_MEMORY_COVERAGE_SCOPE_SUMMARY_FIELDNAMES,
    PROGRAM_MEMORY_COVERAGE_SUMMARY_FIELDNAMES,
    ProgramMemoryDataset,
    build_program_memory_coverage_audit,
    build_program_memory_coverage_focus_report,
    load_program_memory_dataset,
    materialize_program_memory_coverage_denominator_gap_rows,
    materialize_program_memory_coverage_denominator_summary_rows,
    materialize_program_memory_coverage_gap_rows,
    materialize_program_memory_coverage_scope_gap_rows,
    materialize_program_memory_coverage_scope_summary_rows,
    materialize_program_memory_coverage_summary_rows,
    write_program_memory_coverage_outputs,
)


def test_build_program_memory_coverage_audit_distinguishes_sparse_and_incomplete() -> None:
    audit = build_program_memory_coverage_audit(
        Path("data/curated/program_history/v2")
    )
    summaries = {
        (summary.dimension, summary.scope_value): summary for summary in audit.summaries
    }

    chrm4 = summaries[("target", "CHRM4")]
    assert chrm4.coverage_band == "partial"
    assert chrm4.event_count == 2
    assert chrm4.asset_count == 2
    assert chrm4.directionality_hypothesis_count == 1
    assert chrm4.supported_directionality_hypothesis_count == 1
    assert chrm4.gap_codes == ("unresolved_failure_scope",)
    assert "mixed_history" in chrm4.uncertainty_codes

    htr2a = summaries[("target", "HTR2A")]
    assert htr2a.coverage_band == "partial"
    assert "no_directionality_hypothesis" in htr2a.gap_codes

    slc6a1 = summaries[("target", "SLC6A1")]
    assert slc6a1.coverage_band == "thin"
    assert slc6a1.event_count == 0
    assert slc6a1.directionality_hypothesis_count == 1
    assert set(slc6a1.gap_codes) == {
        "low_confidence_directionality",
        "no_checked_in_event_history",
        "unsupported_directionality_hypothesis",
    }

    target_failure_scope = summaries[("failure_scope", "target")]
    assert target_failure_scope.coverage_band == "missing"
    assert target_failure_scope.event_count == 0
    assert target_failure_scope.gap_codes == ("scope_not_yet_adjudicated",)

    assert audit.coverage_manifest["program_universe_row_count"] == 14
    assert audit.coverage_manifest["coverage_state_counts"] == {
        "duplicate": 4,
        "excluded": 1,
        "included": 6,
        "out_of_scope": 1,
        "unresolved": 2,
    }
    assert audit.coverage_manifest["mapped_event_count"] == 7
    assert any(
        row["program_universe_id"]
        == "roluperidone-negative-symptoms-monotherapy-phase-3-or-registration"
        and row["coverage_state"] == "unresolved"
        for row in audit.denominator_gap_rows
    )


def test_program_memory_coverage_audit_restores_absolute_dataset_dir_contract() -> None:
    audit = build_program_memory_coverage_audit(
        Path("data/curated/program_history/v2")
    )

    assert audit.dataset_dir == Path("data/curated/program_history/v2").resolve().as_posix()


def test_checked_in_program_universe_uses_correct_ctgov_studies() -> None:
    dataset = load_program_memory_dataset(Path("data/curated/program_history/v2"))
    rows_by_id = {
        row.program_universe_id: row for row in dataset.program_universe_rows
    }

    brilaroxazine = rows_by_id[
        "brilaroxazine-acute-positive-symptoms-monotherapy-phase-3-or-registration"
    ]
    assert brilaroxazine.discovery_source_id == "NCT05184335"
    assert (
        brilaroxazine.source_candidate_url
        == "https://clinicaltrials.gov/study/NCT05184335"
    )

    karxt_ad_agitation = rows_by_id[
        "xanomeline-trospium-alzheimers-agitation-monotherapy-phase-3"
    ]
    assert karxt_ad_agitation.discovery_source_id == "NCT07011732"
    assert (
        karxt_ad_agitation.source_candidate_url
        == "https://clinicaltrials.gov/study/NCT07011732"
    )


def test_program_memory_coverage_focus_report_keeps_chrm4_provenance() -> None:
    audit = build_program_memory_coverage_audit(
        Path("data/curated/program_history/v2")
    )
    focus = build_program_memory_coverage_focus_report(audit, target="CHRM4")

    assert focus.request == {"target": "CHRM4"}
    assert [summary.scope_value for summary in focus.matched_summaries] == ["CHRM4"]
    assert {evidence.record_kind for evidence in focus.matched_evidence} == {
        "directionality_hypothesis",
        "event",
    }

    event_row = next(
        evidence
        for evidence in focus.matched_evidence
        if evidence.record_kind == "event"
        and evidence.record_id == "emraclidine-empower-acute-scz-topline-2024"
    )
    assert event_row.asset_lineage_id == "asset:emraclidine"
    assert (
        event_row.target_class_lineage_id
        == "target-class:muscarinic-cholinergic-modulation"
    )

    hypothesis_row = next(
        evidence
        for evidence in focus.matched_evidence
        if evidence.record_kind == "directionality_hypothesis"
    )
    assert hypothesis_row.hypothesis_id == "chrm4"
    assert hypothesis_row.supporting_event_ids == (
        "cobenfy-xanomeline-trospium-approval-us-2024",
        "emraclidine-empower-acute-scz-topline-2024",
    )
    assert hypothesis_row.source_tiers == ("regulatory", "company_press_release")


def test_program_memory_coverage_materializers_preserve_scope_contract() -> None:
    audit = build_program_memory_coverage_audit(
        Path("data/curated/program_history/v2")
    )

    summary_rows = materialize_program_memory_coverage_summary_rows(audit)
    scope_summary_rows = materialize_program_memory_coverage_scope_summary_rows(
        audit
    )
    gap_rows = materialize_program_memory_coverage_gap_rows(audit)
    scope_gap_rows = materialize_program_memory_coverage_scope_gap_rows(audit)
    denominator_summary_rows = (
        materialize_program_memory_coverage_denominator_summary_rows(audit)
    )
    denominator_gap_rows = materialize_program_memory_coverage_denominator_gap_rows(
        audit
    )

    assert summary_rows == scope_summary_rows
    assert gap_rows == scope_gap_rows
    assert list(summary_rows[0]) == PROGRAM_MEMORY_COVERAGE_SUMMARY_FIELDNAMES
    assert list(gap_rows[0]) == PROGRAM_MEMORY_COVERAGE_GAP_FIELDNAMES
    assert (
        list(scope_summary_rows[0])
        == PROGRAM_MEMORY_COVERAGE_SCOPE_SUMMARY_FIELDNAMES
    )
    assert list(scope_gap_rows[0]) == PROGRAM_MEMORY_COVERAGE_SCOPE_GAP_FIELDNAMES
    assert (
        list(denominator_summary_rows[0])
        == PROGRAM_MEMORY_COVERAGE_DENOMINATOR_SUMMARY_FIELDNAMES
    )
    assert (
        list(denominator_gap_rows[0])
        == PROGRAM_MEMORY_COVERAGE_DENOMINATOR_GAP_FIELDNAMES
    )
    assert scope_summary_rows[0]["dimension"] in {
        "target",
        "target_class",
        "domain",
        "failure_scope",
    }
    assert scope_gap_rows[0]["gap_code"]
    assert denominator_summary_rows[0]["coverage_state"]
    assert denominator_gap_rows[0]["program_universe_id"]


def test_program_memory_coverage_audit_scope_mode_allows_missing_program_universe(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)
    (dataset_dir / "program_universe.csv").unlink()
    (dataset_dir / "program_memory_dataset_contract.json").write_text(
        json.dumps(
            {
                "schema_version": "program-memory-dataset-contract-v1",
                "requires_program_universe": False,
            }
        ),
        encoding="utf-8",
    )

    audit = build_program_memory_coverage_audit(dataset_dir)

    assert audit.coverage_manifest["program_universe_row_count"] == 0
    assert materialize_program_memory_coverage_summary_rows(audit)
    assert materialize_program_memory_coverage_gap_rows(audit)
    assert materialize_program_memory_coverage_scope_summary_rows(audit)
    assert materialize_program_memory_coverage_scope_gap_rows(audit)
    assert materialize_program_memory_coverage_denominator_summary_rows(audit) == []
    assert materialize_program_memory_coverage_denominator_gap_rows(audit) == []


def test_program_memory_coverage_audit_honors_in_memory_requirement_flag() -> None:
    with pytest.raises(
        ValueError,
        match="program_universe.csv is required for denominator coverage-audit",
    ):
        build_program_memory_coverage_audit(
            ProgramMemoryDataset(
                assets=(),
                events=(),
                provenances=(),
                directionality_hypotheses=(),
                requires_program_universe=True,
            )
        )


def test_program_memory_coverage_outputs_preserve_scope_files_without_denominator(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    output_dir = tmp_path / "coverage"
    copytree(source_dir, dataset_dir)
    (dataset_dir / "program_universe.csv").unlink()
    (dataset_dir / "program_memory_dataset_contract.json").write_text(
        json.dumps(
            {
                "schema_version": "program-memory-dataset-contract-v1",
                "requires_program_universe": False,
            }
        ),
        encoding="utf-8",
    )

    audit = build_program_memory_coverage_audit(dataset_dir)
    write_program_memory_coverage_outputs(output_dir, audit)

    assert len(read_csv_rows(output_dir / "coverage_summary.csv")) == len(audit.summaries)
    assert len(read_csv_rows(output_dir / "coverage_gaps.csv")) == len(audit.gaps)
    assert read_csv_rows(output_dir / "coverage_denominator_summary.csv") == []
    assert read_csv_rows(output_dir / "coverage_denominator_gaps.csv") == []


def test_load_program_memory_dataset_inherits_duplicate_lineage_ids(tmp_path) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    rows = read_csv_rows(universe_path)
    for row in rows:
        if (
            row["program_universe_id"]
            == "karxt-acute-positive-symptoms-monotherapy-approved"
        ):
            row["asset_lineage_id"] = ""
            row["target_class_lineage_id"] = ""

    with universe_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    dataset = load_program_memory_dataset(dataset_dir)
    duplicate_row = next(
        row
        for row in dataset.program_universe_rows
        if row.program_universe_id
        == "karxt-acute-positive-symptoms-monotherapy-approved"
    )

    assert duplicate_row.asset_lineage_id == "asset:xanomeline-trospium"
    assert (
        duplicate_row.target_class_lineage_id
        == "target-class:muscarinic-cholinergic-modulation"
    )
    audit = build_program_memory_coverage_audit(
        dataset_dir,
        require_program_universe=True,
    )
    assert audit.coverage_manifest["program_universe_row_count"] == 14


def test_program_memory_cli_coverage_audit_path(tmp_path) -> None:
    output_dir = tmp_path / "coverage"
    assert (
        main(
            [
                "program-memory",
                "coverage-audit",
                "--dataset-dir",
                str(Path("data/curated/program_history/v2").resolve()),
                "--output-dir",
                str(output_dir),
                "--focus-target",
                "CHRM4",
            ]
        )
        == 0
    )

    audit_payload = read_json(output_dir / "coverage_audit.json")
    assert (
        audit_payload["dataset_dir"]
        == Path("data/curated/program_history/v2").resolve().as_posix()
    )
    assert audit_payload["gap_reason_counts"]["curation_incomplete"] >= 1
    assert audit_payload["gap_reason_counts"]["history_sparse"] >= 1
    assert audit_payload["coverage_manifest"]["program_universe_row_count"] == 14
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["gap_code"] == "unresolved_failure_scope"
        for row in audit_payload["gap_rows"]
    )
    assert audit_payload["gap_rows"][0]["related_event_ids"]
    assert "related_event_ids_json" not in audit_payload["gap_rows"][0]
    assert audit_payload["coverage_summary_rows"] == audit_payload[
        "scope_summary_rows"
    ]
    assert audit_payload["scope_summary_rows"][0]["dimension"] in {
        "target",
        "target_class",
        "domain",
        "failure_scope",
    }
    assert any(
        row["program_universe_id"]
        == "roluperidone-negative-symptoms-monotherapy-phase-3-or-registration"
        for row in audit_payload["denominator_gap_rows"]
    )

    manifest_payload = read_json(output_dir / "coverage_manifest.json")
    assert (
        manifest_payload["dataset_dir"]
        == Path("data/curated/program_history/v2").resolve().as_posix()
    )
    assert manifest_payload["coverage_state_counts"]["included"] == 6
    assert manifest_payload["coverage_reason_counts"]["asset_alias_duplicate"] == 4
    assert manifest_payload["unique_in_scope_program_count"] == 8

    focus_payload = read_json(output_dir / "coverage_focus.json")
    assert focus_payload["request"] == {"target": "CHRM4"}
    assert focus_payload["matched_summaries"][0]["coverage_band"] == "partial"

    summary_rows = read_csv_rows(output_dir / "coverage_summary.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["coverage_band"] == "partial"
        for row in summary_rows
    )

    denominator_summary_rows = read_csv_rows(
        output_dir / "coverage_denominator_summary.csv"
    )
    negative_symptom_summary_rows = [
        row
        for row in denominator_summary_rows
        if row["stage_bucket"] == "phase_3_or_registration"
        and row["domain"] == "negative_symptoms"
        and row["coverage_state"] == "included"
    ]
    assert len(negative_symptom_summary_rows) == 1
    assert negative_symptom_summary_rows[0]["program_count"] == "1"
    assert negative_symptom_summary_rows[0]["mapped_event_count"] == "2"

    gap_rows = read_csv_rows(output_dir / "coverage_gaps.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["gap_code"] == "unresolved_failure_scope"
        for row in gap_rows
    )

    denominator_gap_rows = read_csv_rows(output_dir / "coverage_denominator_gaps.csv")
    assert any(
        row["program_universe_id"]
        == "karxt-acute-positive-symptoms-monotherapy-approved"
        and row["coverage_state"] == "duplicate"
        and row["duplicate_of_program_universe_id"]
        == "xanomeline-trospium-acute-positive-symptoms-monotherapy-approved"
        for row in denominator_gap_rows
    )

    scope_summary_rows = read_csv_rows(output_dir / "coverage_scope_summary.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["coverage_band"] == "partial"
        for row in scope_summary_rows
    )

    scope_gap_rows = read_csv_rows(output_dir / "coverage_scope_gaps.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["gap_code"] == "unresolved_failure_scope"
        for row in scope_gap_rows
    )

    evidence_rows = read_csv_rows(output_dir / "coverage_evidence.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["record_kind"] == "directionality_hypothesis"
        for row in evidence_rows
    )


def test_program_memory_cli_coverage_audit_removes_stale_focus_artifact(
    tmp_path,
    capsys,
) -> None:
    output_dir = tmp_path / "coverage"

    assert (
        main(
            [
                "program-memory",
                "coverage-audit",
                "--dataset-dir",
                str(Path("data/curated/program_history/v2").resolve()),
                "--output-dir",
                str(output_dir),
                "--focus-target",
                "CHRM4",
            ]
        )
        == 0
    )
    first_stdout = json.loads(capsys.readouterr().out)
    assert first_stdout["focus_request"] == {"target": "CHRM4"}
    assert first_stdout["summary_count"] == 26
    assert first_stdout["gap_count"] == 45
    assert first_stdout["denominator_summary_count"] == 14
    assert first_stdout["denominator_gap_count"] == 8
    assert first_stdout["scope_summary_count"] == 26
    assert first_stdout["scope_gap_count"] == 45
    assert (
        first_stdout["dataset_dir"]
        == Path("data/curated/program_history/v2").resolve().as_posix()
    )
    assert first_stdout["coverage_manifest_file"] == str(
        output_dir / "coverage_manifest.json"
    )
    assert first_stdout["coverage_denominator_summary_file"] == str(
        output_dir / "coverage_denominator_summary.csv"
    )
    assert first_stdout["coverage_denominator_gaps_file"] == str(
        output_dir / "coverage_denominator_gaps.csv"
    )
    assert first_stdout["coverage_focus_file"] == str(output_dir / "coverage_focus.json")
    assert (output_dir / "coverage_focus.json").exists()

    assert (
        main(
            [
                "program-memory",
                "coverage-audit",
                "--dataset-dir",
                str(Path("data/curated/program_history/v2").resolve()),
                "--output-dir",
                str(output_dir),
            ]
        )
        == 0
    )
    second_stdout = json.loads(capsys.readouterr().out)
    assert second_stdout["focus_request"] == {}
    assert second_stdout["summary_count"] == 26
    assert second_stdout["gap_count"] == 45
    assert second_stdout["denominator_summary_count"] == 14
    assert second_stdout["denominator_gap_count"] == 8
    assert second_stdout["coverage_focus_file"] is None
    assert not (output_dir / "coverage_focus.json").exists()

    scope_summary_rows = read_csv_rows(output_dir / "coverage_summary.csv")
    assert any(
        row["dimension"] == "target" and row["scope_value"] == "CHRM4"
        for row in scope_summary_rows
    )


def test_program_memory_cli_coverage_audit_honors_optional_dataset_contract(
    tmp_path,
    capsys,
) -> None:
    dataset_dir = tmp_path / "proposal_v2"
    output_dir = tmp_path / "coverage"
    copytree(Path("data/curated/program_history/v2"), dataset_dir)
    (dataset_dir / "program_universe.csv").unlink()
    (dataset_dir / "program_memory_dataset_contract.json").write_text(
        json.dumps(
            {
                "schema_version": "program-memory-dataset-contract-v1",
                "requires_program_universe": False,
            }
        ),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "program-memory",
                "coverage-audit",
                "--dataset-dir",
                str(dataset_dir),
                "--output-dir",
                str(output_dir),
            ]
        )
        == 0
    )

    stdout_payload = json.loads(capsys.readouterr().out)
    assert stdout_payload["coverage_manifest_row_count"] == 0
    assert stdout_payload["summary_count"] == 26
    assert stdout_payload["gap_count"] == 45
    assert stdout_payload["denominator_summary_count"] == 0
    assert stdout_payload["denominator_gap_count"] == 0

    audit_payload = read_json(output_dir / "coverage_audit.json")
    assert audit_payload["coverage_manifest"]["program_universe_row_count"] == 0
    assert audit_payload["coverage_summary_rows"] == audit_payload["scope_summary_rows"]
    assert audit_payload["denominator_summary_rows"] == []
    assert audit_payload["denominator_gap_rows"] == []


def test_program_memory_coverage_audit_requires_mapped_events_for_included_rows(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        '"[""emraclidine-empower-acute-scz-topline-2024""]"',
        "[]",
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="included program universe row 'emraclidine-acute-positive-symptoms-monotherapy-phase-2' must map to checked-in event_ids",
    ):
        build_program_memory_coverage_audit(dataset_dir)


def test_program_memory_coverage_audit_rejects_mismatched_mapped_event_identity(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        '"[""clozapine-clozaril-trs-approval-us-1989""]"',
        '"[""emraclidine-empower-acute-scz-topline-2024""]"',
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="do not match the canonical program-opportunity grain",
    ):
        build_program_memory_coverage_audit(dataset_dir)


@pytest.mark.parametrize(
    ("field_name", "value", "match"),
    (
        ("asset_id", "wrong-asset", "do not match the canonical program-opportunity grain"),
        ("asset_name", "wrong asset", "do not match the canonical program-opportunity grain"),
        ("asset_name", "", "missing required fields \\['asset_name'\\]"),
        ("target", "CHRM4", "do not match the canonical program-opportunity grain"),
        ("target_symbols_json", json.dumps(["CHRM4"]), "do not match the canonical program-opportunity grain"),
        ("target_symbols_json", "", "do not match the canonical program-opportunity grain"),
    ),
)
def test_program_memory_coverage_audit_rejects_included_display_identity_drift(
    tmp_path,
    field_name,
    value,
    match,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    rows = read_csv_rows(universe_path)
    for row in rows:
        if (
            row["program_universe_id"]
            == "xanomeline-trospium-acute-positive-symptoms-monotherapy-approved"
        ):
            row[field_name] = value

    with universe_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(
        ValueError,
        match=match,
    ):
        build_program_memory_coverage_audit(dataset_dir)


def test_program_memory_coverage_audit_rejects_duplicate_mapped_event_ids(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        '"[""emraclidine-empower-acute-scz-topline-2024""]"',
        '"[""emraclidine-empower-acute-scz-topline-2024"", ""emraclidine-empower-acute-scz-topline-2024""]"',
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="repeats mapped_event_ids",
    ):
        build_program_memory_coverage_audit(dataset_dir)


def test_program_memory_coverage_audit_rejects_unsupported_stage_bucket(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        "phase_3_or_registration,unresolved,ctgov_candidate_pending_adjudication,medium,[],,clinicaltrials_gov,NCT05184335",
        "phase_3_or_registrtion,unresolved,ctgov_candidate_pending_adjudication,medium,[],,clinicaltrials_gov,NCT05184335",
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="unsupported stage_bucket",
    ):
        build_program_memory_coverage_audit(dataset_dir)


def test_program_memory_coverage_audit_reports_invalid_coverage_state_cleanly(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        ",included,checked_in_event_history,high,",
        ",includeed,checked_in_event_history,high,",
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="unsupported program universe coverage_state 'includeed'",
    ):
        build_program_memory_coverage_audit(dataset_dir)


def test_program_memory_coverage_audit_rejects_unsupported_discovery_source_type(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    universe_text = universe_path.read_text(encoding="utf-8")
    broken_text = universe_text.replace(
        "clinicaltrials_gov,NCT05184335",
        "clinicaltrialsgov,NCT05184335",
        1,
    )
    universe_path.write_text(broken_text, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="unsupported program universe discovery_source_type",
    ):
        build_program_memory_coverage_audit(dataset_dir)


@pytest.mark.parametrize(
    ("field_name", "value", "match"),
    (
        ("discovery_source_id", "nct05184335", "canonical NCT IDs"),
        (
            "source_candidate_url",
            "https://clinicaltrials.gov/api/v2/studies/NCT05184335",
            "canonical study URL",
        ),
    ),
)
def test_program_memory_coverage_audit_rejects_noncanonical_ctgov_provenance(
    tmp_path,
    field_name,
    value,
    match,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    rows = read_csv_rows(universe_path)
    for row in rows:
        if (
            row["program_universe_id"]
            == "brilaroxazine-acute-positive-symptoms-monotherapy-phase-3-or-registration"
        ):
            row[field_name] = value

    with universe_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match=match):
        build_program_memory_coverage_audit(dataset_dir)


def test_load_program_memory_dataset_rejects_invalid_program_universe_rows(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    rows = read_csv_rows(universe_path)
    rows[0]["mapped_event_ids_json"] = json.dumps(["not-a-real-event"])

    with universe_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match="unknown mapped_event_ids"):
        load_program_memory_dataset(dataset_dir)


def test_load_program_memory_dataset_rejects_missing_required_program_universe(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)
    (dataset_dir / "program_universe.csv").unlink()

    with pytest.raises(
        ValueError,
        match="program_universe.csv is required for denominator coverage-audit",
    ):
        load_program_memory_dataset(dataset_dir)


def test_load_program_memory_dataset_allows_legacy_scope_only_v2_without_contract(
    tmp_path,
) -> None:
    dataset_dir = tmp_path / "legacy_v2"
    dataset_dir.mkdir()
    for name, header in {
        "assets.csv": "asset_id,molecule,target,target_symbols_json,target_class,mechanism,modality\n",
        "events.csv": "event_id,asset_id,sponsor,population,domain,mono_or_adjunct,phase,event_type,event_date,primary_outcome_result,failure_reason_taxonomy,confidence,notes,sort_order\n",
        "event_provenance.csv": "event_id,source_tier,source_url\n",
        "directionality_hypotheses.csv": "hypothesis_id,entity_id,entity_label,desired_perturbation_direction,modality_hypothesis,preferred_modalities_json,confidence,ambiguity,evidence_basis,supporting_event_ids_json,contradiction_conditions_json,falsification_conditions_json,open_risks_json,sort_order\n",
    }.items():
        (dataset_dir / name).write_text(header, encoding="utf-8")

    dataset = load_program_memory_dataset(dataset_dir)
    audit = build_program_memory_coverage_audit(dataset_dir)

    assert dataset.requires_program_universe is False
    assert dataset.program_universe_rows == ()
    assert audit.coverage_manifest["program_universe_row_count"] == 0
    assert audit.summaries


def test_load_program_memory_dataset_rejects_empty_required_program_universe(
    tmp_path,
) -> None:
    source_dir = Path("data/curated/program_history/v2")
    dataset_dir = tmp_path / "v2"
    copytree(source_dir, dataset_dir)

    universe_path = dataset_dir / "program_universe.csv"
    header_only = universe_path.read_text(encoding="utf-8").splitlines()[0] + "\n"
    universe_path.write_text(header_only, encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="program_universe.csv is required for denominator coverage-audit and must contain at least one row",
    ):
        load_program_memory_dataset(dataset_dir)
