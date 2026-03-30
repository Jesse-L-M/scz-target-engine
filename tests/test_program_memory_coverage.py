import json
from pathlib import Path

from scz_target_engine.cli import main
from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.program_memory import (
    build_program_memory_coverage_audit,
    build_program_memory_coverage_focus_report,
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
    assert audit_payload["dataset_dir"].endswith("data/curated/program_history/v2")
    assert audit_payload["gap_reason_counts"]["curation_incomplete"] >= 1
    assert audit_payload["gap_reason_counts"]["history_sparse"] >= 1

    focus_payload = read_json(output_dir / "coverage_focus.json")
    assert focus_payload["request"] == {"target": "CHRM4"}
    assert focus_payload["matched_summaries"][0]["coverage_band"] == "partial"

    summary_rows = read_csv_rows(output_dir / "coverage_summary.csv")
    chrm4_summary_rows = [
        row
        for row in summary_rows
        if row["dimension"] == "target" and row["scope_value"] == "CHRM4"
    ]
    assert len(chrm4_summary_rows) == 1
    assert chrm4_summary_rows[0]["coverage_band"] == "partial"

    gap_rows = read_csv_rows(output_dir / "coverage_gaps.csv")
    assert any(
        row["dimension"] == "target"
        and row["scope_value"] == "CHRM4"
        and row["gap_code"] == "unresolved_failure_scope"
        for row in gap_rows
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
    assert second_stdout["coverage_focus_file"] is None
    assert not (output_dir / "coverage_focus.json").exists()

    summary_rows = read_csv_rows(output_dir / "coverage_summary.csv")
    assert any(
        row["dimension"] == "target" and row["scope_value"] == "CHRM4"
        for row in summary_rows
    )
