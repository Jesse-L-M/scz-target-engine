import hashlib
import json
import os
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.challenge import (
    build_prospective_scoring_records,
    materialize_prospective_prediction_registration,
    reconcile_prospective_forecasts,
)
from scz_target_engine.io import write_json


def _registration_kwargs(output_file: Path) -> dict[str, object]:
    return {
        "packet_id": "ENSG00000180720__acute_translation_guardrails_v1",
        "output_file": output_file,
        "registered_at": "2026-03-31T00:00:00Z",
        "registered_by": "repo_checked_in_example",
        "predicted_outcome": "advance",
        "option_probabilities": {
            "advance": 0.58,
            "hold": 0.32,
            "kill": 0.10,
        },
        "outcome_window_closes_on": "2027-12-31",
        "rationale": [
            "The packet carries a scoreable available policy signal and explicit nonfailure offsetting evidence.",
            "Contradiction and replay uncertainty remain live, so the forecast still assigns substantial hold and kill mass.",
        ],
        "registration_id": "forecast_chrm4_acute_translation_guardrails_2026_03_31",
        "notes": (
            "Checked-in example prospective forecast registered from the shipped "
            "hypothesis packet artifact."
        ),
    }


def _materialize_registration(output_file: Path) -> dict[str, object]:
    return materialize_prospective_prediction_registration(
        Path("examples/v0/output/hypothesis_packets_v1.json"),
        **_registration_kwargs(output_file),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_outcome_log(
    outcome_log_path: Path,
    *,
    registration_path: Path,
    observed_outcome: str,
    outcome_record_id: str,
    evidence_file: Path,
    observed_at: str,
    outcome_log_id: str,
) -> None:
    write_json(
        outcome_log_path,
        {
            "schema_name": "prospective_forecast_outcome_log",
            "schema_version": "v1",
            "outcome_log_id": outcome_log_id,
            "logged_at": "2027-06-30T12:00:00Z",
            "logged_by": "repo_outcome_logger",
            "outcome_records": [
                {
                    "outcome_record_id": outcome_record_id,
                    "registration_artifact_path": os.path.relpath(
                        registration_path.resolve(),
                        outcome_log_path.resolve().parent,
                    ),
                    "registration_artifact_sha256": _sha256(
                        registration_path.resolve()
                    ),
                    "registration_id": "forecast_chrm4_acute_translation_guardrails_2026_03_31",
                    "observed_at": observed_at,
                    "observed_outcome": observed_outcome,
                    "source_evidence": {
                        "source_path": os.path.relpath(
                            evidence_file.resolve(),
                            outcome_log_path.resolve().parent,
                        ),
                        "source_sha256": _sha256(evidence_file.resolve()),
                        "source_pointer": "/outcomes/0",
                        "summary": "Checked-in outcome note recording the realized disposition.",
                    },
                }
            ],
        },
    )


def test_prospective_registration_materializes_from_reviewed_packet_artifact(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "prospective_prediction_registration.json"
    payload = _materialize_registration(output_file)
    artifact = load_artifact(
        output_file,
        artifact_name="prospective_prediction_registration",
    )
    expected = json.loads(
        Path(
            "data/prospective_registry/registrations/"
            "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
        ).read_text(encoding="utf-8")
    )
    normalized_payload = json.loads(json.dumps(payload))
    normalized_expected = json.loads(json.dumps(expected))
    normalized_payload["packet_artifact"]["artifact_path"] = "<normalized>"
    normalized_expected["packet_artifact"]["artifact_path"] = "<normalized>"

    assert normalized_payload == normalized_expected
    assert artifact.artifact_name == "prospective_prediction_registration"
    assert artifact.payload.packet_scope["entity_label"] == "CHRM4"
    assert artifact.payload.frozen_forecast_payload["predicted_outcome"] == "advance"


def test_prospective_registration_rejects_rewrite_of_existing_output_file(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "prospective_prediction_registration.json"
    _materialize_registration(output_file)
    original_contents = output_file.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="prospective registrations are immutable"):
        _materialize_registration(output_file)

    assert output_file.read_text(encoding="utf-8") == original_contents


def test_prospective_registration_rejects_duplicate_registration_id_before_write(
    tmp_path: Path,
) -> None:
    registrations_dir = tmp_path / "registrations"
    first_output = registrations_dir / "first_registration.json"
    second_output = registrations_dir / "second_registration.json"
    _materialize_registration(first_output)

    with pytest.raises(ValueError, match="registration_id must be unique"):
        _materialize_registration(second_output)

    assert first_output.exists()
    assert not second_output.exists()


def test_prospective_reconciliation_prepares_scoreable_records(tmp_path: Path) -> None:
    registrations_dir = tmp_path / "registrations"
    outcomes_dir = tmp_path / "outcomes"
    registration_path = (
        registrations_dir / "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    _materialize_registration(registration_path)

    evidence_file = outcomes_dir / "chrm4_outcome_note.md"
    evidence_file.parent.mkdir(parents=True, exist_ok=True)
    evidence_file.write_text(
        "# Outcome\n\nCHRM4 advanced to follow-up package review.\n",
        encoding="utf-8",
    )
    outcome_log_path = outcomes_dir / "chrm4_outcome_log.json"
    _write_outcome_log(
        outcome_log_path,
        registration_path=registration_path,
        observed_outcome="advance",
        outcome_record_id="chrm4_acute_outcome_2027_06_30",
        evidence_file=evidence_file,
        observed_at="2027-06-30",
        outcome_log_id="prospective_outcomes_2027_06_30",
    )

    outcome_artifact = load_artifact(
        outcome_log_path,
        artifact_name="prospective_forecast_outcome_log",
    )
    reconciliations = reconcile_prospective_forecasts(
        registrations_dir=registrations_dir,
        outcomes_dir=outcomes_dir,
    )
    scoring_records = build_prospective_scoring_records(
        registrations_dir=registrations_dir,
        outcomes_dir=outcomes_dir,
    )

    assert outcome_artifact.artifact_name == "prospective_forecast_outcome_log"
    assert len(reconciliations) == 1
    assert reconciliations[0].resolution_status == "resolved"
    assert reconciliations[0].observed_outcome == "advance"
    assert len(scoring_records) == 1
    assert scoring_records[0].registration_id == (
        "forecast_chrm4_acute_translation_guardrails_2026_03_31"
    )
    assert scoring_records[0].option_probabilities == {
        "advance": 0.58,
        "hold": 0.32,
        "kill": 0.10,
    }


def test_prospective_reconciliation_keeps_out_of_window_outcome_non_scoreable(
    tmp_path: Path,
) -> None:
    registrations_dir = tmp_path / "registrations"
    outcomes_dir = tmp_path / "outcomes"
    registration_path = (
        registrations_dir / "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    _materialize_registration(registration_path)

    evidence_file = outcomes_dir / "late_outcome_note.md"
    evidence_file.parent.mkdir(parents=True, exist_ok=True)
    evidence_file.write_text("late advance\n", encoding="utf-8")
    outcome_log_path = outcomes_dir / "late_outcome_log.json"
    _write_outcome_log(
        outcome_log_path,
        registration_path=registration_path,
        observed_outcome="advance",
        outcome_record_id="chrm4_acute_outcome_2028_01_02",
        evidence_file=evidence_file,
        observed_at="2028-01-02",
        outcome_log_id="prospective_outcomes_2028_01_02",
    )

    reconciliations = reconcile_prospective_forecasts(
        registrations_dir=registrations_dir,
        outcomes_dir=outcomes_dir,
    )

    assert len(reconciliations) == 1
    assert reconciliations[0].resolution_status == "out_of_window"
    assert reconciliations[0].observed_outcome == "advance"
    assert (
        build_prospective_scoring_records(
            registrations_dir=registrations_dir,
            outcomes_dir=outcomes_dir,
        )
        == ()
    )


def test_prospective_reconciliation_marks_conflicting_outcomes(tmp_path: Path) -> None:
    registrations_dir = tmp_path / "registrations"
    outcomes_dir = tmp_path / "outcomes"
    registration_path = (
        registrations_dir / "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    _materialize_registration(registration_path)

    evidence_a = outcomes_dir / "chrm4_outcome_note_a.md"
    evidence_b = outcomes_dir / "chrm4_outcome_note_b.md"
    evidence_a.parent.mkdir(parents=True, exist_ok=True)
    evidence_a.write_text("advance\n", encoding="utf-8")
    evidence_b.write_text("hold\n", encoding="utf-8")

    _write_outcome_log(
        outcomes_dir / "chrm4_outcome_log_a.json",
        registration_path=registration_path,
        observed_outcome="advance",
        outcome_record_id="chrm4_acute_outcome_2027_06_30_a",
        evidence_file=evidence_a,
        observed_at="2027-06-30",
        outcome_log_id="prospective_outcomes_2027_06_30_a",
    )
    _write_outcome_log(
        outcomes_dir / "chrm4_outcome_log_b.json",
        registration_path=registration_path,
        observed_outcome="hold",
        outcome_record_id="chrm4_acute_outcome_2027_07_02_b",
        evidence_file=evidence_b,
        observed_at="2027-07-02",
        outcome_log_id="prospective_outcomes_2027_07_02_b",
    )

    reconciliations = reconcile_prospective_forecasts(
        registrations_dir=registrations_dir,
        outcomes_dir=outcomes_dir,
    )

    assert len(reconciliations) == 1
    assert reconciliations[0].resolution_status == "conflicted"
    assert reconciliations[0].conflict_outcomes == ("advance", "hold")
    assert (
        build_prospective_scoring_records(
            registrations_dir=registrations_dir,
            outcomes_dir=outcomes_dir,
        )
        == ()
    )


def test_prospective_outcome_log_rejects_unknown_outcome_option(tmp_path: Path) -> None:
    registrations_dir = tmp_path / "registrations"
    outcomes_dir = tmp_path / "outcomes"
    registration_path = (
        registrations_dir / "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    _materialize_registration(registration_path)

    evidence_file = outcomes_dir / "invalid_outcome_note.md"
    evidence_file.parent.mkdir(parents=True, exist_ok=True)
    evidence_file.write_text("pivot\n", encoding="utf-8")
    outcome_log_path = outcomes_dir / "invalid_outcome_log.json"
    _write_outcome_log(
        outcome_log_path,
        registration_path=registration_path,
        observed_outcome="pivot",
        outcome_record_id="invalid_outcome_record",
        evidence_file=evidence_file,
        observed_at="2027-06-30",
        outcome_log_id="invalid_outcome_log",
    )

    with pytest.raises(ValueError, match="must match one of the registered outcome options"):
        load_artifact(
            outcome_log_path,
            artifact_name="prospective_forecast_outcome_log",
        )
