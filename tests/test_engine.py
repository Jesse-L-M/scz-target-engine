import csv
import json
from pathlib import Path

import pytest

from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs, validate_inputs


def test_validate_inputs_counts_example_rows() -> None:
    config = load_config(Path("config/v0.toml"))
    result = validate_inputs(config, Path("examples/v0/input").resolve())
    assert result["gene_records"] == 26
    assert result["module_records"] == 16
    assert result["warning_entities"] >= 1


def test_load_config_accepts_deprecated_decision_grade_threshold_alias(
    tmp_path: Path,
) -> None:
    config_text = Path("config/v0.toml").read_text(encoding="utf-8").replace(
        "heuristic_stability_threshold = 0.70",
        "decision_grade_threshold = 0.70",
    )
    config_path = tmp_path / "legacy_v0.toml"
    config_path.write_text(config_text, encoding="utf-8")

    config = load_config(config_path)

    assert config.stability.heuristic_stability_threshold == 0.7
    assert config.stability.decision_grade_threshold == 0.7


def test_load_config_rejects_conflicting_stability_threshold_keys(
    tmp_path: Path,
) -> None:
    config_text = Path("config/v0.toml").read_text(encoding="utf-8").replace(
        "heuristic_stability_threshold = 0.70",
        "heuristic_stability_threshold = 0.70\ndecision_grade_threshold = 0.65",
    )
    config_path = tmp_path / "conflicting_v0.toml"
    config_path.write_text(config_text, encoding="utf-8")

    with pytest.raises(ValueError, match="must match"):
        load_config(config_path)


def test_build_outputs_writes_expected_files(tmp_path: Path) -> None:
    config = load_config(Path("config/v0.toml"))
    result = build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path,
    )
    assert result["gene_ranked_count"] >= 20
    assert result["gene_warning_count"] >= 1
    assert (tmp_path / "gene_rankings.csv").exists()
    assert (tmp_path / "module_rankings.csv").exists()
    assert (tmp_path / "target_cards.md").exists()
    assert (tmp_path / "kill_cards.md").exists()
    assert (tmp_path / "gene_target_ledgers.json").exists()
    target_cards = (tmp_path / "target_cards.md").read_text(encoding="utf-8")
    assert "# Public-Evidence Promising Cards" in target_cards
    assert "- Verdict: public-evidence promising" in target_cards
    assert "- Heuristic-stable: yes" in target_cards
    assert "Verdict: advance" not in target_cards
    kill_cards = (tmp_path / "kill_cards.md").read_text(encoding="utf-8")
    assert "TCF4 (ENSG00000196628)" in kill_cards
    assert "# Fragile Or Insufficient Evidence Cards" in kill_cards
    assert "- Evidence basis:" in kill_cards
    assert "- Evidence coverage:" in kill_cards
    assert "Verdict: do not advance" not in kill_cards
    assert "Decision-grade" not in kill_cards
    with (tmp_path / "gene_rankings.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    required_summary_columns = {
        "program_history_event_count",
        "failure_event_count",
        "failure_scopes",
        "failure_taxonomies",
        "program_history_domains",
        "program_history_populations",
        "directionality_status",
        "desired_perturbation_direction",
        "modality_hypothesis",
        "directionality_confidence",
    }
    assert required_summary_columns.issubset(rows[0].keys())
    tcf4_row = next(row for row in rows if row["entity_label"] == "TCF4")
    assert "heuristic_stable" in tcf4_row
    assert "decision_grade" not in tcf4_row
    assert "warning_count" in tcf4_row
    assert "warning_kinds" in tcf4_row
    assert tcf4_row["warning_count"] == "1"
    assert tcf4_row["present_layer_count"] == "4"
    assert tcf4_row["warning_kinds"] == "source_coverage_gap"
    assert tcf4_row["directionality_status"] == "undetermined"
    assert tcf4_row["desired_perturbation_direction"] == "undetermined"
    grin2a_row = next(row for row in rows if row["entity_label"] == "GRIN2A")
    assert grin2a_row["rank"] == "1"
    assert grin2a_row["composite_score"] == "0.474147"
    c4a_row = next(row for row in rows if row["entity_label"] == "C4A")
    assert c4a_row["rank"] == ""
    chrm4_row = next(row for row in rows if row["entity_label"] == "CHRM4")
    assert chrm4_row["program_history_event_count"] == "2"
    assert chrm4_row["failure_event_count"] == "1"
    assert chrm4_row["failure_scopes"] == "unresolved"
    assert chrm4_row["failure_taxonomies"] == "unresolved"
    assert chrm4_row["program_history_domains"] == "acute_positive_symptoms"
    assert chrm4_row["program_history_populations"] == (
        "adults with schizophrenia | adults with schizophrenia during acute exacerbation of psychotic symptoms"
    )
    assert chrm4_row["directionality_status"] == "curated"
    assert chrm4_row["desired_perturbation_direction"] == "increase_activity"
    assert chrm4_row["directionality_confidence"] == "medium"
    assert (
        chrm4_row["modality_hypothesis"]
        == "muscarinic_agonism_or_positive_allosteric_modulation"
    )
    with (tmp_path / "module_rankings.csv").open(newline="", encoding="utf-8") as handle:
        module_rows = list(csv.DictReader(handle))
    assert module_rows[0]["entity_label"] == "BrainSCOPE L2.3.IT"
    assert module_rows[0]["composite_score"] == "0.816286"
    summary = json.loads((tmp_path / "stability_summary.json").read_text())
    assert "gene" in summary
    assert "baseline_overlap" in summary
    ledger_payload = json.loads((tmp_path / "gene_target_ledgers.json").read_text())
    assert ledger_payload["schema_version"] == "pr7-target-ledger-v1"
    assert ledger_payload["scoring_neutral"] is True
    assert ledger_payload["data_sources"] == {
        "program_history": "data/curated/program_history/programs.csv",
        "directionality_hypotheses": (
            "data/curated/program_history/directionality_hypotheses.csv"
        ),
    }
    assert ledger_payload["target_count"] == 26
    assert ledger_payload["targets_with_program_history"] == 2
    assert ledger_payload["targets_with_curated_directionality"] == 4
    assert any(ledger["entity_label"] == "C4A" for ledger in ledger_payload["targets"])
    chrm4_ledger = next(
        ledger for ledger in ledger_payload["targets"] if ledger["entity_label"] == "CHRM4"
    )
    assert chrm4_ledger["structural_failure_history"]["matched_event_count"] == 2
    assert chrm4_ledger["structural_failure_history"]["failure_event_count"] == 1
    assert chrm4_ledger["structural_failure_history"]["nonfailure_event_count"] == 1
    assert chrm4_ledger["structural_failure_history"]["event_count_by_scope"] == {
        "nonfailure": 1,
        "unresolved": 1,
    }
    assert chrm4_ledger["structural_failure_history"]["events"][1]["failure_scope"] == (
        "unresolved"
    )
    assert chrm4_ledger["structural_failure_history"]["events"][1]["what_failed"] == (
        "undetermined"
    )
    assert chrm4_ledger["directionality_hypothesis"]["status"] == "curated"
    assert (
        chrm4_ledger["directionality_hypothesis"]["desired_perturbation_direction"]
        == "increase_activity"
    )
    build_summary = (tmp_path / "build_summary.md").read_text(encoding="utf-8")
    assert "- Gene heuristic-stable entities:" in build_summary


def test_build_outputs_uses_repo_substrate_when_config_is_copied(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "copied_v0.toml"
    config_path.write_text(Path("config/v0.toml").read_text(encoding="utf-8"), encoding="utf-8")
    config = load_config(config_path)

    result = build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path / "copied-build-output",
    )

    assert result["gene_ranked_count"] == 24
    assert (tmp_path / "copied-build-output" / "gene_target_ledgers.json").exists()
