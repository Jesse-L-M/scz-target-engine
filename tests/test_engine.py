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
    assert (tmp_path / "decision_vectors_v1.json").exists()
    assert (tmp_path / "domain_head_rankings_v1.csv").exists()

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
        "adults with schizophrenia | adults with schizophrenia during acute exacerbation "
        "of psychotic symptoms"
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

    decision_vectors = json.loads((tmp_path / "decision_vectors_v1.json").read_text())
    assert decision_vectors["schema_version"] == "v1"
    assert "decision_head_definitions" in decision_vectors
    assert "domain_head_definitions" in decision_vectors

    gene_vector = next(
        entity
        for entity in decision_vectors["entities"]["gene"]
        if entity["entity_label"] == "TCF4"
    )
    assert gene_vector["heuristic_score_v0"] == float(tcf4_row["composite_score"])
    assert gene_vector["heuristic_rank_v0"] == int(tcf4_row["rank"])
    assert "decision_vector" in gene_vector
    assert (
        gene_vector["human_support_score"]
        == gene_vector["decision_vector"]["human_support_score"]["score"]
    )
    human_head = next(
        head
        for head in gene_vector["head_scores"]
        if head["head_name"] == "human_support_score"
    )
    assert "status" in human_head
    assert (
        gene_vector["decision_vector"]["human_support_score"]["status"]
        == gene_vector["human_support_score_status"]
    )
    failure_head = next(
        head
        for head in gene_vector["head_scores"]
        if head["head_name"] == "failure_burden_score"
    )
    assert failure_head["status"] == "available"
    assert failure_head["score"] == 1.0
    assert gene_vector["directionality_confidence"] == 0.25
    assert gene_vector["subgroup_resolution_score"] == 0.3
    assert gene_vector["domain_profiles"]["negative_symptoms"]["label"] == "Negative symptoms"
    assert gene_vector["domain_profiles"]["negative_symptoms"]["status"] == "available"

    chrm4_vector = next(
        entity
        for entity in decision_vectors["entities"]["gene"]
        if entity["entity_label"] == "CHRM4"
    )
    assert chrm4_vector["failure_burden_score"] == 0.55
    assert chrm4_vector["directionality_confidence"] == 0.5
    assert chrm4_vector["subgroup_resolution_score"] == 0.5

    with (tmp_path / "domain_head_rankings_v1.csv").open(newline="", encoding="utf-8") as handle:
        domain_rows = list(csv.DictReader(handle))
    tcf4_negative = next(
        row
        for row in domain_rows
        if row["entity_label"] == "TCF4" and row["domain_slug"] == "negative_symptoms"
    )
    assert tcf4_negative["heuristic_score_v0"] == tcf4_row["composite_score"]
    assert tcf4_negative["heuristic_rank_v0"] == tcf4_row["rank"]
    assert "human_support_score" in tcf4_negative
    assert tcf4_negative["failure_burden_score"] == "1.0"
    assert tcf4_negative["directionality_confidence"] == "0.25"
    assert tcf4_negative["directionality_confidence_status"] == "available"
    assert tcf4_negative["subgroup_resolution_score"] == "0.3"

    drd2_negative = next(
        row
        for row in domain_rows
        if row["entity_label"] == "DRD2" and row["domain_slug"] == "negative_symptoms"
    )
    drd2_trs = next(
        row
        for row in domain_rows
        if row["entity_label"] == "DRD2"
        and row["domain_slug"] == "treatment_resistant_schizophrenia"
    )
    assert drd2_negative["subgroup_resolution_score"] == "0.3"
    assert drd2_trs["subgroup_resolution_score"] == "0.9"

    chrm4_acute = next(
        row
        for row in domain_rows
        if row["entity_label"] == "CHRM4"
        and row["domain_slug"] == "acute_positive_symptoms"
    )
    chrm4_trs = next(
        row
        for row in domain_rows
        if row["entity_label"] == "CHRM4"
        and row["domain_slug"] == "treatment_resistant_schizophrenia"
    )
    assert chrm4_acute["failure_burden_score"] == "0.55"
    assert chrm4_trs["failure_burden_score"] == "1.0"

    assert result["gene_target_ledger_file"].endswith("gene_target_ledgers.json")
    assert result["decision_vector_artifact"].endswith("decision_vectors_v1.json")
    assert result["domain_head_ranking_artifact"].endswith("domain_head_rankings_v1.csv")

    with Path("examples/v0/output/gene_rankings.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        fixture_gene_rows = list(csv.DictReader(handle))
    generated_gene_rows = {row["entity_id"]: row for row in rows}
    fixture_shared_fields = tuple(
        field_name
        for field_name in fixture_gene_rows[0].keys()
        if field_name in generated_gene_rows[fixture_gene_rows[0]["entity_id"]]
    )
    assert len(generated_gene_rows) == len(fixture_gene_rows)
    for fixture_row in fixture_gene_rows:
        generated_row = generated_gene_rows[fixture_row["entity_id"]]
        for field_name in fixture_shared_fields:
            assert generated_row[field_name] == fixture_row[field_name]

    assert (
        (tmp_path / "module_rankings.csv").read_text(encoding="utf-8")
        == Path("examples/v0/output/module_rankings.csv").read_text(encoding="utf-8")
    )
    assert (
        (tmp_path / "stability_summary.json").read_text(encoding="utf-8")
        == Path("examples/v0/output/stability_summary.json").read_text(encoding="utf-8")
    )

    build_summary = (tmp_path / "build_summary.md").read_text(encoding="utf-8")
    assert "- Gene heuristic-stable entities:" in build_summary


def test_build_outputs_uses_repo_substrate_when_config_is_copied(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "copied_v0.toml"
    config_path.write_text(
        Path("config/v0.toml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    config = load_config(config_path)

    result = build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        tmp_path / "copied-build-output",
    )

    assert result["gene_ranked_count"] == 24
    assert (tmp_path / "copied-build-output" / "gene_target_ledgers.json").exists()
    assert (tmp_path / "copied-build-output" / "decision_vectors_v1.json").exists()
