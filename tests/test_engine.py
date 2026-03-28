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
    tcf4_row = next(row for row in rows if row["entity_label"] == "TCF4")
    assert "heuristic_stable" in tcf4_row
    assert "decision_grade" not in tcf4_row
    assert "warning_count" in tcf4_row
    assert "warning_kinds" in tcf4_row
    assert tcf4_row["warning_count"] == "1"
    assert tcf4_row["present_layer_count"] == "4"
    assert tcf4_row["warning_kinds"] == "source_coverage_gap"
    summary = json.loads((tmp_path / "stability_summary.json").read_text())
    assert "gene" in summary
    assert "baseline_overlap" in summary
    build_summary = (tmp_path / "build_summary.md").read_text(encoding="utf-8")
    assert "- Gene heuristic-stable entities:" in build_summary
