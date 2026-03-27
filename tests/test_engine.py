import csv
import json
from pathlib import Path

from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs, validate_inputs


def test_validate_inputs_counts_example_rows() -> None:
    config = load_config(Path("config/v0.toml"))
    result = validate_inputs(config, Path("examples/v0/input").resolve())
    assert result["gene_records"] == 26
    assert result["module_records"] == 10
    assert result["warning_entities"] >= 1


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
    kill_cards = (tmp_path / "kill_cards.md").read_text(encoding="utf-8")
    assert "TCF4 (ENSGEX0023)" in kill_cards
    assert "- Decision basis:" in kill_cards
    assert "- Evidence coverage:" in kill_cards
    with (tmp_path / "gene_rankings.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    tcf4_row = next(row for row in rows if row["entity_id"] == "ENSGEX0023")
    assert "warning_count" in tcf4_row
    assert "warning_kinds" in tcf4_row
    assert tcf4_row["warning_count"] == "1"
    assert tcf4_row["present_layer_count"] == "3"
    summary = json.loads((tmp_path / "stability_summary.json").read_text())
    assert "gene" in summary
    assert "baseline_overlap" in summary
