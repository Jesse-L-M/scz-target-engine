from pathlib import Path
import json

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
    assert (tmp_path / "gene_rankings.csv").exists()
    assert (tmp_path / "module_rankings.csv").exists()
    assert (tmp_path / "target_cards.md").exists()
    assert (tmp_path / "kill_cards.md").exists()
    summary = json.loads((tmp_path / "stability_summary.json").read_text())
    assert "gene" in summary
    assert "baseline_overlap" in summary
