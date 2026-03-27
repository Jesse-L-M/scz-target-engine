from pathlib import Path

from scz_target_engine.cli import main


def test_cli_validate_runs() -> None:
    exit_code = main(
        [
            "validate",
            "--config",
            str(Path("config/v0.toml").resolve()),
            "--input-dir",
            str(Path("examples/v0/input").resolve()),
        ]
    )
    assert exit_code == 0
