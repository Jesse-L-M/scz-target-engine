from pathlib import Path

from scz_target_engine.cli import build_parser, main


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


def test_cli_prepare_parser_accepts_pgc_file() -> None:
    args = build_parser().parse_args(
        [
            "prepare-gene-table",
            "--seed-file",
            "seed.csv",
            "--output-file",
            "prepared.csv",
            "--pgc-file",
            "pgc.csv",
        ]
    )
    assert args.command == "prepare-gene-table"
    assert args.pgc_file == "pgc.csv"


def test_cli_fetch_pgc_parser_accepts_output_file() -> None:
    args = build_parser().parse_args(
        [
            "fetch-pgc-scz2022",
            "--output-file",
            "pgc.csv",
        ]
    )
    assert args.command == "fetch-pgc-scz2022"
    assert args.output_file == "pgc.csv"
