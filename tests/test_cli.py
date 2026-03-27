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
            "--schema-file",
            "schema.csv",
            "--psychencode-file",
            "psychencode.csv",
        ]
    )
    assert args.command == "prepare-gene-table"
    assert args.pgc_file == "pgc.csv"
    assert args.schema_file == "schema.csv"
    assert args.psychencode_file == "psychencode.csv"


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


def test_cli_fetch_schema_parser_accepts_input_and_output_files() -> None:
    args = build_parser().parse_args(
        [
            "fetch-schema",
            "--input-file",
            "seed.csv",
            "--output-file",
            "schema.csv",
            "--overrides-file",
            "overrides.csv",
        ]
    )
    assert args.command == "fetch-schema"
    assert args.input_file == "seed.csv"
    assert args.output_file == "schema.csv"
    assert args.overrides_file == "overrides.csv"


def test_cli_fetch_psychencode_parser_accepts_input_and_output_files() -> None:
    args = build_parser().parse_args(
        [
            "fetch-psychencode",
            "--input-file",
            "seed.csv",
            "--output-file",
            "psychencode.csv",
        ]
    )
    assert args.command == "fetch-psychencode"
    assert args.input_file == "seed.csv"
    assert args.output_file == "psychencode.csv"
