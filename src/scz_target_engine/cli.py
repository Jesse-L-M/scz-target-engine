from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs, validate_inputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scz-target-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--config", required=True)
    validate_parser.add_argument("--input-dir")

    build_parser_ = subparsers.add_parser("build")
    build_parser_.add_argument("--config", required=True)
    build_parser_.add_argument("--input-dir")
    build_parser_.add_argument("--output-dir")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    config_dir = config.config_path.parent
    input_dir = (
        Path(args.input_dir).resolve()
        if args.input_dir
        else (config_dir.parent / "examples" / "v0" / "input").resolve()
    )

    if args.command == "validate":
        result = validate_inputs(config, input_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (config_dir.parent / config.build.output_dir).resolve()
    )
    result = build_outputs(config, input_dir, output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
