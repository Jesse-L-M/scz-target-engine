from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from scz_target_engine.rescue import (
    DEFAULT_INTERNEURON_BASELINE_IDS,
    VALID_INTERNEURON_AXIS_IDS,
    materialize_interneuron_rescue_lane,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the governed interneuron rescue lane against the checked-in frozen "
            "artifacts only."
        )
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / ".context" / "interneuron_rescue_lane_run"),
        help="Directory where predictions and leakage-safe summaries will be written.",
    )
    parser.add_argument(
        "--axis",
        action="append",
        choices=VALID_INTERNEURON_AXIS_IDS,
        help="Axis to run. Repeat to run multiple axes. Defaults to both.",
    )
    parser.add_argument(
        "--baseline",
        action="append",
        choices=DEFAULT_INTERNEURON_BASELINE_IDS,
        help="Baseline to run. Repeat to run multiple baselines. Defaults to all.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = materialize_interneuron_rescue_lane(
        Path(args.output_dir),
        axis_ids=tuple(args.axis) if args.axis else VALID_INTERNEURON_AXIS_IDS,
        baseline_ids=tuple(args.baseline) if args.baseline else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
