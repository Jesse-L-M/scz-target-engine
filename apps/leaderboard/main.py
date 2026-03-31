"""Browse benchmark leaderboards from generated outputs.

Usage:
    uv run python apps/leaderboard/main.py --list
    uv run python apps/leaderboard/main.py --entity-type gene --horizon 1y --metric average_precision_any_positive_outcome
    uv run python apps/leaderboard/main.py --report-cards
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from scz_target_engine.observatory.benchmark_nav import (
    browse_leaderboard,
    browse_report_cards,
    list_available_leaderboard_slices,
)
from scz_target_engine.observatory.shell import (
    format_leaderboard,
    format_report_cards,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="leaderboard",
        description="Browse benchmark leaderboards and report cards.",
    )
    parser.add_argument(
        "--generated-dir",
        help="Override the generated benchmark output directory.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list",
        action="store_true",
        dest="list_slices",
        help="List available leaderboard slices.",
    )
    group.add_argument(
        "--report-cards",
        action="store_true",
        help="Show available report cards.",
    )
    group.add_argument(
        "--entity-type",
        help="Entity type for leaderboard view (requires --horizon and --metric).",
    )
    parser.add_argument("--horizon", help="Evaluation horizon (e.g. 1y, 3y, 5y).")
    parser.add_argument("--metric", help="Metric name.")

    args = parser.parse_args(argv)
    gen_dir = Path(args.generated_dir).resolve() if args.generated_dir else None

    if args.list_slices:
        slices = list_available_leaderboard_slices(generated_dir=gen_dir)
        if not slices:
            print("No leaderboard slices found. Run benchmark reporting first.")
            return 0
        print("Available Leaderboard Slices")
        print("=" * 50)
        for sl in slices:
            print(f"  {sl.entity_type} / {sl.horizon} / {sl.metric_name}")
        print()
        return 0

    if args.report_cards:
        cards = browse_report_cards(generated_dir=gen_dir)
        print(format_report_cards(cards))
        return 0

    if not args.horizon or not args.metric:
        parser.error(
            "--entity-type requires --horizon and --metric"
        )

    result = browse_leaderboard(
        entity_type=args.entity_type,
        horizon=args.horizon,
        metric_name=args.metric,
        generated_dir=gen_dir,
    )
    if result is None:
        print(
            f"No leaderboard found for {args.entity_type}/{args.horizon}/{args.metric}."
        )
        return 1

    print(format_leaderboard(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
