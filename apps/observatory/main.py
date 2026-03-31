"""Run the observatory shell against the current repo data directory.

Usage:
    uv run python apps/observatory/main.py
    uv run python apps/observatory/main.py --data-dir data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root without install
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from scz_target_engine.observatory.shell import (
    build_observatory_index,
    format_observatory_index,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="observatory",
        description="Observatory shell: browse benchmark suites, tasks, and artifacts.",
    )
    parser.add_argument(
        "--data-dir",
        help="Override the data directory (default: repo data/)",
    )
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir).resolve() if args.data_dir else None
    index = build_observatory_index(data_dir=data_dir)
    print(format_observatory_index(index))
    return 0


if __name__ == "__main__":
    sys.exit(main())
