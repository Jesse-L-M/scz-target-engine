"""Run the observatory shell against the current repo.

Benchmark suites, tasks, and public slices are always read from the repo's
checked-in contract metadata. Generated benchmark artifacts (report cards,
leaderboards, snapshot manifests) default to data/benchmark/generated/ but
can be pointed elsewhere with --generated-dir.

Usage:
    uv run python apps/observatory/main.py
    uv run python apps/observatory/main.py --generated-dir .context/demo/public_payloads
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
        description=(
            "Observatory shell: browse benchmark suites, tasks, and artifacts. "
            "Suites, tasks, and public slices always come from the repo's "
            "checked-in contract metadata. Use --generated-dir to point at a "
            "custom directory for generated benchmark artifacts."
        ),
    )
    parser.add_argument(
        "--generated-dir",
        help="Override the generated benchmark output directory.",
    )
    args = parser.parse_args(argv)

    generated_dir = (
        Path(args.generated_dir).resolve() if args.generated_dir else None
    )
    index = build_observatory_index(generated_dir=generated_dir)
    print(format_observatory_index(index))
    return 0


if __name__ == "__main__":
    sys.exit(main())
