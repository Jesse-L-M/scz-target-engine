from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from scz_target_engine.rescue import validate_rescue_governance_bundle


TASK_CARD_PATH = (
    REPO_ROOT
    / "data/curated/rescue_tasks/governance/interneuron_gene_rescue_task/task_card.json"
)


def _count_rows(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def main() -> None:
    bundle = validate_rescue_governance_bundle(TASK_CARD_PATH)
    dataset_row_counts = {
        dataset.dataset_id: _count_rows(REPO_ROOT / dataset.expected_output_path)
        for dataset in bundle.dataset_cards
    }
    print(
        json.dumps(
            {
                "task_id": bundle.task_card.task_id,
                "freeze_manifest_ids": [
                    manifest.freeze_manifest_id for manifest in bundle.freeze_manifests
                ],
                "dataset_row_counts": dataset_row_counts,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
