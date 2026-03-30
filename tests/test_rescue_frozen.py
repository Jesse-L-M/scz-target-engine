import hashlib
import json
from collections import Counter
from pathlib import Path

from scz_target_engine.rescue import load_frozen_rescue_task_bundle


SOURCE_MANIFEST_PATH = Path(
    "data/raw/rescue/npc_signature_reversal/source_manifest.json"
)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_npc_source_manifest_matches_checked_in_artifacts() -> None:
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text(encoding="utf-8"))

    assert manifest["task_id"] == "scz_npc_signature_reversal_rescue_task"
    assert manifest["cutoff_date"] == "2020-12-31"
    assert {source["availability"] for source in manifest["sources"]} == {
        "pre_cutoff",
        "post_cutoff",
    }
    assert {source["license"] for source in manifest["sources"]} == {
        "CC BY 4.0",
        "CC BY",
    }

    for source in manifest["sources"]:
        materialized_path = Path(source["materialized_path"])
        assert materialized_path.exists()
        assert _sha256_path(materialized_path) == source["materialized_sha256"]
        assert source["upstream_url"].startswith("https://")
        assert source["row_count"] > 0

    for frozen_output in manifest["frozen_outputs"]:
        output_path = Path(frozen_output["path"])
        assert output_path.exists()
        assert _sha256_path(output_path) == frozen_output["sha256"]
        assert frozen_output["row_count"] == 15614


def test_npc_frozen_bundle_loads_checked_in_csvs_only() -> None:
    bundle = load_frozen_rescue_task_bundle(
        rescue_task_id="scz_npc_signature_reversal_rescue_task"
    )

    assert bundle.ranking_input.card.dataset_id == (
        "scz_npc_signature_reversal_ranking_inputs_2020_12_31"
    )
    assert bundle.evaluation_target.card.dataset_id == (
        "scz_npc_signature_reversal_evaluation_labels_2022_02_23"
    )
    assert "split_name" in bundle.ranking_input.columns
    assert "split_name" in bundle.evaluation_target.columns

    ranking_ids = {row["gene_id"] for row in bundle.ranking_input.rows}
    evaluation_ids = {row["gene_id"] for row in bundle.evaluation_target.rows}
    assert ranking_ids == evaluation_ids

    split_counts = Counter(row["split_name"] for row in bundle.ranking_input.rows)
    assert split_counts == {"train": 10929, "validation": 2342, "test": 2343}

    positive_counts = Counter(
        row["split_name"]
        for row in bundle.evaluation_target.rows
        if row["rescue_positive_label"] == "1"
    )
    assert positive_counts == {"train": 9, "validation": 3, "test": 2}
