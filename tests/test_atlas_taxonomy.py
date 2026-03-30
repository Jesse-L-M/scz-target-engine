import csv
import json
from pathlib import Path

from scz_target_engine.atlas.taxonomy import materialize_atlas_taxonomy


FIXTURE_MANIFEST_FILE = Path("data/curated/atlas/example_ingest_manifest.json").resolve()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_materialize_atlas_taxonomy_writes_expected_fixture_surfaces(
    tmp_path: Path,
) -> None:
    result = materialize_atlas_taxonomy(
        ingest_manifest_file=FIXTURE_MANIFEST_FILE,
        output_dir=tmp_path,
    )

    dimensions = _read_csv_rows(Path(result["context_dimensions_file"]))
    members = _read_csv_rows(Path(result["context_members_file"]))
    features = _read_csv_rows(Path(result["feature_taxonomy_file"]))

    assert result["dimension_count"] == 11
    assert result["member_count"] == 27
    assert result["feature_count"] == 12

    dimension_ids = {row["dimension_id"] for row in dimensions}
    assert "channel" in dimension_ids
    assert "criterion" in dimension_ids
    assert "uncertainty_level" in dimension_ids

    member_ids = {row["member_id"] for row in members}
    assert "channel:observed" in member_ids
    assert "source:atlas" in member_ids
    assert "disease:mondo-0005090" in member_ids
    assert "criterion:sig-adultfusion" in member_ids

    feature_rows = {row["feature_id"]: row for row in features}
    assert "opentargets.generic_platform_baseline" in feature_rows
    assert "opentargets.datatype.clinical" in feature_rows
    assert "pgc.common_variant_support" in feature_rows
    assert "atlas.alignment_entity_id_conflict" in feature_rows

    atlas_feature = feature_rows["atlas.alignment_entity_id_conflict"]
    assert json.loads(atlas_feature["channels_json"]) == ["conflict", "uncertainty"]
