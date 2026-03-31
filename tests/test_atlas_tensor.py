import csv
import json
from pathlib import Path

from scz_target_engine.atlas.tensor import materialize_atlas_tensor


FIXTURE_MANIFEST_FILE = Path("data/curated/atlas/example_ingest_manifest.json").resolve()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_materialize_atlas_tensor_emits_provenance_alignment_and_channel_rows(
    tmp_path: Path,
) -> None:
    result = materialize_atlas_tensor(
        ingest_manifest_file=FIXTURE_MANIFEST_FILE,
        output_dir=tmp_path,
    )

    provenance_rows = _read_csv_rows(Path(result["provenance_bundles_file"]))
    alignment_rows = _read_csv_rows(Path(result["entity_alignments_file"]))
    tensor_rows = _read_csv_rows(Path(result["evidence_tensor_file"]))

    assert result["alignment_count"] == 4
    assert result["provenance_bundle_count"] == 2
    assert result["tensor_row_count"] == 90
    assert result["channel_counts"] == {
        "conflict": 1,
        "missingness": 12,
        "observed": 32,
        "uncertainty": 45,
    }

    assert {row["source_name"] for row in provenance_rows} == {"opentargets", "pgc"}
    assert "001_meta.json" in provenance_rows[0]["staged_artifacts_json"]
    assert "001_figshare_article_19426775.json" in provenance_rows[1]["staged_artifacts_json"]

    alignments_by_label = {row["alignment_label"]: row for row in alignment_rows}
    assert alignments_by_label["DRD2"]["alignment_status"] == "id_consistent"
    assert alignments_by_label["CACNA1C"]["alignment_status"] == "id_consistent"
    assert alignments_by_label["SETD1A"]["alignment_status"] == "id_consistent"
    assert alignments_by_label["ZNF804A"]["alignment_status"] == "id_conflict"

    conflict_rows = [
        row
        for row in tensor_rows
        if row["channel"] == "conflict" and row["feature_id"] == "atlas.alignment_entity_id_conflict"
    ]
    assert len(conflict_rows) == 1
    assert conflict_rows[0]["alignment_label"] == "ZNF804A"
    assert conflict_rows[0]["text_value"] == "id_conflict"

    source_absent_rows = [
        row
        for row in tensor_rows
        if row["channel"] == "missingness"
        and row["text_value"] == "source_absent"
        and row["alignment_label"] == "CACNA1C"
        and row["feature_id"] == "opentargets.generic_platform_baseline"
    ]
    assert len(source_absent_rows) == 1

    field_blank_rows = [
        row
        for row in tensor_rows
        if row["channel"] == "missingness"
        and row["text_value"] == "source_field_blank"
        and row["alignment_label"] == "SETD1A"
        and row["feature_id"] == "opentargets.datatype.clinical"
    ]
    assert len(field_blank_rows) == 1

    low_uncertainty_rows = [
        row
        for row in tensor_rows
        if row["channel"] == "uncertainty"
        and row["alignment_label"] == "DRD2"
        and row["feature_id"] == "pgc.common_variant_support"
        and row["text_value"] == "low"
    ]
    assert len(low_uncertainty_rows) == 1
    assert float(low_uncertainty_rows[0]["numeric_value"]) == 0.2

    manifest = json.loads(Path(result["manifest_file"]).read_text(encoding="utf-8"))
    assert manifest["contract_version"] == "atlas-evidence-tensor/v1"
    assert manifest["channel_counts"]["conflict"] == 1
    assert manifest["ingest_manifest_file"] == "data/curated/atlas/example_ingest_manifest.json"
    assert manifest["output_dir"] == "."
    assert manifest["taxonomy_output_dir"] == "taxonomy"
    assert manifest["emitted_artifacts"] == {
        "entity_alignments_file": "entity_alignments.csv",
        "evidence_tensor_file": "evidence_tensor.csv",
        "provenance_bundles_file": "provenance_bundles.csv",
        "taxonomy_manifest_file": "taxonomy/taxonomy_manifest.json",
    }
    assert {
        row["processed_output_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/example_sources/opentargets/schizophrenia_baseline.csv",
        "data/curated/atlas/example_sources/pgc/scz2022_prioritized_genes.csv",
    }
    assert {
        row["processed_metadata_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/example_sources/opentargets/schizophrenia_baseline.metadata.json",
        "data/curated/atlas/example_sources/pgc/scz2022_prioritized_genes.metadata.json",
    }
    assert {
        row["raw_manifest_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/example_raw/opentargets/manifest.json",
        "data/curated/atlas/example_raw/pgc/manifest.json",
    }
