import csv
import json
from pathlib import Path

from scz_target_engine.atlas.mechanistic_axes import materialize_mechanistic_axes
from scz_target_engine.atlas.tensor import materialize_atlas_tensor


FIXTURE_MANIFEST_FILE = Path("data/curated/atlas/example_ingest_manifest.json").resolve()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_materialize_mechanistic_axes_emits_traceable_profiles(
    tmp_path: Path,
) -> None:
    tensor_result = materialize_atlas_tensor(
        ingest_manifest_file=FIXTURE_MANIFEST_FILE,
        output_dir=tmp_path / "tensor",
    )
    result = materialize_mechanistic_axes(
        tensor_manifest_file=Path(tensor_result["manifest_file"]),
        output_dir=tmp_path / "mechanistic_axes",
    )

    axis_rows = _read_csv_rows(Path(result["mechanistic_axes_file"]))
    evidence_rows = _read_csv_rows(Path(result["mechanistic_axis_evidence_links_file"]))

    assert result["axis_definition_count"] == 3
    assert result["axis_profile_count"] == 12

    rows_by_key = {
        (row["alignment_label"], row["axis_id"]): row
        for row in axis_rows
    }

    drd2_disease = rows_by_key[("DRD2", "mechanistic_axis:disease-association")]
    assert drd2_disease["support_state"] == "observed"
    assert drd2_disease["source_coverage_state"] == "cross_source"
    assert drd2_disease["conflict_state"] == "none"
    assert drd2_disease["uncertainty_max_level"] == "low"
    assert json.loads(drd2_disease["observed_source_names_json"]) == ["opentargets", "pgc"]

    cacna1c_disease = rows_by_key[("CACNA1C", "mechanistic_axis:disease-association")]
    assert cacna1c_disease["support_state"] == "partial_observed"
    assert cacna1c_disease["source_coverage_state"] == "single_source"
    assert cacna1c_disease["missingness_state"] == "source_absent"
    assert json.loads(cacna1c_disease["observed_source_names_json"]) == ["pgc"]

    setd1a_clinical = rows_by_key[("SETD1A", "mechanistic_axis:clinical-translation")]
    assert setd1a_clinical["support_state"] == "unobserved"
    assert setd1a_clinical["missingness_state"] == "field_blank"
    assert setd1a_clinical["uncertainty_max_level"] == "medium"
    assert json.loads(setd1a_clinical["missing_feature_ids_json"]) == [
        "opentargets.datatype.clinical"
    ]

    znf804a_variant = rows_by_key[("ZNF804A", "mechanistic_axis:variant-to-gene")]
    assert znf804a_variant["support_state"] == "observed"
    assert znf804a_variant["conflict_state"] == "alignment_id_conflict"
    assert znf804a_variant["uncertainty_max_level"] == "high"
    assert json.loads(znf804a_variant["resolved_provenance_bundle_ids_json"]) == [
        "opentargets:schizophrenia-baseline",
        "pgc:scz2022-prioritized-genes",
    ]

    conflict_link = next(
        row
        for row in evidence_rows
        if row["alignment_label"] == "ZNF804A"
        and row["axis_id"] == "mechanistic_axis:variant-to-gene"
        and row["tensor_channel"] == "conflict"
    )
    assert conflict_link["feature_id"] == "atlas.alignment_entity_id_conflict"
    assert json.loads(conflict_link["resolved_provenance_bundle_ids_json"]) == [
        "opentargets:schizophrenia-baseline",
        "pgc:scz2022-prioritized-genes",
    ]
    assert json.loads(conflict_link["resolved_source_row_indices_json"]) == ["3"]

    manifest = json.loads(Path(result["manifest_file"]).read_text(encoding="utf-8"))
    assert manifest["contract_version"] == "atlas-mechanistic-axes/v1"
    assert manifest["axis_profile_count"] == 12
