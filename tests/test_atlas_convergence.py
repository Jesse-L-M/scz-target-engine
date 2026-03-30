import csv
import json
from pathlib import Path

from scz_target_engine.atlas.convergence import materialize_convergence_hubs
from scz_target_engine.atlas.tensor import materialize_atlas_tensor


FIXTURE_MANIFEST_FILE = Path("data/curated/atlas/example_ingest_manifest.json").resolve()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_materialize_convergence_hubs_emits_axis_and_tensor_links(
    tmp_path: Path,
) -> None:
    tensor_result = materialize_atlas_tensor(
        ingest_manifest_file=FIXTURE_MANIFEST_FILE,
        output_dir=tmp_path / "tensor",
    )
    result = materialize_convergence_hubs(
        tensor_manifest_file=Path(tensor_result["manifest_file"]),
        output_dir=tmp_path / "convergence",
    )

    hub_rows = _read_csv_rows(Path(result["convergence_hubs_file"]))
    axis_member_rows = _read_csv_rows(Path(result["hub_axis_members_file"]))
    evidence_rows = _read_csv_rows(Path(result["hub_evidence_links_file"]))

    assert result["hub_count"] == 4
    assert result["hub_axis_member_count"] == 12

    hubs_by_label = {row["alignment_label"]: row for row in hub_rows}

    drd2_hub = hubs_by_label["DRD2"]
    assert drd2_hub["source_coverage_state"] == "cross_source"
    assert drd2_hub["axis_coverage_state"] == "multi_axis"
    assert drd2_hub["missingness_state"] == "none"
    assert drd2_hub["conflict_state"] == "none"
    assert drd2_hub["uncertainty_max_level"] == "low"
    assert json.loads(drd2_hub["supported_axis_ids_json"]) == [
        "mechanistic_axis:clinical-translation",
        "mechanistic_axis:disease-association",
        "mechanistic_axis:variant-to-gene",
    ]

    cacna1c_hub = hubs_by_label["CACNA1C"]
    assert cacna1c_hub["source_coverage_state"] == "single_source"
    assert cacna1c_hub["missingness_state"] == "source_absent"
    assert json.loads(cacna1c_hub["missing_source_names_json"]) == ["opentargets"]
    assert json.loads(cacna1c_hub["partial_axis_ids_json"]) == [
        "mechanistic_axis:disease-association"
    ]

    znf804a_hub = hubs_by_label["ZNF804A"]
    assert znf804a_hub["source_coverage_state"] == "cross_source"
    assert znf804a_hub["conflict_state"] == "alignment_id_conflict"
    assert znf804a_hub["uncertainty_max_level"] == "high"
    assert json.loads(znf804a_hub["conflicted_axis_ids_json"]) == [
        "mechanistic_axis:clinical-translation",
        "mechanistic_axis:disease-association",
        "mechanistic_axis:variant-to-gene",
    ]
    assert json.loads(znf804a_hub["resolved_provenance_bundle_ids_json"]) == [
        "opentargets:schizophrenia-baseline",
        "pgc:scz2022-prioritized-genes",
    ]

    znf804a_axis_members = [
        row
        for row in axis_member_rows
        if row["alignment_label"] == "ZNF804A"
    ]
    assert len(znf804a_axis_members) == 3
    assert {row["conflict_state"] for row in znf804a_axis_members} == {"alignment_id_conflict"}

    conflict_link = next(
        row
        for row in evidence_rows
        if row["alignment_label"] == "ZNF804A" and row["tensor_channel"] == "conflict"
    )
    assert json.loads(conflict_link["linked_axis_ids_json"]) == [
        "mechanistic_axis:clinical-translation",
        "mechanistic_axis:disease-association",
        "mechanistic_axis:variant-to-gene",
    ]
    assert json.loads(conflict_link["resolved_provenance_bundle_ids_json"]) == [
        "opentargets:schizophrenia-baseline",
        "pgc:scz2022-prioritized-genes",
    ]

    manifest = json.loads(Path(result["manifest_file"]).read_text(encoding="utf-8"))
    assert manifest["contract_version"] == "atlas-convergence-hubs/v1"
    assert manifest["hub_count"] == 4
