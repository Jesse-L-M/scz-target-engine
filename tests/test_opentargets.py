import json
from pathlib import Path

from scz_target_engine.atlas.sources import fetch_atlas_opentargets_baseline
from scz_target_engine.sources.opentargets import (
    fetch_opentargets_baseline,
    flatten_association_rows,
    search_disease,
)


def fake_transport(query: str, variables: dict[str, object]) -> dict[str, object]:
    if "query Meta" in query:
        return {
            "meta": {
                "name": "Open Targets GraphQL & REST API Beta",
                "product": "platform",
                "apiVersion": {"x": "26", "y": "03", "z": "0", "suffix": None},
                "dataVersion": {"year": "26", "month": "03", "iteration": None},
            }
        }
    if "query SearchDisease" in query:
        return {
            "search": {
                "hits": [
                    {"object": {"id": "MONDO_0005090", "name": "schizophrenia"}},
                    {
                        "object": {
                            "id": "EFO_0004609",
                            "name": "treatment refractory schizophrenia",
                        }
                    },
                ]
            }
        }
    if "query DiseaseAssociations" in query:
        index = int(variables["index"])
        if index == 0:
            return {
                "disease": {
                    "id": "MONDO_0005090",
                    "name": "schizophrenia",
                    "associatedTargets": {
                        "count": 3,
                        "rows": [
                            {
                                "score": 0.8,
                                "datatypeScores": [
                                    {"id": "clinical", "score": 0.9},
                                    {"id": "genetic_association", "score": 0.7},
                                ],
                                "target": {
                                    "id": "ENSG000001",
                                    "approvedSymbol": "GENE1",
                                    "approvedName": "Gene one",
                                },
                            },
                            {
                                "score": 0.6,
                                "datatypeScores": [
                                    {"id": "genetic_association", "score": 0.6},
                                ],
                                "target": {
                                    "id": "ENSG000002",
                                    "approvedSymbol": "GENE2",
                                    "approvedName": "Gene two",
                                },
                            },
                        ],
                    },
                }
            }
        return {
            "disease": {
                "id": "MONDO_0005090",
                "name": "schizophrenia",
                "associatedTargets": {
                    "count": 3,
                    "rows": [
                        {
                            "score": 0.55,
                            "datatypeScores": [],
                            "target": {
                                "id": "ENSG000003",
                                "approvedSymbol": "GENE3",
                                "approvedName": "Gene three",
                            },
                        }
                    ],
                },
            }
        }
    raise AssertionError(f"Unexpected query: {query}")


def test_search_disease_prefers_exact_match() -> None:
    result = search_disease("schizophrenia", fake_transport)
    assert result["id"] == "MONDO_0005090"


def test_flatten_association_rows_expands_datatype_columns() -> None:
    fieldnames, rows = flatten_association_rows(
        {"id": "MONDO_0005090", "name": "schizophrenia"},
        {
            "apiVersion": {"x": "26", "y": "03", "z": "0", "suffix": None},
            "dataVersion": {"year": "26", "month": "03", "iteration": None},
        },
        [
            {
                "score": 0.75,
                "datatypeScores": [
                    {"id": "clinical", "score": 0.8},
                    {"id": "literature", "score": 0.6},
                ],
                "target": {
                    "id": "ENSG000001",
                    "approvedSymbol": "GENE1",
                    "approvedName": "Gene one",
                },
            }
        ],
    )
    assert "opentargets_datatype_clinical" in fieldnames
    assert rows[0]["entity_label"] == "GENE1"
    assert rows[0]["generic_platform_baseline"] == 0.75


def test_fetch_opentargets_baseline_writes_csv_and_metadata(tmp_path: Path) -> None:
    output_file = tmp_path / "baseline.csv"
    metadata = fetch_opentargets_baseline(
        output_file=output_file,
        disease_query="schizophrenia",
        page_size=2,
        transport=fake_transport,
    )
    assert metadata["row_count"] == 3
    assert output_file.exists()
    assert output_file.with_suffix(".metadata.json").exists()


def test_fetch_atlas_opentargets_baseline_preserves_processed_output_and_stages_raw_artifacts(
    tmp_path: Path,
) -> None:
    legacy_output_file = tmp_path / "legacy.csv"
    atlas_output_file = tmp_path / "atlas.csv"

    legacy_metadata = fetch_opentargets_baseline(
        output_file=legacy_output_file,
        disease_query="schizophrenia",
        page_size=2,
        transport=fake_transport,
    )
    atlas_metadata = fetch_atlas_opentargets_baseline(
        output_file=atlas_output_file,
        disease_query="schizophrenia",
        page_size=2,
        raw_dir=tmp_path / "raw",
        materialized_at="2026-03-30",
        transport=fake_transport,
    )

    assert atlas_output_file.read_text(encoding="utf-8") == legacy_output_file.read_text(
        encoding="utf-8"
    )
    assert atlas_metadata["legacy_adapter_output"]["row_count"] == legacy_metadata["row_count"]

    manifest = json.loads(
        Path(atlas_metadata["raw_manifest_file"]).read_text(encoding="utf-8")
    )
    assert manifest["status"] == "completed"
    assert manifest["source_contract"]["source_name"] == "opentargets"
    assert manifest["raw_artifact_count"] == 4
    artifact_names = [artifact["artifact_name"] for artifact in manifest["artifacts"]]
    assert artifact_names[0] == "001_meta.json"
    assert artifact_names[1] == "002_searchdisease.json"
    assert artifact_names[2] == "003_diseaseassociations_page_0000.json"
    assert artifact_names[3] == "004_diseaseassociations_page_0001.json"
