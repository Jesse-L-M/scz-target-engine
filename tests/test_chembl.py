from pathlib import Path

from scz_target_engine.sources.chembl import (
    compute_tractability_compoundability,
    fetch_chembl_tractability,
    select_best_target,
)


def fake_transport(url: str) -> dict[str, object]:
    if "target/search" in url:
        return {
            "targets": [
                {
                    "organism": "Homo sapiens",
                    "pref_name": "D(2) dopamine receptor",
                    "score": 14.0,
                    "target_chembl_id": "CHEMBL217",
                    "target_type": "SINGLE PROTEIN",
                    "target_components": [
                        {
                            "target_component_synonyms": [
                                {"component_synonym": "DRD2", "syn_type": "GENE_SYMBOL"}
                            ]
                        }
                    ],
                }
            ]
        }
    if "target/CHEMBL217" in url:
        return {
            "target_chembl_id": "CHEMBL217",
            "pref_name": "D(2) dopamine receptor",
            "organism": "Homo sapiens",
            "target_type": "SINGLE PROTEIN",
        }
    if "activity" in url:
        return {"page_meta": {"total_count": 32479}, "activities": [{"standard_type": "IC50"}]}
    if "mechanism" in url:
        return {
            "page_meta": {"total_count": 68},
            "mechanisms": [
                {"max_phase": 4, "action_type": "ANTAGONIST"},
                {"max_phase": 3, "action_type": "INVERSE AGONIST"},
            ],
        }
    raise AssertionError(f"Unexpected URL: {url}")


def test_select_best_target_requires_exact_human_match() -> None:
    selected, meta = select_best_target(
        "DRD2",
        [
            {
                "organism": "Mus musculus",
                "target_type": "SINGLE PROTEIN",
                "score": 10.0,
                "target_components": [
                    {
                        "target_component_synonyms": [
                            {"component_synonym": "DRD2", "syn_type": "GENE_SYMBOL"}
                        ]
                    }
                ],
            }
        ],
    )
    assert selected is None
    assert meta["match_status"] == "no_exact_human_gene_symbol_match"


def test_compute_tractability_compoundability_is_bounded() -> None:
    score = compute_tractability_compoundability("SINGLE PROTEIN", 1000, 20, 4)
    assert 0 <= score <= 1


def test_fetch_chembl_tractability_writes_rows(tmp_path: Path) -> None:
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        "entity_id,entity_label,approved_name\nENSG000001,DRD2,dopamine receptor D2\n",
        encoding="utf-8",
    )
    output_file = tmp_path / "chembl.csv"
    metadata = fetch_chembl_tractability(
        input_file=input_file,
        output_file=output_file,
        transport=fake_transport,
    )
    assert metadata["row_count"] == 1
    assert output_file.exists()
    csv_text = output_file.read_text(encoding="utf-8")
    assert "tractability_compoundability" in csv_text
    assert "CHEMBL217" in csv_text
