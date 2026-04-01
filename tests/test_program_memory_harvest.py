import csv

import pytest

import scz_target_engine.program_memory.extract as extract_module
from scz_target_engine.program_memory import (
    build_program_memory_harvest_batch,
    build_program_memory_harvest_review_rows,
    load_program_memory_harvest_batch,
    write_program_memory_harvest_batch,
)
from tests.program_memory_fixtures import (
    make_directionality_suggestion,
    make_event_suggestion,
    make_source_document,
)


def test_build_program_memory_harvest_batch_round_trips(tmp_path) -> None:
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-emraclidine",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            make_event_suggestion(),
            make_directionality_suggestion(),
        ],
    )

    assert harvest.harvest_id == "harvest-emraclidine"
    assert len(harvest.source_documents) == 1
    assert len(harvest.suggestions) == 2
    assert harvest.suggestions[0].asset is not None
    assert harvest.suggestions[0].asset.target_symbols == ("CHRM4",)
    assert harvest.suggestions[1].directionality_hypothesis is not None
    assert harvest.suggestions[1].candidate_identifier == "chrm4-candidate"

    harvest_path = tmp_path / "harvest.json"
    write_program_memory_harvest_batch(harvest_path, harvest)
    assert load_program_memory_harvest_batch(harvest_path) == harvest

    review_rows = build_program_memory_harvest_review_rows(harvest)
    assert review_rows == [
        {
            "suggestion_id": "emraclidine-event-suggestion",
            "suggestion_kind": "event",
            "candidate_identifier": "emraclidine-empower-acute-scz-topline-2024-candidate",
            "machine_confidence": "medium",
            "extractor_name": "llm-assisted-extractor",
            "source_document_id": "abbvie-emraclidine-2024-11-11",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/emraclidine",
            "needs_human_adjudication": "true",
            "proposed_record_type": "event",
        },
        {
            "suggestion_id": "chrm4-directionality-suggestion",
            "suggestion_kind": "directionality_hypothesis",
            "candidate_identifier": "chrm4-candidate",
            "machine_confidence": "low",
            "extractor_name": "llm-assisted-extractor",
            "source_document_id": "abbvie-emraclidine-2024-11-11",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/emraclidine",
            "needs_human_adjudication": "true",
            "proposed_record_type": "directionality_hypothesis",
        },
    ]


def test_build_program_memory_harvest_batch_rejects_unknown_source_document() -> None:
    with pytest.raises(ValueError, match="unknown source_document_id"):
        build_program_memory_harvest_batch(
            harvest_id="harvest-emraclidine",
            harvester="llm-assist",
            source_document_payloads=[make_source_document()],
            suggestion_payloads=[
                make_event_suggestion(source_document_id="missing-source"),
            ],
        )


def test_build_program_memory_harvest_batch_canonicalizes_alias_identity() -> None:
    base_suggestion = make_event_suggestion(suggestion_id="karxt-event-suggestion")
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-karxt",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            {
                **base_suggestion,
                "asset": {
                    "asset_id": "karxt",
                    "molecule": "KarXT",
                    "target": "CHRM1 / CHRM4",
                    "target_symbols": ["CHRM1", "CHRM4"],
                    "target_class": "muscarinic receptor modulation",
                    "mechanism": (
                        "preferential M1 and M4 muscarinic agonism paired with "
                        "peripheral antimuscarinic blockade to improve tolerability"
                    ),
                    "modality": "small_molecule_combination",
                },
                "event": {
                    **base_suggestion["event"],
                    "event_id": "karxt-acute-scz-topline-2026-candidate",
                    "asset_id": "karxt",
                    "sponsor": "Bristol Myers Squibb",
                },
                "provenance": {
                    "event_id": "karxt-acute-scz-topline-2026-candidate",
                    "source_tier": "company_press_release",
                    "source_url": "https://example.com/karxt",
                },
            }
        ],
    )

    suggestion = harvest.suggestions[0]
    assert suggestion.asset is not None
    assert suggestion.event is not None
    assert suggestion.asset.asset_id == "xanomeline-trospium"
    assert suggestion.asset.molecule == "xanomeline + trospium"
    assert suggestion.asset.asset_lineage_id == "asset:xanomeline-trospium"
    assert suggestion.asset.target_class == "muscarinic cholinergic modulation"
    assert (
        suggestion.asset.target_class_lineage_id
        == "target-class:muscarinic-cholinergic-modulation"
    )
    assert "KarXT" in suggestion.asset.asset_aliases
    assert "Cobenfy" in suggestion.asset.asset_aliases
    assert suggestion.event.asset_id == "xanomeline-trospium"


def test_build_program_memory_harvest_batch_canonicalizes_denominator_only_alias() -> None:
    base_suggestion = make_event_suggestion(suggestion_id="min-101-event-suggestion")
    harvest = build_program_memory_harvest_batch(
        harvest_id="harvest-min-101",
        harvester="llm-assist",
        created_at="2026-03-30",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            {
                **base_suggestion,
                "asset": {
                    "asset_id": "min-101",
                    "molecule": "MIN-101",
                    "target": "HTR2A / TMEM97",
                    "target_symbols": ["HTR2A", "TMEM97"],
                    "target_class": "sigma-2 / 5-HT2A modulation",
                    "mechanism": "sigma-2 receptor antagonism with 5-HT2A antagonism",
                    "modality": "small_molecule",
                },
                "event": {
                    **base_suggestion["event"],
                    "event_id": "min-101-negative-symptoms-topline-2026-candidate",
                    "asset_id": "min-101",
                    "population": "adults with stable schizophrenia and predominant negative symptoms",
                    "domain": "negative_symptoms",
                    "sponsor": "Minerva Neurosciences",
                    "phase": "phase_3",
                },
                "provenance": {
                    "event_id": "min-101-negative-symptoms-topline-2026-candidate",
                    "source_tier": "company_press_release",
                    "source_url": "https://example.com/min-101",
                },
            }
        ],
    )

    suggestion = harvest.suggestions[0]
    assert suggestion.asset is not None
    assert suggestion.event is not None
    assert suggestion.asset.asset_id == "roluperidone"
    assert suggestion.asset.asset_lineage_id == "asset:roluperidone"
    assert suggestion.asset.molecule == "roluperidone"
    assert suggestion.asset.target_class_lineage_id == "target-class:sigma-2-5ht2a-modulation"
    assert "MIN-101" in suggestion.asset.asset_aliases
    assert suggestion.event.asset_id == "roluperidone"


def test_parse_program_memory_asset_refreshes_identity_catalog_on_file_change(
    tmp_path,
    monkeypatch,
) -> None:
    assets_path = tmp_path / "assets.csv"
    universe_path = tmp_path / "program_universe.csv"
    fieldnames = [
        "asset_id",
        "molecule",
        "target",
        "target_symbols_json",
        "target_class",
        "mechanism",
        "modality",
        "asset_lineage_id",
        "asset_aliases_json",
        "target_class_lineage_id",
        "target_class_aliases_json",
    ]
    canonical_row = {
        "asset_id": "xanomeline-trospium",
        "molecule": "xanomeline + trospium",
        "target": "CHRM1 / CHRM4",
        "target_symbols_json": '["CHRM1", "CHRM4"]',
        "target_class": "muscarinic cholinergic modulation",
        "mechanism": (
            "preferential M1 and M4 muscarinic agonism paired with peripheral "
            "antimuscarinic blockade to improve tolerability"
        ),
        "modality": "small_molecule_combination",
        "asset_lineage_id": "asset:xanomeline-trospium",
        "asset_aliases_json": "[]",
        "target_class_lineage_id": "target-class:muscarinic-cholinergic-modulation",
        "target_class_aliases_json": '["muscarinic receptor modulation"]',
    }
    with assets_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(canonical_row)

    monkeypatch.setattr(
        extract_module,
        "CHECKED_IN_PROGRAM_MEMORY_ASSETS_PATH",
        assets_path,
    )
    monkeypatch.setattr(
        extract_module,
        "CHECKED_IN_PROGRAM_MEMORY_UNIVERSE_PATH",
        universe_path,
    )
    extract_module._load_checked_in_program_memory_identity_catalog.cache_clear()

    payload = {
        "asset_id": "mysteryxt",
        "molecule": "MysteryXT",
        "target": "CHRM1 / CHRM4",
        "target_symbols": ["CHRM1", "CHRM4"],
        "target_class": "muscarinic receptor modulation",
        "mechanism": canonical_row["mechanism"],
        "modality": "small_molecule_combination",
    }
    first_parse = extract_module.parse_program_memory_asset(payload)
    assert first_parse.asset_id == "mysteryxt"

    canonical_row["asset_aliases_json"] = '["MysteryXT"]'
    with assets_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(canonical_row)

    second_parse = extract_module.parse_program_memory_asset(payload)
    assert second_parse.asset_id == "xanomeline-trospium"
    assert second_parse.asset_lineage_id == "asset:xanomeline-trospium"
