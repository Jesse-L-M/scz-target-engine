import json

from scz_target_engine.identity import (
    SourceIdentityMatch,
    build_gene_identity_fields,
)


def make_source_match(
    *,
    source: str,
    matched: bool,
    entity_id: str = "",
    entity_label: str = "",
    match_key: str = "",
    match_status: str = "",
) -> SourceIdentityMatch:
    return SourceIdentityMatch(
        source=source,
        matched=matched,
        entity_id=entity_id,
        entity_label=entity_label,
        match_key=match_key,
        match_status=match_status,
    )


def test_build_gene_identity_fields_covers_all_match_confidence_states() -> None:
    cases = [
        {
            "name": "seed_only",
            "seed_row": {"entity_id": "ENSGSEED1", "entity_label": "GENE1"},
            "source_matches": {
                "seed": make_source_match(
                    source="seed",
                    matched=True,
                    entity_id="ENSGSEED1",
                    entity_label="GENE1",
                    match_key="seed_entity_id",
                    match_status="seed",
                ),
                "pgc": make_source_match(source="pgc", matched=False),
                "schema": make_source_match(source="schema", matched=False),
                "psychencode": make_source_match(source="psychencode", matched=False),
                "opentargets": make_source_match(source="opentargets", matched=False),
                "chembl": make_source_match(source="chembl", matched=False),
            },
            "expected_confidence": "seed_only",
        },
        {
            "name": "id_confirmed",
            "seed_row": {"entity_id": "ENSGSEED2", "entity_label": "GENE2"},
            "source_matches": {
                "seed": make_source_match(
                    source="seed",
                    matched=True,
                    entity_id="ENSGSEED2",
                    entity_label="GENE2",
                    match_key="seed_entity_id",
                    match_status="seed",
                ),
                "pgc": make_source_match(
                    source="pgc",
                    matched=True,
                    entity_id="ENSGSEED2",
                    entity_label="GENE2",
                    match_key="entity_id",
                    match_status="matched",
                ),
                "schema": make_source_match(source="schema", matched=False),
                "psychencode": make_source_match(source="psychencode", matched=False),
                "opentargets": make_source_match(source="opentargets", matched=False),
                "chembl": make_source_match(source="chembl", matched=False),
            },
            "expected_confidence": "id_confirmed",
        },
        {
            "name": "source_confirmed",
            "seed_row": {"entity_id": "ENSGSEED3", "entity_label": "GENE3"},
            "source_matches": {
                "seed": make_source_match(
                    source="seed",
                    matched=True,
                    entity_id="ENSGSEED3",
                    entity_label="GENE3",
                    match_key="seed_entity_id",
                    match_status="seed",
                ),
                "pgc": make_source_match(
                    source="pgc",
                    matched=True,
                    entity_id="ENSGSEED3",
                    entity_label="GENE3",
                    match_key="entity_label",
                    match_status="matched",
                ),
                "schema": make_source_match(source="schema", matched=False),
                "psychencode": make_source_match(source="psychencode", matched=False),
                "opentargets": make_source_match(source="opentargets", matched=False),
                "chembl": make_source_match(source="chembl", matched=False),
            },
            "expected_confidence": "source_confirmed",
        },
        {
            "name": "source_matched",
            "seed_row": {"entity_id": "ENSGSEED4", "entity_label": "GENE4"},
            "source_matches": {
                "seed": make_source_match(
                    source="seed",
                    matched=True,
                    entity_id="ENSGSEED4",
                    entity_label="GENE4",
                    match_key="seed_entity_id",
                    match_status="seed",
                ),
                "pgc": make_source_match(
                    source="pgc",
                    matched=True,
                    entity_id="",
                    entity_label="GENE4",
                    match_key="entity_label",
                    match_status="matched",
                ),
                "schema": make_source_match(source="schema", matched=False),
                "psychencode": make_source_match(source="psychencode", matched=False),
                "opentargets": make_source_match(source="opentargets", matched=False),
                "chembl": make_source_match(source="chembl", matched=False),
            },
            "expected_confidence": "source_matched",
        },
        {
            "name": "source_conflict",
            "seed_row": {"entity_id": "ENSGSEED5", "entity_label": "GENE5"},
            "source_matches": {
                "seed": make_source_match(
                    source="seed",
                    matched=True,
                    entity_id="ENSGSEED5",
                    entity_label="GENE5",
                    match_key="seed_entity_id",
                    match_status="seed",
                ),
                "pgc": make_source_match(
                    source="pgc",
                    matched=True,
                    entity_id="ENSGOTHER5",
                    entity_label="GENE5",
                    match_key="entity_label",
                    match_status="matched",
                ),
                "schema": make_source_match(source="schema", matched=False),
                "psychencode": make_source_match(source="psychencode", matched=False),
                "opentargets": make_source_match(source="opentargets", matched=False),
                "chembl": make_source_match(source="chembl", matched=False),
            },
            "expected_confidence": "source_conflict",
        },
    ]

    for case in cases:
        identity_fields = build_gene_identity_fields(
            case["seed_row"],
            case["source_matches"],
            keep_canonical_alias=True,
        )

        assert identity_fields["primary_gene_id"] == case["seed_row"]["entity_id"]
        assert identity_fields["canonical_entity_id"] == case["seed_row"]["entity_id"]
        assert identity_fields["seed_entity_id"] == case["seed_row"]["entity_id"]
        assert identity_fields["match_confidence"] == case["expected_confidence"]

        provenance = json.loads(identity_fields["match_provenance_json"])
        assert [entry["source"] for entry in provenance] == [
            "seed",
            "pgc",
            "schema",
            "psychencode",
            "opentargets",
            "chembl",
        ]
