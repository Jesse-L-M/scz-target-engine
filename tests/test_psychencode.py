import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

from scz_target_engine.sources.psychencode import (
    build_module_member_gene_entries,
    fetch_psychencode_module_table,
    fetch_psychencode_support,
)


def fake_text_transport(url: str) -> str:
    assert url.endswith("Schizophrenia_DEGcombined.csv")
    return (
        ",gene,baseMean,log2FoldChange,lfcSE,stat,pvalue,padj,cell_type\n"
        "0,DRD2,10,0.8,0.1,0,0.001,0.01,L4.IT\n"
        "1,DRD2,9,0.6,0.1,0,0.005,,OPC\n"
        "2,GRM3,4,0.2,0.1,0,0.2,,Ast\n"
    )


def fake_bytes_transport(url: str) -> bytes:
    assert url.endswith("GRNs.zip")
    payload = io.BytesIO()
    with ZipFile(payload, "w") as zip_file:
        zip_file.writestr(
            "Ast_GRN.txt",
            (
                "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                "TFAP2A\tchr1\tchr1\tDRD2\t0.09\tscGRNom\tAst\t0.5\tActivating\n"
                "CREB1\tchr1\tchr1\tDRD2\t0.07\tscGRNom\tAst\t0.2\tActivating\n"
            ),
        )
        zip_file.writestr(
            "OPC_GRN.txt",
            (
                "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                "SOX10\tchr1\tchr1\tDRD2\t0.11\tscGRNom\tOPC\t0.7\tActivating\n"
                "OLIG2\tchr1\tchr1\tCHRM4\t0.04\tscGRNom\tOPC\t0.3\tRepressing\n"
            ),
        )
    return payload.getvalue()


def fake_module_text_transport(url: str) -> str:
    assert url.endswith("Schizophrenia_DEGcombined.csv")
    return (
        ",gene,baseMean,log2FoldChange,lfcSE,stat,pvalue,padj,cell_type\n"
        "0,DRD2,10,0.8,0.1,0,0.001,0.01,OPC\n"
        "1,GRM3,9,0.7,0.1,0,0.005,0.02,OPC\n"
        "2,SETD1A,8,0.6,0.1,0,0.02,0.05,OPC\n"
        "3,CHRM4,7,0.4,0.1,0,0.04,0.08,Ast\n"
        "4,GRM3,6,0.5,0.1,0,0.03,0.07,Ast\n"
    )


def fake_module_bytes_transport(url: str) -> bytes:
    assert url.endswith("GRNs.zip")
    payload = io.BytesIO()
    with ZipFile(payload, "w") as zip_file:
        zip_file.writestr(
            "OPC_GRN.txt",
            (
                "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                "SOX10\tchr1\tchr1\tDRD2\t0.11\tscGRNom\tOPC\t0.7\tActivating\n"
                "OLIG2\tchr1\tchr1\tGRM3\t0.09\tscGRNom\tOPC\t0.5\tActivating\n"
                "TCF4\tchr1\tchr1\tCACNA1C\t0.08\tscGRNom\tOPC\t0.4\tActivating\n"
                "BCL11B\tchr1\tchr1\tRELN\t0.07\tscGRNom\tOPC\t0.3\tRepressing\n"
            ),
        )
        zip_file.writestr(
            "Ast_GRN.txt",
            (
                "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                "CREB1\tchr1\tchr1\tCHRM4\t0.05\tscGRNom\tAst\t0.2\tActivating\n"
            ),
        )
        zip_file.writestr(
            "Micro_GRN.txt",
            (
                "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                "SPI1\tchr1\tchr1\tDRD2\t0.06\tscGRNom\tMicro\t0.2\tActivating\n"
                "IRF8\tchr1\tchr1\tCHRM4\t0.05\tscGRNom\tMicro\t0.2\tActivating\n"
                "RUNX1\tchr1\tchr1\tSETD1A\t0.04\tscGRNom\tMicro\t0.2\tRepressing\n"
            ),
        )
    return payload.getvalue()


def test_fetch_psychencode_support_writes_shortlist_support_rows(tmp_path: Path) -> None:
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "psychencode.csv"
    input_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0001,DRD2\n"
            "ENSGEX0002,CHRM4\n"
            "ENSGEX0003,SETD1A\n"
        ),
        encoding="utf-8",
    )

    metadata = fetch_psychencode_support(
        input_file=input_file,
        output_file=output_file,
        text_transport=fake_text_transport,
        bytes_transport=fake_bytes_transport,
    )

    assert metadata["input_row_count"] == 3
    assert metadata["unique_input_gene_count"] == 3
    assert metadata["row_count"] == 2
    assert metadata["matched_gene_count"] == 2
    assert metadata["unmatched_gene_count"] == 1
    assert metadata["unmatched_gene_labels"] == ["SETD1A"]
    assert metadata["deg_match_count"] == 1
    assert metadata["grn_match_count"] == 2
    assert metadata["grn_member_count"] == 2
    assert metadata["unmatched_genes"] == [
        {
            "approved_name": "",
            "entity_id": "ENSGEX0003",
            "entity_label": "SETD1A",
            "psychencode_match_status": "absent_from_deg_and_grn",
            "reason": (
                "No exact BrainSCOPE schizophrenia DEG `gene` or GRN `TG` symbol matched "
                "this input gene."
            ),
        }
    ]

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    drd2_row = next(row for row in rows if row["entity_label"] == "DRD2")
    chrm4_row = next(row for row in rows if row["entity_label"] == "CHRM4")

    assert drd2_row["psychencode_match_status"] == "matched_deg_and_grn"
    assert float(drd2_row["cell_state_support"]) > 0.0
    assert float(drd2_row["developmental_regulatory_support"]) > 0.0
    assert drd2_row["psychencode_deg_cell_type_count"] == "2"
    assert drd2_row["psychencode_grn_cell_type_count"] == "2"
    assert json.loads(drd2_row["psychencode_grn_regulation_breakdown_json"]) == {
        "Activating": 3
    }

    assert chrm4_row["psychencode_match_status"] == "matched_grn_only"
    assert chrm4_row["cell_state_support"] == "0.0"
    assert float(chrm4_row["developmental_regulatory_support"]) > 0.0
    assert chrm4_row["psychencode_deg_row_count"] == "0"
    assert json.loads(chrm4_row["psychencode_grn_top_tfs_json"])[0]["tf"] == "OLIG2"


def test_fetch_psychencode_support_does_not_guess_aliases_for_absent_genes(
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "psychencode.csv"
    input_file.write_text(
        (
            "entity_id,entity_label\n"
            "ENSGEX0001,C4A\n"
            "ENSGEX0002,CHRM4\n"
        ),
        encoding="utf-8",
    )

    def fake_text_transport_without_exact_matches(url: str) -> str:
        assert url.endswith("Schizophrenia_DEGcombined.csv")
        return (
            ",gene,baseMean,log2FoldChange,lfcSE,stat,pvalue,padj,cell_type\n"
            "0,C4B,10,0.8,0.1,0,0.001,0.01,Ast\n"
            "1,CHRM5,9,0.6,0.1,0,0.005,,OPC\n"
        )

    def fake_bytes_transport_without_exact_matches(url: str) -> bytes:
        assert url.endswith("GRNs.zip")
        payload = io.BytesIO()
        with ZipFile(payload, "w") as zip_file:
            zip_file.writestr(
                "Ast_GRN.txt",
                (
                    "TF\tenhancer\tpromoter\tTG\tedgeWeight\tmethod\tcelltype\tCorrelation\tRegulation\n"
                    "TFAP2A\tchr1\tchr1\tC4B\t0.09\tscGRNom\tAst\t0.5\tActivating\n"
                    "CREB1\tchr1\tchr1\tCHRM5\t0.07\tscGRNom\tAst\t0.2\tActivating\n"
                ),
            )
        return payload.getvalue()

    metadata = fetch_psychencode_support(
        input_file=input_file,
        output_file=output_file,
        text_transport=fake_text_transport_without_exact_matches,
        bytes_transport=fake_bytes_transport_without_exact_matches,
    )

    assert metadata["row_count"] == 0
    assert metadata["matched_gene_count"] == 0
    assert metadata["unmatched_gene_count"] == 2
    assert metadata["unmatched_gene_labels"] == ["C4A", "CHRM4"]
    assert metadata["deg_match_count"] == 0
    assert metadata["grn_match_count"] == 0

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows == []


def test_fetch_psychencode_module_table_derives_cell_type_modules(tmp_path: Path) -> None:
    input_file = tmp_path / "candidate_gene_registry.csv"
    output_file = tmp_path / "module_evidence.csv"
    input_file.write_text(
        (
            "entity_id,entity_label,approved_name,common_variant_support,"
            "match_confidence,registry_sources_json,seed_entity_id\n"
            "ENSGEX0001,DRD2,dopamine receptor D2,0.20,source_confirmed,"
            "\"[\"\"pgc\"\", \"\"opentargets\"\"]\",\n"
            "ENSGEX0002,CHRM4,cholinergic receptor muscarinic 4,0.10,source_confirmed,"
            "\"[\"\"pgc\"\"]\",\n"
            "ENSGEX0003,GRM3,glutamate metabotropic receptor 3,0.30,source_confirmed,"
            "\"[\"\"pgc\"\"]\",\n"
            "ENSGEX0004,SETD1A,SET domain containing 1A,0.80,source_confirmed,"
            "\"[\"\"pgc\"\"]\",\n"
            "ENSGEX0005,CACNA1C,calcium voltage-gated channel subunit alpha1 C,0.25,source_confirmed,"
            "\"[\"\"opentargets\"\"]\",\n"
            "ENSGEX0006,RELN,reelin,0.15,source_confirmed,"
            "\"[\"\"opentargets\"\"]\",\n"
        ),
        encoding="utf-8",
    )

    metadata = fetch_psychencode_module_table(
        input_file=input_file,
        output_file=output_file,
        text_transport=fake_module_text_transport,
        bytes_transport=fake_module_bytes_transport,
    )

    assert metadata["input_gene_count"] == 6
    assert metadata["input_row_count"] == 6
    assert metadata["duplicate_input_gene_label_count"] == 0
    assert metadata["duplicate_input_gene_labels"] == []
    assert metadata["candidate_cell_type_count"] == 3
    assert metadata["row_count"] == 1
    assert metadata["retained_cell_type_count"] == 1
    assert metadata["dropped_cell_type_count"] == 2
    assert metadata["dropped_cell_types"] == ["Ast", "Micro"]
    assert metadata["minimum_member_gene_count"] == 5
    assert metadata["minimum_genetically_supported_member_gene_count"] == 3
    assert metadata["minimum_deg_gene_count"] == 2
    assert metadata["minimum_grn_target_gene_count"] == 2
    ast_dropped = next(
        module for module in metadata["dropped_modules"] if module["cell_type"] == "Ast"
    )
    assert ast_dropped["drop_reasons"] == [
        "member_gene_count_below_minimum",
        "genetically_supported_member_gene_count_below_minimum",
        "grn_target_gene_count_below_minimum",
    ]

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    opc_row = next(row for row in rows if row["psychencode_module_cell_type"] == "OPC")

    assert opc_row["entity_id"] == "psychencode:opc"
    assert opc_row["entity_label"] == "BrainSCOPE OPC"
    assert float(opc_row["member_gene_genetic_enrichment"]) > 0.0
    assert float(opc_row["cell_state_specificity"]) > 0.0
    assert float(opc_row["developmental_regulatory_relevance"]) > 0.0
    assert opc_row["psychencode_module_member_gene_count"] == "5"
    assert opc_row["psychencode_module_genetically_supported_gene_count"] == "5"
    assert json.loads(opc_row["psychencode_module_member_source_breakdown_json"]) == {
        "deg_and_grn": 2,
        "deg_only": 1,
        "grn_only": 2,
    }
    admissibility = json.loads(opc_row["psychencode_module_admissibility_json"])
    assert admissibility["status"] == "kept"
    assert admissibility["drop_reasons"] == []
    top_members = json.loads(opc_row["psychencode_module_top_member_genes_json"])
    assert top_members[0]["entity_label"] == "SETD1A"
    assert top_members[0]["membership_sources"] == ["deg"]
    assert top_members[0]["provenance_sources"] == ["pgc"]
    assert ("grn",) in {
        tuple(entry["membership_sources"])
        for entry in top_members
    }
    assert {entry["entity_label"] for entry in json.loads(opc_row["psychencode_module_top_grn_targets_json"])} == {
        "CACNA1C",
        "DRD2",
        "GRM3",
        "RELN",
    }
    assert {entry["tf"] for entry in json.loads(opc_row["psychencode_module_top_tfs_json"])} == {
        "BCL11B",
        "SOX10",
        "OLIG2",
        "TCF4",
    }
    retained_opc = next(
        module for module in metadata["retained_modules"] if module["cell_type"] == "OPC"
    )
    assert retained_opc["admissibility"]["status"] == "kept"
    assert len(retained_opc["top_member_genes"]) == 5
    assert retained_opc["top_member_genes"][0]["entity_label"] == "SETD1A"
    assert retained_opc["top_member_genes"][0]["provenance_sources"] == ["pgc"]
    assert "member_gene_provenance" not in retained_opc


def test_fetch_psychencode_module_table_keeps_same_label_candidates_distinct(
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "candidate_gene_registry.csv"
    output_file = tmp_path / "module_evidence.csv"
    input_file.write_text(
        (
            "entity_id,entity_label,approved_name,common_variant_support,rare_variant_support,"
            "match_confidence,registry_sources_json,registry_source_count\n"
            "ENSGDRD2LOW,DRD2,dopamine receptor D2,0.10,,source_confirmed,"
            "\"[\"\"opentargets\"\"]\",1\n"
            "ENSGDRD2HIGH,DRD2,dopamine receptor D2,0.40,0.20,id_confirmed,"
            "\"[\"\"pgc\"\", \"\"opentargets\"\"]\",2\n"
            "ENSGEX0003,GRM3,glutamate metabotropic receptor 3,0.30,,source_confirmed,"
            "\"[\"\"pgc\"\"]\",1\n"
            "ENSGEX0004,SETD1A,SET domain containing 1A,0.80,,source_confirmed,"
            "\"[\"\"pgc\"\"]\",1\n"
            "ENSGEX0005,CACNA1C,calcium voltage-gated channel subunit alpha1 C,0.25,,source_confirmed,"
            "\"[\"\"opentargets\"\"]\",1\n"
            "ENSGEX0006,RELN,reelin,0.15,,source_confirmed,"
            "\"[\"\"opentargets\"\"]\",1\n"
        ),
        encoding="utf-8",
    )

    metadata = fetch_psychencode_module_table(
        input_file=input_file,
        output_file=output_file,
        text_transport=fake_module_text_transport,
        bytes_transport=fake_module_bytes_transport,
    )

    assert metadata["input_row_count"] == 6
    assert metadata["input_gene_count"] == 6
    assert metadata["duplicate_input_gene_label_count"] == 1
    assert metadata["duplicate_input_gene_labels"] == [
        {
            "entity_label": "DRD2",
            "representative_entity_id": "ENSGDRD2HIGH",
            "candidate_row_count": 2,
            "candidate_entity_ids": ["ENSGDRD2HIGH", "ENSGDRD2LOW"],
            "provenance_sources": ["pgc", "opentargets"],
            "match_confidences": ["id_confirmed", "source_confirmed"],
        }
    ]

    with output_file.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    opc_row = next(row for row in rows if row["psychencode_module_cell_type"] == "OPC")
    assert opc_row["psychencode_module_member_gene_count"] == "6"
    assert opc_row["psychencode_module_genetically_supported_gene_count"] == "6"
    assert json.loads(opc_row["psychencode_module_member_source_breakdown_json"]) == {
        "deg_and_grn": 3,
        "deg_only": 1,
        "grn_only": 2,
    }
    top_members = json.loads(opc_row["psychencode_module_top_member_genes_json"])
    drd2_entries = [
        entry for entry in top_members if entry["entity_label"] == "DRD2"
    ]

    assert len(drd2_entries) == 2
    assert {entry["entity_id"] for entry in drd2_entries} == {
        "ENSGDRD2HIGH",
        "ENSGDRD2LOW",
    }
    assert all(entry["candidate_row_count"] == 1 for entry in drd2_entries)
    assert {tuple(entry["candidate_entity_ids"]) for entry in drd2_entries} == {
        ("ENSGDRD2HIGH",),
        ("ENSGDRD2LOW",),
    }
    high_entry = next(entry for entry in drd2_entries if entry["entity_id"] == "ENSGDRD2HIGH")
    low_entry = next(entry for entry in drd2_entries if entry["entity_id"] == "ENSGDRD2LOW")
    assert high_entry["common_variant_support"] == 0.4
    assert high_entry["rare_variant_support"] == 0.2
    assert high_entry["provenance_sources"] == ["pgc", "opentargets"]
    assert low_entry["common_variant_support"] == 0.1
    assert low_entry["rare_variant_support"] is None
    assert low_entry["provenance_sources"] == ["opentargets"]


def test_build_module_member_gene_entries_keeps_same_label_candidates_distinct() -> None:
    member_gene_entries = build_module_member_gene_entries(
        {
            "entity_id::ENSGDRD2COMMON",
            "entity_id::ENSGDRD2RARE",
        },
        {
            "entity_id::ENSGDRD2COMMON": {
                "entity_id": "ENSGDRD2COMMON",
                "entity_label": "DRD2",
                "approved_name": "dopamine receptor D2",
                "common_variant_support": "0.6",
                "rare_variant_support": "",
                "match_confidence": "source_confirmed",
                "registry_sources_json": "[\"pgc\"]",
            },
            "entity_id::ENSGDRD2RARE": {
                "entity_id": "ENSGDRD2RARE",
                "entity_label": "DRD2",
                "approved_name": "dopamine receptor D2",
                "common_variant_support": "",
                "rare_variant_support": "0.9",
                "match_confidence": "id_confirmed",
                "registry_sources_json": "[\"opentargets\"]",
            },
        },
        {
            "entity_id::ENSGDRD2COMMON",
            "entity_id::ENSGDRD2RARE",
        },
        set(),
    )

    assert len(member_gene_entries) == 2

    common_entry = next(
        entry for entry in member_gene_entries if entry["entity_id"] == "ENSGDRD2COMMON"
    )
    rare_entry = next(
        entry for entry in member_gene_entries if entry["entity_id"] == "ENSGDRD2RARE"
    )

    assert common_entry["common_variant_support"] == 0.6
    assert common_entry["rare_variant_support"] is None
    assert common_entry["genetic_score"] == 0.6
    assert common_entry["candidate_row_count"] == 1
    assert common_entry["candidate_entity_ids"] == ["ENSGDRD2COMMON"]
    assert common_entry["candidate_match_confidences"] == ["source_confirmed"]
    assert common_entry["provenance_sources"] == ["pgc"]
    assert common_entry["membership_source_type"] == "deg_only"

    assert rare_entry["common_variant_support"] is None
    assert rare_entry["rare_variant_support"] == 0.9
    assert rare_entry["genetic_score"] == 0.9
    assert rare_entry["candidate_row_count"] == 1
    assert rare_entry["candidate_entity_ids"] == ["ENSGDRD2RARE"]
    assert rare_entry["candidate_match_confidences"] == ["id_confirmed"]
    assert rare_entry["provenance_sources"] == ["opentargets"]
    assert rare_entry["membership_source_type"] == "deg_only"


def test_build_module_member_gene_entries_breaks_ties_stably() -> None:
    member_gene_entries = build_module_member_gene_entries(
        {"GENE_B", "GENE_A", "GENE_C"},
        {
            "GENE_A": {
                "entity_id": "ENSGA",
                "entity_label": "GENE_A",
                "approved_name": "Gene A",
                "common_variant_support": "0.2",
                "rare_variant_support": "0.2",
            },
            "GENE_B": {
                "entity_id": "ENSGB",
                "entity_label": "GENE_B",
                "approved_name": "Gene B",
                "common_variant_support": "0.2",
                "rare_variant_support": "0.2",
            },
            "GENE_C": {
                "entity_id": "ENSGC",
                "entity_label": "GENE_C",
                "approved_name": "Gene C",
                "common_variant_support": "0.4",
                "rare_variant_support": "0.4",
            },
        },
        {"GENE_A", "GENE_C"},
        {"GENE_B", "GENE_C"},
    )

    assert [entry["entity_label"] for entry in member_gene_entries] == [
        "GENE_C",
        "GENE_A",
        "GENE_B",
    ]
    assert member_gene_entries[0]["membership_source_type"] == "deg_and_grn"
    assert member_gene_entries[1]["membership_source_type"] == "deg_only"
    assert member_gene_entries[2]["membership_source_type"] == "grn_only"
