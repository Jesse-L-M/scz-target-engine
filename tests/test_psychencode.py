import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

from scz_target_engine.sources.psychencode import fetch_psychencode_support


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
