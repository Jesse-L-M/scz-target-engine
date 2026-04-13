from __future__ import annotations

from datetime import date
from pathlib import Path

from scz_target_engine.io import read_csv_table, read_json, write_csv, write_json


PROGRAM_MEMORY_V3_SOURCE_MANIFEST = "program_memory_v3_source_manifest"
PROGRAM_MEMORY_V3_STUDY_INDEX = "program_memory_v3_study_index"
PROGRAM_MEMORY_V3_RESULT_OBSERVATIONS = "program_memory_v3_result_observations"
PROGRAM_MEMORY_V3_HARM_OBSERVATIONS = "program_memory_v3_harm_observations"
PROGRAM_MEMORY_V3_CONTRADICTION_LOG = "program_memory_v3_contradiction_log"
PROGRAM_MEMORY_V3_CLAIM_LEDGER = "program_memory_v3_claim_ledger"
PROGRAM_MEMORY_V3_CAVEATS = "program_memory_v3_caveats"
PROGRAM_MEMORY_V3_BELIEF_UPDATES = "program_memory_v3_belief_updates"
PROGRAM_MEMORY_V3_PROGRAM_CARD = "program_memory_v3_program_card"
PROGRAM_MEMORY_V3_INSIGHT_PACKET = "program_memory_v3_insight_packet"
PROGRAM_MEMORY_V3_SCHEMA_VERSION = "v1"

PROGRAM_MEMORY_V3_SOURCE_MANIFEST_FILE_NAME = "source_manifest.json"
PROGRAM_MEMORY_V3_STUDY_INDEX_FILE_NAME = "study_index.csv"
PROGRAM_MEMORY_V3_RESULT_OBSERVATIONS_FILE_NAME = "result_observations.csv"
PROGRAM_MEMORY_V3_HARM_OBSERVATIONS_FILE_NAME = "harm_observations.csv"
PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FILE_NAME = "contradictions.csv"
PROGRAM_MEMORY_V3_CLAIM_LEDGER_FILE_NAME = "claims.csv"
PROGRAM_MEMORY_V3_CAVEATS_FILE_NAME = "caveats.csv"
PROGRAM_MEMORY_V3_BELIEF_UPDATES_FILE_NAME = "belief_updates.csv"
PROGRAM_MEMORY_V3_PROGRAM_CARD_FILE_NAME = "program_card.json"
PROGRAM_MEMORY_V3_INSIGHT_PACKET_FILE_NAME = "insight_packet.json"

PROGRAM_MEMORY_V3_STUDY_INDEX_FIELDNAMES = [
    "program_id",
    "study_id",
    "study_label",
    "study_phase",
    "condition_scope",
    "population_scope",
    "study_status",
    "source_document_id",
    "nct_id",
    "design_summary",
    "comparator_type",
    "notes",
]

PROGRAM_MEMORY_V3_RESULT_OBSERVATION_FIELDNAMES = [
    "program_id",
    "study_id",
    "arm_id",
    "endpoint_id",
    "endpoint_role",
    "endpoint_domain",
    "timepoint_label",
    "result_direction",
    "result_summary",
    "source_document_id",
    "analysis_population",
    "treatment_label",
    "comparator_label",
    "effect_size",
    "effect_size_unit",
    "p_value",
    "confidence_interval",
    "notes",
]

PROGRAM_MEMORY_V3_HARM_OBSERVATION_FIELDNAMES = [
    "program_id",
    "study_id",
    "arm_id",
    "harm_id",
    "harm_term",
    "harm_category",
    "severity_scope",
    "result_summary",
    "source_document_id",
    "treatment_label",
    "comparator_label",
    "incidence_percent",
    "incidence_count",
    "serious_flag",
    "discontinuation_flag",
    "notes",
]

PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FIELDNAMES = [
    "program_id",
    "contradiction_id",
    "claim_topic",
    "source_document_id_a",
    "source_document_id_b",
    "contradiction_summary",
    "adjudication_status",
    "preferred_source_document_id",
    "rationale",
    "notes",
]

PROGRAM_MEMORY_V3_CLAIM_LEDGER_FIELDNAMES = [
    "program_id",
    "claim_id",
    "claim_kind",
    "claim_statement",
    "evidence_scope",
    "primary_source_document_id",
    "adjudication_status",
    "study_id",
    "confidence_label",
    "supporting_source_document_ids",
    "notes",
]

PROGRAM_MEMORY_V3_CAVEATS_FIELDNAMES = [
    "program_id",
    "caveat_id",
    "caveat_kind",
    "applies_to_kind",
    "applies_to_id",
    "caveat_summary",
    "severity",
    "source_document_id",
    "notes",
]

PROGRAM_MEMORY_V3_BELIEF_UPDATE_FIELDNAMES = [
    "program_id",
    "belief_update_id",
    "belief_domain",
    "update_direction",
    "update_summary",
    "confidence_label",
    "target_id",
    "mechanism_id",
    "affected_population",
    "supporting_claim_ids",
    "notes",
]


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _defaulted_text(value: str, fallback: str) -> str:
    cleaned = _clean_text(value)
    return cleaned or fallback


def _today_string() -> str:
    return date.today().isoformat()


def _default_program_label(program_id: str) -> str:
    return _clean_text(program_id.replace("_", " ").replace("-", " "))


def _validated_json_object(path: Path) -> dict[str, object]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _load_registered_artifact(path: Path, *, artifact_name: str) -> None:
    from scz_target_engine.artifacts import load_artifact

    load_artifact(path, artifact_name=artifact_name)


def _source_document_rows(program_id: str, source_urls: tuple[str, ...]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for index, source_url in enumerate(source_urls, start=1):
        rows.append(
            {
                "source_document_id": f"{program_id}__src_{index:03d}",
                "source_kind": "url",
                "source_label": source_url,
                "source_locator": source_url,
                "source_tier": "public_unreviewed",
                "extraction_status": "pending",
            }
        )
    return rows


def _write_empty_csv(path: Path, fieldnames: list[str]) -> None:
    write_csv(path, [], fieldnames=fieldnames)


def _copy_csv_artifact(source_path: Path, target_path: Path) -> None:
    fieldnames, rows = read_csv_table(source_path)
    write_csv(target_path, rows, fieldnames=fieldnames)


def materialize_program_memory_v3_harvest_bundle(
    *,
    output_dir: Path,
    program_id: str,
    program_label: str = "",
    materialized_at: str = "",
    source_urls: tuple[str, ...] = (),
    corpus_tier: str = "",
) -> dict[str, object]:
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    normalized_program_id = _clean_text(program_id)
    if not normalized_program_id:
        raise ValueError("program_id is required")
    normalized_program_label = _defaulted_text(
        program_label,
        _default_program_label(normalized_program_id),
    )
    normalized_materialized_at = _defaulted_text(materialized_at, _today_string())
    normalized_corpus_tier = _defaulted_text(corpus_tier, "unspecified")
    source_documents = _source_document_rows(normalized_program_id, source_urls)

    source_manifest_path = (
        resolved_output_dir / PROGRAM_MEMORY_V3_SOURCE_MANIFEST_FILE_NAME
    )
    study_index_path = resolved_output_dir / PROGRAM_MEMORY_V3_STUDY_INDEX_FILE_NAME
    result_observations_path = (
        resolved_output_dir / PROGRAM_MEMORY_V3_RESULT_OBSERVATIONS_FILE_NAME
    )
    harm_observations_path = (
        resolved_output_dir / PROGRAM_MEMORY_V3_HARM_OBSERVATIONS_FILE_NAME
    )
    contradiction_log_path = (
        resolved_output_dir / PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FILE_NAME
    )

    source_manifest_payload = {
        "schema_name": PROGRAM_MEMORY_V3_SOURCE_MANIFEST,
        "schema_version": PROGRAM_MEMORY_V3_SCHEMA_VERSION,
        "program_id": normalized_program_id,
        "program_label": normalized_program_label,
        "review_stage": "harvest",
        "corpus_tier": normalized_corpus_tier,
        "materialized_at": normalized_materialized_at,
        "source_document_count": len(source_documents),
        "source_documents": source_documents,
        "unresolved_questions": [],
    }

    write_json(source_manifest_path, source_manifest_payload)
    _write_empty_csv(study_index_path, PROGRAM_MEMORY_V3_STUDY_INDEX_FIELDNAMES)
    _write_empty_csv(
        result_observations_path,
        PROGRAM_MEMORY_V3_RESULT_OBSERVATION_FIELDNAMES,
    )
    _write_empty_csv(
        harm_observations_path,
        PROGRAM_MEMORY_V3_HARM_OBSERVATION_FIELDNAMES,
    )
    _write_empty_csv(
        contradiction_log_path,
        PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FIELDNAMES,
    )

    _load_registered_artifact(
        source_manifest_path,
        artifact_name=PROGRAM_MEMORY_V3_SOURCE_MANIFEST,
    )
    _load_registered_artifact(
        study_index_path,
        artifact_name=PROGRAM_MEMORY_V3_STUDY_INDEX,
    )
    _load_registered_artifact(
        result_observations_path,
        artifact_name=PROGRAM_MEMORY_V3_RESULT_OBSERVATIONS,
    )
    _load_registered_artifact(
        harm_observations_path,
        artifact_name=PROGRAM_MEMORY_V3_HARM_OBSERVATIONS,
    )
    _load_registered_artifact(
        contradiction_log_path,
        artifact_name=PROGRAM_MEMORY_V3_CONTRADICTION_LOG,
    )

    return {
        "program_id": normalized_program_id,
        "program_label": normalized_program_label,
        "materialized_at": normalized_materialized_at,
        "source_document_count": len(source_documents),
        "output_dir": str(resolved_output_dir),
        "source_manifest_file": str(source_manifest_path),
        "study_index_file": str(study_index_path),
        "result_observations_file": str(result_observations_path),
        "harm_observations_file": str(harm_observations_path),
        "contradictions_file": str(contradiction_log_path),
    }


def materialize_program_memory_v3_adjudication_bundle(
    *,
    harvest_dir: Path,
    output_dir: Path,
    adjudication_id: str,
    reviewer: str,
    reviewed_at: str = "",
) -> dict[str, object]:
    resolved_harvest_dir = harvest_dir.resolve()
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    source_manifest_path = (
        resolved_harvest_dir / PROGRAM_MEMORY_V3_SOURCE_MANIFEST_FILE_NAME
    )
    source_manifest = _validated_json_object(source_manifest_path)
    normalized_adjudication_id = _clean_text(adjudication_id)
    normalized_reviewer = _clean_text(reviewer)
    if not normalized_adjudication_id:
        raise ValueError("adjudication_id is required")
    if not normalized_reviewer:
        raise ValueError("reviewer is required")

    program_id = str(source_manifest.get("program_id") or "").strip()
    program_label = str(source_manifest.get("program_label") or "").strip()
    materialized_at = _defaulted_text(reviewed_at, _today_string())
    source_document_count = int(source_manifest.get("source_document_count") or 0)

    claim_ledger_path = resolved_output_dir / PROGRAM_MEMORY_V3_CLAIM_LEDGER_FILE_NAME
    caveats_path = resolved_output_dir / PROGRAM_MEMORY_V3_CAVEATS_FILE_NAME
    belief_updates_path = resolved_output_dir / PROGRAM_MEMORY_V3_BELIEF_UPDATES_FILE_NAME
    contradiction_log_path = (
        resolved_output_dir / PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FILE_NAME
    )
    program_card_path = resolved_output_dir / PROGRAM_MEMORY_V3_PROGRAM_CARD_FILE_NAME
    harvest_contradiction_log_path = (
        resolved_harvest_dir / PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FILE_NAME
    )

    _write_empty_csv(claim_ledger_path, PROGRAM_MEMORY_V3_CLAIM_LEDGER_FIELDNAMES)
    _write_empty_csv(caveats_path, PROGRAM_MEMORY_V3_CAVEATS_FIELDNAMES)
    _write_empty_csv(
        belief_updates_path,
        PROGRAM_MEMORY_V3_BELIEF_UPDATE_FIELDNAMES,
    )
    if harvest_contradiction_log_path.exists():
        _copy_csv_artifact(harvest_contradiction_log_path, contradiction_log_path)
    else:
        _write_empty_csv(
            contradiction_log_path,
            PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FIELDNAMES,
        )
    write_json(
        program_card_path,
        {
            "schema_name": PROGRAM_MEMORY_V3_PROGRAM_CARD,
            "schema_version": PROGRAM_MEMORY_V3_SCHEMA_VERSION,
            "program_id": program_id,
            "program_label": program_label,
            "adjudication_id": normalized_adjudication_id,
            "reviewer": normalized_reviewer,
            "review_status": "draft",
            "overall_verdict": "unresolved",
            "materialized_at": materialized_at,
            "source_document_count": source_document_count,
            "claim_count": 0,
            "caveat_count": 0,
            "belief_update_count": 0,
            "key_takeaways": [],
            "top_caveats": [],
            "evidence_summary": "Draft adjudication scaffold with no accepted claims yet.",
        },
    )

    _load_registered_artifact(
        claim_ledger_path,
        artifact_name=PROGRAM_MEMORY_V3_CLAIM_LEDGER,
    )
    _load_registered_artifact(caveats_path, artifact_name=PROGRAM_MEMORY_V3_CAVEATS)
    _load_registered_artifact(
        belief_updates_path,
        artifact_name=PROGRAM_MEMORY_V3_BELIEF_UPDATES,
    )
    _load_registered_artifact(
        contradiction_log_path,
        artifact_name=PROGRAM_MEMORY_V3_CONTRADICTION_LOG,
    )
    _load_registered_artifact(
        program_card_path,
        artifact_name=PROGRAM_MEMORY_V3_PROGRAM_CARD,
    )

    return {
        "program_id": program_id,
        "program_label": program_label,
        "adjudication_id": normalized_adjudication_id,
        "reviewer": normalized_reviewer,
        "output_dir": str(resolved_output_dir),
        "claims_file": str(claim_ledger_path),
        "caveats_file": str(caveats_path),
        "belief_updates_file": str(belief_updates_path),
        "contradictions_file": str(contradiction_log_path),
        "program_card_file": str(program_card_path),
    }


def materialize_program_memory_v3_insight_packet(
    *,
    program_dir: Path,
    output_file: Path,
    packet_id: str,
    packet_question: str = "",
    scope_summary: str = "",
    generated_at: str = "",
) -> dict[str, object]:
    resolved_program_dir = program_dir.resolve()
    resolved_output_file = output_file.resolve()
    program_card_path = resolved_program_dir / PROGRAM_MEMORY_V3_PROGRAM_CARD_FILE_NAME
    program_card = _validated_json_object(program_card_path)

    normalized_packet_id = _clean_text(packet_id)
    if not normalized_packet_id:
        raise ValueError("packet_id is required")

    program_id = str(program_card.get("program_id") or "").strip()
    generated_timestamp = _defaulted_text(generated_at, _today_string())
    question = _defaulted_text(
        packet_question,
        f"What should change about beliefs for {program_id}?",
    )
    scope = _defaulted_text(
        scope_summary,
        "Single-program draft packet built from adjudicated program-memory v3 artifacts.",
    )

    payload = {
        "schema_name": PROGRAM_MEMORY_V3_INSIGHT_PACKET,
        "schema_version": PROGRAM_MEMORY_V3_SCHEMA_VERSION,
        "packet_id": normalized_packet_id,
        "packet_question": question,
        "scope_summary": scope,
        "generated_at": generated_timestamp,
        "program_ids": [program_id],
        "evidence_artifacts": [
            {
                "artifact_name": PROGRAM_MEMORY_V3_PROGRAM_CARD,
                "path": str(program_card_path),
            },
            {
                "artifact_name": PROGRAM_MEMORY_V3_CLAIM_LEDGER,
                "path": str(
                    resolved_program_dir / PROGRAM_MEMORY_V3_CLAIM_LEDGER_FILE_NAME
                ),
            },
            {
                "artifact_name": PROGRAM_MEMORY_V3_CAVEATS,
                "path": str(resolved_program_dir / PROGRAM_MEMORY_V3_CAVEATS_FILE_NAME),
            },
            {
                "artifact_name": PROGRAM_MEMORY_V3_BELIEF_UPDATES,
                "path": str(
                    resolved_program_dir / PROGRAM_MEMORY_V3_BELIEF_UPDATES_FILE_NAME
                ),
            },
            {
                "artifact_name": PROGRAM_MEMORY_V3_CONTRADICTION_LOG,
                "path": str(
                    resolved_program_dir / PROGRAM_MEMORY_V3_CONTRADICTION_LOG_FILE_NAME
                ),
            },
        ],
        "candidate_insights": [],
    }
    write_json(resolved_output_file, payload)
    _load_registered_artifact(
        resolved_output_file,
        artifact_name=PROGRAM_MEMORY_V3_INSIGHT_PACKET,
    )

    return {
        "packet_id": normalized_packet_id,
        "program_ids": [program_id],
        "output_file": str(resolved_output_file),
        "evidence_artifact_count": len(payload["evidence_artifacts"]),
        "candidate_insight_count": 0,
    }
