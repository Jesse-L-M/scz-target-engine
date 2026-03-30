from scz_target_engine.program_memory import (
    PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION,
    PROGRAM_MEMORY_EVENT_SUGGESTION,
)


def make_source_document(
    *,
    source_document_id: str = "abbvie-emraclidine-2024-11-11",
) -> dict[str, str]:
    return {
        "source_document_id": source_document_id,
        "title": "AbbVie phase 2 emraclidine update",
        "source_tier": "company_press_release",
        "source_url": "https://example.com/emraclidine",
        "publisher": "AbbVie",
        "published_at": "2024-11-11",
        "evidence_excerpt": "The study did not meet the primary endpoint.",
        "notes": "Machine-harvested direct source.",
    }


def make_event_suggestion(
    *,
    source_document_id: str = "abbvie-emraclidine-2024-11-11",
    suggestion_id: str = "emraclidine-event-suggestion",
) -> dict[str, object]:
    return {
        "suggestion_id": suggestion_id,
        "suggestion_kind": PROGRAM_MEMORY_EVENT_SUGGESTION,
        "source_document_id": source_document_id,
        "extractor_name": "llm-assisted-extractor",
        "extractor_version": "2026-03-30",
        "machine_confidence": "medium",
        "rationale": "Detected a dated topline miss with direct sponsor provenance.",
        "evidence_excerpt": "Did not meet the primary endpoint in acute schizophrenia.",
        "asset": {
            "asset_id": "emraclidine",
            "molecule": "emraclidine",
            "target": "CHRM4",
            "target_class": "muscarinic cholinergic modulation",
            "mechanism": "selective M4 muscarinic receptor positive allosteric modulator",
            "modality": "small_molecule",
        },
        "event": {
            "event_id": "emraclidine-empower-acute-scz-topline-2024-candidate",
            "asset_id": "emraclidine",
            "sponsor": "AbbVie",
            "population": "adults with schizophrenia during acute exacerbation of psychotic symptoms",
            "domain": "acute_positive_symptoms",
            "mono_or_adjunct": "monotherapy",
            "phase": "phase_2",
            "event_type": "topline_readout",
            "event_date": "2024-11-11",
            "primary_outcome_result": "did_not_meet_primary_endpoint",
            "failure_reason_taxonomy": "unresolved",
            "confidence": "medium",
            "notes": "Machine suggestion pending curator review.",
            "sort_order": 1,
        },
        "provenance": {
            "event_id": "emraclidine-empower-acute-scz-topline-2024-candidate",
            "source_tier": "company_press_release",
            "source_url": "https://example.com/emraclidine",
        },
    }


def make_directionality_suggestion(
    *,
    source_document_id: str = "abbvie-emraclidine-2024-11-11",
    suggestion_id: str = "chrm4-directionality-suggestion",
) -> dict[str, object]:
    return {
        "suggestion_id": suggestion_id,
        "suggestion_kind": PROGRAM_MEMORY_DIRECTIONALITY_SUGGESTION,
        "source_document_id": source_document_id,
        "extractor_name": "llm-assisted-extractor",
        "extractor_version": "2026-03-30",
        "machine_confidence": "low",
        "rationale": "Proposed a CHRM4 directionality hypothesis from the source set.",
        "evidence_excerpt": "Selective CHRM4 execution remains unresolved.",
        "directionality_hypothesis": {
            "hypothesis_id": "chrm4-candidate",
            "entity_id": "ENSG00000180720",
            "entity_label": "CHRM4",
            "desired_perturbation_direction": "increase_activity",
            "modality_hypothesis": "muscarinic_agonism_or_positive_allosteric_modulation",
            "preferred_modalities": ["small_molecule", "positive_allosteric_modulator"],
            "confidence": "low",
            "ambiguity": "Selective CHRM4 execution still needs curator review.",
            "evidence_basis": "Source set included one positive class anchor and one miss.",
            "supporting_event_ids": [
                "emraclidine-empower-acute-scz-topline-2024-candidate"
            ],
            "contradiction_conditions": [
                "Repeated selective CHRM4 failures in aligned populations."
            ],
            "falsification_conditions": [
                "Adequately engaged CHRM4 programs repeatedly fail."
            ],
            "open_risks": ["Signal may depend on broader muscarinic pharmacology."],
            "sort_order": 2,
        },
    }
