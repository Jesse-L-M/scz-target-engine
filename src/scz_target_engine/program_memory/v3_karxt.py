from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scz_target_engine.program_memory._helpers import clean_text, slugify
from scz_target_engine.program_memory.extract import resolve_program_memory_asset_identifier


KARXT_V3_PROGRAM_ID = "xanomeline-trospium-schizophrenia"
KARXT_V3_PROGRAM_LABEL = "KarXT / xanomeline + trospium in schizophrenia"
KARXT_CANONICAL_ASSET_ID = "xanomeline-trospium"
KARXT_ALIAS_HINT_TOKENS = ("karxt", "cobenfy", "xanomeline", "trospium")
_KARXT_PROGRAM_ALIAS_KEYS = {
    "karxt",
    "karxt-schizophrenia",
    "cobenfy",
    "cobenfy-schizophrenia",
    "xanomeline-trospium",
    "xanomeline-trospium-schizophrenia",
    "xanomeline-trospium-in-schizophrenia",
    "xanomeline-and-trospium",
    "xanomeline-and-trospium-schizophrenia",
    "xanomeline-trospium-schizophrenia-program",
}


@dataclass(frozen=True)
class ProgramMemoryV3PilotResolution:
    canonical_program_id: str
    canonical_program_label: str
    canonical_asset_id: str
    requested_program_id: str
    requested_program_label: str
    resolved_from: str


def _normalize_identity(value: str) -> str:
    return slugify(clean_text(value))


def _source_document_id(suffix: str) -> str:
    return f"{KARXT_V3_PROGRAM_ID}__{suffix}"


def _copy_rows(rows: tuple[dict[str, str], ...]) -> list[dict[str, str]]:
    return [dict(row) for row in rows]


def _copy_objects(rows: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _looks_like_karxt_identity(value: str) -> bool:
    normalized = _normalize_identity(value)
    if not normalized:
        return False
    return any(token in normalized for token in KARXT_ALIAS_HINT_TOKENS)


def resolve_karxt_schizophrenia_pilot(
    program_id: str,
    program_label: str = "",
) -> ProgramMemoryV3PilotResolution | None:
    requested_program_id = clean_text(program_id)
    requested_program_label = clean_text(program_label)
    for candidate, source_name in (
        (requested_program_id, "program_id"),
        (requested_program_label, "program_label"),
    ):
        normalized = _normalize_identity(candidate)
        if not normalized:
            continue
        if normalized == _normalize_identity(KARXT_V3_PROGRAM_ID):
            return ProgramMemoryV3PilotResolution(
                canonical_program_id=KARXT_V3_PROGRAM_ID,
                canonical_program_label=KARXT_V3_PROGRAM_LABEL,
                canonical_asset_id=KARXT_CANONICAL_ASSET_ID,
                requested_program_id=requested_program_id,
                requested_program_label=requested_program_label,
                resolved_from=source_name,
            )
        if normalized in _KARXT_PROGRAM_ALIAS_KEYS:
            return ProgramMemoryV3PilotResolution(
                canonical_program_id=KARXT_V3_PROGRAM_ID,
                canonical_program_label=KARXT_V3_PROGRAM_LABEL,
                canonical_asset_id=KARXT_CANONICAL_ASSET_ID,
                requested_program_id=requested_program_id,
                requested_program_label=requested_program_label,
                resolved_from=source_name,
            )
        resolved_asset_id = resolve_program_memory_asset_identifier(candidate)
        if resolved_asset_id == KARXT_CANONICAL_ASSET_ID:
            return ProgramMemoryV3PilotResolution(
                canonical_program_id=KARXT_V3_PROGRAM_ID,
                canonical_program_label=KARXT_V3_PROGRAM_LABEL,
                canonical_asset_id=KARXT_CANONICAL_ASSET_ID,
                requested_program_id=requested_program_id,
                requested_program_label=requested_program_label,
                resolved_from=source_name,
            )
    return None


def unresolved_karxt_schizophrenia_identity(
    program_id: str,
    program_label: str = "",
) -> bool:
    if resolve_karxt_schizophrenia_pilot(program_id, program_label) is not None:
        return False
    return _looks_like_karxt_identity(program_id) or _looks_like_karxt_identity(
        program_label
    )


_SOURCE_DOCUMENTS: tuple[dict[str, str], ...] = (
    {
        "source_document_id": _source_document_id("ctgov_current_nct03697252"),
        "source_kind": "clinicaltrials_gov_current",
        "source_label": "ClinicalTrials.gov current record: EMERGENT-1 / NCT03697252",
        "source_locator": "https://clinicaltrials.gov/study/NCT03697252",
        "source_tier": "trial_registry",
        "extraction_status": "linked_for_context",
    },
    {
        "source_document_id": _source_document_id("ctgov_history_nct03697252"),
        "source_kind": "clinicaltrials_gov_history",
        "source_label": "ClinicalTrials.gov history tab: EMERGENT-1 / NCT03697252",
        "source_locator": "https://clinicaltrials.gov/study/NCT03697252?tab=history",
        "source_tier": "trial_registry_history",
        "extraction_status": "linked_for_context",
    },
    {
        "source_document_id": _source_document_id("nejm_emergent_1_2021"),
        "source_kind": "journal_article",
        "source_label": "NEJM 2021: EMERGENT-1 primary results",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/33626254/",
        "source_tier": "peer_reviewed_primary_results",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("ctgov_current_nct04659161"),
        "source_kind": "clinicaltrials_gov_current",
        "source_label": "ClinicalTrials.gov current record: EMERGENT-2 / NCT04659161",
        "source_locator": "https://clinicaltrials.gov/study/NCT04659161",
        "source_tier": "trial_registry",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "source_kind": "clinicaltrials_gov_history",
        "source_label": "ClinicalTrials.gov record history v21: EMERGENT-2 / NCT04659161",
        "source_locator": "https://clinicaltrials.gov/study/NCT04659161?a=21&tab=history",
        "source_tier": "trial_registry_history",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("lancet_emergent_2_2024"),
        "source_kind": "journal_article",
        "source_label": "Lancet 2024: EMERGENT-2 primary results",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/38104575/",
        "source_tier": "peer_reviewed_primary_results",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("ctgov_current_nct04738123"),
        "source_kind": "clinicaltrials_gov_current",
        "source_label": "ClinicalTrials.gov current record: EMERGENT-3 / NCT04738123",
        "source_locator": "https://clinicaltrials.gov/study/NCT04738123",
        "source_tier": "trial_registry",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("ctgov_history_nct04738123"),
        "source_kind": "clinicaltrials_gov_history",
        "source_label": "ClinicalTrials.gov history tab: EMERGENT-3 / NCT04738123",
        "source_locator": "https://clinicaltrials.gov/study/NCT04738123?tab=history",
        "source_tier": "trial_registry_history",
        "extraction_status": "linked_for_context",
    },
    {
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "source_kind": "journal_article",
        "source_label": "JAMA Psychiatry 2024: EMERGENT-3 primary results",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/38691387/",
        "source_tier": "peer_reviewed_primary_results",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("ctgov_current_nct04820309"),
        "source_kind": "clinicaltrials_gov_current",
        "source_label": "ClinicalTrials.gov current record: EMERGENT-5 / NCT04820309",
        "source_locator": "https://clinicaltrials.gov/study/NCT04820309",
        "source_tier": "trial_registry",
        "extraction_status": "linked_for_context",
    },
    {
        "source_document_id": _source_document_id("schres_emergent_5_2026"),
        "source_kind": "journal_article",
        "source_label": "Schizophrenia Research 2026: EMERGENT-5 open-label results",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/41506001/",
        "source_tier": "peer_reviewed_primary_results",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "source_kind": "journal_article",
        "source_label": "Schizophrenia 2024: pooled EMERGENT efficacy analysis",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/39488504/",
        "source_tier": "peer_reviewed_secondary_analysis",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("jcp_pooled_safety_2025"),
        "source_kind": "journal_article",
        "source_label": "Journal of Clinical Psychiatry 2025: pooled EMERGENT safety analysis",
        "source_locator": "https://pubmed.ncbi.nlm.nih.gov/40047530/",
        "source_tier": "peer_reviewed_secondary_analysis",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("fda_approval_announcement_2024_09_27"),
        "source_kind": "regulatory_announcement",
        "source_label": "FDA approval announcement for Cobenfy",
        "source_locator": "https://www.fda.gov/news-events/press-announcements/fda-approves-drug-new-mechanism-action-treatment-schizophrenia",
        "source_tier": "regulatory",
        "extraction_status": "extracted",
    },
    {
        "source_document_id": _source_document_id("dailymed_label_2024"),
        "source_kind": "regulatory_label",
        "source_label": "DailyMed prescribing information for Cobenfy",
        "source_locator": "https://dailymed.nlm.nih.gov/dailymed/fda/fdaDrugXsl.cfm?setid=8f0e73bf-6025-44f6-ab64-0983322de0df&type=display",
        "source_tier": "regulatory",
        "extraction_status": "extracted",
    },
)

_HARVEST_UNRESOLVED_QUESTIONS: tuple[str, ...] = (
    "ClinicalTrials.gov history tabs are linked for each study, but the current v3 contract still lacks a first-class source-history diff artifact.",
    "Direct schizophrenia evidence in this pilot is combination-level and does not isolate xanomeline, trospium, CHRM1, or CHRM4 contributions.",
)

_STUDY_INDEX_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-1",
        "study_label": "EMERGENT-1",
        "study_phase": "phase_2",
        "condition_scope": "schizophrenia",
        "population_scope": "adults with schizophrenia during acute exacerbation or relapse requiring hospitalization",
        "study_status": "completed",
        "source_document_id": _source_document_id("ctgov_current_nct03697252"),
        "nct_id": "NCT03697252",
        "design_summary": "5-week randomized double-blind placebo-controlled inpatient trial",
        "comparator_type": "placebo",
        "notes": "Primary efficacy results published in NEJM 2021.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "study_label": "EMERGENT-2",
        "study_phase": "phase_3",
        "condition_scope": "schizophrenia",
        "population_scope": "adults with acute psychosis requiring hospitalization",
        "study_status": "completed",
        "source_document_id": _source_document_id("ctgov_current_nct04659161"),
        "nct_id": "NCT04659161",
        "design_summary": "5-week randomized double-blind flexible-dose placebo-controlled inpatient trial",
        "comparator_type": "placebo",
        "notes": "ClinicalTrials.gov history v21 includes adverse-event tables and results posting metadata.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "study_label": "EMERGENT-3",
        "study_phase": "phase_3",
        "condition_scope": "schizophrenia",
        "population_scope": "adults with acute psychosis requiring hospitalization",
        "study_status": "completed",
        "source_document_id": _source_document_id("ctgov_current_nct04738123"),
        "nct_id": "NCT04738123",
        "design_summary": "5-week randomized double-blind placebo-controlled inpatient trial",
        "comparator_type": "placebo",
        "notes": "ClinicalTrials.gov current record lists 256 enrolled participants and results first posted on 2024-12-09.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-5",
        "study_label": "EMERGENT-5",
        "study_phase": "phase_3_extension",
        "condition_scope": "schizophrenia",
        "population_scope": "psychiatrically stable adults with schizophrenia switched from prior antipsychotics",
        "study_status": "completed",
        "source_document_id": _source_document_id("ctgov_current_nct04820309"),
        "nct_id": "NCT04820309",
        "design_summary": "52-week open-label single-arm long-term safety and tolerability study",
        "comparator_type": "open_label_single_arm",
        "notes": "Supportive long-term safety evidence only; not interpretable as comparative efficacy evidence.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "pooled-emergent-acute",
        "study_label": "Pooled acute EMERGENT trials",
        "study_phase": "pooled_phase_2_and_phase_3",
        "condition_scope": "schizophrenia",
        "population_scope": "adults with acute schizophrenia across EMERGENT-1, EMERGENT-2, and EMERGENT-3",
        "study_status": "completed",
        "source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "nct_id": "",
        "design_summary": "Pooled post hoc efficacy and safety analyses across three 5-week randomized placebo-controlled inpatient trials",
        "comparator_type": "placebo",
        "notes": "Used to summarize consistency and subgroup stability across the acute randomized dataset.",
    },
)

_RESULT_OBSERVATION_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-1",
        "arm_id": "karxt",
        "endpoint_id": "panss_total_week_5",
        "endpoint_role": "primary",
        "endpoint_domain": "acute_positive_symptoms",
        "timepoint_label": "week_5",
        "result_direction": "improved_vs_placebo",
        "result_summary": "PANSS total score improved more than placebo at week 5 (-17.4 vs -5.9; least-squares mean difference -11.6).",
        "source_document_id": _source_document_id("nejm_emergent_1_2021"),
        "analysis_population": "randomized_participants",
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "effect_size": "-11.6",
        "effect_size_unit": "ls_mean_difference_panss_points",
        "p_value": "<0.001",
        "confidence_interval": "-16.1 to -7.1",
        "notes": "Phase 2 NEJM trial NCT03697252.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "endpoint_id": "panss_total_week_5",
        "endpoint_role": "primary",
        "endpoint_domain": "acute_positive_symptoms",
        "timepoint_label": "week_5",
        "result_direction": "improved_vs_placebo",
        "result_summary": "PANSS total score improved more than placebo at week 5 (-21.2 vs -11.6; least-squares mean difference -9.6).",
        "source_document_id": _source_document_id("lancet_emergent_2_2024"),
        "analysis_population": "modified_intention_to_treat",
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "effect_size": "-9.6",
        "effect_size_unit": "ls_mean_difference_panss_points",
        "p_value": "<0.0001",
        "confidence_interval": "-13.9 to -5.2",
        "notes": "Lancet abstract reports 126/126 randomized; FDA label table reports mITT denominators of 117/119.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "endpoint_id": "panss_total_week_5",
        "endpoint_role": "primary",
        "endpoint_domain": "acute_positive_symptoms",
        "timepoint_label": "week_5",
        "result_direction": "improved_vs_placebo",
        "result_summary": "PANSS total score improved more than placebo at week 5 (-20.6 vs -12.2; least-squares mean difference -8.4).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "analysis_population": "randomized_participants_reported_in_abstract",
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "effect_size": "-8.4",
        "effect_size_unit": "ls_mean_difference_panss_points",
        "p_value": "<0.001",
        "confidence_interval": "-12.4 to -4.3",
        "notes": "JAMA Psychiatry abstract reports 125/131 randomized; FDA label table reports mITT denominators of 114/120.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "pooled-emergent-acute",
        "arm_id": "xanomeline-trospium",
        "endpoint_id": "panss_total_week_5",
        "endpoint_role": "pooled_primary",
        "endpoint_domain": "acute_positive_symptoms",
        "timepoint_label": "week_5",
        "result_direction": "improved_vs_placebo",
        "result_summary": "Pooled acute EMERGENT analysis favored xanomeline/trospium for PANSS total score at week 5.",
        "source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "analysis_population": "pooled_randomized_placebo_controlled_trials",
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "effect_size": "-9.9",
        "effect_size_unit": "ls_mean_difference_panss_points",
        "p_value": "<0.0001",
        "confidence_interval": "-12.4 to -7.3",
        "notes": "Pooled EMERGENT-1, EMERGENT-2, and EMERGENT-3 analyses; Cohen d 0.65 in the published abstract.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-5",
        "arm_id": "xanomeline-trospium",
        "endpoint_id": "panss_total_over_52_weeks",
        "endpoint_role": "supportive_long_term",
        "endpoint_domain": "schizophrenia_symptoms",
        "timepoint_label": "week_52",
        "result_direction": "improved_over_time",
        "result_summary": "Open-label EMERGENT-5 reported improvement in PANSS total, PANSS subscales, and CGI-S over 52 weeks without a comparator arm.",
        "source_document_id": _source_document_id("schres_emergent_5_2026"),
        "analysis_population": "single_arm_open_label",
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "",
        "effect_size": "",
        "effect_size_unit": "",
        "p_value": "",
        "confidence_interval": "",
        "notes": "Supportive only because the study is open-label and enrolled psychiatrically stable adults switched from prior antipsychotics.",
    },
)

_HARM_OBSERVATION_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-constipation",
        "harm_term": "constipation",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Constipation was more frequent with xanomeline/trospium than placebo (21.4% vs 10.4%).",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "21.4",
        "incidence_count": "27",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "ClinicalTrials.gov history v21 adverse-event table reports 27/126 vs 13/125.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-dyspepsia",
        "harm_term": "dyspepsia",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Dyspepsia was more frequent with xanomeline/trospium than placebo (19.1% vs 8.0%).",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "19.1",
        "incidence_count": "24",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "ClinicalTrials.gov history v21 adverse-event table reports 24/126 vs 10/125.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-nausea",
        "harm_term": "nausea",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Nausea was more frequent with xanomeline/trospium than placebo (19.1% vs 5.6%).",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "19.1",
        "incidence_count": "24",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "ClinicalTrials.gov history v21 adverse-event table reports 24/126 vs 7/125.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-vomiting",
        "harm_term": "vomiting",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Vomiting was more frequent with xanomeline/trospium than placebo (14.3% vs 0.8%).",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "14.3",
        "incidence_count": "18",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "ClinicalTrials.gov history v21 adverse-event table reports 18/126 vs 1/125.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-hypertension",
        "harm_term": "hypertension",
        "harm_category": "cardiovascular",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Hypertension was more frequent with xanomeline/trospium than placebo (9.5% vs 0.8%).",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "9.5",
        "incidence_count": "12",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "ClinicalTrials.gov history v21 adverse-event table reports 12/126 vs 1/125.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-2",
        "arm_id": "karxt",
        "harm_id": "emergent-2-teae-discontinuation",
        "harm_term": "treatment_emergent_adverse_event_discontinuation",
        "harm_category": "discontinuation",
        "severity_scope": "treatment_discontinuation",
        "result_summary": "Adverse-event-related discontinuation rates were similar between xanomeline/trospium and placebo (7% vs 6%).",
        "source_document_id": _source_document_id("lancet_emergent_2_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "7.0",
        "incidence_count": "9",
        "serious_flag": "false",
        "discontinuation_flag": "true",
        "notes": "Lancet abstract reports 9/126 discontinuations in the active arm and 7/126 in placebo.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "harm_id": "emergent-3-nausea",
        "harm_term": "nausea",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Nausea was more frequent with xanomeline/trospium than placebo (19.2% vs 1.6%).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "19.2",
        "incidence_count": "24",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "JAMA abstract reports 24/125 vs 2/131.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "harm_id": "emergent-3-dyspepsia",
        "harm_term": "dyspepsia",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Dyspepsia was more frequent with xanomeline/trospium than placebo (16.0% vs 1.6%).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "16.0",
        "incidence_count": "20",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "JAMA abstract reports 20/125 vs 2/131.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "harm_id": "emergent-3-vomiting",
        "harm_term": "vomiting",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Vomiting was more frequent with xanomeline/trospium than placebo (16.0% vs 0.8%).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "16.0",
        "incidence_count": "20",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "JAMA abstract reports 20/125 vs 1/131.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "harm_id": "emergent-3-constipation",
        "harm_term": "constipation",
        "harm_category": "gastrointestinal",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Constipation was more frequent with xanomeline/trospium than placebo (12.8% vs 3.9%).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "12.8",
        "incidence_count": "16",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "JAMA abstract reports 16/125 vs 5/131.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "emergent-3",
        "arm_id": "karxt",
        "harm_id": "emergent-3-teae-discontinuation",
        "harm_term": "treatment_emergent_adverse_event_discontinuation",
        "harm_category": "discontinuation",
        "severity_scope": "treatment_discontinuation",
        "result_summary": "Discontinuation due to treatment-emergent adverse events was similar between xanomeline/trospium and placebo (6.4% vs 5.5%).",
        "source_document_id": _source_document_id("jama_emergent_3_2024"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "6.4",
        "incidence_count": "8",
        "serious_flag": "false",
        "discontinuation_flag": "true",
        "notes": "JAMA abstract reports 8/125 vs 7/131.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "pooled-emergent-acute",
        "arm_id": "xanomeline-trospium",
        "harm_id": "pooled-any-teae",
        "harm_term": "any_treatment_emergent_adverse_event",
        "harm_category": "overall_tolerability",
        "severity_scope": "treatment_emergent_adverse_event",
        "result_summary": "Across pooled acute EMERGENT trials, any treatment-emergent adverse event was more common with xanomeline/trospium than placebo (67.9% vs 51.3%).",
        "source_document_id": _source_document_id("jcp_pooled_safety_2025"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "67.9",
        "incidence_count": "",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "Pooled safety abstract does not report an active-arm numerator in the abstract text.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "study_id": "pooled-emergent-acute",
        "arm_id": "xanomeline-trospium",
        "harm_id": "pooled-low-eps-weight-somnolence",
        "harm_term": "eps_weight_gain_and_somnolence",
        "harm_category": "neurologic_and_metabolic",
        "severity_scope": "cross_trial_safety_pattern",
        "result_summary": "Across pooled acute EMERGENT trials, rates of EPS, somnolence, and weight gain were described as low in both groups.",
        "source_document_id": _source_document_id("jcp_pooled_safety_2025"),
        "treatment_label": "xanomeline + trospium",
        "comparator_label": "placebo",
        "incidence_percent": "",
        "incidence_count": "",
        "serious_flag": "false",
        "discontinuation_flag": "false",
        "notes": "The pooled safety abstract does not provide exact percentages for these events in the abstract text.",
    },
)

_HARVEST_CONTRADICTIONS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "emergent-2-analysis-population-denominator",
        "claim_topic": "analysis_population_denominator",
        "source_document_id_a": _source_document_id("lancet_emergent_2_2024"),
        "source_document_id_b": _source_document_id("dailymed_label_2024"),
        "contradiction_summary": "EMERGENT-2 denominator differs across sources: randomized totals in the Lancet abstract versus smaller mITT denominators in the FDA label table.",
        "adjudication_status": "preserved_for_adjudication",
        "preferred_source_document_id": "",
        "rationale": "This appears to be a randomized-versus-efficacy-population distinction rather than a data error, but the current contract lacks first-class denominator fields.",
        "notes": "Lancet abstract reports 126/126 randomized; DailyMed Table 4 reports 117/119 for the primary efficacy analysis.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "emergent-3-analysis-population-denominator",
        "claim_topic": "analysis_population_denominator",
        "source_document_id_a": _source_document_id("jama_emergent_3_2024"),
        "source_document_id_b": _source_document_id("dailymed_label_2024"),
        "contradiction_summary": "EMERGENT-3 denominator differs across sources: randomized totals in the JAMA abstract versus smaller mITT denominators in the FDA label table.",
        "adjudication_status": "preserved_for_adjudication",
        "preferred_source_document_id": "",
        "rationale": "This appears to be a randomized-versus-efficacy-population distinction rather than a data error, but it should remain explicit.",
        "notes": "JAMA abstract reports 125/131 randomized; DailyMed Table 4 reports 114/120 for the primary efficacy analysis.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "molecule-vs-mechanism-generalization",
        "claim_topic": "mechanism_scope",
        "source_document_id_a": _source_document_id("fda_approval_announcement_2024_09_27"),
        "source_document_id_b": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "contradiction_summary": "Regulatory framing emphasizes a new cholinergic mechanism, but the direct schizophrenia evidence tests the fixed xanomeline-trospium combination rather than an isolated generalizable mechanism effect.",
        "adjudication_status": "open",
        "preferred_source_document_id": "",
        "rationale": "The tension is not whether the trials were positive; it is how far the observed combination-level signal should generalize beyond the approved molecule.",
        "notes": "Keep explicit until the dossier distinguishes molecule-specific, mechanism-general, and target-specific updates more cleanly.",
    },
)

_CLAIM_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "acute-efficacy-three-rcts",
        "claim_kind": "efficacy",
        "claim_statement": "Three 5-week randomized placebo-controlled schizophrenia trials consistently improved PANSS total score versus placebo.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "adjudication_status": "accepted",
        "study_id": "pooled-emergent-acute",
        "confidence_label": "high",
        "supporting_source_document_ids": "|".join(
            (
                _source_document_id("nejm_emergent_1_2021"),
                _source_document_id("lancet_emergent_2_2024"),
                _source_document_id("jama_emergent_3_2024"),
            )
        ),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=low_to_medium; transportability_confidence=medium for acute hospitalized adults; interpretation_confidence=high at molecule scope.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "acute-effect-size-range",
        "claim_kind": "efficacy",
        "claim_statement": "Observed placebo-subtracted PANSS improvement was roughly 8 to 12 points at week 5 across EMERGENT acute trials.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "adjudication_status": "accepted",
        "study_id": "pooled-emergent-acute",
        "confidence_label": "high",
        "supporting_source_document_ids": "|".join(
            (
                _source_document_id("nejm_emergent_1_2021"),
                _source_document_id("lancet_emergent_2_2024"),
                _source_document_id("jama_emergent_3_2024"),
            )
        ),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=low_to_medium; transportability_confidence=medium; interpretation_confidence=high.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "gi-tolerability-burden-real",
        "claim_kind": "safety",
        "claim_statement": "Acute treatment causes a real gastrointestinal and cholinergic tolerability burden, especially nausea, dyspepsia, constipation, and vomiting.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("jcp_pooled_safety_2025"),
        "adjudication_status": "accepted",
        "study_id": "pooled-emergent-acute",
        "confidence_label": "high",
        "supporting_source_document_ids": "|".join(
            (
                _source_document_id("ctgov_history_nct04659161_v21"),
                _source_document_id("jama_emergent_3_2024"),
                _source_document_id("dailymed_label_2024"),
            )
        ),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=medium because harms are summarized differently across sources; transportability_confidence=medium; interpretation_confidence=high.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "eps-weight-somnolence-low",
        "claim_kind": "safety",
        "claim_statement": "Available acute trials do not show the typical EPS, weight-gain, or somnolence burden seen with many dopamine-blocking antipsychotics.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("jcp_pooled_safety_2025"),
        "adjudication_status": "accepted_with_caveat",
        "study_id": "pooled-emergent-acute",
        "confidence_label": "medium",
        "supporting_source_document_ids": "|".join(
            (
                _source_document_id("lancet_emergent_2_2024"),
                _source_document_id("jama_emergent_3_2024"),
            )
        ),
        "notes": "extraction_confidence=medium; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=medium because pooled abstracts do not give exact percentages for every event; transportability_confidence=medium; interpretation_confidence=medium.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "phase3-ae-discontinuation-similar",
        "claim_kind": "safety",
        "claim_statement": "Despite higher adverse-event burden, phase 3 adverse-event-related discontinuation rates were similar to placebo.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("jama_emergent_3_2024"),
        "adjudication_status": "accepted_with_caveat",
        "study_id": "emergent-3",
        "confidence_label": "medium",
        "supporting_source_document_ids": _source_document_id("lancet_emergent_2_2024"),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=low; transportability_confidence=medium; interpretation_confidence=medium because discontinuation reflects short inpatient trials only.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "evidence-is-combination-specific",
        "claim_kind": "mechanism_scope",
        "claim_statement": "Direct schizophrenia evidence supports the fixed xanomeline plus trospium combination rather than isolated xanomeline, isolated trospium, or a selective CHRM4-only strategy.",
        "evidence_scope": "molecule_vs_mechanism",
        "primary_source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "adjudication_status": "accepted",
        "study_id": "",
        "confidence_label": "high",
        "supporting_source_document_ids": "|".join(
            (
                _source_document_id("nejm_emergent_1_2021"),
                _source_document_id("lancet_emergent_2_2024"),
                _source_document_id("jama_emergent_3_2024"),
                _source_document_id("fda_approval_announcement_2024_09_27"),
            )
        ),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=medium; reporting_integrity_risk=low; transportability_confidence=not_applicable_to_mechanism_generalization; interpretation_confidence=high that the evidence is molecule-specific.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "broad-muscarinic-class-validation",
        "claim_kind": "mechanism_scope",
        "claim_statement": "KarXT alone proves a broad muscarinic class effect or selective CHRM4 sufficiency in schizophrenia.",
        "evidence_scope": "mechanism",
        "primary_source_document_id": _source_document_id("fda_approval_announcement_2024_09_27"),
        "adjudication_status": "rejected",
        "study_id": "",
        "confidence_label": "low",
        "supporting_source_document_ids": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "notes": "extraction_confidence=high; source_reliability=high for the approval fact but not for broader mechanism extrapolation; risk_of_bias=medium; reporting_integrity_risk=low; transportability_confidence=low; interpretation_confidence=low because the public program does not decompose component or target contributions.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "claim_id": "open-label-no-new-safety-signal",
        "claim_kind": "long_term_safety",
        "claim_statement": "The 52-week open-label EMERGENT-5 study did not reveal a new safety signal beyond the known cholinergic and gastrointestinal profile.",
        "evidence_scope": "molecule",
        "primary_source_document_id": _source_document_id("schres_emergent_5_2026"),
        "adjudication_status": "accepted_with_caveat",
        "study_id": "emergent-5",
        "confidence_label": "medium",
        "supporting_source_document_ids": _source_document_id("jcp_pooled_safety_2025"),
        "notes": "extraction_confidence=high; source_reliability=high; risk_of_bias=high because the study is open-label and uncontrolled; reporting_integrity_risk=medium; transportability_confidence=medium for stable adults switched from prior antipsychotics; interpretation_confidence=medium.",
    },
)

_CAVEAT_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "caveat_id": "acute-inpatient-transportability",
        "caveat_kind": "transportability",
        "applies_to_kind": "claim",
        "applies_to_id": "acute-efficacy-three-rcts",
        "caveat_summary": "The acute randomized evidence comes from hospitalized adults with marked psychotic exacerbation and does not automatically transport to first-episode, treatment-resistant, adolescent, or medically complex populations.",
        "severity": "moderate",
        "source_document_id": _source_document_id("ctgov_current_nct04738123"),
        "notes": "Eligibility criteria excluded first treated episode, recent clozapine-level treatment resistance, and substantial medical comorbidity in phase 3.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "caveat_id": "short-duration-no-head-to-head",
        "caveat_kind": "design_limit",
        "applies_to_kind": "program",
        "applies_to_id": KARXT_V3_PROGRAM_ID,
        "caveat_summary": "The placebo-controlled efficacy base is 5 weeks long and does not answer comparative effectiveness or long-term durability versus standard antipsychotics.",
        "severity": "high",
        "source_document_id": _source_document_id("dailymed_label_2024"),
        "notes": "The approval package is built from short inpatient placebo-controlled studies plus uncontrolled extension evidence.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "caveat_id": "combination-mechanism-decomposition",
        "caveat_kind": "mechanism_scope",
        "applies_to_kind": "claim",
        "applies_to_id": "evidence-is-combination-specific",
        "caveat_summary": "No public schizophrenia source in this pilot cleanly separates xanomeline, trospium, CHRM1, and CHRM4 contributions.",
        "severity": "high",
        "source_document_id": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "notes": "Treat molecule-level efficacy as stronger than any target-specific or class-general mechanism conclusion.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "caveat_id": "gastrointestinal-burden-real",
        "caveat_kind": "tolerability",
        "applies_to_kind": "claim",
        "applies_to_id": "gi-tolerability-burden-real",
        "caveat_summary": "The main tolerability tradeoff is gastrointestinal and cholinergic; that burden is common enough that it should stay prominent in any public update.",
        "severity": "moderate",
        "source_document_id": _source_document_id("dailymed_label_2024"),
        "notes": "DailyMed highlights nausea, dyspepsia, constipation, vomiting, hypertension, abdominal pain, diarrhea, tachycardia, dizziness, and gastroesophageal reflux disease as common adverse reactions.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "caveat_id": "history-diffs-not-first-class",
        "caveat_kind": "contract_gap",
        "applies_to_kind": "program",
        "applies_to_id": KARXT_V3_PROGRAM_ID,
        "caveat_summary": "This pilot links ClinicalTrials.gov history pages but still cannot materialize field-level source-history diffs as first-class artifacts.",
        "severity": "low",
        "source_document_id": _source_document_id("ctgov_history_nct04659161_v21"),
        "notes": "Important denominator and results-posting shifts remain preserved in notes and contradiction rows rather than a dedicated diff artifact.",
    },
)

_BELIEF_UPDATE_ROWS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "molecule-acute-efficacy",
        "belief_domain": "molecule",
        "update_direction": "increase_confidence",
        "update_summary": "Increase confidence that xanomeline plus trospium can produce acute antipsychotic efficacy in adults hospitalized for schizophrenia exacerbation.",
        "confidence_label": "high",
        "target_id": "",
        "mechanism_id": "xanomeline-plus-trospium",
        "affected_population": "acute_hospitalized_adults_with_schizophrenia",
        "supporting_claim_ids": "acute-efficacy-three-rcts|acute-effect-size-range",
        "notes": "Scope is molecule-level and population-specific; do not treat as a proof of broad class equivalence.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "molecule-gi-burden",
        "belief_domain": "molecule",
        "update_direction": "increase_confidence",
        "update_summary": "Increase confidence that cholinergic and gastrointestinal adverse events are part of the package for the approved combination, even when discontinuation rates stay close to placebo in short inpatient trials.",
        "confidence_label": "medium",
        "target_id": "",
        "mechanism_id": "xanomeline-plus-trospium",
        "affected_population": "acute_hospitalized_adults_with_schizophrenia",
        "supporting_claim_ids": "gi-tolerability-burden-real|phase3-ae-discontinuation-similar",
        "notes": "The burden is common but often transient; short-trial discontinuation rates should not erase it.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "mechanism-nond2-combination",
        "belief_domain": "mechanism",
        "update_direction": "increase_confidence",
        "update_summary": "Increase confidence that a non-D2 muscarinic-directed combination can show antipsychotic efficacy in schizophrenia.",
        "confidence_label": "medium",
        "target_id": "",
        "mechanism_id": "muscarinic_cholinergic_modulation",
        "affected_population": "acute_hospitalized_adults_with_schizophrenia",
        "supporting_claim_ids": "acute-efficacy-three-rcts|evidence-is-combination-specific",
        "notes": "Update is narrower than a class-wide claim because the direct evidence remains combination-specific.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "target-specificity-unresolved",
        "belief_domain": "target",
        "update_direction": "preserve_uncertainty",
        "update_summary": "Do not update strongly on CHRM1 versus CHRM4 contribution or on selective CHRM4 sufficiency from this program alone.",
        "confidence_label": "low",
        "target_id": "CHRM1 / CHRM4",
        "mechanism_id": "muscarinic_cholinergic_modulation",
        "affected_population": "schizophrenia",
        "supporting_claim_ids": "evidence-is-combination-specific|broad-muscarinic-class-validation",
        "notes": "The public schizophrenia program does not isolate component or target contributions cleanly enough for a stronger target-specific belief update.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "population-acute-inpatient-adults",
        "belief_domain": "population",
        "update_direction": "increase_confidence",
        "update_summary": "Increase confidence specifically for adults with acute psychosis requiring hospitalization, while keeping broader transportability guarded.",
        "confidence_label": "medium",
        "target_id": "",
        "mechanism_id": "xanomeline-plus-trospium",
        "affected_population": "acute_hospitalized_adults_with_schizophrenia",
        "supporting_claim_ids": "acute-efficacy-three-rcts",
        "notes": "This update should not be extrapolated to treatment-resistant schizophrenia, first episode populations, adolescents, or maintenance settings without additional data.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "belief_update_id": "design-lesson-denominators-and-mechanism",
        "belief_domain": "design_lesson",
        "update_direction": "increase_priority",
        "update_summary": "Future programs should report analysis-population denominators cleanly and separate molecule-level evidence from mechanism-general claims.",
        "confidence_label": "medium",
        "target_id": "",
        "mechanism_id": "",
        "affected_population": "program_memory_v3_pipeline",
        "supporting_claim_ids": "evidence-is-combination-specific|broad-muscarinic-class-validation",
        "notes": "The KarXT pilot surfaced both denominator shifts and mechanism-scope ambiguity as recurring interpretation risks.",
    },
)

_ADJUDICATED_CONTRADICTIONS: tuple[dict[str, str], ...] = (
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "emergent-2-analysis-population-denominator",
        "claim_topic": "analysis_population_denominator",
        "source_document_id_a": _source_document_id("lancet_emergent_2_2024"),
        "source_document_id_b": _source_document_id("dailymed_label_2024"),
        "contradiction_summary": "EMERGENT-2 uses different denominators across sources because randomized totals and mITT efficacy populations are both reported publicly.",
        "adjudication_status": "resolved_with_context",
        "preferred_source_document_id": _source_document_id("lancet_emergent_2_2024"),
        "rationale": "Use the Lancet article for study-level efficacy interpretation, while preserving the FDA label denominator shift explicitly in notes and contradiction rows.",
        "notes": "The current contract still lacks first-class fields for randomized, treated, and efficacy-analysis denominators.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "emergent-3-analysis-population-denominator",
        "claim_topic": "analysis_population_denominator",
        "source_document_id_a": _source_document_id("jama_emergent_3_2024"),
        "source_document_id_b": _source_document_id("dailymed_label_2024"),
        "contradiction_summary": "EMERGENT-3 uses different denominators across sources because randomized totals and mITT efficacy populations are both reported publicly.",
        "adjudication_status": "resolved_with_context",
        "preferred_source_document_id": _source_document_id("jama_emergent_3_2024"),
        "rationale": "Use the JAMA article for study-level efficacy interpretation, while preserving the FDA label denominator shift explicitly in notes and contradiction rows.",
        "notes": "The difference is explanatory rather than disqualifying, but it should remain visible until the schema grows denominator columns.",
    },
    {
        "program_id": KARXT_V3_PROGRAM_ID,
        "contradiction_id": "molecule-vs-mechanism-generalization",
        "claim_topic": "mechanism_scope",
        "source_document_id_a": _source_document_id("fda_approval_announcement_2024_09_27"),
        "source_document_id_b": _source_document_id("schizophrenia_pooled_efficacy_2024"),
        "contradiction_summary": "Positive combination-level efficacy does not settle how much of the update should be assigned to a broader muscarinic mechanism or to a selective CHRM4 strategy.",
        "adjudication_status": "preserved_unresolved",
        "preferred_source_document_id": "",
        "rationale": "Keep the mechanism-scope conflict explicit and narrow the accepted belief updates instead of forcing a winner.",
        "notes": "This contradiction is the core truth test for whether v3 can preserve molecule-specific versus mechanism-general reasoning without laundering the distinction away.",
    },
)

_PROGRAM_CARD_KEY_TAKEAWAYS: tuple[str, ...] = (
    "Three placebo-controlled acute schizophrenia trials support real molecule-level efficacy for xanomeline plus trospium.",
    "The main tolerability burden is gastrointestinal and cholinergic, not a classic EPS or weight-gain signal.",
    "The public dataset supports the fixed combination more strongly than any broader mechanism-general or selective CHRM4 claim.",
)

_PROGRAM_CARD_TOP_CAVEATS: tuple[str, ...] = (
    "All comparative efficacy trials are short 5-week inpatient studies.",
    "Mechanism decomposition remains unresolved because the approved evidence is combination-level.",
    "ClinicalTrials.gov history shifts and analysis denominators still require notes rather than first-class schema columns.",
)


def karxt_harvest_source_documents() -> list[dict[str, str]]:
    return _copy_rows(_SOURCE_DOCUMENTS)


def karxt_harvest_unresolved_questions() -> list[str]:
    return list(_HARVEST_UNRESOLVED_QUESTIONS)


def karxt_study_index_rows() -> list[dict[str, str]]:
    return _copy_rows(_STUDY_INDEX_ROWS)


def karxt_result_observation_rows() -> list[dict[str, str]]:
    return _copy_rows(_RESULT_OBSERVATION_ROWS)


def karxt_harm_observation_rows() -> list[dict[str, str]]:
    return _copy_rows(_HARM_OBSERVATION_ROWS)


def karxt_harvest_contradiction_rows() -> list[dict[str, str]]:
    return _copy_rows(_HARVEST_CONTRADICTIONS)


def karxt_claim_rows() -> list[dict[str, str]]:
    return _copy_rows(_CLAIM_ROWS)


def karxt_caveat_rows() -> list[dict[str, str]]:
    return _copy_rows(_CAVEAT_ROWS)


def karxt_belief_update_rows() -> list[dict[str, str]]:
    return _copy_rows(_BELIEF_UPDATE_ROWS)


def karxt_adjudicated_contradiction_rows() -> list[dict[str, str]]:
    return _copy_rows(_ADJUDICATED_CONTRADICTIONS)


def karxt_program_card_payload(
    *,
    adjudication_id: str,
    reviewer: str,
    reviewed_at: str,
    source_document_count: int,
) -> dict[str, Any]:
    return {
        "program_id": KARXT_V3_PROGRAM_ID,
        "program_label": KARXT_V3_PROGRAM_LABEL,
        "adjudication_id": adjudication_id,
        "reviewer": reviewer,
        "review_status": "reviewed",
        "overall_verdict": "acute_efficacy_supported_with_cholinergic_tolerability_tradeoff",
        "materialized_at": reviewed_at,
        "source_document_count": source_document_count,
        "claim_count": len(_CLAIM_ROWS),
        "caveat_count": len(_CAVEAT_ROWS),
        "belief_update_count": len(_BELIEF_UPDATE_ROWS),
        "key_takeaways": list(_PROGRAM_CARD_KEY_TAKEAWAYS),
        "top_caveats": list(_PROGRAM_CARD_TOP_CAVEATS),
        "evidence_summary": (
            "KarXT/xanomeline-trospium now has a real end-to-end v3 dossier anchored "
            "to three acute placebo-controlled schizophrenia trials, pooled efficacy "
            "and safety analyses, FDA approval materials, and a long-term open-label "
            "follow-up. The accepted update is strong at the fixed-combination level "
            "for acute efficacy and medium-to-high for a real gastrointestinal and "
            "cholinergic tolerability burden, while mechanism-general or selective "
            "CHRM4 claims remain intentionally narrow."
        ),
        "notes": (
            "Canonical v3 program identity resolved from KarXT/Cobenfy/xanomeline-trospium "
            "aliases to xanomeline-trospium-schizophrenia."
        ),
    }


def karxt_candidate_insights() -> list[dict[str, Any]]:
    return _copy_objects(
        (
            {
                "insight_id": "acute-efficacy",
                "title": "Acute efficacy is now a strong molecule-level update",
                "judgment": "update_up",
                "summary": "Three acute placebo-controlled EMERGENT trials and the pooled efficacy analysis support a real antipsychotic signal for the fixed xanomeline plus trospium combination in hospitalized adults with schizophrenia.",
                "supporting_claim_ids": [
                    "acute-efficacy-three-rcts",
                    "acute-effect-size-range",
                ],
                "caveat_ids": [
                    "acute-inpatient-transportability",
                    "short-duration-no-head-to-head",
                ],
                "contradiction_ids": [],
                "belief_update_ids": [
                    "molecule-acute-efficacy",
                    "population-acute-inpatient-adults",
                ],
            },
            {
                "insight_id": "tolerability-burden",
                "title": "Tolerability burden is real, but it is not a classic EPS-heavy profile",
                "judgment": "mixed_update",
                "summary": "The public evidence shows frequent gastrointestinal and cholinergic adverse events, yet acute phase 3 adverse-event discontinuation stayed close to placebo and EPS, weight gain, and somnolence were low in the available datasets.",
                "supporting_claim_ids": [
                    "gi-tolerability-burden-real",
                    "eps-weight-somnolence-low",
                    "phase3-ae-discontinuation-similar",
                ],
                "caveat_ids": [
                    "gastrointestinal-burden-real",
                    "short-duration-no-head-to-head",
                ],
                "contradiction_ids": [],
                "belief_update_ids": [
                    "molecule-gi-burden",
                ],
            },
            {
                "insight_id": "molecule-vs-mechanism",
                "title": "The evidence is molecule-specific first and mechanism-general second",
                "judgment": "narrow_generalization",
                "summary": "KarXT should update beliefs about the approved fixed combination more strongly than beliefs about broad muscarinic class validation or selective CHRM4 sufficiency. The direct schizophrenia evidence does not isolate component or target contributions well enough to overclaim.",
                "supporting_claim_ids": [
                    "evidence-is-combination-specific",
                    "broad-muscarinic-class-validation",
                ],
                "caveat_ids": [
                    "combination-mechanism-decomposition",
                    "history-diffs-not-first-class",
                ],
                "contradiction_ids": [
                    "molecule-vs-mechanism-generalization",
                ],
                "belief_update_ids": [
                    "mechanism-nond2-combination",
                    "target-specificity-unresolved",
                    "design-lesson-denominators-and-mechanism",
                ],
            },
        )
    )
