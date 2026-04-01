from __future__ import annotations

import re


CLINICALTRIALS_GOV_STUDY_BASE_URL = "https://clinicaltrials.gov/study"
NCT_ID_PATTERN = re.compile(r"^NCT\d{8}$")


def normalize_nct_id(value: str) -> str:
    normalized = (value or "").strip().upper()
    if not NCT_ID_PATTERN.fullmatch(normalized):
        raise ValueError(f"invalid ClinicalTrials.gov study identifier {value!r}")
    return normalized


def build_clinicaltrials_study_url(nct_id: str) -> str:
    return f"{CLINICALTRIALS_GOV_STUDY_BASE_URL}/{normalize_nct_id(nct_id)}"
