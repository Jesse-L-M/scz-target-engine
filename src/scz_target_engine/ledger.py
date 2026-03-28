from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
from typing import Any

from scz_target_engine.io import read_csv_rows
from scz_target_engine.scoring import RankedEntity, WarningRecord


FAILURE_SCOPE_BY_TAXONOMY = {
    "not_applicable_nonfailure": "nonfailure",
    "unresolved": "unresolved",
    "molecule_failure": "molecule",
    "target_class_failure": "target_class",
    "endpoint_mismatch": "endpoint",
    "population_mismatch": "population",
    "dosing_or_exposure_issue": "molecule",
    "heterogeneity_or_subgroup_dilution": "population",
    "probable_target_invalidity": "target",
}

EVIDENCE_STRENGTH_BY_CONFIDENCE = {
    "high": "strong",
    "medium": "moderate",
    "low": "provisional",
}

GENE_SOURCE_PREFIXES = (
    "pgc_",
    "schema_",
    "psychencode_",
    "opentargets_",
    "chembl_",
)

GENE_IDENTITY_FIELDS = (
    "primary_gene_id",
    "canonical_entity_id",
    "approved_name",
    "seed_entity_id",
    "source_entity_ids_json",
    "match_confidence",
    "match_provenance_json",
    "provenance_sources_json",
    "generic_platform_baseline",
)

INTEGER_PATTERN = re.compile(r"^-?\d+$")
FLOAT_PATTERN = re.compile(r"^-?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?$")


@dataclass(frozen=True)
class ProgramHistoryEvent:
    program_id: str
    sponsor: str
    molecule: str
    target: str
    target_symbols: tuple[str, ...]
    target_class: str
    mechanism: str
    modality: str
    population: str
    domain: str
    mono_or_adjunct: str
    phase: str
    event_type: str
    date: str
    primary_outcome_result: str
    failure_reason_taxonomy: str
    source_tier: str
    source_url: str
    confidence: str
    notes: str


@dataclass(frozen=True)
class DirectionalityHypothesis:
    entity_id: str
    entity_label: str
    desired_perturbation_direction: str
    modality_hypothesis: str
    preferred_modalities: tuple[str, ...]
    confidence: str
    ambiguity: str
    evidence_basis: str
    supporting_program_ids: tuple[str, ...]
    contradiction_conditions: tuple[str, ...]
    falsification_conditions: tuple[str, ...]
    open_risks: tuple[str, ...]


@dataclass(frozen=True)
class StructuralFailureEvent:
    program_id: str
    event_date: str
    event_type: str
    molecule: str
    target: str
    target_class: str
    mechanism: str
    modality: str
    phase: str
    mono_or_adjunct: str
    primary_outcome_result: str
    failure_reason_taxonomy: str
    failure_scope: str
    what_failed: str
    evidence_strength: str
    confidence: str
    source_tier: str
    source_url: str
    where: dict[str, str]
    notes: str


@dataclass(frozen=True)
class TargetLedger:
    entity_id: str
    entity_label: str
    v0_snapshot: dict[str, object]
    source_primitives: dict[str, object]
    subgroup_domain_relevance: dict[str, object]
    structural_failure_history: dict[str, object]
    directionality_hypothesis: dict[str, object]
    falsification_conditions: list[str]
    open_risks: list[dict[str, object]]


def load_program_history(path: Path) -> list[ProgramHistoryEvent]:
    events: list[ProgramHistoryEvent] = []
    for row in read_csv_rows(path):
        target = (row.get("target") or "").strip()
        target_symbols = tuple(
            token.strip().upper()
            for token in target.split("/")
            if token.strip()
        )
        events.append(
            ProgramHistoryEvent(
                program_id=(row.get("program_id") or "").strip(),
                sponsor=(row.get("sponsor") or "").strip(),
                molecule=(row.get("molecule") or "").strip(),
                target=target,
                target_symbols=target_symbols,
                target_class=(row.get("target_class") or "").strip(),
                mechanism=(row.get("mechanism") or "").strip(),
                modality=(row.get("modality") or "").strip(),
                population=(row.get("population") or "").strip(),
                domain=(row.get("domain") or "").strip(),
                mono_or_adjunct=(row.get("mono_or_adjunct") or "").strip(),
                phase=(row.get("phase") or "").strip(),
                event_type=(row.get("event_type") or "").strip(),
                date=(row.get("date") or "").strip(),
                primary_outcome_result=(row.get("primary_outcome_result") or "").strip(),
                failure_reason_taxonomy=(row.get("failure_reason_taxonomy") or "").strip(),
                source_tier=(row.get("source_tier") or "").strip(),
                source_url=(row.get("source_url") or "").strip(),
                confidence=(row.get("confidence") or "").strip().lower(),
                notes=(row.get("notes") or "").strip(),
            )
        )
    return events


def load_directionality_hypotheses(path: Path) -> dict[tuple[str, str], DirectionalityHypothesis]:
    hypotheses: dict[tuple[str, str], DirectionalityHypothesis] = {}
    for row in read_csv_rows(path):
        entity_id = (row.get("entity_id") or "").strip()
        entity_label = (row.get("entity_label") or "").strip()
        if not entity_label:
            raise ValueError("directionality hypotheses require entity_label")
        hypothesis = DirectionalityHypothesis(
            entity_id=entity_id,
            entity_label=entity_label,
            desired_perturbation_direction=(
                row.get("desired_perturbation_direction") or "undetermined"
            ).strip(),
            modality_hypothesis=(row.get("modality_hypothesis") or "undetermined").strip(),
            preferred_modalities=tuple(
                parse_string_list(row.get("preferred_modalities_json"))
            ),
            confidence=(row.get("confidence") or "low").strip().lower(),
            ambiguity=(row.get("ambiguity") or "").strip(),
            evidence_basis=(row.get("evidence_basis") or "").strip(),
            supporting_program_ids=tuple(
                parse_string_list(row.get("supporting_program_ids_json"))
            ),
            contradiction_conditions=tuple(
                parse_string_list(row.get("contradiction_conditions_json"))
            ),
            falsification_conditions=tuple(
                parse_string_list(row.get("falsification_conditions_json"))
            ),
            open_risks=tuple(parse_string_list(row.get("open_risks_json"))),
        )
        hypotheses[(entity_id, entity_label.upper())] = hypothesis
        hypotheses.setdefault(("", entity_label.upper()), hypothesis)
    return hypotheses


def parse_string_list(value: str | None) -> list[str]:
    parsed = parse_metadata_value("list_json", value)
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    if parsed is None:
        return []
    return [str(parsed)]


def parse_metadata_value(key: str, value: str | None) -> Any:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    if key.endswith("_json") or cleaned[:1] in {"[", "{"}:
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return cleaned

    if INTEGER_PATTERN.fullmatch(cleaned):
        return int(cleaned)
    if FLOAT_PATTERN.fullmatch(cleaned):
        return float(cleaned)
    return cleaned


def match_program_history(
    entity: RankedEntity,
    program_history: list[ProgramHistoryEvent],
) -> list[ProgramHistoryEvent]:
    target_symbol = entity.entity_label.upper()
    return [
        event
        for event in program_history
        if target_symbol in event.target_symbols
    ]


def determine_failure_scope(taxonomy: str) -> str:
    return FAILURE_SCOPE_BY_TAXONOMY.get(taxonomy, "unresolved")


def determine_what_failed(event: ProgramHistoryEvent, failure_scope: str) -> str:
    if failure_scope == "target_class":
        return event.target_class
    if failure_scope == "endpoint":
        return event.primary_outcome_result
    if failure_scope == "population":
        return event.population
    if failure_scope == "target":
        return event.target
    if failure_scope == "unresolved":
        return "undetermined"
    if failure_scope == "nonfailure":
        return "not_applicable_nonfailure"
    return event.molecule


def build_failure_event(event: ProgramHistoryEvent) -> StructuralFailureEvent:
    failure_scope = determine_failure_scope(event.failure_reason_taxonomy)
    return StructuralFailureEvent(
        program_id=event.program_id,
        event_date=event.date,
        event_type=event.event_type,
        molecule=event.molecule,
        target=event.target,
        target_class=event.target_class,
        mechanism=event.mechanism,
        modality=event.modality,
        phase=event.phase,
        mono_or_adjunct=event.mono_or_adjunct,
        primary_outcome_result=event.primary_outcome_result,
        failure_reason_taxonomy=event.failure_reason_taxonomy,
        failure_scope=failure_scope,
        what_failed=determine_what_failed(event, failure_scope),
        evidence_strength=EVIDENCE_STRENGTH_BY_CONFIDENCE.get(
            event.confidence,
            "provisional",
        ),
        confidence=event.confidence,
        source_tier=event.source_tier,
        source_url=event.source_url,
        where={
            "domain": event.domain,
            "population": event.population,
            "phase": event.phase,
            "mono_or_adjunct": event.mono_or_adjunct,
        },
        notes=event.notes,
    )


def build_v0_snapshot(entity: RankedEntity) -> dict[str, object]:
    return {
        "rank": entity.rank,
        "eligible": entity.eligible,
        "composite_score": entity.composite_score,
        "heuristic_stable": entity.heuristic_stable,
        "sensitivity_survival_rate": entity.sensitivity_survival_rate,
        "warning_count": entity.warning_count,
        "warning_severity": entity.warning_severity,
    }


def build_source_primitives(entity: RankedEntity) -> dict[str, object]:
    metadata = entity.metadata
    identity: dict[str, object] = {}
    for field_name in GENE_IDENTITY_FIELDS:
        parsed = parse_metadata_value(field_name, metadata.get(field_name))
        if parsed is not None:
            identity[field_name] = parsed

    source_presence: dict[str, object] = {}
    for key, value in metadata.items():
        if not key.startswith("source_present_"):
            continue
        parsed = parse_metadata_value(key, value)
        if parsed is None:
            continue
        source_presence[key.removeprefix("source_present_")] = parsed

    grouped_sources: dict[str, dict[str, object]] = {}
    for prefix in GENE_SOURCE_PREFIXES:
        source_name = prefix.removesuffix("_")
        values: dict[str, object] = {}
        for key, raw_value in metadata.items():
            if not key.startswith(prefix):
                continue
            parsed = parse_metadata_value(key, raw_value)
            if parsed is None:
                continue
            values[key.removeprefix(prefix)] = parsed
        if values:
            grouped_sources[source_name] = values

    return {
        "layers": dict(entity.layer_values),
        "identity": identity,
        "source_presence": source_presence,
        "sources": grouped_sources,
    }


def extract_cell_type_context(
    metadata: dict[str, str],
    field_name: str,
) -> list[dict[str, object]]:
    parsed = parse_metadata_value(field_name, metadata.get(field_name))
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def build_subgroup_domain_relevance(
    entity: RankedEntity,
    program_events: list[ProgramHistoryEvent],
) -> dict[str, object]:
    clinical_domains = sorted({event.domain for event in program_events if event.domain})
    clinical_populations = sorted(
        {event.population for event in program_events if event.population}
    )
    mono_or_adjunct_contexts = sorted(
        {event.mono_or_adjunct for event in program_events if event.mono_or_adjunct}
    )
    psychencode_deg_context = extract_cell_type_context(
        entity.metadata,
        "psychencode_deg_top_cell_types_json",
    )
    psychencode_grn_context = extract_cell_type_context(
        entity.metadata,
        "psychencode_grn_top_cell_types_json",
    )
    return {
        "clinical_domains": clinical_domains,
        "clinical_populations": clinical_populations,
        "mono_or_adjunct_contexts": mono_or_adjunct_contexts,
        "psychencode_deg_top_cell_types": psychencode_deg_context,
        "psychencode_grn_top_cell_types": psychencode_grn_context,
    }


def build_directionality_payload(
    entity: RankedEntity,
    hypothesis: DirectionalityHypothesis | None,
    program_events: list[ProgramHistoryEvent],
) -> dict[str, object]:
    if hypothesis is None:
        return {
            "status": "undetermined",
            "desired_perturbation_direction": "undetermined",
            "modality_hypothesis": "undetermined",
            "preferred_modalities": [],
            "confidence": "low",
            "ambiguity": "No curated target-level directionality hypothesis has been recorded yet.",
            "evidence_basis": "",
            "supporting_program_ids": [event.program_id for event in program_events],
            "contradiction_conditions": [],
            "falsification_conditions": [],
            "open_risks": [
                "Directionality and modality fit remain uncurated for this target."
            ],
        }

    supporting_program_ids = list(hypothesis.supporting_program_ids)
    if not supporting_program_ids and program_events:
        supporting_program_ids = [event.program_id for event in program_events]

    return {
        "status": "curated",
        "desired_perturbation_direction": hypothesis.desired_perturbation_direction,
        "modality_hypothesis": hypothesis.modality_hypothesis,
        "preferred_modalities": list(hypothesis.preferred_modalities),
        "confidence": hypothesis.confidence,
        "ambiguity": hypothesis.ambiguity,
        "evidence_basis": hypothesis.evidence_basis,
        "supporting_program_ids": supporting_program_ids,
        "contradiction_conditions": list(hypothesis.contradiction_conditions),
        "falsification_conditions": list(hypothesis.falsification_conditions),
        "open_risks": list(hypothesis.open_risks),
    }


def build_open_risks(
    warning_records: list[WarningRecord],
    directionality_payload: dict[str, object],
) -> list[dict[str, object]]:
    risks = [
        {
            "source": "warning_overlay",
            "risk_kind": record.warning_kind,
            "severity": record.severity,
            "text": record.warning_text,
        }
        for record in warning_records
    ]
    for risk_text in directionality_payload.get("open_risks", []):
        risks.append(
            {
                "source": "directionality_hypothesis",
                "risk_kind": "open_risk",
                "severity": directionality_payload.get("confidence", "low"),
                "text": risk_text,
            }
        )
    return risks


def build_target_ledger(
    entity: RankedEntity,
    program_history: list[ProgramHistoryEvent],
    hypotheses: dict[tuple[str, str], DirectionalityHypothesis],
) -> TargetLedger:
    matched_events = match_program_history(entity, program_history)
    failure_events = [build_failure_event(event) for event in matched_events]
    hypothesis = hypotheses.get((entity.entity_id, entity.entity_label.upper()))
    if hypothesis is None:
        hypothesis = hypotheses.get(("", entity.entity_label.upper()))
    directionality_payload = build_directionality_payload(entity, hypothesis, matched_events)

    scope_counts = Counter(event.failure_scope for event in failure_events)
    taxonomy_counts = Counter(event.failure_reason_taxonomy for event in failure_events)
    failure_scopes = sorted(
        scope
        for scope in scope_counts
        if scope != "nonfailure"
    )
    failure_taxonomies = sorted(
        taxonomy
        for taxonomy in taxonomy_counts
        if taxonomy != "not_applicable_nonfailure"
    )

    return TargetLedger(
        entity_id=entity.entity_id,
        entity_label=entity.entity_label,
        v0_snapshot=build_v0_snapshot(entity),
        source_primitives=build_source_primitives(entity),
        subgroup_domain_relevance=build_subgroup_domain_relevance(entity, matched_events),
        structural_failure_history={
            "matched_event_count": len(failure_events),
            "failure_event_count": sum(
                1 for event in failure_events if event.failure_scope != "nonfailure"
            ),
            "nonfailure_event_count": sum(
                1 for event in failure_events if event.failure_scope == "nonfailure"
            ),
            "event_count_by_scope": dict(sorted(scope_counts.items())),
            "failure_taxonomy_counts": dict(sorted(taxonomy_counts.items())),
            "failure_scopes": failure_scopes,
            "failure_taxonomies": failure_taxonomies,
            "events": [asdict(event) for event in failure_events],
        },
        directionality_hypothesis=directionality_payload,
        falsification_conditions=list(
            directionality_payload.get("falsification_conditions", [])
        ),
        open_risks=build_open_risks(entity.warning_records, directionality_payload),
    )


def build_target_ledgers(
    entities: list[RankedEntity],
    program_history_path: Path,
    directionality_hypotheses_path: Path,
) -> list[TargetLedger]:
    program_history = load_program_history(program_history_path)
    hypotheses = load_directionality_hypotheses(directionality_hypotheses_path)
    return [
        build_target_ledger(entity, program_history, hypotheses)
        for entity in entities
    ]


def target_ledgers_to_payload(
    ledgers: list[TargetLedger],
    *,
    program_history_path: Path,
    directionality_hypotheses_path: Path,
    repo_root: Path,
) -> dict[str, object]:
    def relativize(path: Path) -> str:
        try:
            return str(path.resolve().relative_to(repo_root.resolve()))
        except ValueError:
            return str(path.resolve())

    return {
        "schema_version": "pr7-target-ledger-v1",
        "scoring_neutral": True,
        "data_sources": {
            "program_history": relativize(program_history_path),
            "directionality_hypotheses": relativize(directionality_hypotheses_path),
        },
        "target_count": len(ledgers),
        "targets_with_program_history": sum(
            1
            for ledger in ledgers
            if ledger.structural_failure_history["matched_event_count"]
        ),
        "targets_with_curated_directionality": sum(
            1
            for ledger in ledgers
            if ledger.directionality_hypothesis["status"] == "curated"
        ),
        "targets": [asdict(ledger) for ledger in ledgers],
    }


def ledger_summary_fields(ledger: TargetLedger) -> dict[str, object]:
    return {
        "program_history_event_count": ledger.structural_failure_history["matched_event_count"],
        "failure_event_count": ledger.structural_failure_history["failure_event_count"],
        "failure_scopes": " | ".join(
            ledger.structural_failure_history["failure_scopes"]
        ),
        "failure_taxonomies": " | ".join(
            ledger.structural_failure_history["failure_taxonomies"]
        ),
        "program_history_domains": " | ".join(
            ledger.subgroup_domain_relevance["clinical_domains"]
        ),
        "program_history_populations": " | ".join(
            ledger.subgroup_domain_relevance["clinical_populations"]
        ),
        "directionality_status": ledger.directionality_hypothesis["status"],
        "desired_perturbation_direction": ledger.directionality_hypothesis[
            "desired_perturbation_direction"
        ],
        "modality_hypothesis": ledger.directionality_hypothesis["modality_hypothesis"],
        "directionality_confidence": ledger.directionality_hypothesis["confidence"],
    }
