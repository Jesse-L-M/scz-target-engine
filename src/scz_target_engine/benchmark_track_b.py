from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from datetime import date
import json
from pathlib import Path
import random
from typing import Any

from scz_target_engine.benchmark_protocol import INTERVENTION_OBJECT_ENTITY_TYPE
from scz_target_engine.io import read_csv_rows, read_json, write_json
from scz_target_engine.program_memory.counterfactuals import FAILURE_SCOPE_BY_TAXONOMY
from scz_target_engine.program_memory import (
    InterventionProposal,
    ProgramMemoryAnalog,
    ProgramMemoryDataset,
    assess_counterfactual_replay_risk,
    load_program_memory_dataset,
    retrieve_program_memory_analogs,
)

TRACK_B_TASK_ID = "scz_failure_memory_track_b_task"
TRACK_B_ASSETS_FILE_NAME = "assets.csv"
TRACK_B_CASEBOOK_FILE_NAME = "track_b_casebook.csv"
TRACK_B_DIRECTIONALITY_HYPOTHESES_FILE_NAME = "directionality_hypotheses.csv"
TRACK_B_EVENT_PROVENANCE_FILE_NAME = "event_provenance.csv"
TRACK_B_EVENTS_FILE_NAME = "events.csv"
TRACK_B_PROGRAM_UNIVERSE_FILE_NAME = "program_universe.csv"
TRACK_B_ENTITY_TYPE = INTERVENTION_OBJECT_ENTITY_TYPE
TRACK_B_HORIZON = "structural_replay"
TRACK_B_BOOTSTRAP_RESAMPLE_UNIT = "case"
TRACK_B_CASE_OUTPUT_SCHEMA_NAME = "benchmark_track_b_case_output_payload"
TRACK_B_CASE_OUTPUT_SCHEMA_VERSION = "v1"
TRACK_B_CONFUSION_SCHEMA_NAME = "benchmark_track_b_confusion_summary"
TRACK_B_CONFUSION_SCHEMA_VERSION = "v1"
TRACK_B_ALLOWED_REPLAY_STATUSES = (
    "replay_supported",
    "replay_not_supported",
    "replay_inconclusive",
    "insufficient_history",
)
TRACK_B_ALLOWED_FAILURE_SCOPES = (
    "target_class",
    "molecule",
    "endpoint",
    "population",
    "target",
    "unresolved",
    "nonfailure",
)
TRACK_B_REQUIRED_DIFFERENCE_ITEMS = (
    "population_enrichment_or_trial_conduct_change",
    "failure_scope_resolution",
    "direct_history_recovery",
    "target_differentiation",
    "target_class_differentiation",
    "molecule_differentiation",
    "endpoint_change",
)
TRACK_B_METRIC_NAMES = (
    "analog_recall_at_3",
    "failure_scope_macro_f1",
    "what_must_differ_checklist_f1",
    "replay_status_exact_match",
)
TRACK_B_RETRIEVAL_LIMIT = 3


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _normalize_text(value: str) -> str:
    return value.strip().lower()


def _round_metric(value: float) -> float:
    return round(value, 6)


def _json_list(value: str, field_name: str) -> tuple[str, ...]:
    cleaned = value.strip()
    if not cleaned:
        return ()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be a JSON array") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{field_name} must be a JSON array")
    values = []
    for item in payload:
        text = str(item).strip()
        if text:
            values.append(text)
    return tuple(values)


def _population_relation(left: str, right: str) -> str:
    left_words = {word for word in _normalize_text(left).replace("/", " ").split() if word}
    right_words = {word for word in _normalize_text(right).replace("/", " ").split() if word}
    if not left_words or not right_words:
        return "no_match"
    if left_words == right_words:
        return "exact_match"
    if left_words.intersection(right_words):
        return "partial_overlap"
    return "no_match"


def is_track_b_task(task_id: str) -> bool:
    return task_id == TRACK_B_TASK_ID


def track_b_casebook_path_for_archive_index_file(archive_index_file: Path) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_CASEBOOK_FILE_NAME


def track_b_assets_path_for_archive_index_file(archive_index_file: Path) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_ASSETS_FILE_NAME


def track_b_directionality_hypotheses_path_for_archive_index_file(
    archive_index_file: Path,
) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_DIRECTIONALITY_HYPOTHESES_FILE_NAME


def track_b_event_provenance_path_for_archive_index_file(archive_index_file: Path) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_EVENT_PROVENANCE_FILE_NAME


def track_b_events_path_for_archive_index_file(archive_index_file: Path) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_EVENTS_FILE_NAME


def track_b_program_universe_path_for_archive_index_file(archive_index_file: Path) -> Path:
    return archive_index_file.resolve().parent / TRACK_B_PROGRAM_UNIVERSE_FILE_NAME


def track_b_case_output_path(output_dir: Path, *, run_id: str) -> Path:
    return output_dir / "track_b_case_outputs" / f"{run_id}.json"


def track_b_confusion_summary_path(output_dir: Path, *, run_id: str) -> Path:
    return output_dir / "track_b_confusion_summaries" / f"{run_id}.json"


@dataclass(frozen=True)
class TrackBCase:
    case_id: str
    proposal_entity_id: str
    proposal_entity_label: str
    source_program_universe_id: str
    proposal: InterventionProposal
    gold_analog_event_ids: tuple[str, ...]
    gold_failure_scope: str
    gold_replay_status: str
    gold_required_differences: tuple[str, ...]
    coverage_state_at_cutoff: str
    coverage_reason_at_cutoff: str
    notes: str = ""


@dataclass(frozen=True)
class TrackBAnalogCandidate:
    event_id: str
    asset_id: str
    molecule: str
    target: str
    target_class: str
    domain: str
    population: str
    mono_or_adjunct: str
    phase: str
    event_date: str
    primary_outcome_result: str
    failure_reason_taxonomy: str
    failure_scope: str
    match_tier: str
    biological_anchor: bool
    source_tier: str
    source_url: str
    match_dimensions: tuple[str, ...]

    @property
    def is_nonfailure(self) -> bool:
        return self.failure_scope == "nonfailure"

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "asset_id": self.asset_id,
            "molecule": self.molecule,
            "target": self.target,
            "target_class": self.target_class,
            "domain": self.domain,
            "population": self.population,
            "mono_or_adjunct": self.mono_or_adjunct,
            "phase": self.phase,
            "event_date": self.event_date,
            "primary_outcome_result": self.primary_outcome_result,
            "failure_reason_taxonomy": self.failure_reason_taxonomy,
            "failure_scope": self.failure_scope,
            "match_tier": self.match_tier,
            "biological_anchor": self.biological_anchor,
            "source_tier": self.source_tier,
            "source_url": self.source_url,
            "match_dimensions": list(self.match_dimensions),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBAnalogCandidate:
        return cls(
            event_id=str(payload["event_id"]),
            asset_id=str(payload["asset_id"]),
            molecule=str(payload["molecule"]),
            target=str(payload["target"]),
            target_class=str(payload["target_class"]),
            domain=str(payload["domain"]),
            population=str(payload["population"]),
            mono_or_adjunct=str(payload["mono_or_adjunct"]),
            phase=str(payload["phase"]),
            event_date=str(payload["event_date"]),
            primary_outcome_result=str(payload["primary_outcome_result"]),
            failure_reason_taxonomy=str(payload["failure_reason_taxonomy"]),
            failure_scope=str(payload["failure_scope"]),
            match_tier=str(payload["match_tier"]),
            biological_anchor=bool(payload["biological_anchor"]),
            source_tier=str(payload["source_tier"]),
            source_url=str(payload["source_url"]),
            match_dimensions=tuple(
                str(item) for item in payload.get("match_dimensions", [])
            ),
        )

    @classmethod
    def from_program_memory_analog(
        cls,
        analog: ProgramMemoryAnalog,
    ) -> TrackBAnalogCandidate:
        return cls(
            event_id=analog.event_id,
            asset_id=analog.asset_id,
            molecule=analog.molecule,
            target=analog.target,
            target_class=analog.target_class,
            domain=analog.domain,
            population=analog.population,
            mono_or_adjunct=analog.mono_or_adjunct,
            phase=analog.phase,
            event_date=analog.event_date,
            primary_outcome_result=analog.primary_outcome_result,
            failure_reason_taxonomy=analog.failure_reason_taxonomy,
            failure_scope=FAILURE_SCOPE_BY_TAXONOMY.get(
                analog.failure_reason_taxonomy,
                "unresolved",
            ),
            match_tier=analog.match_tier,
            biological_anchor=True,
            source_tier=analog.record_ref.source_tier,
            source_url=analog.record_ref.source_url,
            match_dimensions=tuple(
                sorted({reason.dimension for reason in analog.match_reasons})
            ),
        )


@dataclass(frozen=True)
class TrackBCaseOutput:
    case_id: str
    baseline_id: str
    proposal_entity_id: str
    proposal_entity_label: str
    source_program_universe_id: str
    coverage_state_at_cutoff: str
    coverage_reason_at_cutoff: str
    gold_analog_event_ids: tuple[str, ...]
    retrieved_analog_event_ids: tuple[str, ...]
    retrieved_analogs: tuple[TrackBAnalogCandidate, ...]
    gold_failure_scope: str
    predicted_failure_scope: str
    gold_replay_status: str
    predicted_replay_status: str
    gold_required_differences: tuple[str, ...]
    predicted_required_differences: tuple[str, ...]
    analog_recall_at_3: float | None
    checklist_f1: float
    replay_status_exact_match: bool
    failure_scope_exact_match: bool
    reasoning_summary: str
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "case_id": self.case_id,
            "baseline_id": self.baseline_id,
            "proposal_entity_id": self.proposal_entity_id,
            "proposal_entity_label": self.proposal_entity_label,
            "source_program_universe_id": self.source_program_universe_id,
            "coverage_state_at_cutoff": self.coverage_state_at_cutoff,
            "coverage_reason_at_cutoff": self.coverage_reason_at_cutoff,
            "gold_analog_event_ids": list(self.gold_analog_event_ids),
            "retrieved_analog_event_ids": list(self.retrieved_analog_event_ids),
            "retrieved_analogs": [analog.to_dict() for analog in self.retrieved_analogs],
            "gold_failure_scope": self.gold_failure_scope,
            "predicted_failure_scope": self.predicted_failure_scope,
            "gold_replay_status": self.gold_replay_status,
            "predicted_replay_status": self.predicted_replay_status,
            "gold_required_differences": list(self.gold_required_differences),
            "predicted_required_differences": list(self.predicted_required_differences),
            "checklist_f1": self.checklist_f1,
            "replay_status_exact_match": self.replay_status_exact_match,
            "failure_scope_exact_match": self.failure_scope_exact_match,
            "reasoning_summary": self.reasoning_summary,
            "notes": self.notes,
        }
        if self.analog_recall_at_3 is not None:
            payload["analog_recall_at_3"] = self.analog_recall_at_3
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBCaseOutput:
        analog_recall_at_3 = payload.get("analog_recall_at_3")
        return cls(
            case_id=str(payload["case_id"]),
            baseline_id=str(payload["baseline_id"]),
            proposal_entity_id=str(payload["proposal_entity_id"]),
            proposal_entity_label=str(payload["proposal_entity_label"]),
            source_program_universe_id=str(payload["source_program_universe_id"]),
            coverage_state_at_cutoff=str(payload["coverage_state_at_cutoff"]),
            coverage_reason_at_cutoff=str(payload["coverage_reason_at_cutoff"]),
            gold_analog_event_ids=tuple(
                str(item) for item in payload.get("gold_analog_event_ids", [])
            ),
            retrieved_analog_event_ids=tuple(
                str(item) for item in payload.get("retrieved_analog_event_ids", [])
            ),
            retrieved_analogs=tuple(
                TrackBAnalogCandidate.from_dict(item)
                for item in payload.get("retrieved_analogs", [])
            ),
            gold_failure_scope=str(payload["gold_failure_scope"]),
            predicted_failure_scope=str(payload["predicted_failure_scope"]),
            gold_replay_status=str(payload["gold_replay_status"]),
            predicted_replay_status=str(payload["predicted_replay_status"]),
            gold_required_differences=tuple(
                str(item) for item in payload.get("gold_required_differences", [])
            ),
            predicted_required_differences=tuple(
                str(item) for item in payload.get("predicted_required_differences", [])
            ),
            analog_recall_at_3=(
                None if analog_recall_at_3 is None else float(analog_recall_at_3)
            ),
            checklist_f1=float(payload["checklist_f1"]),
            replay_status_exact_match=bool(payload["replay_status_exact_match"]),
            failure_scope_exact_match=bool(payload["failure_scope_exact_match"]),
            reasoning_summary=str(payload["reasoning_summary"]),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class TrackBCaseOutputPayload:
    run_id: str
    baseline_id: str
    snapshot_id: str
    as_of_date: str
    cases: tuple[TrackBCaseOutput, ...]
    schema_name: str = TRACK_B_CASE_OUTPUT_SCHEMA_NAME
    schema_version: str = TRACK_B_CASE_OUTPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "baseline_id": self.baseline_id,
            "snapshot_id": self.snapshot_id,
            "as_of_date": self.as_of_date,
            "cases": [case_output.to_dict() for case_output in self.cases],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBCaseOutputPayload:
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            run_id=str(payload["run_id"]),
            baseline_id=str(payload["baseline_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            as_of_date=str(payload["as_of_date"]),
            cases=tuple(
                TrackBCaseOutput.from_dict(item) for item in payload.get("cases", [])
            ),
        )


@dataclass(frozen=True)
class TrackBCountRow:
    label: str
    count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "label": self.label,
            "count": self.count,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBCountRow:
        return cls(
            label=str(payload["label"]),
            count=int(payload["count"]),
        )


@dataclass(frozen=True)
class TrackBConfusionCount:
    gold_label: str
    predicted_label: str
    count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "gold_label": self.gold_label,
            "predicted_label": self.predicted_label,
            "count": self.count,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBConfusionCount:
        return cls(
            gold_label=str(payload["gold_label"]),
            predicted_label=str(payload["predicted_label"]),
            count=int(payload["count"]),
        )


@dataclass(frozen=True)
class TrackBConfusionSummary:
    run_id: str
    baseline_id: str
    snapshot_id: str
    case_count: int
    analog_evaluable_case_count: int
    metric_values: dict[str, float]
    mismatched_case_ids: tuple[str, ...]
    failure_scope_confusions: tuple[TrackBConfusionCount, ...]
    replay_status_confusions: tuple[TrackBConfusionCount, ...]
    checklist_false_positives: tuple[TrackBCountRow, ...]
    checklist_false_negatives: tuple[TrackBCountRow, ...]
    schema_name: str = TRACK_B_CONFUSION_SCHEMA_NAME
    schema_version: str = TRACK_B_CONFUSION_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "baseline_id": self.baseline_id,
            "snapshot_id": self.snapshot_id,
            "case_count": self.case_count,
            "analog_evaluable_case_count": self.analog_evaluable_case_count,
            "metric_values": dict(self.metric_values),
            "mismatched_case_ids": list(self.mismatched_case_ids),
            "failure_scope_confusions": [
                row.to_dict() for row in self.failure_scope_confusions
            ],
            "replay_status_confusions": [
                row.to_dict() for row in self.replay_status_confusions
            ],
            "checklist_false_positives": [
                row.to_dict() for row in self.checklist_false_positives
            ],
            "checklist_false_negatives": [
                row.to_dict() for row in self.checklist_false_negatives
            ],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrackBConfusionSummary:
        metric_values = payload.get("metric_values", {})
        if not isinstance(metric_values, dict):
            raise ValueError("metric_values must be a JSON object")
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            run_id=str(payload["run_id"]),
            baseline_id=str(payload["baseline_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            case_count=int(payload["case_count"]),
            analog_evaluable_case_count=int(payload["analog_evaluable_case_count"]),
            metric_values={
                str(key): float(value)
                for key, value in metric_values.items()
            },
            mismatched_case_ids=tuple(
                str(item) for item in payload.get("mismatched_case_ids", [])
            ),
            failure_scope_confusions=tuple(
                TrackBConfusionCount.from_dict(item)
                for item in payload.get("failure_scope_confusions", [])
            ),
            replay_status_confusions=tuple(
                TrackBConfusionCount.from_dict(item)
                for item in payload.get("replay_status_confusions", [])
            ),
            checklist_false_positives=tuple(
                TrackBCountRow.from_dict(item)
                for item in payload.get("checklist_false_positives", [])
            ),
            checklist_false_negatives=tuple(
                TrackBCountRow.from_dict(item)
                for item in payload.get("checklist_false_negatives", [])
            ),
        )


def write_track_b_case_output_payload(
    path: Path,
    payload: TrackBCaseOutputPayload,
) -> None:
    write_json(path, payload.to_dict())


def read_track_b_case_output_payload(path: Path) -> TrackBCaseOutputPayload:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("track B case output payload must be a JSON object")
    return TrackBCaseOutputPayload.from_dict(payload)


def write_track_b_confusion_summary(
    path: Path,
    summary: TrackBConfusionSummary,
) -> None:
    write_json(path, summary.to_dict())


def read_track_b_confusion_summary(path: Path) -> TrackBConfusionSummary:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("track B confusion summary must be a JSON object")
    return TrackBConfusionSummary.from_dict(payload)


def _load_program_universe_index(path: Path) -> dict[str, dict[str, str]]:
    rows = read_csv_rows(path)
    index: dict[str, dict[str, str]] = {}
    for row in rows:
        program_universe_id = str(row.get("program_universe_id", "")).strip()
        if not program_universe_id:
            raise ValueError("program_universe.csv requires program_universe_id")
        if program_universe_id in index:
            raise ValueError(f"duplicate program_universe_id {program_universe_id!r}")
        index[program_universe_id] = row
    return index


def _load_pre_cutoff_event_ids(events_path: Path, *, as_of_date: str) -> set[str]:
    cutoff = _parse_iso_date(as_of_date, "as_of_date")
    event_ids: set[str] = set()
    for row in read_csv_rows(events_path):
        event_id = str(row.get("event_id", "")).strip()
        event_date = str(row.get("event_date", "")).strip()
        if not event_id or not event_date:
            raise ValueError("Track B events.csv requires event_id and event_date")
        if _parse_iso_date(event_date, "event_date") <= cutoff:
            event_ids.add(event_id)
    return event_ids


def load_track_b_casebook(
    path: Path,
    *,
    as_of_date: str,
    program_universe_path: Path,
    events_path: Path,
) -> tuple[TrackBCase, ...]:
    program_universe_index = _load_program_universe_index(program_universe_path)
    pre_cutoff_event_ids = _load_pre_cutoff_event_ids(events_path, as_of_date=as_of_date)
    rows = read_csv_rows(path)
    if not rows:
        raise ValueError("track_b_casebook.csv must contain at least one case")
    cases: list[TrackBCase] = []
    seen_case_ids: set[str] = set()
    seen_proposal_entity_ids: set[str] = set()
    for row in rows:
        case_id = _require_text(str(row.get("case_id", "")), "case_id")
        if case_id in seen_case_ids:
            raise ValueError(f"duplicate case_id in track_b_casebook.csv: {case_id}")
        seen_case_ids.add(case_id)
        proposal_entity_id = _require_text(
            str(row.get("proposal_entity_id", "")),
            "proposal_entity_id",
        )
        if proposal_entity_id in seen_proposal_entity_ids:
            raise ValueError(
                "duplicate proposal_entity_id in track_b_casebook.csv: "
                f"{proposal_entity_id}"
            )
        seen_proposal_entity_ids.add(proposal_entity_id)
        source_program_universe_id = _require_text(
            str(row.get("source_program_universe_id", "")),
            "source_program_universe_id",
        )
        source_row = program_universe_index.get(source_program_universe_id)
        if source_row is None:
            raise ValueError(
                "track_b_casebook.csv references unknown source_program_universe_id "
                f"{source_program_universe_id!r}"
            )
        coverage_state_at_cutoff = _require_text(
            str(row.get("coverage_state_at_cutoff", "")),
            "coverage_state_at_cutoff",
        )
        if coverage_state_at_cutoff != str(source_row.get("coverage_state", "")).strip():
            raise ValueError(
                "track_b_casebook.csv coverage_state_at_cutoff must match "
                f"program_universe.csv for {source_program_universe_id}"
            )
        coverage_reason_at_cutoff = _require_text(
            str(row.get("coverage_reason_at_cutoff", "")),
            "coverage_reason_at_cutoff",
        )
        if coverage_reason_at_cutoff != str(source_row.get("coverage_reason", "")).strip():
            raise ValueError(
                "track_b_casebook.csv coverage_reason_at_cutoff must match "
                f"program_universe.csv for {source_program_universe_id}"
            )
        gold_analog_event_ids = _json_list(
            str(row.get("gold_analog_event_ids_json", "")),
            "gold_analog_event_ids_json",
        )
        for event_id in gold_analog_event_ids:
            if event_id not in pre_cutoff_event_ids:
                raise ValueError(
                    "track_b_casebook.csv gold analog must exist in the pre-cutoff "
                    f"slice events.csv: {event_id}"
                )
        gold_failure_scope = _require_text(
            str(row.get("gold_failure_scope", "")),
            "gold_failure_scope",
        )
        if gold_failure_scope not in TRACK_B_ALLOWED_FAILURE_SCOPES:
            raise ValueError(
                f"unsupported gold_failure_scope {gold_failure_scope!r}"
            )
        gold_replay_status = _require_text(
            str(row.get("gold_replay_status", "")),
            "gold_replay_status",
        )
        if gold_replay_status not in TRACK_B_ALLOWED_REPLAY_STATUSES:
            raise ValueError(
                f"unsupported gold_replay_status {gold_replay_status!r}"
            )
        gold_required_differences = _json_list(
            str(row.get("gold_required_differences_json", "")),
            "gold_required_differences_json",
        )
        unknown_checklist_items = sorted(
            set(gold_required_differences).difference(TRACK_B_REQUIRED_DIFFERENCE_ITEMS)
        )
        if unknown_checklist_items:
            raise ValueError(
                "unsupported gold_required_differences_json items: "
                + ", ".join(unknown_checklist_items)
            )
        proposal = InterventionProposal(
            entity_id=str(row.get("proposal_entity_id", "")).strip(),
            target_symbol=str(row.get("proposal_target_symbol", "")).strip(),
            target_class=str(row.get("proposal_target_class", "")).strip(),
            molecule=str(row.get("proposal_molecule", "")).strip(),
            domain=str(row.get("proposal_domain", "")).strip(),
            population=str(row.get("proposal_population", "")).strip(),
            modality=str(row.get("proposal_modality", "")).strip(),
            mono_or_adjunct=str(row.get("proposal_mono_or_adjunct", "")).strip(),
            phase=str(row.get("proposal_phase", "")).strip(),
        )
        cases.append(
            TrackBCase(
                case_id=case_id,
                proposal_entity_id=proposal_entity_id,
                proposal_entity_label=_require_text(
                    str(row.get("proposal_entity_label", "")),
                    "proposal_entity_label",
                ),
                source_program_universe_id=source_program_universe_id,
                proposal=proposal,
                gold_analog_event_ids=gold_analog_event_ids,
                gold_failure_scope=gold_failure_scope,
                gold_replay_status=gold_replay_status,
                gold_required_differences=gold_required_differences,
                coverage_state_at_cutoff=coverage_state_at_cutoff,
                coverage_reason_at_cutoff=coverage_reason_at_cutoff,
                notes=str(row.get("notes", "")).strip(),
            )
        )
    return tuple(sorted(cases, key=lambda item: item.case_id))


def validate_track_b_casebook_against_cohort_members(
    *,
    cases: tuple[TrackBCase, ...],
    cohort_members: tuple[object, ...],
) -> None:
    case_index = {
        case.proposal_entity_id: case.proposal_entity_label
        for case in cases
    }
    member_index: dict[str, str] = {}
    for member in cohort_members:
        entity_type = str(getattr(member, "entity_type", "")).strip()
        entity_id = str(getattr(member, "entity_id", "")).strip()
        entity_label = str(getattr(member, "entity_label", "")).strip()
        if entity_type != TRACK_B_ENTITY_TYPE:
            continue
        member_index[entity_id] = entity_label
    missing_entity_ids = sorted(set(case_index).difference(member_index))
    unexpected_entity_ids = sorted(set(member_index).difference(case_index))
    label_mismatches = sorted(
        entity_id
        for entity_id, entity_label in member_index.items()
        if entity_id in case_index and case_index[entity_id] != entity_label
    )
    if not missing_entity_ids and not unexpected_entity_ids and not label_mismatches:
        return
    details: list[str] = []
    if missing_entity_ids:
        details.append(
            "missing="
            + ", ".join(missing_entity_ids[:5])
        )
    if unexpected_entity_ids:
        details.append(
            "unexpected="
            + ", ".join(unexpected_entity_ids[:5])
        )
    if label_mismatches:
        details.append(
            "label_mismatches="
            + ", ".join(label_mismatches[:5])
        )
    raise ValueError(
        "Track B cohort members must match track_b_casebook.csv proposal_entity_id "
        "and proposal_entity_label"
        + (f" ({'; '.join(details)})" if details else "")
    )


def build_track_b_program_memory_dataset(
    *,
    as_of_date: str,
    events_path: Path,
    dataset_dir: Path | None = None,
) -> ProgramMemoryDataset:
    resolved_dataset_dir = (
        events_path.resolve().parent
        if dataset_dir is None
        else dataset_dir.resolve()
    )
    required_paths = (
        resolved_dataset_dir / TRACK_B_ASSETS_FILE_NAME,
        resolved_dataset_dir / TRACK_B_EVENTS_FILE_NAME,
        resolved_dataset_dir / TRACK_B_EVENT_PROVENANCE_FILE_NAME,
        resolved_dataset_dir / TRACK_B_DIRECTIONALITY_HYPOTHESES_FILE_NAME,
        resolved_dataset_dir / TRACK_B_PROGRAM_UNIVERSE_FILE_NAME,
    )
    missing_paths = [path.name for path in required_paths if not path.exists()]
    if missing_paths:
        raise ValueError(
            "Track B benchmark requires a full pinned program-memory dataset beside "
            f"events.csv, missing: {', '.join(sorted(missing_paths))}"
        )
    root_dataset = load_program_memory_dataset(resolved_dataset_dir)
    pre_cutoff_event_ids = _load_pre_cutoff_event_ids(events_path, as_of_date=as_of_date)
    events = tuple(
        event for event in root_dataset.events if event.event_id in pre_cutoff_event_ids
    )
    provenances = tuple(
        provenance
        for provenance in root_dataset.provenances
        if provenance.event_id in pre_cutoff_event_ids
    )
    directionality_hypotheses = tuple(
        replace(
            hypothesis,
            supporting_event_ids=tuple(
                event_id
                for event_id in hypothesis.supporting_event_ids
                if event_id in pre_cutoff_event_ids
            ),
        )
        for hypothesis in root_dataset.directionality_hypotheses
    )
    return ProgramMemoryDataset(
        assets=root_dataset.assets,
        events=events,
        provenances=provenances,
        directionality_hypotheses=directionality_hypotheses,
        program_universe_rows=root_dataset.program_universe_rows,
        requires_program_universe=root_dataset.requires_program_universe,
    )


def _filtered_candidates(
    analogs: tuple[TrackBAnalogCandidate, ...],
    *,
    predicate: Any,
) -> tuple[TrackBAnalogCandidate, ...]:
    return tuple(analog for analog in analogs if predicate(analog))


def _sorted_unique_candidates(
    candidates: list[tuple[float, TrackBAnalogCandidate]],
) -> tuple[TrackBAnalogCandidate, ...]:
    seen_event_ids: set[str] = set()
    ordered: list[TrackBAnalogCandidate] = []
    for _, candidate in sorted(
        candidates,
        key=lambda item: (
            -item[0],
            -int(item[1].event_date.replace("-", "")),
            item[1].event_id,
        ),
    ):
        if candidate.event_id in seen_event_ids:
            continue
        seen_event_ids.add(candidate.event_id)
        ordered.append(candidate)
    return tuple(ordered)


def _nearest_history_candidates(
    dataset: ProgramMemoryDataset,
    proposal: InterventionProposal,
) -> tuple[TrackBAnalogCandidate, ...]:
    assets_by_id = {
        asset.asset_id: asset
        for asset in dataset.assets
    }
    provenance_by_event_id = {
        provenance.event_id: provenance
        for provenance in dataset.provenances
    }
    candidates: list[tuple[float, TrackBAnalogCandidate]] = []
    normalized_target_symbol = proposal.target_symbol.strip().upper()
    normalized_target_class = _normalize_text(proposal.target_class)
    normalized_molecule = _normalize_text(proposal.molecule)
    normalized_domain = _normalize_text(proposal.domain)
    normalized_modality = _normalize_text(proposal.modality)
    normalized_regimen = _normalize_text(proposal.mono_or_adjunct)
    normalized_phase = _normalize_text(proposal.phase)

    for event in dataset.events:
        asset = assets_by_id[event.asset_id]
        provenance = provenance_by_event_id[event.event_id]
        match_dimensions: list[str] = []
        score = 0.0
        if normalized_molecule and _normalize_text(asset.molecule) == normalized_molecule:
            score += 4.0
            match_dimensions.append("molecule")
        if normalized_target_symbol and normalized_target_symbol in asset.target_symbols:
            score += 3.0
            match_dimensions.append("target_symbol")
        if normalized_target_class and _normalize_text(asset.target_class) == normalized_target_class:
            score += 2.5
            match_dimensions.append("target_class")
        if normalized_domain and _normalize_text(event.domain) == normalized_domain:
            score += 2.0
            match_dimensions.append("domain")
        if normalized_modality and _normalize_text(asset.modality) == normalized_modality:
            score += 1.0
            match_dimensions.append("modality")
        if normalized_regimen and _normalize_text(event.mono_or_adjunct) == normalized_regimen:
            score += 1.0
            match_dimensions.append("mono_or_adjunct")
        if normalized_phase and _normalize_text(event.phase) == normalized_phase:
            score += 0.5
            match_dimensions.append("phase")
        population_relation = _population_relation(proposal.population, event.population)
        if population_relation == "exact_match":
            score += 1.5
            match_dimensions.append("population")
        elif population_relation == "partial_overlap":
            score += 0.5
            match_dimensions.append("population")
        if score <= 0:
            continue
        candidates.append(
            (
                score,
                TrackBAnalogCandidate(
                    event_id=event.event_id,
                    asset_id=asset.asset_id,
                    molecule=asset.molecule,
                    target=asset.target,
                    target_class=asset.target_class,
                    domain=event.domain,
                    population=event.population,
                    mono_or_adjunct=event.mono_or_adjunct,
                    phase=event.phase,
                    event_date=event.event_date,
                    primary_outcome_result=event.primary_outcome_result,
                    failure_reason_taxonomy=event.failure_reason_taxonomy,
                    failure_scope=FAILURE_SCOPE_BY_TAXONOMY.get(
                        event.failure_reason_taxonomy,
                        "unresolved",
                    ),
                    match_tier="nearest_history",
                    biological_anchor=False,
                    source_tier=provenance.source_tier,
                    source_url=provenance.source_url,
                    match_dimensions=tuple(sorted(set(match_dimensions))),
                ),
            )
        )
    return _sorted_unique_candidates(candidates)


def _retrieved_analogs_for_baseline(
    *,
    dataset: ProgramMemoryDataset,
    case: TrackBCase,
    baseline_id: str,
) -> tuple[TrackBAnalogCandidate, ...]:
    analog_search = retrieve_program_memory_analogs(
        dataset,
        case.proposal,
        limit=None,
    )
    converted = tuple(
        TrackBAnalogCandidate.from_program_memory_analog(analog)
        for analog in analog_search.all_matched_analogs
    )
    if baseline_id == "track_b_exact_target":
        return _filtered_candidates(
            converted,
            predicate=lambda analog: "target_symbol" in analog.match_dimensions
            or analog.match_tier == "molecule",
        )[:TRACK_B_RETRIEVAL_LIMIT]
    if baseline_id == "track_b_target_class":
        return _filtered_candidates(
            converted,
            predicate=lambda analog: "target_class" in analog.match_dimensions
            or analog.match_tier == "molecule",
        )[:TRACK_B_RETRIEVAL_LIMIT]
    if baseline_id == "track_b_nearest_history":
        return _nearest_history_candidates(dataset, case.proposal)[
            :TRACK_B_RETRIEVAL_LIMIT
        ]
    if baseline_id == "track_b_structural_current":
        return converted[:TRACK_B_RETRIEVAL_LIMIT]
    raise ValueError(f"unsupported Track B baseline_id: {baseline_id}")


def _predict_failure_scope(
    *,
    case: TrackBCase,
    retrieved_analogs: tuple[TrackBAnalogCandidate, ...],
) -> str:
    if not retrieved_analogs:
        return "unresolved"
    aligned_failures = [
        analog.failure_scope
        for analog in retrieved_analogs
        if analog.failure_scope not in {"nonfailure", "unresolved"}
        and _normalize_text(analog.domain) == _normalize_text(case.proposal.domain)
        and analog.biological_anchor
    ]
    if aligned_failures:
        return Counter(aligned_failures).most_common(1)[0][0]
    if any(analog.failure_scope == "nonfailure" for analog in retrieved_analogs):
        if any(
            analog.failure_scope == "unresolved" for analog in retrieved_analogs
        ):
            return "unresolved"
        return "nonfailure"
    return "unresolved"


def _predict_replay_status(
    *,
    case: TrackBCase,
    retrieved_analogs: tuple[TrackBAnalogCandidate, ...],
    predicted_failure_scope: str,
    current_assessment_status: str | None = None,
) -> str:
    if current_assessment_status:
        return current_assessment_status
    if not retrieved_analogs:
        return "insufficient_history"
    if (
        case.coverage_state_at_cutoff != "included"
        and not any(analog.biological_anchor for analog in retrieved_analogs)
    ):
        return "insufficient_history"
    aligned_nonfailures = [
        analog
        for analog in retrieved_analogs
        if analog.failure_scope == "nonfailure"
        and _normalize_text(analog.domain) == _normalize_text(case.proposal.domain)
    ]
    aligned_supported_failures = [
        analog
        for analog in retrieved_analogs
        if analog.biological_anchor
        and analog.failure_scope not in {"nonfailure", "unresolved"}
        and _normalize_text(analog.domain) == _normalize_text(case.proposal.domain)
    ]
    if predicted_failure_scope == "unresolved":
        if aligned_nonfailures:
            return "replay_not_supported"
        return "replay_inconclusive"
    if aligned_supported_failures and aligned_nonfailures:
        return "replay_inconclusive"
    if aligned_supported_failures:
        return "replay_supported"
    if aligned_nonfailures:
        return "replay_not_supported"
    return "replay_inconclusive"


def _predict_required_differences(
    *,
    case: TrackBCase,
    predicted_failure_scope: str,
    predicted_replay_status: str,
    retrieved_analogs: tuple[TrackBAnalogCandidate, ...],
) -> tuple[str, ...]:
    if not retrieved_analogs or (
        predicted_replay_status == "insufficient_history"
        and case.coverage_state_at_cutoff != "included"
    ):
        return ("direct_history_recovery",)
    if predicted_failure_scope == "population":
        return ("population_enrichment_or_trial_conduct_change",)
    if predicted_failure_scope == "unresolved":
        return ("failure_scope_resolution",)
    if predicted_failure_scope == "target":
        return ("target_differentiation",)
    if predicted_failure_scope == "target_class":
        return ("target_class_differentiation",)
    if predicted_failure_scope == "molecule":
        return ("molecule_differentiation",)
    if predicted_failure_scope == "endpoint":
        return ("endpoint_change",)
    return ()


def _build_reasoning_summary(
    *,
    case: TrackBCase,
    retrieved_analogs: tuple[TrackBAnalogCandidate, ...],
    predicted_failure_scope: str,
    predicted_replay_status: str,
    current_summary: str = "",
) -> str:
    if current_summary:
        return current_summary
    if not retrieved_analogs:
        return (
            f"{case.case_id} has no retrieved analogs at the cutoff, so Track B keeps "
            "the case in insufficient-history mode."
        )
    analog_ids = ", ".join(analog.event_id for analog in retrieved_analogs)
    return (
        f"Retrieved {len(retrieved_analogs)} analog(s) [{analog_ids}] and predicted "
        f"{predicted_failure_scope} / {predicted_replay_status}."
    )


def _checklist_f1(
    gold_items: tuple[str, ...],
    predicted_items: tuple[str, ...],
) -> float:
    gold = set(gold_items)
    predicted = set(predicted_items)
    if not gold and not predicted:
        return 1.0
    if not gold or not predicted:
        return 0.0
    true_positive = len(gold.intersection(predicted))
    if true_positive == 0:
        return 0.0
    precision = true_positive / len(predicted)
    recall = true_positive / len(gold)
    return (2 * precision * recall) / (precision + recall)


def _analog_recall_at_3(
    gold_event_ids: tuple[str, ...],
    retrieved_event_ids: tuple[str, ...],
) -> float | None:
    if not gold_event_ids:
        return None
    retrieved = set(retrieved_event_ids[:TRACK_B_RETRIEVAL_LIMIT])
    gold = set(gold_event_ids)
    return len(gold.intersection(retrieved)) / len(gold)


def validate_track_b_case_output_payload(
    payload: TrackBCaseOutputPayload,
    *,
    expected_as_of_date: str | None = None,
) -> None:
    _parse_iso_date(payload.as_of_date, "as_of_date")
    if expected_as_of_date is not None:
        _parse_iso_date(expected_as_of_date, "expected_as_of_date")
        if payload.as_of_date != expected_as_of_date:
            raise ValueError(
                "Track B case output payload as_of_date does not match snapshot manifest "
                f"for {payload.run_id}"
            )
    for case_output in payload.cases:
        if case_output.baseline_id != payload.baseline_id:
            raise ValueError(
                "Track B case output baseline_id does not match payload baseline_id "
                f"for {payload.run_id}/{case_output.case_id}"
            )
        if len(case_output.retrieved_analogs) > TRACK_B_RETRIEVAL_LIMIT:
            raise ValueError(
                "Track B case output retrieved_analogs exceed the Track B retrieval "
                f"limit for {payload.run_id}/{case_output.case_id}"
            )
        expected_retrieved_event_ids = tuple(
            analog.event_id for analog in case_output.retrieved_analogs
        )
        if case_output.retrieved_analog_event_ids != expected_retrieved_event_ids:
            raise ValueError(
                "Track B case output retrieved_analog_event_ids do not match "
                f"retrieved_analogs for {payload.run_id}/{case_output.case_id}"
            )
        expected_analog_recall = _analog_recall_at_3(
            case_output.gold_analog_event_ids,
            case_output.retrieved_analog_event_ids,
        )
        if case_output.analog_recall_at_3 != expected_analog_recall:
            raise ValueError(
                "Track B case output analog_recall_at_3 does not match analog event "
                f"ids for {payload.run_id}/{case_output.case_id}"
            )


def build_track_b_case_outputs(
    *,
    cases: tuple[TrackBCase, ...],
    dataset: ProgramMemoryDataset,
    baseline_id: str,
) -> tuple[TrackBCaseOutput, ...]:
    outputs: list[TrackBCaseOutput] = []
    for case in cases:
        current_assessment_status = None
        current_summary = ""
        retrieved_analogs = _retrieved_analogs_for_baseline(
            dataset=dataset,
            case=case,
            baseline_id=baseline_id,
        )
        if baseline_id == "track_b_structural_current":
            assessment = assess_counterfactual_replay_risk(
                dataset,
                case.proposal,
                limit=3,
            )
            current_assessment_status = assessment.status
            current_summary = assessment.summary
        predicted_failure_scope = _predict_failure_scope(
            case=case,
            retrieved_analogs=retrieved_analogs,
        )
        predicted_replay_status = _predict_replay_status(
            case=case,
            retrieved_analogs=retrieved_analogs,
            predicted_failure_scope=predicted_failure_scope,
            current_assessment_status=current_assessment_status,
        )
        predicted_required_differences = _predict_required_differences(
            case=case,
            predicted_failure_scope=predicted_failure_scope,
            predicted_replay_status=predicted_replay_status,
            retrieved_analogs=retrieved_analogs,
        )
        retrieved_analog_event_ids = tuple(
            analog.event_id for analog in retrieved_analogs
        )
        outputs.append(
            TrackBCaseOutput(
                case_id=case.case_id,
                baseline_id=baseline_id,
                proposal_entity_id=case.proposal_entity_id,
                proposal_entity_label=case.proposal_entity_label,
                source_program_universe_id=case.source_program_universe_id,
                coverage_state_at_cutoff=case.coverage_state_at_cutoff,
                coverage_reason_at_cutoff=case.coverage_reason_at_cutoff,
                gold_analog_event_ids=case.gold_analog_event_ids,
                retrieved_analog_event_ids=retrieved_analog_event_ids,
                retrieved_analogs=retrieved_analogs,
                gold_failure_scope=case.gold_failure_scope,
                predicted_failure_scope=predicted_failure_scope,
                gold_replay_status=case.gold_replay_status,
                predicted_replay_status=predicted_replay_status,
                gold_required_differences=case.gold_required_differences,
                predicted_required_differences=predicted_required_differences,
                analog_recall_at_3=_analog_recall_at_3(
                    case.gold_analog_event_ids,
                    retrieved_analog_event_ids,
                ),
                checklist_f1=_round_metric(
                    _checklist_f1(
                        case.gold_required_differences,
                        predicted_required_differences,
                    )
                ),
                replay_status_exact_match=(
                    predicted_replay_status == case.gold_replay_status
                ),
                failure_scope_exact_match=(
                    predicted_failure_scope == case.gold_failure_scope
                ),
                reasoning_summary=_build_reasoning_summary(
                    case=case,
                    retrieved_analogs=retrieved_analogs,
                    predicted_failure_scope=predicted_failure_scope,
                    predicted_replay_status=predicted_replay_status,
                    current_summary=current_summary,
                ),
                notes=case.notes,
            )
        )
    return tuple(outputs)


def _failure_scope_macro_f1(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> float:
    labels = sorted(
        {
            case_output.gold_failure_scope
            for case_output in case_outputs
        }.union(
            case_output.predicted_failure_scope
            for case_output in case_outputs
        )
    )
    if not labels:
        return 0.0
    f1_scores: list[float] = []
    for label in labels:
        true_positive = sum(
            1
            for case_output in case_outputs
            if case_output.gold_failure_scope == label
            and case_output.predicted_failure_scope == label
        )
        false_positive = sum(
            1
            for case_output in case_outputs
            if case_output.gold_failure_scope != label
            and case_output.predicted_failure_scope == label
        )
        false_negative = sum(
            1
            for case_output in case_outputs
            if case_output.gold_failure_scope == label
            and case_output.predicted_failure_scope != label
        )
        if true_positive == 0 and false_positive == 0 and false_negative == 0:
            continue
        precision = (
            true_positive / (true_positive + false_positive)
            if (true_positive + false_positive)
            else 0.0
        )
        recall = (
            true_positive / (true_positive + false_negative)
            if (true_positive + false_negative)
            else 0.0
        )
        if precision == 0.0 or recall == 0.0:
            f1_scores.append(0.0)
            continue
        f1_scores.append((2 * precision * recall) / (precision + recall))
    if not f1_scores:
        return 0.0
    return sum(f1_scores) / len(f1_scores)


def _checklist_macro_f1(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> float:
    if not case_outputs:
        return 0.0
    return sum(case_output.checklist_f1 for case_output in case_outputs) / len(case_outputs)


def _replay_status_exact_match_rate(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> float:
    if not case_outputs:
        return 0.0
    return sum(
        1 for case_output in case_outputs if case_output.replay_status_exact_match
    ) / len(case_outputs)


def _analog_recall_macro_at_3(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> tuple[float, int]:
    evaluable = [
        case_output.analog_recall_at_3
        for case_output in case_outputs
        if case_output.analog_recall_at_3 is not None
    ]
    if not evaluable:
        return 0.0, 0
    return sum(evaluable) / len(evaluable), len(evaluable)


def score_track_b_case_outputs(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> dict[str, float]:
    analog_recall, _ = _analog_recall_macro_at_3(case_outputs)
    return {
        "analog_recall_at_3": _round_metric(analog_recall),
        "failure_scope_macro_f1": _round_metric(
            _failure_scope_macro_f1(case_outputs)
        ),
        "what_must_differ_checklist_f1": _round_metric(
            _checklist_macro_f1(case_outputs)
        ),
        "replay_status_exact_match": _round_metric(
            _replay_status_exact_match_rate(case_outputs)
        ),
    }


def _track_b_case_has_mismatch(case_output: TrackBCaseOutput) -> bool:
    analog_retrieval_miss = (
        case_output.analog_recall_at_3 is not None
        and case_output.analog_recall_at_3 < 1.0
    )
    return analog_retrieval_miss or not (
        case_output.failure_scope_exact_match
        and case_output.replay_status_exact_match
        and case_output.checklist_f1 == 1.0
    )


def track_b_metric_cohort_sizes(
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> dict[str, int]:
    analog_recall, analog_case_count = _analog_recall_macro_at_3(case_outputs)
    _ = analog_recall
    return {
        "analog_recall_at_3": analog_case_count,
        "failure_scope_macro_f1": len(case_outputs),
        "what_must_differ_checklist_f1": len(case_outputs),
        "replay_status_exact_match": len(case_outputs),
    }


def _percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def estimate_track_b_metric_intervals(
    case_outputs: tuple[TrackBCaseOutput, ...],
    *,
    iterations: int,
    confidence_level: float,
    random_seed: int,
) -> dict[str, tuple[float, float, float]]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    point_estimates = score_track_b_case_outputs(case_outputs)
    if not case_outputs:
        return {
            metric_name: (point_estimate, point_estimate, point_estimate)
            for metric_name, point_estimate in point_estimates.items()
        }
    rng = random.Random(random_seed)
    samples_by_metric = {
        metric_name: []
        for metric_name in TRACK_B_METRIC_NAMES
    }
    for _ in range(iterations):
        sample = tuple(
            case_outputs[rng.randrange(len(case_outputs))]
            for _ in range(len(case_outputs))
        )
        sample_scores = score_track_b_case_outputs(sample)
        _, analog_case_count = _analog_recall_macro_at_3(sample)
        for metric_name, metric_value in sample_scores.items():
            if metric_name == "analog_recall_at_3" and analog_case_count == 0:
                continue
            samples_by_metric[metric_name].append(metric_value)
    alpha = (1.0 - confidence_level) / 2.0
    intervals: dict[str, tuple[float, float, float]] = {}
    for metric_name, point_estimate in point_estimates.items():
        sorted_values = sorted(samples_by_metric[metric_name])
        if not sorted_values:
            intervals[metric_name] = (
                point_estimate,
                point_estimate,
                point_estimate,
            )
            continue
        intervals[metric_name] = (
            point_estimate,
            _round_metric(_percentile(sorted_values, alpha)),
            _round_metric(_percentile(sorted_values, 1.0 - alpha)),
        )
    return intervals


def build_track_b_confusion_summary(
    *,
    run_id: str,
    baseline_id: str,
    snapshot_id: str,
    case_outputs: tuple[TrackBCaseOutput, ...],
) -> TrackBConfusionSummary:
    metrics = score_track_b_case_outputs(case_outputs)
    analog_evaluable_case_count = sum(
        1 for case_output in case_outputs if case_output.analog_recall_at_3 is not None
    )
    mismatch_ids = tuple(
        case_output.case_id
        for case_output in case_outputs
        if _track_b_case_has_mismatch(case_output)
    )
    failure_scope_counter: Counter[tuple[str, str]] = Counter(
        (
            case_output.gold_failure_scope,
            case_output.predicted_failure_scope,
        )
        for case_output in case_outputs
    )
    replay_status_counter: Counter[tuple[str, str]] = Counter(
        (
            case_output.gold_replay_status,
            case_output.predicted_replay_status,
        )
        for case_output in case_outputs
    )
    checklist_false_positives: Counter[str] = Counter()
    checklist_false_negatives: Counter[str] = Counter()
    for case_output in case_outputs:
        gold = set(case_output.gold_required_differences)
        predicted = set(case_output.predicted_required_differences)
        for item in sorted(predicted.difference(gold)):
            checklist_false_positives[item] += 1
        for item in sorted(gold.difference(predicted)):
            checklist_false_negatives[item] += 1
    return TrackBConfusionSummary(
        run_id=run_id,
        baseline_id=baseline_id,
        snapshot_id=snapshot_id,
        case_count=len(case_outputs),
        analog_evaluable_case_count=analog_evaluable_case_count,
        metric_values=metrics,
        mismatched_case_ids=mismatch_ids,
        failure_scope_confusions=tuple(
            TrackBConfusionCount(
                gold_label=gold_label,
                predicted_label=predicted_label,
                count=count,
            )
            for (gold_label, predicted_label), count in sorted(
                failure_scope_counter.items()
            )
        ),
        replay_status_confusions=tuple(
            TrackBConfusionCount(
                gold_label=gold_label,
                predicted_label=predicted_label,
                count=count,
            )
            for (gold_label, predicted_label), count in sorted(
                replay_status_counter.items()
            )
        ),
        checklist_false_positives=tuple(
            TrackBCountRow(label=label, count=count)
            for label, count in sorted(checklist_false_positives.items())
        ),
        checklist_false_negatives=tuple(
            TrackBCountRow(label=label, count=count)
            for label, count in sorted(checklist_false_negatives.items())
        ),
    )


def build_track_b_error_analysis_markdown(
    *,
    payload: TrackBCaseOutputPayload,
    confusion_summary: TrackBConfusionSummary,
    metric_intervals: dict[str, tuple[float, float, float]],
) -> str:
    lines = [
        f"# Track B Case Review: {payload.run_id}",
        "",
        f"- baseline: `{payload.baseline_id}`",
        f"- snapshot: `{payload.snapshot_id}`",
        f"- as_of_date: `{payload.as_of_date}`",
        f"- case count: {len(payload.cases)}",
        f"- analog-evaluable cases: {confusion_summary.analog_evaluable_case_count}",
        "",
        "## Metric Summary",
    ]
    for metric_name in TRACK_B_METRIC_NAMES:
        point_estimate, interval_low, interval_high = metric_intervals[metric_name]
        lines.append(
            f"- `{metric_name}`: {point_estimate:.3f} "
            f"[{interval_low:.3f}, {interval_high:.3f}]"
        )
    lines.extend(["", "## Mismatched Cases"])
    mismatches = [
        case_output
        for case_output in payload.cases
        if case_output.case_id in set(confusion_summary.mismatched_case_ids)
    ]
    if not mismatches:
        lines.append("- none")
    for case_output in mismatches:
        lines.append(
            f"### {case_output.case_id} ({case_output.proposal_entity_label})"
        )
        lines.append(
            f"- gold analogs: {', '.join(case_output.gold_analog_event_ids) or 'none'}"
        )
        lines.append(
            "- retrieved analogs: "
            + (", ".join(case_output.retrieved_analog_event_ids) or "none")
        )
        lines.append(
            f"- failure scope: gold `{case_output.gold_failure_scope}` vs "
            f"predicted `{case_output.predicted_failure_scope}`"
        )
        lines.append(
            f"- replay status: gold `{case_output.gold_replay_status}` vs "
            f"predicted `{case_output.predicted_replay_status}`"
        )
        lines.append(
            "- analog recall@3: "
            + (
                f"{case_output.analog_recall_at_3:.3f}"
                if case_output.analog_recall_at_3 is not None
                else "not evaluable"
            )
        )
        lines.append(
            "- required differences: gold "
            + (
                f"`{', '.join(case_output.gold_required_differences)}`"
                if case_output.gold_required_differences
                else "`(none)`"
            )
            + " vs predicted "
            + (
                f"`{', '.join(case_output.predicted_required_differences)}`"
                if case_output.predicted_required_differences
                else "`(none)`"
            )
        )
        lines.append(f"- coverage state at cutoff: `{case_output.coverage_state_at_cutoff}`")
        lines.append(f"- reasoning: {case_output.reasoning_summary}")
        lines.append("")
    lines.append("## Confusion Summary")
    lines.append("- Failure scope confusions:")
    if confusion_summary.failure_scope_confusions:
        for row in confusion_summary.failure_scope_confusions:
            lines.append(
                f"  - {row.gold_label} -> {row.predicted_label}: {row.count}"
            )
    else:
        lines.append("  - none")
    lines.append("- Replay status confusions:")
    if confusion_summary.replay_status_confusions:
        for row in confusion_summary.replay_status_confusions:
            lines.append(
                f"  - {row.gold_label} -> {row.predicted_label}: {row.count}"
            )
    else:
        lines.append("  - none")
    if confusion_summary.checklist_false_negatives:
        lines.append("- Checklist false negatives:")
        for row in confusion_summary.checklist_false_negatives:
            lines.append(f"  - {row.label}: {row.count}")
    if confusion_summary.checklist_false_positives:
        lines.append("- Checklist false positives:")
        for row in confusion_summary.checklist_false_positives:
            lines.append(f"  - {row.label}: {row.count}")
    return "\n".join(lines).rstrip() + "\n"
