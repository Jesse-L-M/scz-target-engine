from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from scz_target_engine.benchmark_protocol import (
    BENCHMARK_QUESTION_V1,
    BenchmarkSnapshotManifest,
    VALID_ENTITY_TYPES,
)
from scz_target_engine.io import read_csv_rows, write_csv


BENCHMARK_COHORT_LABEL_FIELDNAMES = [
    "cohort_id",
    "snapshot_id",
    "entity_type",
    "entity_id",
    "entity_label",
    "label_name",
    "label_value",
    "horizon",
    "outcome_date",
    "label_source",
    "label_notes",
]
NO_OUTCOME_LABEL = "no_qualifying_future_outcome"
OBSERVED_LABEL_VALUE = "true"
NOT_OBSERVED_LABEL_VALUE = "false"


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from exc


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def _parse_horizon_years(horizon: str) -> int:
    if not horizon.endswith("y"):
        raise ValueError(f"unsupported evaluation horizon: {horizon}")
    return int(horizon.removesuffix("y"))


@dataclass(frozen=True)
class CohortMember:
    entity_type: str
    entity_id: str
    entity_label: str

    def __post_init__(self) -> None:
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        _require_text(self.entity_label, "entity_label")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CohortMember:
        return cls(
            entity_type=str(payload["entity_type"]),
            entity_id=str(payload["entity_id"]),
            entity_label=str(payload["entity_label"]),
        )


@dataclass(frozen=True)
class FutureOutcomeRecord:
    entity_type: str
    entity_id: str
    outcome_label: str
    outcome_date: str
    label_source: str
    label_notes: str = ""

    def __post_init__(self) -> None:
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        if self.outcome_label == NO_OUTCOME_LABEL:
            raise ValueError("future outcome inputs must not precompute no_qualifying_future_outcome")
        if self.outcome_label not in BENCHMARK_QUESTION_V1.translational_outcome_labels:
            raise ValueError("outcome_label must match the frozen benchmark question labels")
        _parse_iso_date(self.outcome_date, "outcome_date")
        _require_text(self.label_source, "label_source")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FutureOutcomeRecord:
        return cls(
            entity_type=str(payload["entity_type"]),
            entity_id=str(payload["entity_id"]),
            outcome_label=str(payload["outcome_label"]),
            outcome_date=str(payload["outcome_date"]),
            label_source=str(payload["label_source"]),
            label_notes=str(payload.get("label_notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkCohortLabel:
    cohort_id: str
    snapshot_id: str
    entity_type: str
    entity_id: str
    entity_label: str
    label_name: str
    label_value: str
    horizon: str
    outcome_date: str
    label_source: str
    label_notes: str = ""

    def __post_init__(self) -> None:
        _require_text(self.cohort_id, "cohort_id")
        _require_text(self.snapshot_id, "snapshot_id")
        if self.entity_type not in VALID_ENTITY_TYPES:
            raise ValueError("entity_type must be a supported benchmark entity type")
        _require_text(self.entity_id, "entity_id")
        _require_text(self.entity_label, "entity_label")
        if self.label_name not in BENCHMARK_QUESTION_V1.translational_outcome_labels:
            raise ValueError("label_name must match the frozen benchmark question labels")
        if self.label_value not in {OBSERVED_LABEL_VALUE, NOT_OBSERVED_LABEL_VALUE}:
            raise ValueError("label_value must be true or false")
        if self.horizon not in BENCHMARK_QUESTION_V1.evaluation_horizons:
            raise ValueError("horizon must match the frozen benchmark question horizons")
        if self.outcome_date:
            _parse_iso_date(self.outcome_date, "outcome_date")
        _require_text(self.label_source, "label_source")

    def to_dict(self) -> dict[str, object]:
        return {
            "cohort_id": self.cohort_id,
            "snapshot_id": self.snapshot_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "entity_label": self.entity_label,
            "label_name": self.label_name,
            "label_value": self.label_value,
            "horizon": self.horizon,
            "outcome_date": self.outcome_date,
            "label_source": self.label_source,
            "label_notes": self.label_notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkCohortLabel:
        return cls(
            cohort_id=str(payload["cohort_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            entity_type=str(payload["entity_type"]),
            entity_id=str(payload["entity_id"]),
            entity_label=str(payload["entity_label"]),
            label_name=str(payload["label_name"]),
            label_value=str(payload["label_value"]),
            horizon=str(payload["horizon"]),
            outcome_date=str(payload.get("outcome_date", "")),
            label_source=str(payload["label_source"]),
            label_notes=str(payload.get("label_notes", "")),
        )


def load_cohort_members(path: Path) -> tuple[CohortMember, ...]:
    members = tuple(CohortMember.from_dict(row) for row in read_csv_rows(path))
    seen = set()
    for member in members:
        key = (member.entity_type, member.entity_id)
        if key in seen:
            raise ValueError("cohort members must not repeat entity_type/entity_id")
        seen.add(key)
    return members


def load_future_outcomes(path: Path) -> tuple[FutureOutcomeRecord, ...]:
    return tuple(FutureOutcomeRecord.from_dict(row) for row in read_csv_rows(path))


def write_benchmark_cohort_labels(
    path: Path,
    labels: tuple[BenchmarkCohortLabel, ...],
) -> None:
    write_csv(
        path,
        [label.to_dict() for label in labels],
        BENCHMARK_COHORT_LABEL_FIELDNAMES,
    )


def read_benchmark_cohort_labels(path: Path) -> tuple[BenchmarkCohortLabel, ...]:
    return tuple(BenchmarkCohortLabel.from_dict(row) for row in read_csv_rows(path))


def build_benchmark_cohort_labels(
    manifest: BenchmarkSnapshotManifest,
    cohort_members: tuple[CohortMember, ...],
    future_outcomes: tuple[FutureOutcomeRecord, ...],
) -> tuple[BenchmarkCohortLabel, ...]:
    as_of_date = _parse_iso_date(manifest.as_of_date, "as_of_date")
    outcome_closed_at = _parse_iso_date(
        manifest.outcome_observation_closed_at,
        "outcome_observation_closed_at",
    )
    snapshot_entity_types = set(manifest.entity_types)
    member_keys = {
        (member.entity_type, member.entity_id)
        for member in cohort_members
    }
    horizon_cutoffs = {
        horizon: min(
            _add_years(as_of_date, _parse_horizon_years(horizon)),
            outcome_closed_at,
        )
        for horizon in BENCHMARK_QUESTION_V1.evaluation_horizons
    }
    grouped_outcomes: dict[tuple[str, str], list[FutureOutcomeRecord]] = {}
    for outcome in future_outcomes:
        if outcome.entity_type not in snapshot_entity_types:
            raise ValueError(
                f"future outcome {outcome.entity_type}/{outcome.entity_id} is outside the snapshot entity_types"
            )
        if (outcome.entity_type, outcome.entity_id) not in member_keys:
            raise ValueError(
                f"future outcome {outcome.entity_type}/{outcome.entity_id} does not match any cohort member"
            )
        grouped_outcomes.setdefault((outcome.entity_type, outcome.entity_id), []).append(outcome)

    labels: list[BenchmarkCohortLabel] = []
    for member in sorted(
        cohort_members,
        key=lambda item: (item.entity_type, item.entity_id, item.entity_label.lower()),
    ):
        if member.entity_type not in snapshot_entity_types:
            raise ValueError(
                f"cohort member {member.entity_type}/{member.entity_id} is outside the snapshot entity_types"
            )
        member_outcomes = sorted(
            grouped_outcomes.get((member.entity_type, member.entity_id), []),
            key=lambda item: (item.outcome_date, item.outcome_label, item.label_source),
        )
        for horizon in BENCHMARK_QUESTION_V1.evaluation_horizons:
            horizon_cutoff = horizon_cutoffs[horizon]
            qualifying_outcomes = [
                outcome
                for outcome in member_outcomes
                if as_of_date
                < _parse_iso_date(outcome.outcome_date, "outcome_date")
                <= horizon_cutoff
            ]
            outcomes_by_label: dict[str, list[FutureOutcomeRecord]] = {}
            for outcome in qualifying_outcomes:
                outcomes_by_label.setdefault(outcome.outcome_label, []).append(outcome)

            for label_name in BENCHMARK_QUESTION_V1.translational_outcome_labels:
                if label_name == NO_OUTCOME_LABEL:
                    label_is_observed = not qualifying_outcomes
                    outcome_date = ""
                    label_source = (
                        "benchmark_label_builder"
                        if label_is_observed
                        else "qualifying_outcome_observed"
                    )
                    label_notes = (
                        f"no qualifying future outcome observed through {horizon_cutoff.isoformat()}"
                        if label_is_observed
                        else ""
                    )
                else:
                    matched_outcomes = outcomes_by_label.get(label_name, [])
                    label_is_observed = bool(matched_outcomes)
                    outcome_date = (
                        matched_outcomes[0].outcome_date
                        if matched_outcomes
                        else ""
                    )
                    label_source = (
                        "; ".join(
                            sorted({outcome.label_source for outcome in matched_outcomes})
                        )
                        if matched_outcomes
                        else "not_observed_within_horizon"
                    )
                    observed_dates = ",".join(
                        outcome.outcome_date for outcome in matched_outcomes
                    )
                    observed_notes = "; ".join(
                        note
                        for note in sorted(
                            {
                                outcome.label_notes
                                for outcome in matched_outcomes
                                if outcome.label_notes
                            }
                        )
                    )
                    label_notes = ""
                    if observed_dates:
                        label_notes = f"observed_dates={observed_dates}"
                    if observed_notes:
                        label_notes = (
                            f"{label_notes}; {observed_notes}"
                            if label_notes
                            else observed_notes
                        )
                labels.append(
                    BenchmarkCohortLabel(
                        cohort_id=manifest.cohort_id,
                        snapshot_id=manifest.snapshot_id,
                        entity_type=member.entity_type,
                        entity_id=member.entity_id,
                        entity_label=member.entity_label,
                        label_name=label_name,
                        label_value=(
                            OBSERVED_LABEL_VALUE
                            if label_is_observed
                            else NOT_OBSERVED_LABEL_VALUE
                        ),
                        horizon=horizon,
                        outcome_date=outcome_date,
                        label_source=label_source,
                        label_notes=label_notes,
                    )
                )
    return tuple(labels)


def materialize_benchmark_cohort_labels(
    *,
    manifest: BenchmarkSnapshotManifest,
    cohort_members_file: Path,
    future_outcomes_file: Path,
    output_file: Path,
) -> dict[str, object]:
    labels = build_benchmark_cohort_labels(
        manifest,
        load_cohort_members(cohort_members_file),
        load_future_outcomes(future_outcomes_file),
    )
    write_benchmark_cohort_labels(output_file, labels)
    observed_label_rows = sum(
        label.label_value == OBSERVED_LABEL_VALUE for label in labels
    )
    return {
        "snapshot_id": manifest.snapshot_id,
        "cohort_id": manifest.cohort_id,
        "output_file": str(output_file),
        "row_count": len(labels),
        "observed_label_rows": observed_label_rows,
    }
