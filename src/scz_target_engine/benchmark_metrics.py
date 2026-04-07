from __future__ import annotations

from dataclasses import dataclass
from math import ceil, floor
from pathlib import Path
import random
from typing import Any

from scz_target_engine.benchmark_labels import (
    BenchmarkCohortLabel,
    OBSERVED_LABEL_VALUE,
)
from scz_target_engine.io import read_json, write_json


METRIC_PAYLOAD_SCHEMA_NAME = "benchmark_metric_output_payload"
METRIC_PAYLOAD_SCHEMA_VERSION = "v1"
INTERVAL_PAYLOAD_SCHEMA_NAME = "benchmark_confidence_interval_payload"
INTERVAL_PAYLOAD_SCHEMA_VERSION = "v1"

POSITIVE_OUTCOME_LABELS = (
    "future_schizophrenia_program_started",
    "future_schizophrenia_program_advanced",
    "future_schizophrenia_positive_signal",
)
POSITIVE_OUTCOME_LABEL_SET = set(POSITIVE_OUTCOME_LABELS)

RETRIEVAL_METRIC_NAMES = (
    "average_precision_any_positive_outcome",
    "mean_reciprocal_rank_any_positive_outcome",
    "precision_at_1_any_positive_outcome",
    "precision_at_3_any_positive_outcome",
    "precision_at_5_any_positive_outcome",
    "recall_at_1_any_positive_outcome",
    "recall_at_3_any_positive_outcome",
    "recall_at_5_any_positive_outcome",
)

DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL = 0.95
DEFAULT_BOOTSTRAP_ITERATIONS = 1000
DETERMINISTIC_TEST_BOOTSTRAP_ITERATIONS = 100
BOOTSTRAP_INTERVAL_METHOD = "percentile_bootstrap"
BOOTSTRAP_RESAMPLE_UNIT = "entity"


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _round_metric(value: float) -> float:
    return round(value, 6)


@dataclass(frozen=True)
class RankedEvaluationRow:
    entity_id: str
    relevant: bool


@dataclass(frozen=True)
class BenchmarkMetricOutputPayload:
    run_id: str
    snapshot_id: str
    baseline_id: str
    entity_type: str
    horizon: str
    metric_name: str
    metric_value: float
    cohort_size: int
    metric_unit: str = "fraction"
    notes: str = ""
    schema_name: str = METRIC_PAYLOAD_SCHEMA_NAME
    schema_version: str = METRIC_PAYLOAD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.baseline_id, "baseline_id")
        _require_text(self.entity_type, "entity_type")
        _require_text(self.horizon, "horizon")
        _require_text(self.metric_name, "metric_name")
        _require_text(self.metric_unit, "metric_unit")
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")
        if self.schema_name != METRIC_PAYLOAD_SCHEMA_NAME:
            raise ValueError(
                f"{METRIC_PAYLOAD_SCHEMA_NAME} schema_name must be "
                f"{METRIC_PAYLOAD_SCHEMA_NAME}"
            )
        if self.schema_version != METRIC_PAYLOAD_SCHEMA_VERSION:
            raise ValueError(
                f"{METRIC_PAYLOAD_SCHEMA_NAME} schema_version must be "
                f"{METRIC_PAYLOAD_SCHEMA_VERSION}"
            )
        if self.cohort_size < 0:
            raise ValueError("cohort_size must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "baseline_id": self.baseline_id,
            "entity_type": self.entity_type,
            "horizon": self.horizon,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "metric_unit": self.metric_unit,
            "cohort_size": self.cohort_size,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkMetricOutputPayload:
        metric_unit = payload.get("metric_unit")
        if metric_unit is None:
            raise ValueError(f"{METRIC_PAYLOAD_SCHEMA_NAME} metric_unit is required")
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            run_id=str(payload["run_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            baseline_id=str(payload["baseline_id"]),
            entity_type=str(payload["entity_type"]),
            horizon=str(payload["horizon"]),
            metric_name=str(payload["metric_name"]),
            metric_value=float(payload["metric_value"]),
            metric_unit=str(metric_unit),
            cohort_size=int(payload["cohort_size"]),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class BenchmarkConfidenceIntervalPayload:
    run_id: str
    snapshot_id: str
    baseline_id: str
    entity_type: str
    horizon: str
    metric_name: str
    point_estimate: float
    interval_low: float
    interval_high: float
    confidence_level: float
    bootstrap_iterations: int
    resample_unit: str
    random_seed: int | None = None
    notes: str = ""
    schema_name: str = INTERVAL_PAYLOAD_SCHEMA_NAME
    schema_version: str = INTERVAL_PAYLOAD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        _require_text(self.snapshot_id, "snapshot_id")
        _require_text(self.baseline_id, "baseline_id")
        _require_text(self.entity_type, "entity_type")
        _require_text(self.horizon, "horizon")
        _require_text(self.metric_name, "metric_name")
        _require_text(self.resample_unit, "resample_unit")
        _require_text(self.schema_name, "schema_name")
        _require_text(self.schema_version, "schema_version")
        if self.schema_name != INTERVAL_PAYLOAD_SCHEMA_NAME:
            raise ValueError(
                f"{INTERVAL_PAYLOAD_SCHEMA_NAME} schema_name must be "
                f"{INTERVAL_PAYLOAD_SCHEMA_NAME}"
            )
        if self.schema_version != INTERVAL_PAYLOAD_SCHEMA_VERSION:
            raise ValueError(
                f"{INTERVAL_PAYLOAD_SCHEMA_NAME} schema_version must be "
                f"{INTERVAL_PAYLOAD_SCHEMA_VERSION}"
            )
        if self.bootstrap_iterations <= 0:
            raise ValueError("bootstrap_iterations must be positive")
        if self.interval_low > self.interval_high:
            raise ValueError("interval_low cannot exceed interval_high")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "baseline_id": self.baseline_id,
            "entity_type": self.entity_type,
            "horizon": self.horizon,
            "metric_name": self.metric_name,
            "point_estimate": self.point_estimate,
            "interval_low": self.interval_low,
            "interval_high": self.interval_high,
            "confidence_level": self.confidence_level,
            "bootstrap_iterations": self.bootstrap_iterations,
            "resample_unit": self.resample_unit,
            "random_seed": self.random_seed,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
    ) -> BenchmarkConfidenceIntervalPayload:
        random_seed = payload.get("random_seed")
        return cls(
            schema_name=str(payload["schema_name"]),
            schema_version=str(payload["schema_version"]),
            run_id=str(payload["run_id"]),
            snapshot_id=str(payload["snapshot_id"]),
            baseline_id=str(payload["baseline_id"]),
            entity_type=str(payload["entity_type"]),
            horizon=str(payload["horizon"]),
            metric_name=str(payload["metric_name"]),
            point_estimate=float(payload["point_estimate"]),
            interval_low=float(payload["interval_low"]),
            interval_high=float(payload["interval_high"]),
            confidence_level=float(payload["confidence_level"]),
            bootstrap_iterations=int(payload["bootstrap_iterations"]),
            resample_unit=str(payload["resample_unit"]),
            random_seed=None if random_seed in {None, ""} else int(random_seed),
            notes=str(payload.get("notes", "")),
        )


def write_benchmark_metric_output_payload(
    path: Path,
    payload: BenchmarkMetricOutputPayload,
) -> None:
    write_json(path, payload.to_dict())


def read_benchmark_metric_output_payload(path: Path) -> BenchmarkMetricOutputPayload:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("benchmark metric output payload must be a JSON object")
    return BenchmarkMetricOutputPayload.from_dict(payload)


def write_benchmark_confidence_interval_payload(
    path: Path,
    payload: BenchmarkConfidenceIntervalPayload,
) -> None:
    write_json(path, payload.to_dict())


def read_benchmark_confidence_interval_payload(
    path: Path,
) -> BenchmarkConfidenceIntervalPayload:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError("benchmark confidence interval payload must be a JSON object")
    return BenchmarkConfidenceIntervalPayload.from_dict(payload)


def build_positive_relevance_index(
    labels: tuple[BenchmarkCohortLabel, ...],
    *,
    entity_type: str,
    horizon: str,
) -> dict[str, bool]:
    relevance: dict[str, bool] = {}
    for label in labels:
        if label.entity_type != entity_type or label.horizon != horizon:
            continue
        relevance.setdefault(label.entity_id, False)
        if (
            label.label_name in POSITIVE_OUTCOME_LABEL_SET
            and label.label_value == OBSERVED_LABEL_VALUE
        ):
            relevance[label.entity_id] = True
    return relevance


def build_ranked_evaluation_rows(
    admissible_entity_ids: tuple[str, ...],
    ranked_entity_ids: tuple[str, ...],
    relevance_index: dict[str, bool],
) -> tuple[RankedEvaluationRow, ...]:
    admissible_entity_id_set = set(admissible_entity_ids)
    seen_entity_ids: set[str] = set()
    ordered_entity_ids: list[str] = []

    for entity_id in ranked_entity_ids:
        if entity_id not in admissible_entity_id_set or entity_id in seen_entity_ids:
            continue
        seen_entity_ids.add(entity_id)
        ordered_entity_ids.append(entity_id)

    for entity_id in admissible_entity_ids:
        if entity_id in seen_entity_ids:
            continue
        seen_entity_ids.add(entity_id)
        ordered_entity_ids.append(entity_id)

    return tuple(
        RankedEvaluationRow(
            entity_id=entity_id,
            relevant=relevance_index.get(entity_id, False),
        )
        for entity_id in ordered_entity_ids
    )


def count_relevant(rows: tuple[RankedEvaluationRow, ...]) -> int:
    return sum(1 for row in rows if row.relevant)


def _average_precision(rows: tuple[RankedEvaluationRow, ...]) -> float:
    relevant_total = count_relevant(rows)
    if relevant_total == 0:
        return 0.0
    hit_count = 0
    precision_sum = 0.0
    for index, row in enumerate(rows, start=1):
        if not row.relevant:
            continue
        hit_count += 1
        precision_sum += hit_count / index
    return precision_sum / relevant_total


def _mean_reciprocal_rank(rows: tuple[RankedEvaluationRow, ...]) -> float:
    for index, row in enumerate(rows, start=1):
        if row.relevant:
            return 1.0 / index
    return 0.0


def _precision_at_k(rows: tuple[RankedEvaluationRow, ...], *, k: int) -> float:
    if not rows:
        return 0.0
    window = rows[:k]
    denominator = min(k, len(rows))
    if denominator == 0:
        return 0.0
    return sum(1 for row in window if row.relevant) / denominator


def _recall_at_k(rows: tuple[RankedEvaluationRow, ...], *, k: int) -> float:
    relevant_total = count_relevant(rows)
    if relevant_total == 0:
        return 0.0
    window = rows[:k]
    return sum(1 for row in window if row.relevant) / relevant_total


def calculate_metric_values(
    rows: tuple[RankedEvaluationRow, ...],
) -> dict[str, float]:
    return {
        "average_precision_any_positive_outcome": _round_metric(
            _average_precision(rows)
        ),
        "mean_reciprocal_rank_any_positive_outcome": _round_metric(
            _mean_reciprocal_rank(rows)
        ),
        "precision_at_1_any_positive_outcome": _round_metric(
            _precision_at_k(rows, k=1)
        ),
        "precision_at_3_any_positive_outcome": _round_metric(
            _precision_at_k(rows, k=3)
        ),
        "precision_at_5_any_positive_outcome": _round_metric(
            _precision_at_k(rows, k=5)
        ),
        "recall_at_1_any_positive_outcome": _round_metric(
            _recall_at_k(rows, k=1)
        ),
        "recall_at_3_any_positive_outcome": _round_metric(
            _recall_at_k(rows, k=3)
        ),
        "recall_at_5_any_positive_outcome": _round_metric(
            _recall_at_k(rows, k=5)
        ),
    }


def _percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = quantile * (len(sorted_values) - 1)
    lower_index = floor(position)
    upper_index = ceil(position)
    if lower_index == upper_index:
        return sorted_values[lower_index]
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    fraction = position - lower_index
    return lower_value + ((upper_value - lower_value) * fraction)


def _resample_rows_preserving_rank_order(
    rows: tuple[RankedEvaluationRow, ...],
    rng: random.Random,
) -> tuple[RankedEvaluationRow, ...]:
    sampled_counts = [0] * len(rows)
    for _ in range(len(rows)):
        sampled_counts[rng.randrange(len(rows))] += 1
    return tuple(
        row
        for row, sampled_count in zip(rows, sampled_counts)
        for _ in range(sampled_count)
    )


def estimate_bootstrap_intervals(
    rows: tuple[RankedEvaluationRow, ...],
    *,
    iterations: int,
    confidence_level: float = DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL,
    random_seed: int,
) -> dict[str, tuple[float, float, float]]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    metric_values = calculate_metric_values(rows)
    if not rows:
        return {
            metric_name: (metric_value, metric_value, metric_value)
            for metric_name, metric_value in metric_values.items()
        }

    rng = random.Random(random_seed)
    samples: dict[str, list[float]] = {
        metric_name: []
        for metric_name in metric_values
    }
    for _ in range(iterations):
        resampled_rows = _resample_rows_preserving_rank_order(rows, rng)
        resampled_metrics = calculate_metric_values(resampled_rows)
        for metric_name, metric_value in resampled_metrics.items():
            samples[metric_name].append(metric_value)

    alpha = (1.0 - confidence_level) / 2.0
    results: dict[str, tuple[float, float, float]] = {}
    for metric_name, point_estimate in metric_values.items():
        sorted_values = sorted(samples[metric_name])
        interval_low = _round_metric(_percentile(sorted_values, alpha))
        interval_high = _round_metric(_percentile(sorted_values, 1.0 - alpha))
        results[metric_name] = (point_estimate, interval_low, interval_high)
    return results


__all__ = [
    "BOOTSTRAP_INTERVAL_METHOD",
    "BOOTSTRAP_RESAMPLE_UNIT",
    "BenchmarkConfidenceIntervalPayload",
    "BenchmarkMetricOutputPayload",
    "DEFAULT_BOOTSTRAP_CONFIDENCE_LEVEL",
    "DEFAULT_BOOTSTRAP_ITERATIONS",
    "DETERMINISTIC_TEST_BOOTSTRAP_ITERATIONS",
    "INTERVAL_PAYLOAD_SCHEMA_NAME",
    "INTERVAL_PAYLOAD_SCHEMA_VERSION",
    "METRIC_PAYLOAD_SCHEMA_NAME",
    "METRIC_PAYLOAD_SCHEMA_VERSION",
    "POSITIVE_OUTCOME_LABELS",
    "RETRIEVAL_METRIC_NAMES",
    "build_positive_relevance_index",
    "build_ranked_evaluation_rows",
    "calculate_metric_values",
    "count_relevant",
    "estimate_bootstrap_intervals",
    "read_benchmark_confidence_interval_payload",
    "read_benchmark_metric_output_payload",
    "write_benchmark_confidence_interval_payload",
    "write_benchmark_metric_output_payload",
]
