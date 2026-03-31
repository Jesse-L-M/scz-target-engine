from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from scz_target_engine.rescue.baselines.reporting import RescueComparisonRow
from scz_target_engine.rescue.models.base import RescueModelDefinition


_LOWER_IS_BETTER_METRIC_NAMES = {"first_positive_rank"}


def _metric_sort_value(
    value: float | int | None,
    *,
    metric_name: str,
) -> float:
    if value is None:
        if metric_name in _LOWER_IS_BETTER_METRIC_NAMES:
            return float("inf")
        return float("-inf")
    numeric_value = float(value)
    if metric_name in _LOWER_IS_BETTER_METRIC_NAMES:
        return -numeric_value
    return numeric_value


def _metric_value_beats(
    model_value: float | int | None,
    baseline_value: float | int | None,
    *,
    metric_name: str,
) -> bool:
    if model_value is None or baseline_value is None:
        return False
    if metric_name in _LOWER_IS_BETTER_METRIC_NAMES:
        return float(model_value) < float(baseline_value)
    return float(model_value) > float(baseline_value)


@dataclass(frozen=True)
class RescueModelAdmissionDecision:
    model_id: str
    model_label: str
    principal_split: str
    admission_metric_names: tuple[str, ...]
    admitted: bool
    blocking_metric_names: tuple[str, ...]
    model_metrics: Mapping[str, float | int | None]
    best_baseline_by_metric: Mapping[str, dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return {
            "model_id": self.model_id,
            "model_label": self.model_label,
            "principal_split": self.principal_split,
            "admission_metric_names": list(self.admission_metric_names),
            "admitted": self.admitted,
            "blocking_metric_names": list(self.blocking_metric_names),
            "model_metrics": dict(self.model_metrics),
            "best_baseline_by_metric": {
                metric_name: dict(metric_payload)
                for metric_name, metric_payload in self.best_baseline_by_metric.items()
            },
        }


def build_rescue_model_admission_summary(
    *,
    comparison_rows: tuple[RescueComparisonRow, ...],
    model_definitions: tuple[RescueModelDefinition, ...],
    principal_split: str,
    baseline_scorer_ids: tuple[str, ...],
) -> dict[str, object]:
    split_rows = tuple(
        row
        for row in comparison_rows
        if row.evaluation_split == principal_split
    )
    baseline_rows = tuple(
        row
        for row in split_rows
        if row.scorer_role == "baseline" and row.scorer_id in baseline_scorer_ids
    )
    if baseline_scorer_ids and not baseline_rows:
        raise ValueError(
            "principal split comparison rows must include the declared baseline scorers"
        )

    decisions: list[RescueModelAdmissionDecision] = []
    for model_definition in model_definitions:
        model_row = next(
            (
                row
                for row in split_rows
                if row.scorer_role == "model"
                and row.scorer_id == model_definition.model_id
            ),
            None,
        )
        if model_row is None:
            raise ValueError(
                "principal split comparison rows must include model scorer "
                f"{model_definition.model_id}"
            )

        best_baseline_by_metric: dict[str, dict[str, object]] = {}
        blocking_metric_names: list[str] = []
        for metric_name in model_definition.admission_metric_names:
            candidate_rows = tuple(
                row
                for row in baseline_rows
                if row.metrics.get(metric_name) is not None
            )
            if not candidate_rows:
                raise ValueError(
                    "baseline comparison rows must expose metric "
                    f"{metric_name} on principal split {principal_split}"
                )
            best_baseline = max(
                candidate_rows,
                key=lambda row: (
                    _metric_sort_value(
                        row.metrics.get(metric_name),
                        metric_name=metric_name,
                    ),
                    row.scorer_id,
                ),
            )
            model_value = model_row.metrics.get(metric_name)
            baseline_value = best_baseline.metrics.get(metric_name)
            best_baseline_by_metric[metric_name] = {
                "baseline_id": best_baseline.scorer_id,
                "baseline_label": best_baseline.scorer_label,
                "metric_value": baseline_value,
                "model_metric_value": model_value,
                "model_beats_baseline": _metric_value_beats(
                    model_value,
                    baseline_value,
                    metric_name=metric_name,
                ),
            }
            if not best_baseline_by_metric[metric_name]["model_beats_baseline"]:
                blocking_metric_names.append(metric_name)

        decisions.append(
            RescueModelAdmissionDecision(
                model_id=model_definition.model_id,
                model_label=model_definition.label,
                principal_split=principal_split,
                admission_metric_names=model_definition.admission_metric_names,
                admitted=not blocking_metric_names,
                blocking_metric_names=tuple(blocking_metric_names),
                model_metrics={
                    metric_name: model_row.metrics.get(metric_name)
                    for metric_name in model_definition.admission_metric_names
                },
                best_baseline_by_metric=best_baseline_by_metric,
            )
        )

    return {
        "principal_split": principal_split,
        "baseline_scorer_ids": list(baseline_scorer_ids),
        "candidate_model_ids": [
            definition.model_id for definition in model_definitions
        ],
        "admitted_model_ids": [
            decision.model_id
            for decision in decisions
            if decision.admitted
        ],
        "decisions": [decision.to_dict() for decision in decisions],
    }


__all__ = [
    "RescueModelAdmissionDecision",
    "build_rescue_model_admission_summary",
]
