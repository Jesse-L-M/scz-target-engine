from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from scz_target_engine.io import write_csv, write_json


_BASE_FIELDNAMES = [
    "task_id",
    "task_label",
    "axis_id",
    "evaluation_split",
    "scorer_id",
    "scorer_label",
    "scorer_role",
    "candidate_count",
    "positive_count",
]
_LOWER_IS_BETTER_METRIC_NAMES = {"first_positive_rank"}


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


@dataclass(frozen=True)
class RescueBaselineDefinition:
    baseline_id: str
    label: str
    description: str
    leakage_rule: str
    scorer_role: str = "baseline"

    def __post_init__(self) -> None:
        _require_text(self.baseline_id, "baseline_id")
        _require_text(self.label, "label")
        _require_text(self.description, "description")
        _require_text(self.leakage_rule, "leakage_rule")
        if self.scorer_role not in {"baseline", "model"}:
            raise ValueError("scorer_role must be baseline or model")

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "label": self.label,
            "description": self.description,
            "leakage_rule": self.leakage_rule,
            "scorer_role": self.scorer_role,
        }


@dataclass(frozen=True)
class RescueComparisonRow:
    task_id: str
    task_label: str
    evaluation_split: str
    scorer_id: str
    scorer_label: str
    scorer_role: str
    candidate_count: int
    positive_count: int
    metrics: Mapping[str, float | int | None]
    axis_id: str = ""

    def __post_init__(self) -> None:
        _require_text(self.task_id, "task_id")
        _require_text(self.task_label, "task_label")
        _require_text(self.evaluation_split, "evaluation_split")
        _require_text(self.scorer_id, "scorer_id")
        _require_text(self.scorer_label, "scorer_label")
        if self.scorer_role not in {"baseline", "model"}:
            raise ValueError("scorer_role must be baseline or model")
        if self.candidate_count < 0:
            raise ValueError("candidate_count must be non-negative")
        if self.positive_count < 0:
            raise ValueError("positive_count must be non-negative")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "task_id": self.task_id,
            "task_label": self.task_label,
            "axis_id": self.axis_id,
            "evaluation_split": self.evaluation_split,
            "scorer_id": self.scorer_id,
            "scorer_label": self.scorer_label,
            "scorer_role": self.scorer_role,
            "candidate_count": self.candidate_count,
            "positive_count": self.positive_count,
        }
        payload.update(self.metrics)
        return payload


def _metric_names(
    comparison_rows: tuple[RescueComparisonRow, ...],
) -> list[str]:
    return sorted(
        {
            metric_name
            for row in comparison_rows
            for metric_name in row.metrics
        }
    )


def comparison_rows_to_dicts(
    comparison_rows: tuple[RescueComparisonRow, ...],
) -> tuple[list[dict[str, object]], list[str]]:
    metric_names = _metric_names(comparison_rows)
    return (
        [row.to_dict() for row in comparison_rows],
        _BASE_FIELDNAMES + metric_names,
    )


def write_rescue_comparison_rows(
    path: Path,
    comparison_rows: tuple[RescueComparisonRow, ...],
) -> None:
    serialized_rows, fieldnames = comparison_rows_to_dicts(comparison_rows)
    write_csv(path, serialized_rows, fieldnames=fieldnames)


def _metric_sort_key(
    row: RescueComparisonRow,
    *,
    metric_name: str,
) -> tuple[float, str]:
    raw_value = row.metrics.get(metric_name)
    if raw_value is None:
        fallback = float("inf") if metric_name in _LOWER_IS_BETTER_METRIC_NAMES else float(
            "-inf"
        )
        return (fallback, row.scorer_id)
    numeric_value = float(raw_value)
    if metric_name in _LOWER_IS_BETTER_METRIC_NAMES:
        return (numeric_value, row.scorer_id)
    return (-numeric_value, row.scorer_id)


def build_rescue_comparison_summary(
    *,
    task_id: str,
    task_label: str,
    principal_split: str,
    comparison_rows: tuple[RescueComparisonRow, ...],
    scorer_definitions: tuple[dict[str, object], ...],
    axis_id: str = "",
    notes: str = "",
) -> dict[str, object]:
    metric_names = _metric_names(comparison_rows)
    best_by_split: dict[str, dict[str, object]] = {}
    for split_name in sorted({row.evaluation_split for row in comparison_rows}):
        split_rows = tuple(
            row for row in comparison_rows if row.evaluation_split == split_name
        )
        split_best: dict[str, object] = {}
        for metric_name in metric_names:
            metric_rows = tuple(
                row
                for row in split_rows
                if row.metrics.get(metric_name) is not None
            )
            if not metric_rows:
                continue
            best_row = min(
                metric_rows,
                key=lambda row: _metric_sort_key(row, metric_name=metric_name),
            )
            split_best[metric_name] = {
                "scorer_id": best_row.scorer_id,
                "scorer_label": best_row.scorer_label,
                "scorer_role": best_row.scorer_role,
                "metric_value": best_row.metrics[metric_name],
            }
        best_by_split[split_name] = split_best

    summary: dict[str, object] = {
        "task_id": task_id,
        "task_label": task_label,
        "principal_split": principal_split,
        "metric_names": metric_names,
        "comparison_row_count": len(comparison_rows),
        "scorers": list(scorer_definitions),
        "comparison_rows": [row.to_dict() for row in comparison_rows],
        "best_by_split": best_by_split,
        "notes": notes,
    }
    if axis_id:
        summary["axis_id"] = axis_id
    return summary


def materialize_rescue_comparison_report(
    output_dir: Path,
    *,
    task_id: str,
    task_label: str,
    principal_split: str,
    comparison_rows: tuple[RescueComparisonRow, ...],
    scorer_definitions: tuple[dict[str, object], ...],
    axis_id: str = "",
    notes: str = "",
    rows_file_name: str = "baseline_comparison_rows.csv",
    summary_file_name: str = "baseline_comparison_summary.json",
) -> dict[str, object]:
    rows_file = output_dir.resolve() / rows_file_name
    summary_file = output_dir.resolve() / summary_file_name
    write_rescue_comparison_rows(rows_file, comparison_rows)
    summary_payload = build_rescue_comparison_summary(
        task_id=task_id,
        task_label=task_label,
        principal_split=principal_split,
        comparison_rows=comparison_rows,
        scorer_definitions=scorer_definitions,
        axis_id=axis_id,
        notes=notes,
    )
    write_json(summary_file, summary_payload)
    return {
        "comparison_rows_file": str(rows_file),
        "comparison_summary_file": str(summary_file),
        "comparison_row_count": len(comparison_rows),
    }


__all__ = [
    "RescueBaselineDefinition",
    "RescueComparisonRow",
    "build_rescue_comparison_summary",
    "comparison_rows_to_dicts",
    "materialize_rescue_comparison_report",
    "write_rescue_comparison_rows",
]
