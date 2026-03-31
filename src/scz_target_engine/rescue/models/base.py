from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _format_entity_id_sample(
    entity_ids: tuple[str, ...],
    *,
    limit: int = 5,
) -> str:
    if not entity_ids:
        return ""
    sample = ", ".join(entity_ids[:limit])
    if len(entity_ids) > limit:
        return sample + ", ..."
    return sample


def _duplicate_entity_ids(entity_ids: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for entity_id in entity_ids:
        if entity_id in seen and entity_id not in duplicates:
            duplicates.append(entity_id)
            continue
        seen.add(entity_id)
    return tuple(duplicates)


@dataclass(frozen=True)
class RescueModelInput:
    task_id: str
    ranking_dataset_id: str
    ranking_rows: tuple[dict[str, str], ...]
    ranking_columns: tuple[str, ...]
    entity_id_field: str = "gene_id"
    task_label: str = ""
    axis_id: str = ""
    principal_split: str = ""

    def __post_init__(self) -> None:
        _require_text(self.task_id, "task_id")
        _require_text(self.ranking_dataset_id, "ranking_dataset_id")
        _require_text(self.entity_id_field, "entity_id_field")
        if not self.ranking_rows:
            raise ValueError("ranking_rows must not be empty")
        if self.entity_id_field not in self.ranking_columns:
            raise ValueError(
                "entity_id_field must exist in ranking_columns: "
                f"{self.entity_id_field}"
            )
        entity_ids = [
            row.get(self.entity_id_field, "").strip()
            for row in self.ranking_rows
        ]
        if any(not entity_id for entity_id in entity_ids):
            raise ValueError(
                "ranking_rows must populate entity_id_field for every row"
            )
        if len(entity_ids) != len(set(entity_ids)):
            raise ValueError(
                "ranking_rows must not repeat entity ids for model evaluation"
            )

    @property
    def entity_ids(self) -> tuple[str, ...]:
        return tuple(row[self.entity_id_field] for row in self.ranking_rows)

    def require_columns(self, field_names: tuple[str, ...]) -> None:
        missing = sorted(
            field_name
            for field_name in field_names
            if field_name not in self.ranking_columns
        )
        if missing:
            raise ValueError(
                "frozen ranking input is missing required model columns: "
                + ", ".join(missing)
            )

    def validate_ranked_entity_ids(
        self,
        ranked_entity_ids: tuple[str, ...],
        *,
        model_id: str,
    ) -> tuple[str, ...]:
        resolved_model_id = _require_text(model_id, "model_id")
        normalized_entity_ids = tuple(
            _require_text(entity_id, self.entity_id_field)
            for entity_id in ranked_entity_ids
        )
        expected_entity_ids = self.entity_ids
        expected_entity_id_set = set(expected_entity_ids)
        normalized_entity_id_set = set(normalized_entity_ids)
        missing_entity_ids = tuple(
            entity_id
            for entity_id in expected_entity_ids
            if entity_id not in normalized_entity_id_set
        )
        duplicate_entity_ids = _duplicate_entity_ids(normalized_entity_ids)
        unknown_entity_ids = tuple(
            sorted(
                entity_id
                for entity_id in normalized_entity_id_set
                if entity_id not in expected_entity_id_set
            )
        )

        if (
            len(normalized_entity_ids) != len(expected_entity_ids)
            or missing_entity_ids
            or duplicate_entity_ids
            or unknown_entity_ids
        ):
            problems = [
                (
                    f"expected {len(expected_entity_ids)} {self.entity_id_field} values "
                    f"but found {len(normalized_entity_ids)}"
                )
            ]
            if missing_entity_ids:
                problems.append(
                    "missing ids: "
                    + _format_entity_id_sample(missing_entity_ids)
                )
            if duplicate_entity_ids:
                problems.append(
                    "duplicate ids: "
                    + _format_entity_id_sample(duplicate_entity_ids)
                )
            if unknown_entity_ids:
                problems.append(
                    "unknown ids: "
                    + _format_entity_id_sample(unknown_entity_ids)
                )
            raise ValueError(
                f"{resolved_model_id} ranking must contain every governed "
                "candidate exactly once; "
                + "; ".join(problems)
            )
        return normalized_entity_ids


@dataclass(frozen=True)
class RescueModelDefinition:
    model_id: str
    task_id: str
    label: str
    description: str
    leakage_rule: str
    input_fields: tuple[str, ...]
    admission_metric_names: tuple[str, ...]
    principal_split: str
    tie_break_input_fields: tuple[str, ...] = ()
    stage: str = "shipped"

    def __post_init__(self) -> None:
        _require_text(self.model_id, "model_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.label, "label")
        _require_text(self.description, "description")
        _require_text(self.leakage_rule, "leakage_rule")
        _require_text(self.principal_split, "principal_split")
        if self.stage not in {"shipped", "candidate"}:
            raise ValueError("stage must be shipped or candidate")
        if not self.input_fields:
            raise ValueError("input_fields must contain at least one field")
        if not self.admission_metric_names:
            raise ValueError(
                "admission_metric_names must contain at least one metric"
            )
        for field_name in self.input_fields:
            _require_text(field_name, "input_fields")
        for field_name in self.tie_break_input_fields:
            _require_text(field_name, "tie_break_input_fields")
        for metric_name in self.admission_metric_names:
            _require_text(metric_name, "admission_metric_names")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "model_id": self.model_id,
            "task_id": self.task_id,
            "label": self.label,
            "description": self.description,
            "leakage_rule": self.leakage_rule,
            "input_fields": list(self.input_fields),
            "admission_metric_names": list(self.admission_metric_names),
            "principal_split": self.principal_split,
            "stage": self.stage,
        }
        if self.tie_break_input_fields:
            payload["tie_break_input_fields"] = list(
                self.tie_break_input_fields
            )
        return payload

    def to_scorer_definition(self) -> dict[str, object]:
        payload = {
            "comparison_id": self.model_id,
            "scorer_id": self.model_id,
            "scorer_label": self.label,
            "scorer_role": "model",
            "description": self.description,
            "leakage_rule": self.leakage_rule,
            "input_fields": list(self.input_fields),
            "admission_metric_names": list(self.admission_metric_names),
            "principal_split": self.principal_split,
            "stage": self.stage,
        }
        if self.tie_break_input_fields:
            payload["tie_break_input_fields"] = list(
                self.tie_break_input_fields
            )
        return payload


class RescueModelPlugin(Protocol):
    @property
    def definition(self) -> RescueModelDefinition: ...

    def rank_entities(
        self,
        model_input: RescueModelInput,
    ) -> tuple[str, ...]: ...


__all__ = [
    "RescueModelDefinition",
    "RescueModelInput",
    "RescueModelPlugin",
]
