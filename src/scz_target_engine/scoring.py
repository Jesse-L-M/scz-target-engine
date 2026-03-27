from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from statistics import median


GENE_REQUIRED_GROUPS = (
    ("common_variant_support", "rare_variant_support"),
    ("cell_state_support", "developmental_regulatory_support"),
)

MODULE_REQUIRED_GROUPS = (
    ("member_gene_genetic_enrichment",),
    ("cell_state_specificity", "developmental_regulatory_relevance"),
)


@dataclass(frozen=True)
class EntityRecord:
    entity_type: str
    entity_id: str
    entity_label: str
    layer_values: dict[str, float | None]
    metadata: dict[str, str]


@dataclass(frozen=True)
class RankedEntity:
    entity_type: str
    entity_id: str
    entity_label: str
    composite_score: float | None
    eligible: bool
    rank: int | None
    decision_grade: bool
    sensitivity_survival_rate: float
    layer_values: dict[str, float | None]
    warnings: list[str]
    warning_severity: str
    metadata: dict[str, str]


@dataclass(frozen=True)
class StabilityResult:
    perturbation_overlaps: list[float]
    leave_one_out_overlaps: dict[str, float]
    leave_one_out_top10_ejections: dict[str, int]
    survival_rates: dict[str, float]
    pass_condition: bool


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return float(cleaned)


def parse_entity_rows(
    rows: list[dict[str, str]],
    entity_type: str,
    layer_names: list[str],
) -> list[EntityRecord]:
    records: list[EntityRecord] = []
    for row in rows:
        if not row.get("entity_id") or not row.get("entity_label"):
            raise ValueError(f"{entity_type} rows require entity_id and entity_label")
        layer_values = {name: parse_optional_float(row.get(name)) for name in layer_names}
        metadata = {
            key: value
            for key, value in row.items()
            if key not in {"entity_id", "entity_label", *layer_names}
        }
        records.append(
            EntityRecord(
                entity_type=entity_type,
                entity_id=row["entity_id"].strip(),
                entity_label=row["entity_label"].strip(),
                layer_values=layer_values,
                metadata=metadata,
            )
        )
    return records


def validate_layer_ranges(records: list[EntityRecord]) -> None:
    for record in records:
        for layer_name, value in record.layer_values.items():
            if value is None:
                continue
            if value < 0 or value > 1:
                raise ValueError(
                    f"{record.entity_type} {record.entity_id} layer {layer_name} "
                    f"must be in [0, 1], got {value}"
                )


def build_warning_index(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    index: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    required = {"entity_type", "entity_id", "severity", "warning_kind", "warning_text"}
    for row in rows:
        if not required.issubset(row):
            missing = sorted(required.difference(row))
            raise ValueError(f"warning rows missing required columns: {missing}")
        key = (row["entity_type"].strip(), row["entity_id"].strip())
        index[key].append(row)
    return index


def check_required_groups(
    layer_values: dict[str, float | None],
    required_groups: tuple[tuple[str, ...], ...],
) -> bool:
    return all(any(layer_values.get(name) is not None for name in group) for group in required_groups)


def compute_weighted_score(
    layer_values: dict[str, float | None],
    layer_weights: dict[str, float],
    required_groups: tuple[tuple[str, ...], ...],
) -> tuple[float | None, bool]:
    eligible = check_required_groups(layer_values, required_groups)
    if not eligible:
        return None, False

    numerator = 0.0
    denominator = 0.0
    for layer_name, weight in layer_weights.items():
        value = layer_values.get(layer_name)
        if value is None:
            continue
        numerator += value * weight
        denominator += weight

    if denominator == 0:
        return None, False

    return numerator / denominator, True


def rank_records(
    records: list[EntityRecord],
    layer_weights: dict[str, float],
    required_groups: tuple[tuple[str, ...], ...],
) -> list[dict[str, object]]:
    ranked: list[dict[str, object]] = []
    for record in records:
        score, eligible = compute_weighted_score(
            record.layer_values,
            layer_weights,
            required_groups,
        )
        ranked.append(
            {
                "entity_type": record.entity_type,
                "entity_id": record.entity_id,
                "entity_label": record.entity_label,
                "layer_values": record.layer_values,
                "metadata": record.metadata,
                "composite_score": score,
                "eligible": eligible,
            }
        )

    eligible_rows = [row for row in ranked if row["eligible"]]
    ineligible_rows = [row for row in ranked if not row["eligible"]]
    eligible_rows.sort(
        key=lambda row: (-float(row["composite_score"]), str(row["entity_label"]).lower())
    )
    ineligible_rows.sort(key=lambda row: str(row["entity_label"]).lower())

    for index, row in enumerate(eligible_rows, start=1):
        row["rank"] = index
    for row in ineligible_rows:
        row["rank"] = None

    return eligible_rows + ineligible_rows


def top_ids(rows: list[dict[str, object]], top_n: int) -> list[str]:
    eligible = [row["entity_id"] for row in rows if row["eligible"]]
    return eligible[: min(top_n, len(eligible))]


def build_weight_perturbations(
    layer_weights: dict[str, float],
    fraction: float,
) -> list[dict[str, float]]:
    perturbations: list[dict[str, float]] = []
    for layer_name in layer_weights:
        for direction in (-1, 1):
            updated = dict(layer_weights)
            updated[layer_name] = updated[layer_name] * (1 + (direction * fraction))
            total = sum(updated.values())
            perturbations.append({key: value / total for key, value in updated.items()})
    return perturbations


def build_leave_one_out_weights(layer_weights: dict[str, float]) -> dict[str, dict[str, float]]:
    leave_one_out: dict[str, dict[str, float]] = {}
    for omitted in layer_weights:
        remaining = {
            key: value
            for key, value in layer_weights.items()
            if key != omitted
        }
        total = sum(remaining.values())
        leave_one_out[omitted] = {key: value / total for key, value in remaining.items()}
    return leave_one_out


def top_overlap(reference_ids: list[str], candidate_ids: list[str]) -> float:
    if not reference_ids:
        return 0.0
    reference_set = set(reference_ids)
    candidate_set = set(candidate_ids)
    return len(reference_set.intersection(candidate_set)) / len(reference_ids)


def run_stability_analysis(
    records: list[EntityRecord],
    layer_weights: dict[str, float],
    required_groups: tuple[tuple[str, ...], ...],
    top_n: int,
    perturbation_fraction: float,
    decision_grade_threshold: float,
    top10_ejection_limit: float,
) -> StabilityResult:
    reference_rows = rank_records(records, layer_weights, required_groups)
    reference_top = top_ids(reference_rows, top_n)
    reference_top10 = reference_top[:10]

    survival_counts = defaultdict(int)
    sensitivity_runs = 0

    perturbation_overlaps: list[float] = []
    for weights in build_weight_perturbations(layer_weights, perturbation_fraction):
        candidate_top = top_ids(rank_records(records, weights, required_groups), top_n)
        perturbation_overlaps.append(top_overlap(reference_top, candidate_top))
        for entity_id in reference_top:
            if entity_id in candidate_top:
                survival_counts[entity_id] += 1
        sensitivity_runs += 1

    leave_one_out_overlaps: dict[str, float] = {}
    leave_one_out_top10_ejections: dict[str, int] = {}
    for omitted, weights in build_leave_one_out_weights(layer_weights).items():
        candidate_top = top_ids(rank_records(records, weights, required_groups), top_n)
        leave_one_out_overlaps[omitted] = top_overlap(reference_top, candidate_top)
        ejected_top10 = len(set(reference_top10).difference(candidate_top))
        leave_one_out_top10_ejections[omitted] = ejected_top10
        for entity_id in reference_top:
            if entity_id in candidate_top:
                survival_counts[entity_id] += 1
        sensitivity_runs += 1

    survival_rates = {
        entity_id: (survival_counts[entity_id] / sensitivity_runs if sensitivity_runs else 0.0)
        for entity_id in reference_top
    }

    median_overlap = median(perturbation_overlaps) if perturbation_overlaps else 0.0
    max_ejection_fraction = 0.0
    if reference_top10:
        max_ejection_fraction = max(
            count / len(reference_top10)
            for count in leave_one_out_top10_ejections.values()
        )

    return StabilityResult(
        perturbation_overlaps=perturbation_overlaps,
        leave_one_out_overlaps=leave_one_out_overlaps,
        leave_one_out_top10_ejections=leave_one_out_top10_ejections,
        survival_rates=survival_rates,
        pass_condition=(
            median_overlap >= decision_grade_threshold
            and max_ejection_fraction <= top10_ejection_limit
        ),
    )


def annotate_ranked_entities(
    ranked_rows: list[dict[str, object]],
    warnings_index: dict[tuple[str, str], list[dict[str, str]]],
    stability: StabilityResult,
    decision_grade_threshold: float,
) -> list[RankedEntity]:
    entities: list[RankedEntity] = []
    for row in ranked_rows:
        key = (str(row["entity_type"]), str(row["entity_id"]))
        warnings = warnings_index.get(key, [])
        warning_lines = [
            f"[{warning['severity']}] {warning['warning_kind']}: {warning['warning_text']}"
            for warning in warnings
        ]
        severity_rank = {"high": 3, "medium": 2, "low": 1}
        warning_severity = "none"
        if warnings:
            warning_severity = max(
                (warning["severity"].lower() for warning in warnings),
                key=lambda item: severity_rank.get(item, 0),
            )
        survival_rate = stability.survival_rates.get(str(row["entity_id"]), 0.0)
        decision_grade = bool(
            row["eligible"] and survival_rate >= decision_grade_threshold
        )
        entities.append(
            RankedEntity(
                entity_type=str(row["entity_type"]),
                entity_id=str(row["entity_id"]),
                entity_label=str(row["entity_label"]),
                composite_score=(
                    round(float(row["composite_score"]), 6)
                    if row["composite_score"] is not None
                    else None
                ),
                eligible=bool(row["eligible"]),
                rank=int(row["rank"]) if row["rank"] is not None else None,
                decision_grade=decision_grade,
                sensitivity_survival_rate=round(survival_rate, 6),
                layer_values=dict(row["layer_values"]),
                warnings=warning_lines,
                warning_severity=warning_severity,
                metadata=dict(row["metadata"]),
            )
        )
    return entities


def compare_baseline_overlap(
    ranked_entities: list[RankedEntity],
    top_n: int,
    baseline_field: str,
) -> dict[str, object]:
    reference_top = [
        entity.entity_id
        for entity in ranked_entities
        if entity.eligible
    ][:top_n]
    naive_top = [
        entity.entity_id
        for entity in sorted(
            ranked_entities,
            key=lambda item: (
                -(item.layer_values.get("common_variant_support") or -1.0),
                item.entity_label.lower(),
            ),
        )
        if entity.layer_values.get("common_variant_support") is not None
    ][: len(reference_top)]
    generic_top = [
        entity.entity_id
        for entity in sorted(
            ranked_entities,
            key=lambda item: (
                -float(item.metadata.get(baseline_field, "-1") or -1),
                item.entity_label.lower(),
            ),
        )
        if entity.metadata.get(baseline_field)
    ][: len(reference_top)]

    return {
        "reference_top_n": len(reference_top),
        "naive_overlap": round(top_overlap(reference_top, naive_top), 6),
        "generic_overlap": round(top_overlap(reference_top, generic_top), 6),
        "naive_only": sorted(set(naive_top).difference(reference_top)),
        "generic_only": sorted(set(generic_top).difference(reference_top)),
    }
