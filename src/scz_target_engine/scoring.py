from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from statistics import median


SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1}

GENE_REQUIRED_GROUPS = (
    ("common_variant_support", "rare_variant_support"),
    ("cell_state_support", "developmental_regulatory_support"),
)

MODULE_REQUIRED_GROUPS = (
    ("member_gene_genetic_enrichment",),
    ("cell_state_specificity", "developmental_regulatory_relevance"),
)


@dataclass(frozen=True)
class LayerGroupSpec:
    label: str
    layer_names: tuple[str, ...]


@dataclass(frozen=True)
class SourceCoverageSpec:
    flag_name: str
    source_label: str
    field_names: tuple[str, ...]


@dataclass(frozen=True)
class WarningRecord:
    severity: str
    warning_kind: str
    warning_text: str
    source: str


@dataclass(frozen=True)
class SourceCoverageSummary:
    matched_sources: tuple[str, ...]
    missing_sources: tuple[str, ...]
    missing_required_groups: tuple[str, ...]
    known_source_count: int


REQUIRED_LAYER_GROUP_SPECS = {
    "gene": (
        LayerGroupSpec(
            label="genetic support",
            layer_names=("common_variant_support", "rare_variant_support"),
        ),
        LayerGroupSpec(
            label="biological support",
            layer_names=("cell_state_support", "developmental_regulatory_support"),
        ),
    ),
    "module": (
        LayerGroupSpec(
            label="member-gene enrichment",
            layer_names=("member_gene_genetic_enrichment",),
        ),
        LayerGroupSpec(
            label="biological support",
            layer_names=("cell_state_specificity", "developmental_regulatory_relevance"),
        ),
    ),
}

GENE_SOURCE_COVERAGE_SPECS = (
    SourceCoverageSpec(
        flag_name="source_present_pgc",
        source_label="PGC common-variant support",
        field_names=("common_variant_support",),
    ),
    SourceCoverageSpec(
        flag_name="source_present_schema",
        source_label="SCHEMA rare-variant support",
        field_names=("rare_variant_support",),
    ),
    SourceCoverageSpec(
        flag_name="source_present_psychencode",
        source_label="PsychENCODE biological support",
        field_names=("cell_state_support", "developmental_regulatory_support"),
    ),
    SourceCoverageSpec(
        flag_name="source_present_chembl",
        source_label="ChEMBL tractability support",
        field_names=("tractability_compoundability",),
    ),
    SourceCoverageSpec(
        flag_name="source_present_opentargets",
        source_label="Open Targets baseline context",
        field_names=("generic_platform_baseline",),
    ),
)

GENE_REQUIRED_SOURCE_GROUPS = (
    ("genetic support", ("source_present_pgc", "source_present_schema")),
    ("biological support", ("source_present_psychencode",)),
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
    warning_records: list[WarningRecord]
    warnings: list[str]
    warning_count: int
    warning_severity: str
    metadata: dict[str, str]

    @property
    def heuristic_stable(self) -> bool:
        return self.decision_grade


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


def parse_optional_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if cleaned in {"true", "1", "yes"}:
        return True
    if cleaned in {"false", "0", "no"}:
        return False
    return None


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


def format_layer_name_list(layer_names: tuple[str, ...]) -> str:
    if len(layer_names) == 1:
        return layer_names[0]
    if len(layer_names) == 2:
        return f"{layer_names[0]} or {layer_names[1]}"
    return ", ".join(layer_names[:-1]) + f", or {layer_names[-1]}"


def format_label_list(labels: list[str]) -> str:
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return ", ".join(labels[:-1]) + f", and {labels[-1]}"


def find_missing_required_layer_groups(
    entity_type: str,
    layer_values: dict[str, float | None],
) -> list[LayerGroupSpec]:
    specs = REQUIRED_LAYER_GROUP_SPECS.get(entity_type, ())
    return [
        spec
        for spec in specs
        if not any(layer_values.get(layer_name) is not None for layer_name in spec.layer_names)
    ]


def summarize_source_coverage(
    entity_type: str,
    layer_values: dict[str, float | None],
    metadata: dict[str, str],
) -> SourceCoverageSummary | None:
    if entity_type != "gene":
        return None

    states: dict[str, bool] = {}
    matched_sources: list[str] = []
    missing_sources: list[str] = []
    for spec in GENE_SOURCE_COVERAGE_SPECS:
        state = parse_optional_bool(metadata.get(spec.flag_name))
        if state is None:
            continue
        states[spec.flag_name] = state
        if state:
            matched_sources.append(spec.source_label)
            continue
        field_has_value = any(
            layer_values.get(field_name) is not None for field_name in spec.field_names
        )
        if not field_has_value:
            field_has_value = any(
                bool((metadata.get(field_name) or "").strip()) for field_name in spec.field_names
            )
        if field_has_value:
            missing_sources.append(f"{spec.source_label} provenance")
        else:
            missing_sources.append(spec.source_label)

    if not states:
        return None

    missing_required_groups = [
        label
        for label, flag_names in GENE_REQUIRED_SOURCE_GROUPS
        if all(states.get(flag_name) is False for flag_name in flag_names)
        and all(flag_name in states for flag_name in flag_names)
    ]
    return SourceCoverageSummary(
        matched_sources=tuple(matched_sources),
        missing_sources=tuple(missing_sources),
        missing_required_groups=tuple(missing_required_groups),
        known_source_count=len(states),
    )


def format_warning_record(record: WarningRecord) -> str:
    return f"[{record.severity}] {record.warning_kind}: {record.warning_text}"


def sort_warning_records(records: list[WarningRecord]) -> list[WarningRecord]:
    return sorted(
        records,
        key=lambda record: (
            -SEVERITY_RANK.get(record.severity.lower(), 0),
            record.source != "input",
            record.warning_kind,
            record.warning_text,
        ),
    )


def build_automatic_warning_records(
    entity_type: str,
    layer_values: dict[str, float | None],
    metadata: dict[str, str],
    existing_warning_kinds: set[str],
) -> list[WarningRecord]:
    warnings: list[WarningRecord] = []

    missing_groups = find_missing_required_layer_groups(entity_type, layer_values)
    if missing_groups and not existing_warning_kinds.intersection(
        {"evidence_missingness", "required_layer_group_missing"}
    ):
        missing_text = "; ".join(
            f"{group.label} ({format_layer_name_list(group.layer_names)})"
            for group in missing_groups
        )
        warnings.append(
            WarningRecord(
                severity="high",
                warning_kind="required_layer_group_missing",
                warning_text=(
                    f"Missing required layer groups: {missing_text}. "
                    "Entity is ineligible for composite ranking until they are sourced."
                ),
                source="auto",
            )
        )

    source_summary = summarize_source_coverage(entity_type, layer_values, metadata)
    if source_summary is not None and source_summary.missing_sources and (
        "source_coverage_gap" not in existing_warning_kinds
    ):
        severity = "medium" if source_summary.missing_required_groups else "low"
        matched_text = format_label_list(list(source_summary.matched_sources)) or "none"
        missing_text = format_label_list(list(source_summary.missing_sources))
        if source_summary.missing_required_groups:
            prefix = (
                f"Missing source-backed {format_label_list(list(source_summary.missing_required_groups))}"
            )
        else:
            prefix = "Source coverage is partial"
        warnings.append(
            WarningRecord(
                severity=severity,
                warning_kind="source_coverage_gap",
                warning_text=f"{prefix}; matched {matched_text}; missing {missing_text}.",
                source="auto",
            )
        )

    return warnings


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
        warning_rows = warnings_index.get(key, [])
        warning_records = [
            WarningRecord(
                severity=warning["severity"].strip().lower(),
                warning_kind=warning["warning_kind"].strip(),
                warning_text=warning["warning_text"].strip(),
                source="input",
            )
            for warning in warning_rows
        ]
        auto_warning_records = build_automatic_warning_records(
            entity_type=str(row["entity_type"]),
            layer_values=dict(row["layer_values"]),
            metadata=dict(row["metadata"]),
            existing_warning_kinds={
                warning_record.warning_kind.lower()
                for warning_record in warning_records
            },
        )
        warning_records = sort_warning_records(warning_records + auto_warning_records)
        warning_lines = [format_warning_record(record) for record in warning_records]
        warning_severity = "none"
        if warning_records:
            warning_severity = max(
                (warning.severity.lower() for warning in warning_records),
                key=lambda item: SEVERITY_RANK.get(item, 0),
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
                warning_records=warning_records,
                warnings=warning_lines,
                warning_count=len(warning_records),
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
