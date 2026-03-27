from __future__ import annotations

from statistics import median

from scz_target_engine.scoring import (
    REQUIRED_LAYER_GROUP_SPECS,
    RankedEntity,
    StabilityResult,
    format_label_list,
    format_layer_name_list,
    summarize_source_coverage,
)


def format_score(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def build_required_group_lines(entity: RankedEntity) -> list[str]:
    lines: list[str] = []
    for group in REQUIRED_LAYER_GROUP_SPECS.get(entity.entity_type, ()):
        present_layers = [
            layer_name
            for layer_name in group.layer_names
            if entity.layer_values.get(layer_name) is not None
        ]
        if present_layers:
            lines.append(
                f"  - {group.label}: present via {format_label_list(present_layers)}"
            )
            continue
        lines.append(
            f"  - {group.label}: missing; needs {format_layer_name_list(group.layer_names)}"
        )
    return lines


def build_decision_basis_lines(
    entity: RankedEntity,
    include_decision_grade: bool,
    decision_grade_threshold: float,
) -> list[str]:
    lines: list[str] = []
    missing_groups = [
        group.label
        for group in REQUIRED_LAYER_GROUP_SPECS.get(entity.entity_type, ())
        if not any(entity.layer_values.get(layer_name) is not None for layer_name in group.layer_names)
    ]
    if missing_groups:
        lines.append(
            f"  - Ineligible because required {format_label_list(missing_groups)} is missing."
        )
    elif include_decision_grade:
        lines.append(
            "  - Eligible across the required evidence groups and stable enough for decision-grade use."
        )
        lines.append(
            f"  - Stability bar cleared: {entity.sensitivity_survival_rate:.2%} survival vs "
            f"{decision_grade_threshold:.0%} required."
        )
    else:
        lines.append(
            f"  - Missed the decision-grade stability bar: {entity.sensitivity_survival_rate:.2%} "
            f"survival vs {decision_grade_threshold:.0%} required."
        )

    if entity.warning_count:
        lines.append(
            f"  - Carries {entity.warning_count} warning(s); highest severity is {entity.warning_severity}."
        )
    else:
        lines.append("  - No warnings are attached to this entity.")
    return lines


def build_source_coverage_lines(entity: RankedEntity) -> list[str]:
    source_summary = summarize_source_coverage(
        entity.entity_type,
        entity.layer_values,
        entity.metadata,
    )
    if source_summary is None:
        return []

    matched_sources = format_label_list(list(source_summary.matched_sources)) or "none"
    lines = [
        "- Source coverage:",
        f"  - Matched {len(source_summary.matched_sources)}/{source_summary.known_source_count} "
        f"known source checks: {matched_sources}",
    ]
    if source_summary.missing_sources:
        lines.append(
            f"  - Missing: {format_label_list(list(source_summary.missing_sources))}"
        )
    if source_summary.missing_required_groups:
        lines.append(
            "  - Required coverage gaps: "
            f"{format_label_list(list(source_summary.missing_required_groups))}"
        )
    return lines


def build_source_coverage_summary(entity: RankedEntity) -> str:
    source_summary = summarize_source_coverage(
        entity.entity_type,
        entity.layer_values,
        entity.metadata,
    )
    if source_summary is None:
        return ""
    matched_sources = format_label_list(list(source_summary.matched_sources)) or "none"
    summary = (
        f"matched {len(source_summary.matched_sources)}/{source_summary.known_source_count}: "
        f"{matched_sources}"
    )
    if source_summary.missing_sources:
        summary += f"; missing {format_label_list(list(source_summary.missing_sources))}"
    return summary


def ranked_entities_to_rows(entities: list[RankedEntity]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entity in entities:
        present_layer_count = len(
            [value for value in entity.layer_values.values() if value is not None]
        )
        missing_layers = [
            layer_name
            for layer_name, value in entity.layer_values.items()
            if value is None
        ]
        row: dict[str, object] = {
            "entity_type": entity.entity_type,
            "entity_id": entity.entity_id,
            "entity_label": entity.entity_label,
            "rank": entity.rank,
            "eligible": entity.eligible,
            "composite_score": entity.composite_score,
            "decision_grade": entity.decision_grade,
            "sensitivity_survival_rate": entity.sensitivity_survival_rate,
            "present_layer_count": present_layer_count,
            "missing_layers": " | ".join(missing_layers),
            "warning_count": entity.warning_count,
            "warning_severity": entity.warning_severity,
            "warning_kinds": " | ".join(
                warning.warning_kind for warning in entity.warning_records
            ),
            "warnings": " | ".join(entity.warnings),
            "source_coverage_summary": build_source_coverage_summary(entity),
        }
        row.update(entity.layer_values)
        row.update(entity.metadata)
        rows.append(row)
    return rows


def build_cards_markdown(
    title: str,
    entities: list[RankedEntity],
    limit: int,
    include_decision_grade: bool,
    decision_grade_threshold: float,
) -> str:
    lines = [f"# {title}", ""]
    if not entities:
        lines.extend(["No entities matched this selection.", ""])
        return "\n".join(lines)

    for entity in entities[:limit]:
        present_layer_count = len(
            [value for value in entity.layer_values.values() if value is not None]
        )
        missing_layers = [
            layer_name
            for layer_name, value in entity.layer_values.items()
            if value is None
        ]
        lines.append(f"## {entity.entity_label} ({entity.entity_id})")
        lines.append("")
        if include_decision_grade:
            lines.append("- Verdict: advance")
        else:
            lines.append("- Verdict: do not advance")
        lines.append(f"- Rank: {entity.rank if entity.rank is not None else 'ineligible'}")
        lines.append(f"- Composite score: {format_score(entity.composite_score)}")
        lines.append(f"- Decision-grade: {'yes' if entity.decision_grade else 'no'}")
        lines.append(
            f"- Sensitivity survival rate: {entity.sensitivity_survival_rate:.2%}"
        )
        lines.append("- Decision basis:")
        lines.extend(
            build_decision_basis_lines(
                entity,
                include_decision_grade,
                decision_grade_threshold,
            )
        )
        lines.append("- Evidence coverage:")
        lines.append(
            f"  - {present_layer_count}/{len(entity.layer_values)} scoring layers present"
        )
        if missing_layers:
            lines.append(f"  - Missing layers: {', '.join(missing_layers)}")
        else:
            lines.append("  - Missing layers: none")
        lines.append("- Required group status:")
        lines.extend(build_required_group_lines(entity))
        source_coverage_lines = build_source_coverage_lines(entity)
        if source_coverage_lines:
            lines.extend(source_coverage_lines)
        if entity.warnings:
            lines.append(
                f"- Warnings ({entity.warning_count}, highest {entity.warning_severity}):"
            )
            for warning in entity.warnings:
                lines.append(f"  - {warning}")
        else:
            lines.append("- Warnings: none")
        lines.append("- Layer values:")
        for layer_name, value in entity.layer_values.items():
            pretty_value = "missing" if value is None else f"{value:.2f}"
            lines.append(f"  - {layer_name}: {pretty_value}")
        lines.append("")
    return "\n".join(lines)


def build_summary_markdown(
    gene_entities: list[RankedEntity],
    module_entities: list[RankedEntity],
    gene_stability: StabilityResult,
    module_stability: StabilityResult,
    baseline_overlap: dict[str, object],
) -> str:
    gene_warning_entities = [entity for entity in gene_entities if entity.warning_count]
    gene_ineligible_entities = [entity for entity in gene_entities if not entity.eligible]
    top_gene_lines = [
        f"- {entity.rank}. {entity.entity_label} ({format_score(entity.composite_score)})"
        for entity in gene_entities
        if entity.rank is not None
    ][:5]
    top_module_lines = [
        f"- {entity.rank}. {entity.entity_label} ({format_score(entity.composite_score)})"
        for entity in module_entities
        if entity.rank is not None
    ][:5]

    lines = [
        "# Target Engine V0 Summary",
        "",
        f"- Gene stability pass: {'yes' if gene_stability.pass_condition else 'no'}",
        f"- Module stability pass: {'yes' if module_stability.pass_condition else 'no'}",
        f"- Gene perturbation median overlap: {median(gene_stability.perturbation_overlaps):.2%}",
        f"- Module perturbation median overlap: {median(module_stability.perturbation_overlaps):.2%}",
        f"- Naive baseline overlap: {baseline_overlap['naive_overlap']:.2%}",
        f"- Generic platform overlap: {baseline_overlap['generic_overlap']:.2%}",
        f"- Gene entities with warnings: {len(gene_warning_entities)}",
        f"- Gene entities ineligible for ranking: {len(gene_ineligible_entities)}",
        "",
        "## Top Genes",
        *top_gene_lines,
        "",
        "## Top Modules",
        *top_module_lines,
        "",
    ]
    return "\n".join(lines)
