from __future__ import annotations

from statistics import median

from scz_target_engine.scoring import RankedEntity, StabilityResult


def ranked_entities_to_rows(entities: list[RankedEntity]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entity in entities:
        row: dict[str, object] = {
            "entity_type": entity.entity_type,
            "entity_id": entity.entity_id,
            "entity_label": entity.entity_label,
            "rank": entity.rank,
            "eligible": entity.eligible,
            "composite_score": entity.composite_score,
            "decision_grade": entity.decision_grade,
            "sensitivity_survival_rate": entity.sensitivity_survival_rate,
            "warning_severity": entity.warning_severity,
            "warnings": " | ".join(entity.warnings),
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
) -> str:
    lines = [f"# {title}", ""]
    if not entities:
        lines.extend(["No entities matched this selection.", ""])
        return "\n".join(lines)

    for entity in entities[:limit]:
        lines.append(f"## {entity.entity_label} ({entity.entity_id})")
        lines.append("")
        lines.append(f"- Rank: {entity.rank}")
        lines.append(f"- Composite score: {entity.composite_score}")
        lines.append(f"- Decision-grade: {'yes' if entity.decision_grade else 'no'}")
        lines.append(
            f"- Sensitivity survival rate: {entity.sensitivity_survival_rate:.2%}"
        )
        if include_decision_grade:
            lines.append(
                "- Why it survived: appears stable across perturbation and leave-one-layer-out checks."
            )
        else:
            lines.append(
                "- Why it failed: unstable near the decision boundary, under-supported across layers, or warning-heavy."
            )
        if entity.warnings:
            lines.append("- Warnings:")
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
    top_gene_lines = [
        f"- {entity.rank}. {entity.entity_label} ({entity.composite_score})"
        for entity in gene_entities
        if entity.rank is not None
    ][:5]
    top_module_lines = [
        f"- {entity.rank}. {entity.entity_label} ({entity.composite_score})"
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
        "",
        "## Top Genes",
        *top_gene_lines,
        "",
        "## Top Modules",
        *top_module_lines,
        "",
    ]
    return "\n".join(lines)
