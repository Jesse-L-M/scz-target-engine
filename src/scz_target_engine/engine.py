from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from scz_target_engine.config import EngineConfig
from scz_target_engine.io import read_csv_rows, write_csv, write_json, write_text
from scz_target_engine.reporting import (
    build_cards_markdown,
    build_summary_markdown,
    ranked_entities_to_rows,
)
from scz_target_engine.scoring import (
    GENE_REQUIRED_GROUPS,
    MODULE_REQUIRED_GROUPS,
    annotate_ranked_entities,
    build_warning_index,
    compare_baseline_overlap,
    parse_entity_rows,
    rank_records,
    run_stability_analysis,
    validate_layer_ranges,
)


def resolve_path(base_dir: Path, candidate: str | None, fallback: str) -> Path:
    if candidate:
        return Path(candidate).resolve()
    return (base_dir / fallback).resolve()


def load_inputs(config: EngineConfig, input_dir: Path) -> tuple[list, list, dict]:
    gene_rows = read_csv_rows(input_dir / config.build.gene_input_file)
    module_rows = read_csv_rows(input_dir / config.build.module_input_file)
    warning_rows = read_csv_rows(input_dir / config.build.warning_input_file)

    gene_records = parse_entity_rows(
        gene_rows,
        entity_type="gene",
        layer_names=list(config.gene_layers.keys()),
    )
    module_records = parse_entity_rows(
        module_rows,
        entity_type="module",
        layer_names=list(config.module_layers.keys()),
    )
    validate_layer_ranges(gene_records)
    validate_layer_ranges(module_records)
    warning_index = build_warning_index(warning_rows)
    return gene_records, module_records, warning_index


def validate_inputs(config: EngineConfig, input_dir: Path) -> dict[str, object]:
    gene_records, module_records, warning_index = load_inputs(config, input_dir)
    return {
        "gene_records": len(gene_records),
        "module_records": len(module_records),
        "warning_entities": len(warning_index),
    }


def build_outputs(config: EngineConfig, input_dir: Path, output_dir: Path) -> dict[str, object]:
    gene_records, module_records, warning_index = load_inputs(config, input_dir)

    gene_ranked = rank_records(
        gene_records,
        layer_weights=config.gene_layers,
        required_groups=GENE_REQUIRED_GROUPS,
    )
    module_ranked = rank_records(
        module_records,
        layer_weights=config.module_layers,
        required_groups=MODULE_REQUIRED_GROUPS,
    )

    gene_stability = run_stability_analysis(
        gene_records,
        layer_weights=config.gene_layers,
        required_groups=GENE_REQUIRED_GROUPS,
        top_n=config.build.top_n,
        perturbation_fraction=config.stability.perturbation_fraction,
        decision_grade_threshold=config.stability.decision_grade_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )
    module_stability = run_stability_analysis(
        module_records,
        layer_weights=config.module_layers,
        required_groups=MODULE_REQUIRED_GROUPS,
        top_n=config.build.top_n,
        perturbation_fraction=config.stability.perturbation_fraction,
        decision_grade_threshold=config.stability.decision_grade_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )

    gene_entities = annotate_ranked_entities(
        gene_ranked,
        warning_index,
        gene_stability,
        config.stability.decision_grade_threshold,
    )
    module_entities = annotate_ranked_entities(
        module_ranked,
        warning_index,
        module_stability,
        config.stability.decision_grade_threshold,
    )

    baseline_overlap = compare_baseline_overlap(
        gene_entities,
        top_n=config.build.top_n,
        baseline_field="generic_platform_baseline",
    )

    gene_rows = ranked_entities_to_rows(gene_entities)
    module_rows = ranked_entities_to_rows(module_entities)

    top_targets = [
        entity
        for entity in gene_entities
        if entity.rank is not None and entity.decision_grade
    ]
    kill_cards = [
        entity
        for entity in gene_entities
        if entity.rank is not None and not entity.decision_grade
    ]

    write_csv(
        output_dir / "gene_rankings.csv",
        gene_rows,
        fieldnames=list(gene_rows[0].keys()) if gene_rows else [],
    )
    write_csv(
        output_dir / "module_rankings.csv",
        module_rows,
        fieldnames=list(module_rows[0].keys()) if module_rows else [],
    )
    write_json(
        output_dir / "stability_summary.json",
        {
            "gene": asdict(gene_stability),
            "module": asdict(module_stability),
            "baseline_overlap": baseline_overlap,
        },
    )
    write_text(
        output_dir / "target_cards.md",
        build_cards_markdown(
            "Target Cards",
            top_targets,
            limit=5,
            include_decision_grade=True,
        ),
    )
    write_text(
        output_dir / "kill_cards.md",
        build_cards_markdown(
            "Kill Cards",
            kill_cards,
            limit=5,
            include_decision_grade=False,
        ),
    )
    write_text(
        output_dir / "build_summary.md",
        build_summary_markdown(
            gene_entities,
            module_entities,
            gene_stability,
            module_stability,
            baseline_overlap,
        ),
    )

    return {
        "output_dir": str(output_dir),
        "gene_stability_pass": gene_stability.pass_condition,
        "module_stability_pass": module_stability.pass_condition,
        "baseline_overlap": baseline_overlap,
        "gene_ranked_count": len([entity for entity in gene_entities if entity.rank is not None]),
        "module_ranked_count": len(
            [entity for entity in module_entities if entity.rank is not None]
        ),
    }
