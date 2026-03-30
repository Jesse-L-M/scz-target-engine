from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from scz_target_engine.config import EngineConfig
from scz_target_engine.decision_vector import (
    build_decision_vector_payload,
    build_decision_vectors,
    rank_domain_head_rows,
)
from scz_target_engine.io import read_csv_rows, write_csv, write_json, write_text
from scz_target_engine.ledger import (
    build_target_ledgers,
    ledger_summary_fields,
    target_ledgers_to_payload,
)
from scz_target_engine.hypothesis_lab import materialize_hypothesis_packets
from scz_target_engine.policy import build_policy_artifacts
from scz_target_engine.reporting import (
    build_cards_markdown,
    build_summary_markdown,
    ranked_entities_to_rows,
)
from scz_target_engine.scoring import (
    GENE_REQUIRED_GROUPS,
    MODULE_REQUIRED_GROUPS,
    SEVERITY_RANK,
    annotate_ranked_entities,
    build_warning_index,
    compare_baseline_overlap,
    parse_entity_rows,
    rank_records,
    run_stability_analysis,
    validate_layer_ranges,
)


LEDGER_SUBSTRATE_FILES = (
    Path("data/curated/program_history/programs.csv"),
    Path("data/curated/program_history/directionality_hypotheses.csv"),
)


def resolve_path(base_dir: Path, candidate: str | None, fallback: str) -> Path:
    if candidate:
        return Path(candidate).resolve()
    return (base_dir / fallback).resolve()


def resolve_repo_root(config: EngineConfig) -> Path:
    candidate_roots = [
        config.config_path.resolve().parents[1],
        Path(__file__).resolve().parents[2],
    ]
    for candidate_root in candidate_roots:
        if all((candidate_root / relative_path).exists() for relative_path in LEDGER_SUBSTRATE_FILES):
            return candidate_root
    return Path(__file__).resolve().parents[2]


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
    repo_root = resolve_repo_root(config)
    program_history_path = repo_root / "data/curated/program_history/programs.csv"
    directionality_hypotheses_path = (
        repo_root / "data/curated/program_history/directionality_hypotheses.csv"
    )

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
        decision_grade_threshold=config.stability.heuristic_stability_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )
    module_stability = run_stability_analysis(
        module_records,
        layer_weights=config.module_layers,
        required_groups=MODULE_REQUIRED_GROUPS,
        top_n=config.build.top_n,
        perturbation_fraction=config.stability.perturbation_fraction,
        decision_grade_threshold=config.stability.heuristic_stability_threshold,
        top10_ejection_limit=config.stability.top10_ejection_limit,
    )

    gene_entities = annotate_ranked_entities(
        gene_ranked,
        warning_index,
        gene_stability,
        config.stability.heuristic_stability_threshold,
    )
    module_entities = annotate_ranked_entities(
        module_ranked,
        warning_index,
        module_stability,
        config.stability.heuristic_stability_threshold,
    )

    baseline_overlap = compare_baseline_overlap(
        gene_entities,
        top_n=config.build.top_n,
        baseline_field="generic_platform_baseline",
    )

    gene_rows = ranked_entities_to_rows(gene_entities)
    module_rows = ranked_entities_to_rows(module_entities)
    target_ledgers = build_target_ledgers(
        gene_entities,
        program_history_path=program_history_path,
        directionality_hypotheses_path=directionality_hypotheses_path,
    )
    target_ledger_index = {ledger.entity_id: ledger for ledger in target_ledgers}
    for row in gene_rows:
        ledger = target_ledger_index.get(str(row["entity_id"]))
        if ledger is None:
            continue
        row.update(ledger_summary_fields(ledger))
    gene_vectors = build_decision_vectors(gene_entities, ledger_index=target_ledger_index)
    module_vectors = build_decision_vectors(module_entities)
    domain_head_rows = rank_domain_head_rows(gene_vectors + module_vectors)
    policy_vector_payload, policy_pareto_payload = build_policy_artifacts(
        gene_vectors,
        module_vectors,
        ledger_index=target_ledger_index,
        repo_root=repo_root,
    )

    top_targets = [
        entity
        for entity in gene_entities
        if entity.rank is not None and entity.heuristic_stable
    ]
    kill_cards = [
        entity
        for entity in gene_entities
        if entity.rank is None or not entity.heuristic_stable
    ]
    kill_cards.sort(
        key=lambda entity: (
            entity.rank is not None,
            -SEVERITY_RANK.get(entity.warning_severity, 0),
            entity.sensitivity_survival_rate,
            entity.composite_score if entity.composite_score is not None else -1.0,
            entity.entity_label.lower(),
        )
    )

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
    ledger_payload = target_ledgers_to_payload(
        target_ledgers,
        program_history_path=program_history_path,
        directionality_hypotheses_path=directionality_hypotheses_path,
        repo_root=repo_root,
    )
    write_json(
        output_dir / "gene_target_ledgers.json",
        ledger_payload,
    )
    write_json(
        output_dir / "decision_vectors_v1.json",
        build_decision_vector_payload(gene_vectors, module_vectors),
    )
    write_json(
        output_dir / "policy_decision_vectors_v2.json",
        policy_vector_payload,
    )
    write_csv(
        output_dir / "domain_head_rankings_v1.csv",
        domain_head_rows,
        fieldnames=list(domain_head_rows[0].keys()) if domain_head_rows else [],
    )
    write_json(
        output_dir / "policy_pareto_fronts_v1.json",
        policy_pareto_payload,
    )
    materialize_hypothesis_packets(
        output_dir / "policy_decision_vectors_v2.json",
        output_dir / "gene_target_ledgers.json",
        output_file=output_dir / "hypothesis_packets_v1.json",
    )
    write_text(
        output_dir / "target_cards.md",
        build_cards_markdown(
            "Public-Evidence Promising Cards",
            top_targets,
            limit=5,
            include_decision_grade=True,
            decision_grade_threshold=config.stability.heuristic_stability_threshold,
        ),
    )
    write_text(
        output_dir / "kill_cards.md",
        build_cards_markdown(
            "Fragile Or Insufficient Evidence Cards",
            kill_cards,
            limit=5,
            include_decision_grade=False,
            decision_grade_threshold=config.stability.heuristic_stability_threshold,
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
        "gene_warning_count": len([entity for entity in gene_entities if entity.warning_count]),
        "gene_target_ledger_file": str((output_dir / "gene_target_ledgers.json").resolve()),
        "decision_vector_artifact": str(output_dir / "decision_vectors_v1.json"),
        "policy_decision_vector_artifact": str(output_dir / "policy_decision_vectors_v2.json"),
        "domain_head_ranking_artifact": str(output_dir / "domain_head_rankings_v1.csv"),
        "policy_pareto_front_artifact": str(output_dir / "policy_pareto_fronts_v1.json"),
        "hypothesis_packet_artifact": str(output_dir / "hypothesis_packets_v1.json"),
    }
