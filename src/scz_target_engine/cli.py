from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Callable

from scz_target_engine.atlas.convergence import materialize_convergence_hubs
from scz_target_engine.atlas.ingest import refresh_atlas_candidate_registry
from scz_target_engine.atlas.mechanistic_axes import materialize_mechanistic_axes
from scz_target_engine.atlas.sources import (
    fetch_atlas_opentargets_baseline,
    fetch_atlas_pgc_scz2022_prioritized_genes,
)
from scz_target_engine.atlas.taxonomy import materialize_atlas_taxonomy
from scz_target_engine.atlas.tensor import materialize_atlas_tensor
from scz_target_engine.benchmark_backfill import materialize_public_benchmark_slices
from scz_target_engine.benchmark_leaderboard import materialize_benchmark_reporting
from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.challenge import (
    materialize_prospective_prediction_registration,
)
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs, validate_inputs
from scz_target_engine.hidden_eval import (
    materialize_hidden_eval_simulation,
    materialize_hidden_eval_submission_archive,
    materialize_rescue_hidden_eval_task_package,
)
from scz_target_engine.hypothesis_lab import (
    materialize_blinded_expert_review_packets,
    materialize_hypothesis_packets,
)
from scz_target_engine.ingest import refresh_candidate_registry
from scz_target_engine.io import read_json, write_csv
from scz_target_engine.program_memory import (
    apply_program_memory_adjudication,
    build_program_memory_adjudication_record,
    build_program_memory_coverage_audit,
    build_program_memory_harvest_batch,
    build_program_memory_harvest_review_rows,
    load_program_memory_harvest_batch,
    write_program_memory_adjudication_outputs,
    write_program_memory_coverage_outputs,
    write_program_memory_harvest_batch,
)
from scz_target_engine.prepare import (
    prepare_gene_table,
    refresh_example_gene_table,
    refresh_example_input_tables,
    refresh_example_module_table,
)
from scz_target_engine.rescue.baselines import materialize_rescue_baseline_suite
from scz_target_engine.rescue.tasks import materialize_npc_signature_reversal_run
from scz_target_engine.registry import build_candidate_registry
from scz_target_engine.rescue.tasks import (
    DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
    materialize_glutamatergic_convergence_rescue_evaluation,
)
from scz_target_engine.sources.chembl import fetch_chembl_tractability
from scz_target_engine.sources.opentargets import fetch_opentargets_baseline
from scz_target_engine.sources.pgc import fetch_pgc_scz2022_prioritized_genes
from scz_target_engine.sources.psychencode import (
    fetch_psychencode_module_table,
    fetch_psychencode_support,
)
from scz_target_engine.observatory.benchmark_nav import (
    browse_leaderboard,
    browse_report_cards,
    list_available_leaderboard_slices,
)
from scz_target_engine.observatory.packet_nav import (
    browse_failure_analog,
    browse_packet,
    browse_policy_comparison,
    list_failure_analogs,
    list_packets,
    list_rescue_evidence,
    list_rescue_tasks,
)
from scz_target_engine.observatory.shell import (
    build_observatory_index,
    format_failure_analogs,
    format_leaderboard,
    format_observatory_index,
    format_packet_detail,
    format_packet_list,
    format_policy_comparison,
    format_report_cards,
    format_rescue_evidence_list,
    format_rescue_tasks,
)
from scz_target_engine.sources.schema import fetch_schema_rare_variant_support


ParserConfigurer = Callable[[argparse.ArgumentParser], None]

REPO_ROOT_MARKERS = (
    Path("pyproject.toml"),
    Path("examples") / "v0" / "input",
)


@dataclass(frozen=True)
class CommandRoute:
    command: str
    namespaced_path: tuple[str, ...]
    configure_parser: ParserConfigurer


def _configure_validate_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True)
    parser.add_argument("--input-dir")


def _configure_build_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True)
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")


def _configure_build_hypothesis_packets_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--policy-artifact", required=True)
    parser.add_argument("--ledger-artifact", required=True)
    parser.add_argument("--output-file", required=True)


def _configure_build_expert_review_packets_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--hypothesis-artifact", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--rubric-file")


def _configure_register_prospective_prediction_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--hypothesis-artifact", required=True)
    parser.add_argument("--packet-id", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--registered-at", required=True)
    parser.add_argument("--registered-by", required=True)
    parser.add_argument("--predicted-outcome", required=True)
    parser.add_argument(
        "--option-probability",
        action="append",
        default=[],
        help="Repeat as OPTION=PROBABILITY for each reviewed packet decision option.",
    )
    parser.add_argument("--outcome-window-closes-on", required=True)
    parser.add_argument("--outcome-window-opens-on")
    parser.add_argument("--rationale", action="append", default=[])
    parser.add_argument("--registration-id")
    parser.add_argument("--notes")


def _configure_program_memory_harvest_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--harvest-id", required=True)
    parser.add_argument("--harvester", required=True)
    parser.add_argument("--created-at")
    parser.add_argument("--review-file")


def _configure_program_memory_adjudicate_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--harvest-file", required=True)
    parser.add_argument("--decisions-file", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--adjudication-id", required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--reviewed-at")
    parser.add_argument("--notes")


def _configure_program_memory_coverage_audit_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--dataset-dir", default="data/curated/program_history/v2")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--focus-target")
    parser.add_argument("--focus-target-class")
    parser.add_argument("--focus-domain")
    parser.add_argument("--focus-failure-scope")


def _configure_fetch_opentargets_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int)


def _configure_fetch_chembl_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--limit", type=int)


def _configure_fetch_pgc_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-file", required=True)


def _configure_fetch_schema_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--overrides-file")


def _configure_fetch_psychencode_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--limit", type=int)


def _configure_fetch_psychencode_modules_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--limit", type=int)


def _configure_build_candidate_registry_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--opentargets-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--pgc-file")


def _configure_refresh_candidate_registry_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-file")
    parser.add_argument("--work-dir")
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--skip-pgc", action="store_true")


def _configure_atlas_fetch_opentargets_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--raw-dir")
    parser.add_argument("--materialized-at")


def _configure_atlas_fetch_pgc_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--materialized-at")


def _configure_atlas_refresh_candidate_registry_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-file")
    parser.add_argument("--work-dir")
    parser.add_argument("--raw-dir")
    parser.add_argument("--materialized-at")
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--skip-pgc", action="store_true")


def _configure_atlas_build_taxonomy_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ingest-manifest-file", required=True)
    parser.add_argument("--output-dir")


def _configure_atlas_build_tensor_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ingest-manifest-file", required=True)
    parser.add_argument("--output-dir")
    parser.add_argument("--taxonomy-dir")


def _configure_atlas_build_mechanistic_axes_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--tensor-manifest-file", required=True)
    parser.add_argument("--output-dir")


def _configure_atlas_build_convergence_hubs_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--tensor-manifest-file", required=True)
    parser.add_argument("--output-dir")


def _configure_run_glutamatergic_convergence_rescue_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--task-card-path")
    parser.add_argument(
        "--baseline-id",
        default=DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
    )


def _configure_prepare_gene_table_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--pgc-file")
    parser.add_argument("--schema-file")
    parser.add_argument("--psychencode-file")
    parser.add_argument("--opentargets-file")
    parser.add_argument("--chembl-file")


def _configure_refresh_example_gene_table_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--seed-file")
    parser.add_argument("--output-file")
    parser.add_argument("--work-dir")
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--overrides-file")


def _configure_refresh_example_module_table_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--gene-file")
    parser.add_argument("--output-file")
    parser.add_argument("--work-dir")


def _configure_refresh_example_inputs_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--seed-file")
    parser.add_argument("--gene-output-file")
    parser.add_argument("--module-output-file")
    parser.add_argument("--gene-work-dir")
    parser.add_argument("--module-work-dir")
    parser.add_argument("--disease-id")
    parser.add_argument("--disease-query")
    parser.add_argument("--overrides-file")


def _configure_build_benchmark_snapshot_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--request-file", required=True)
    parser.add_argument("--archive-index-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--materialized-at", required=True)


def _configure_build_benchmark_cohort_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--manifest-file", required=True)
    parser.add_argument("--cohort-members-file", required=True)
    parser.add_argument("--future-outcomes-file", required=True)
    parser.add_argument("--output-file", required=True)


def _configure_run_benchmark_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest-file", required=True)
    parser.add_argument("--cohort-labels-file", required=True)
    parser.add_argument("--archive-index-file", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--config")
    parser.add_argument("--bootstrap-iterations", type=int)
    parser.add_argument(
        "--bootstrap-confidence-level",
        type=float,
        default=0.95,
    )
    parser.add_argument("--random-seed", type=int, default=17)
    parser.add_argument(
        "--deterministic-test-mode",
        action="store_true",
    )


def _configure_backfill_benchmark_public_slices_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-dir")
    parser.add_argument("--benchmark-task-id")
    parser.add_argument("--task-registry-path")


def _configure_build_benchmark_reporting_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--manifest-file", required=True)
    parser.add_argument("--cohort-labels-file", required=True)
    parser.add_argument("--runner-output-dir", required=True)
    parser.add_argument("--output-dir", required=True)


def _configure_run_npc_signature_reversal_rescue_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--task-card-path")


def _configure_rescue_compare_baselines_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--output-dir", required=True)


def _configure_hidden_eval_task_package_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--task-card-path")


def _configure_hidden_eval_pack_submission_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--task-package-dir", required=True)
    parser.add_argument("--predictions-file", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--submitter-id", required=True)
    parser.add_argument("--submission-id", required=True)
    parser.add_argument("--scorer-id", required=True)
    parser.add_argument("--notes")


def _configure_hidden_eval_simulate_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--task-package-dir", required=True)
    parser.add_argument("--submission-file", required=True)
    parser.add_argument("--output-dir", required=True)


def _configure_observatory_browse_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--generated-dir")


def _configure_observatory_leaderboard_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--entity-type", required=True)
    parser.add_argument("--horizon", required=True)
    parser.add_argument("--metric", required=True)
    parser.add_argument("--generated-dir")


def _configure_observatory_report_cards_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--generated-dir")


def _configure_observatory_leaderboard_slices_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--generated-dir")


def _configure_observatory_packets_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--packets-file", help="Hypothesis packets JSON file.")


def _configure_observatory_packet_detail_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("packet_id", help="Packet ID to browse.")
    parser.add_argument("--packets-file", help="Hypothesis packets JSON file.")


def _configure_observatory_failure_analogs_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--packets-file", help="Hypothesis packets JSON file.")


def _configure_observatory_policy_comparison_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--packets-file", help="Hypothesis packets JSON file.")


def _configure_observatory_rescue_tasks_parser(
    parser: argparse.ArgumentParser,
) -> None:
    pass


def _configure_observatory_rescue_evidence_parser(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--packets-file", help="Rescue-augmented packets JSON file.")


COMMAND_ROUTES = (
    CommandRoute("validate", ("engine", "validate"), _configure_validate_parser),
    CommandRoute("build", ("engine", "build"), _configure_build_parser),
    CommandRoute(
        "build-hypothesis-packets",
        ("hypothesis-lab", "build-packets"),
        _configure_build_hypothesis_packets_parser,
    ),
    CommandRoute(
        "build-expert-review-packets",
        ("hypothesis-lab", "build-expert-review"),
        _configure_build_expert_review_packets_parser,
    ),
    CommandRoute(
        "register-prospective-prediction",
        ("challenge", "prospective", "register"),
        _configure_register_prospective_prediction_parser,
    ),
    CommandRoute(
        "program-memory-harvest",
        ("program-memory", "harvest"),
        _configure_program_memory_harvest_parser,
    ),
    CommandRoute(
        "program-memory-adjudicate",
        ("program-memory", "adjudicate"),
        _configure_program_memory_adjudicate_parser,
    ),
    CommandRoute(
        "program-memory-coverage-audit",
        ("program-memory", "coverage-audit"),
        _configure_program_memory_coverage_audit_parser,
    ),
    CommandRoute(
        "fetch-opentargets",
        ("sources", "opentargets"),
        _configure_fetch_opentargets_parser,
    ),
    CommandRoute(
        "fetch-chembl",
        ("sources", "chembl"),
        _configure_fetch_chembl_parser,
    ),
    CommandRoute(
        "fetch-pgc-scz2022",
        ("sources", "pgc", "scz2022"),
        _configure_fetch_pgc_parser,
    ),
    CommandRoute(
        "fetch-schema",
        ("sources", "schema"),
        _configure_fetch_schema_parser,
    ),
    CommandRoute(
        "fetch-psychencode",
        ("sources", "psychencode", "support"),
        _configure_fetch_psychencode_parser,
    ),
    CommandRoute(
        "fetch-psychencode-modules",
        ("sources", "psychencode", "modules"),
        _configure_fetch_psychencode_modules_parser,
    ),
    CommandRoute(
        "build-candidate-registry",
        ("registry", "build"),
        _configure_build_candidate_registry_parser,
    ),
    CommandRoute(
        "refresh-candidate-registry",
        ("registry", "refresh"),
        _configure_refresh_candidate_registry_parser,
    ),
    CommandRoute(
        "atlas-fetch-opentargets",
        ("atlas", "sources", "opentargets"),
        _configure_atlas_fetch_opentargets_parser,
    ),
    CommandRoute(
        "atlas-fetch-pgc-scz2022",
        ("atlas", "sources", "pgc", "scz2022"),
        _configure_atlas_fetch_pgc_parser,
    ),
    CommandRoute(
        "atlas-refresh-candidate-registry",
        ("atlas", "ingest", "candidate-registry"),
        _configure_atlas_refresh_candidate_registry_parser,
    ),
    CommandRoute(
        "atlas-build-taxonomy",
        ("atlas", "build", "taxonomy"),
        _configure_atlas_build_taxonomy_parser,
    ),
    CommandRoute(
        "atlas-build-tensor",
        ("atlas", "build", "tensor"),
        _configure_atlas_build_tensor_parser,
    ),
    CommandRoute(
        "atlas-build-mechanistic-axes",
        ("atlas", "build", "mechanistic-axes"),
        _configure_atlas_build_mechanistic_axes_parser,
    ),
    CommandRoute(
        "atlas-build-convergence-hubs",
        ("atlas", "build", "convergence-hubs"),
        _configure_atlas_build_convergence_hubs_parser,
    ),
    CommandRoute(
        "rescue-run-glutamatergic-convergence",
        ("rescue", "run", "glutamatergic-convergence"),
        _configure_run_glutamatergic_convergence_rescue_parser,
    ),
    CommandRoute(
        "rescue-compare-baselines",
        ("rescue", "compare", "baselines"),
        _configure_rescue_compare_baselines_parser,
    ),
    CommandRoute(
        "hidden-eval-task-package",
        ("hidden-eval", "task-package"),
        _configure_hidden_eval_task_package_parser,
    ),
    CommandRoute(
        "hidden-eval-pack-submission",
        ("hidden-eval", "pack-submission"),
        _configure_hidden_eval_pack_submission_parser,
    ),
    CommandRoute(
        "hidden-eval-simulate",
        ("hidden-eval", "simulate"),
        _configure_hidden_eval_simulate_parser,
    ),
    CommandRoute(
        "prepare-gene-table",
        ("prepare", "gene-table"),
        _configure_prepare_gene_table_parser,
    ),
    CommandRoute(
        "refresh-example-gene-table",
        ("prepare", "example-gene-table"),
        _configure_refresh_example_gene_table_parser,
    ),
    CommandRoute(
        "refresh-example-module-table",
        ("prepare", "example-module-table"),
        _configure_refresh_example_module_table_parser,
    ),
    CommandRoute(
        "refresh-example-inputs",
        ("prepare", "example-inputs"),
        _configure_refresh_example_inputs_parser,
    ),
    CommandRoute(
        "build-benchmark-snapshot",
        ("benchmark", "snapshot"),
        _configure_build_benchmark_snapshot_parser,
    ),
    CommandRoute(
        "build-benchmark-cohort",
        ("benchmark", "cohort"),
        _configure_build_benchmark_cohort_parser,
    ),
    CommandRoute(
        "run-benchmark",
        ("benchmark", "run"),
        _configure_run_benchmark_parser,
    ),
    CommandRoute(
        "backfill-benchmark-public-slices",
        ("benchmark", "backfill", "public-slices"),
        _configure_backfill_benchmark_public_slices_parser,
    ),
    CommandRoute(
        "build-benchmark-reporting",
        ("benchmark", "reporting"),
        _configure_build_benchmark_reporting_parser,
    ),
    CommandRoute(
        "run-rescue-npc-signature-reversal",
        ("rescue", "npc-signature-reversal"),
        _configure_run_npc_signature_reversal_rescue_parser,
    ),
    CommandRoute(
        "observatory-browse",
        ("observatory", "browse"),
        _configure_observatory_browse_parser,
    ),
    CommandRoute(
        "observatory-leaderboard",
        ("observatory", "leaderboard"),
        _configure_observatory_leaderboard_parser,
    ),
    CommandRoute(
        "observatory-report-cards",
        ("observatory", "report-cards"),
        _configure_observatory_report_cards_parser,
    ),
    CommandRoute(
        "observatory-leaderboard-slices",
        ("observatory", "leaderboard-slices"),
        _configure_observatory_leaderboard_slices_parser,
    ),
    CommandRoute(
        "observatory-packets",
        ("observatory", "packets"),
        _configure_observatory_packets_parser,
    ),
    CommandRoute(
        "observatory-packet-detail",
        ("observatory", "packet-detail"),
        _configure_observatory_packet_detail_parser,
    ),
    CommandRoute(
        "observatory-failure-analogs",
        ("observatory", "failure-analogs"),
        _configure_observatory_failure_analogs_parser,
    ),
    CommandRoute(
        "observatory-policy-comparison",
        ("observatory", "policy-comparison"),
        _configure_observatory_policy_comparison_parser,
    ),
    CommandRoute(
        "observatory-rescue-tasks",
        ("observatory", "rescue-tasks"),
        _configure_observatory_rescue_tasks_parser,
    ),
    CommandRoute(
        "observatory-rescue-evidence",
        ("observatory", "rescue-evidence"),
        _configure_observatory_rescue_evidence_parser,
    ),
)


def _register_legacy_route(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    route: CommandRoute,
) -> None:
    parser = subparsers.add_parser(route.command)
    route.configure_parser(parser)
    parser.set_defaults(command=route.command, command_path=(route.command,))


def _register_namespaced_route(
    route: CommandRoute,
    parser_cache: dict[tuple[str, ...], argparse.ArgumentParser],
    subparser_cache: dict[
        tuple[str, ...],
        argparse._SubParsersAction[argparse.ArgumentParser],
    ],
) -> None:
    parent_path: tuple[str, ...] = ()
    last_index = len(route.namespaced_path) - 1

    for depth, segment in enumerate(route.namespaced_path):
        current_path = parent_path + (segment,)
        parent_subparsers = subparser_cache[parent_path]
        parser = parser_cache.get(current_path)
        if parser is None:
            parser = parent_subparsers.add_parser(segment)
            parser_cache[current_path] = parser

        if depth < last_index and current_path not in subparser_cache:
            subparser_cache[current_path] = parser.add_subparsers(
                dest=f"_segment_{depth + 1}",
                required=True,
            )

        if depth == last_index:
            route.configure_parser(parser)
            parser.set_defaults(
                command=route.command,
                command_path=route.namespaced_path,
            )

        parent_path = current_path


def _resolve_repo_root_from_config_path(config_path: Path) -> Path:
    resolved_config_path = config_path.resolve()
    for candidate_root in resolved_config_path.parents:
        if all((candidate_root / marker).exists() for marker in REPO_ROOT_MARKERS):
            return candidate_root
    return Path(__file__).resolve().parents[2]


def _load_json_object(path: Path) -> dict[str, object]:
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _load_program_memory_decision_payloads(path: Path) -> tuple[list[dict[str, object]], str]:
    payload = read_json(path)
    if isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError(f"program memory decisions must be JSON objects: {path}")
        return list(payload), ""
    if isinstance(payload, dict):
        decisions = payload.get("decisions")
        if not isinstance(decisions, list):
            raise ValueError(
                "program memory decision files must contain a decisions list"
            )
        if not all(isinstance(item, dict) for item in decisions):
            raise ValueError(f"program memory decisions must be JSON objects: {path}")
        notes = payload.get("notes")
        return list(decisions), str(notes) if notes is not None else ""
    raise ValueError(f"unsupported program memory decisions payload in {path}")


def _parse_option_probability_args(entries: list[str]) -> dict[str, float]:
    if not entries:
        raise ValueError(
            "provide at least one --option-probability OPTION=PROBABILITY entry"
        )
    probabilities: dict[str, float] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(
                f"--option-probability entries must use OPTION=PROBABILITY, found {entry!r}"
            )
        option, probability_text = entry.split("=", 1)
        option = option.strip()
        probability_text = probability_text.strip()
        if not option:
            raise ValueError("option names in --option-probability must be non-empty")
        try:
            probability = float(probability_text)
        except ValueError as exc:
            raise ValueError(
                f"--option-probability {entry!r} does not contain a valid number"
            ) from exc
        probabilities[option] = probability
    return probabilities


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scz-target-engine")
    subparsers = parser.add_subparsers(dest="_segment_0", required=True)
    parser_cache: dict[tuple[str, ...], argparse.ArgumentParser] = {}
    subparser_cache: dict[
        tuple[str, ...],
        argparse._SubParsersAction[argparse.ArgumentParser],
    ] = {(): subparsers}

    for route in COMMAND_ROUTES:
        _register_legacy_route(subparsers, route)
        _register_namespaced_route(route, parser_cache, subparser_cache)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "program-memory-harvest":
        payload = _load_json_object(Path(args.input_file).resolve())
        source_documents = payload.get("source_documents")
        suggestions = payload.get("suggestions", payload.get("machine_suggestions"))
        if not isinstance(source_documents, list) or not isinstance(suggestions, list):
            raise ValueError(
                "program memory harvest input files require source_documents and suggestions lists"
            )
        harvest = build_program_memory_harvest_batch(
            harvest_id=args.harvest_id,
            harvester=args.harvester,
            created_at=args.created_at or "",
            source_document_payloads=source_documents,
            suggestion_payloads=suggestions,
        )
        output_file = Path(args.output_file).resolve()
        write_program_memory_harvest_batch(output_file, harvest)
        review_rows = build_program_memory_harvest_review_rows(harvest)
        if args.review_file:
            review_file = Path(args.review_file).resolve()
            write_csv(
                review_file,
                review_rows,
                fieldnames=list(review_rows[0].keys()) if review_rows else [],
            )
        print(
            json.dumps(
                {
                    "harvest_id": harvest.harvest_id,
                    "harvester": harvest.harvester,
                    "source_document_count": len(harvest.source_documents),
                    "suggestion_count": len(harvest.suggestions),
                    "output_file": str(output_file),
                    "review_file": (
                        str(Path(args.review_file).resolve()) if args.review_file else None
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "program-memory-adjudicate":
        harvest_file = Path(args.harvest_file).resolve()
        decisions_file = Path(args.decisions_file).resolve()
        harvest = load_program_memory_harvest_batch(harvest_file)
        decision_payloads, decision_notes = _load_program_memory_decision_payloads(
            decisions_file
        )
        adjudication = build_program_memory_adjudication_record(
            adjudication_id=args.adjudication_id,
            harvest_id=harvest.harvest_id,
            reviewer=args.reviewer,
            reviewed_at=args.reviewed_at or "",
            decision_payloads=decision_payloads,
            notes=args.notes if args.notes is not None else decision_notes,
        )
        outcome = apply_program_memory_adjudication(harvest, adjudication)
        output_dir = Path(args.output_dir).resolve()
        dataset = write_program_memory_adjudication_outputs(
            output_dir,
            adjudication,
            outcome,
        )
        print(
            json.dumps(
                {
                    "adjudication_id": adjudication.adjudication_id,
                    "harvest_id": adjudication.harvest_id,
                    "reviewer": adjudication.reviewer,
                    "accepted_event_count": len(dataset.events),
                    "accepted_directionality_count": len(
                        dataset.directionality_hypotheses
                    ),
                    "rejected_suggestion_ids": list(outcome.rejected_suggestion_ids),
                    "pending_suggestion_ids": list(outcome.pending_suggestion_ids),
                    "output_dir": str(output_dir),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "program-memory-coverage-audit":
        dataset_dir = Path(args.dataset_dir).resolve()
        output_dir = Path(args.output_dir).resolve()
        audit = build_program_memory_coverage_audit(dataset_dir)
        focus_report = write_program_memory_coverage_outputs(
            output_dir,
            audit,
            target=args.focus_target or "",
            target_class=args.focus_target_class or "",
            domain=args.focus_domain or "",
            failure_scope=args.focus_failure_scope or "",
        )
        print(
            json.dumps(
                {
                    "dataset_dir": audit.dataset_dir,
                    "coverage_manifest_row_count": audit.coverage_manifest[
                        "program_universe_row_count"
                    ],
                    "summary_count": len(audit.summaries),
                    "gap_count": len(audit.gaps),
                    "denominator_summary_count": len(audit.denominator_summary_rows),
                    "denominator_gap_count": len(audit.denominator_gap_rows),
                    "scope_summary_count": len(audit.summaries),
                    "scope_gap_count": len(audit.gaps),
                    "evidence_row_count": len(audit.evidence_rows),
                    "focus_request": dict(focus_report.request),
                    "output_dir": str(output_dir),
                    "coverage_audit_file": str(output_dir / "coverage_audit.json"),
                    "coverage_manifest_file": str(output_dir / "coverage_manifest.json"),
                    "coverage_summary_file": str(output_dir / "coverage_summary.csv"),
                    "coverage_gaps_file": str(output_dir / "coverage_gaps.csv"),
                    "coverage_denominator_summary_file": str(
                        output_dir / "coverage_denominator_summary.csv"
                    ),
                    "coverage_denominator_gaps_file": str(
                        output_dir / "coverage_denominator_gaps.csv"
                    ),
                    "coverage_scope_summary_file": str(
                        output_dir / "coverage_scope_summary.csv"
                    ),
                    "coverage_scope_gaps_file": str(
                        output_dir / "coverage_scope_gaps.csv"
                    ),
                    "coverage_evidence_file": str(output_dir / "coverage_evidence.csv"),
                    "coverage_focus_file": (
                        str(output_dir / "coverage_focus.json")
                        if focus_report.request
                        else None
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "rescue-run-glutamatergic-convergence":
        result = materialize_glutamatergic_convergence_rescue_evaluation(
            output_dir=Path(args.output_dir).resolve(),
            task_card_path=(
                None
                if not args.task_card_path
                else Path(args.task_card_path).resolve()
            ),
            baseline_id=args.baseline_id,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "rescue-compare-baselines":
        result = materialize_rescue_baseline_suite(
            output_dir=Path(args.output_dir).resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "hidden-eval-task-package":
        result = materialize_rescue_hidden_eval_task_package(
            task_id=args.task_id,
            output_dir=Path(args.output_dir).resolve(),
            task_card_path=(
                Path(args.task_card_path).resolve()
                if args.task_card_path
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "hidden-eval-pack-submission":
        result = materialize_hidden_eval_submission_archive(
            task_package_dir=Path(args.task_package_dir).resolve(),
            predictions_file=Path(args.predictions_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            submitter_id=args.submitter_id,
            submission_id=args.submission_id,
            scorer_id=args.scorer_id,
            notes=args.notes or "",
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "hidden-eval-simulate":
        result = materialize_hidden_eval_simulation(
            task_package_dir=Path(args.task_package_dir).resolve(),
            submission_file=Path(args.submission_file).resolve(),
            output_dir=Path(args.output_dir).resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "build-benchmark-snapshot":
        result = materialize_benchmark_snapshot_manifest(
            request_file=Path(args.request_file).resolve(),
            archive_index_file=Path(args.archive_index_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            materialized_at=args.materialized_at,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "build-benchmark-cohort":
        result = materialize_benchmark_cohort_labels(
            manifest=read_benchmark_snapshot_manifest(Path(args.manifest_file).resolve()),
            cohort_members_file=Path(args.cohort_members_file).resolve(),
            future_outcomes_file=Path(args.future_outcomes_file).resolve(),
            output_file=Path(args.output_file).resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "run-benchmark":
        result = materialize_benchmark_run(
            manifest_file=Path(args.manifest_file).resolve(),
            cohort_labels_file=Path(args.cohort_labels_file).resolve(),
            archive_index_file=Path(args.archive_index_file).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            config_file=Path(args.config).resolve() if args.config else None,
            bootstrap_iterations=args.bootstrap_iterations,
            bootstrap_confidence_level=args.bootstrap_confidence_level,
            random_seed=args.random_seed,
            deterministic_test_mode=args.deterministic_test_mode,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "backfill-benchmark-public-slices":
        result = materialize_public_benchmark_slices(
            output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
            benchmark_task_id=args.benchmark_task_id,
            task_registry_path=(
                Path(args.task_registry_path).resolve()
                if args.task_registry_path
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "build-benchmark-reporting":
        result = materialize_benchmark_reporting(
            manifest_file=Path(args.manifest_file).resolve(),
            cohort_labels_file=Path(args.cohort_labels_file).resolve(),
            runner_output_dir=Path(args.runner_output_dir).resolve(),
            output_dir=Path(args.output_dir).resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "run-rescue-npc-signature-reversal":
        result = materialize_npc_signature_reversal_run(
            output_dir=Path(args.output_dir).resolve(),
            task_card_path=(
                Path(args.task_card_path).resolve()
                if args.task_card_path
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "observatory-browse":
        gen_dir = (
            Path(args.generated_dir).resolve() if args.generated_dir else None
        )
        index = build_observatory_index(generated_dir=gen_dir)
        print(format_observatory_index(index))
        return 0

    if args.command == "observatory-leaderboard":
        gen_dir = (
            Path(args.generated_dir).resolve() if args.generated_dir else None
        )
        result = browse_leaderboard(
            entity_type=args.entity_type,
            horizon=args.horizon,
            metric_name=args.metric,
            generated_dir=gen_dir,
        )
        if result is None:
            print(
                f"No leaderboard found for "
                f"{args.entity_type}/{args.horizon}/{args.metric}."
            )
            return 1
        print(format_leaderboard(result))
        return 0

    if args.command == "observatory-report-cards":
        gen_dir = (
            Path(args.generated_dir).resolve() if args.generated_dir else None
        )
        cards = browse_report_cards(generated_dir=gen_dir)
        print(format_report_cards(cards))
        return 0

    if args.command == "observatory-leaderboard-slices":
        gen_dir = (
            Path(args.generated_dir).resolve() if args.generated_dir else None
        )
        slices = list_available_leaderboard_slices(generated_dir=gen_dir)
        if not slices:
            print("No leaderboard slices found. Run benchmark reporting first.")
            return 0
        for sl in slices:
            print(f"{sl.entity_type} / {sl.horizon} / {sl.metric_name}")
        return 0

    if args.command == "observatory-packets":
        packets_file = (
            Path(args.packets_file).resolve() if args.packets_file else None
        )
        packets = list_packets(packets_file=packets_file)
        print(format_packet_list(packets))
        return 0

    if args.command == "observatory-packet-detail":
        packets_file = (
            Path(args.packets_file).resolve() if args.packets_file else None
        )
        detail = browse_packet(args.packet_id, packets_file=packets_file)
        if detail is None:
            print(f"No packet found with id '{args.packet_id}'.")
            return 1
        print(format_packet_detail(detail))
        return 0

    if args.command == "observatory-failure-analogs":
        packets_file = (
            Path(args.packets_file).resolve() if args.packets_file else None
        )
        analogs = list_failure_analogs(packets_file=packets_file)
        print(format_failure_analogs(analogs))
        return 0

    if args.command == "observatory-policy-comparison":
        packets_file = (
            Path(args.packets_file).resolve() if args.packets_file else None
        )
        comparison = browse_policy_comparison(packets_file=packets_file)
        if comparison is None:
            print("No packets available for policy comparison.")
            return 1
        print(format_policy_comparison(comparison))
        return 0

    if args.command == "observatory-rescue-tasks":
        tasks = list_rescue_tasks()
        print(format_rescue_tasks(tasks))
        return 0

    if args.command == "observatory-rescue-evidence":
        packets_file = (
            Path(args.packets_file).resolve() if args.packets_file else None
        )
        evidence = list_rescue_evidence(packets_file=packets_file)
        print(format_rescue_evidence_list(evidence))
        return 0

    if args.command == "build-hypothesis-packets":
        output_file = Path(args.output_file).resolve()
        payload = materialize_hypothesis_packets(
            Path(args.policy_artifact).resolve(),
            Path(args.ledger_artifact).resolve(),
            output_file=output_file,
        )
        print(
            json.dumps(
                {
                    "packet_count": payload["packet_count"],
                    "output_file": str(output_file),
                    "policy_artifact": str(Path(args.policy_artifact).resolve()),
                    "ledger_artifact": str(Path(args.ledger_artifact).resolve()),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "build-expert-review-packets":
        output_dir = Path(args.output_dir).resolve()
        result = materialize_blinded_expert_review_packets(
            Path(args.hypothesis_artifact).resolve(),
            output_dir=output_dir,
            rubric_file=Path(args.rubric_file).resolve() if args.rubric_file else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "register-prospective-prediction":
        output_file = Path(args.output_file).resolve()
        payload = materialize_prospective_prediction_registration(
            Path(args.hypothesis_artifact).resolve(),
            packet_id=args.packet_id,
            output_file=output_file,
            registered_at=args.registered_at,
            registered_by=args.registered_by,
            predicted_outcome=args.predicted_outcome,
            option_probabilities=_parse_option_probability_args(
                list(args.option_probability or [])
            ),
            outcome_window_closes_on=args.outcome_window_closes_on,
            outcome_window_opens_on=args.outcome_window_opens_on,
            rationale=list(args.rationale or []),
            registration_id=args.registration_id,
            notes=args.notes or "",
        )
        print(
            json.dumps(
                {
                    "registration_id": payload["registration_id"],
                    "packet_id": payload["packet_artifact"]["packet_id"],
                    "registered_at": payload["registered_at"],
                    "output_file": str(output_file),
                    "hypothesis_artifact": str(Path(args.hypothesis_artifact).resolve()),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "fetch-opentargets":
        result = fetch_opentargets_baseline(
            output_file=Path(args.output_file).resolve(),
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "fetch-chembl":
        result = fetch_chembl_tractability(
            input_file=Path(args.input_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "fetch-pgc-scz2022":
        result = fetch_pgc_scz2022_prioritized_genes(
            output_file=Path(args.output_file).resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "fetch-schema":
        result = fetch_schema_rare_variant_support(
            input_file=Path(args.input_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            limit=args.limit,
            overrides_file=(
                Path(args.overrides_file).resolve()
                if args.overrides_file
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "fetch-psychencode":
        result = fetch_psychencode_support(
            input_file=Path(args.input_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "fetch-psychencode-modules":
        result = fetch_psychencode_module_table(
            input_file=Path(args.input_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            limit=args.limit,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "build-candidate-registry":
        result = build_candidate_registry(
            opentargets_file=Path(args.opentargets_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            pgc_file=Path(args.pgc_file).resolve() if args.pgc_file else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "refresh-candidate-registry":
        result = refresh_candidate_registry(
            output_file=Path(args.output_file).resolve() if args.output_file else None,
            work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            include_pgc=not args.skip_pgc,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-fetch-opentargets":
        result = fetch_atlas_opentargets_baseline(
            output_file=Path(args.output_file).resolve(),
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            page_size=args.page_size,
            max_pages=args.max_pages,
            raw_dir=Path(args.raw_dir).resolve() if args.raw_dir else None,
            materialized_at=args.materialized_at,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-fetch-pgc-scz2022":
        result = fetch_atlas_pgc_scz2022_prioritized_genes(
            output_file=Path(args.output_file).resolve(),
            raw_dir=Path(args.raw_dir).resolve() if args.raw_dir else None,
            materialized_at=args.materialized_at,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-refresh-candidate-registry":
        result = refresh_atlas_candidate_registry(
            output_file=Path(args.output_file).resolve() if args.output_file else None,
            work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
            raw_dir=Path(args.raw_dir).resolve() if args.raw_dir else None,
            materialized_at=args.materialized_at,
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            include_pgc=not args.skip_pgc,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-build-taxonomy":
        result = materialize_atlas_taxonomy(
            ingest_manifest_file=Path(args.ingest_manifest_file).resolve(),
            output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-build-tensor":
        result = materialize_atlas_tensor(
            ingest_manifest_file=Path(args.ingest_manifest_file).resolve(),
            output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
            taxonomy_dir=Path(args.taxonomy_dir).resolve() if args.taxonomy_dir else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-build-mechanistic-axes":
        result = materialize_mechanistic_axes(
            tensor_manifest_file=Path(args.tensor_manifest_file).resolve(),
            output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "atlas-build-convergence-hubs":
        result = materialize_convergence_hubs(
            tensor_manifest_file=Path(args.tensor_manifest_file).resolve(),
            output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "prepare-gene-table":
        result = prepare_gene_table(
            seed_file=Path(args.seed_file).resolve(),
            output_file=Path(args.output_file).resolve(),
            pgc_file=Path(args.pgc_file).resolve() if args.pgc_file else None,
            schema_file=Path(args.schema_file).resolve() if args.schema_file else None,
            psychencode_file=(
                Path(args.psychencode_file).resolve()
                if args.psychencode_file
                else None
            ),
            opentargets_file=(
                Path(args.opentargets_file).resolve()
                if args.opentargets_file
                else None
            ),
            chembl_file=(
                Path(args.chembl_file).resolve()
                if args.chembl_file
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "refresh-example-gene-table":
        result = refresh_example_gene_table(
            seed_file=Path(args.seed_file).resolve() if args.seed_file else None,
            output_file=Path(args.output_file).resolve() if args.output_file else None,
            work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            overrides_file=(
                Path(args.overrides_file).resolve()
                if args.overrides_file
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "refresh-example-module-table":
        result = refresh_example_module_table(
            gene_file=Path(args.gene_file).resolve() if args.gene_file else None,
            output_file=Path(args.output_file).resolve() if args.output_file else None,
            work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "refresh-example-inputs":
        result = refresh_example_input_tables(
            seed_file=Path(args.seed_file).resolve() if args.seed_file else None,
            gene_output_file=(
                Path(args.gene_output_file).resolve()
                if args.gene_output_file
                else None
            ),
            module_output_file=(
                Path(args.module_output_file).resolve()
                if args.module_output_file
                else None
            ),
            gene_work_dir=(
                Path(args.gene_work_dir).resolve() if args.gene_work_dir else None
            ),
            module_work_dir=(
                Path(args.module_work_dir).resolve()
                if args.module_work_dir
                else None
            ),
            disease_id=args.disease_id,
            disease_query=args.disease_query,
            overrides_file=(
                Path(args.overrides_file).resolve()
                if args.overrides_file
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    config = load_config(args.config)
    repo_root = _resolve_repo_root_from_config_path(config.config_path)
    input_dir = (
        Path(args.input_dir).resolve()
        if args.input_dir
        else (repo_root / "examples" / "v0" / "input").resolve()
    )

    if args.command == "validate":
        result = validate_inputs(config, input_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (repo_root / config.build.output_dir).resolve()
    )
    result = build_outputs(config, input_dir, output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
