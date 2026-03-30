from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Callable

import scz_target_engine.benchmark_runner as benchmark_runner_module

from scz_target_engine.atlas.ingest import refresh_atlas_candidate_registry
from scz_target_engine.atlas.sources import (
    fetch_atlas_opentargets_baseline,
    fetch_atlas_pgc_scz2022_prioritized_genes,
)
from scz_target_engine.atlas.taxonomy import materialize_atlas_taxonomy
from scz_target_engine.atlas.tensor import materialize_atlas_tensor
from scz_target_engine.benchmark_backfill import materialize_public_benchmark_slices
from scz_target_engine.benchmark_labels import materialize_benchmark_cohort_labels
from scz_target_engine.benchmark_runner import materialize_benchmark_run
from scz_target_engine.benchmark_snapshots import (
    materialize_benchmark_snapshot_manifest,
    read_benchmark_snapshot_manifest,
)
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs, validate_inputs
from scz_target_engine.ingest import refresh_candidate_registry
from scz_target_engine.prepare import (
    prepare_gene_table,
    refresh_example_gene_table,
    refresh_example_input_tables,
    refresh_example_module_table,
)
from scz_target_engine.registry import build_candidate_registry
from scz_target_engine.sources.chembl import fetch_chembl_tractability
from scz_target_engine.sources.opentargets import fetch_opentargets_baseline
from scz_target_engine.sources.pgc import fetch_pgc_scz2022_prioritized_genes
from scz_target_engine.sources.psychencode import (
    fetch_psychencode_module_table,
    fetch_psychencode_support,
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


COMMAND_ROUTES = (
    CommandRoute("validate", ("engine", "validate"), _configure_validate_parser),
    CommandRoute("build", ("engine", "build"), _configure_build_parser),
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


def _materialize_benchmark_run_with_manifest_provenance(
    *,
    manifest_file: Path,
    cohort_labels_file: Path,
    archive_index_file: Path,
    output_dir: Path,
    config_file: Path | None,
    bootstrap_iterations: int | None,
    bootstrap_confidence_level: float,
    random_seed: int,
    deterministic_test_mode: bool,
) -> dict[str, object]:
    try:
        manifest = read_benchmark_snapshot_manifest(manifest_file)
    except (FileNotFoundError, ValueError):
        manifest = None
    task_registry_path = None
    if manifest is not None and getattr(manifest, "task_registry_path", ""):
        task_registry_path = Path(manifest.task_registry_path).resolve()
    if task_registry_path is None:
        return materialize_benchmark_run(
            manifest_file=manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=archive_index_file,
            output_dir=output_dir,
            config_file=config_file,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence_level=bootstrap_confidence_level,
            random_seed=random_seed,
            deterministic_test_mode=deterministic_test_mode,
        )

    original_resolve = benchmark_runner_module.resolve_benchmark_task_contract
    original_read_manifest = benchmark_runner_module.read_benchmark_snapshot_manifest

    def _resolve_with_registry_provenance(
        *,
        benchmark_task_id: str | None = None,
        benchmark_question_id: str | None = None,
        benchmark_suite_id: str | None = None,
        task_registry_path: Path | None = None,
    ):
        return original_resolve(
            benchmark_task_id=benchmark_task_id,
            benchmark_question_id=benchmark_question_id,
            benchmark_suite_id=benchmark_suite_id,
            task_registry_path=(
                task_registry_path
                if task_registry_path is not None
                else task_registry_path_from_manifest
            ),
        )

    def _read_manifest_with_registry_provenance(
        path: Path,
        *,
        task_registry_path: Path | None = None,
    ):
        return original_read_manifest(
            path,
            task_registry_path=(
                task_registry_path
                if task_registry_path is not None
                else task_registry_path_from_manifest
            ),
        )

    task_registry_path_from_manifest = task_registry_path
    benchmark_runner_module.resolve_benchmark_task_contract = (
        _resolve_with_registry_provenance
    )
    benchmark_runner_module.read_benchmark_snapshot_manifest = (
        _read_manifest_with_registry_provenance
    )
    try:
        return materialize_benchmark_run(
            manifest_file=manifest_file,
            cohort_labels_file=cohort_labels_file,
            archive_index_file=archive_index_file,
            output_dir=output_dir,
            config_file=config_file,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence_level=bootstrap_confidence_level,
            random_seed=random_seed,
            deterministic_test_mode=deterministic_test_mode,
        )
    finally:
        benchmark_runner_module.resolve_benchmark_task_contract = original_resolve
        benchmark_runner_module.read_benchmark_snapshot_manifest = original_read_manifest


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
        result = _materialize_benchmark_run_with_manifest_provenance(
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
