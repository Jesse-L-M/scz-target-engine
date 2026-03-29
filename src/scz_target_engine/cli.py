from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scz-target-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--config", required=True)
    validate_parser.add_argument("--input-dir")

    build_parser_ = subparsers.add_parser("build")
    build_parser_.add_argument("--config", required=True)
    build_parser_.add_argument("--input-dir")
    build_parser_.add_argument("--output-dir")

    opentargets_parser = subparsers.add_parser("fetch-opentargets")
    opentargets_parser.add_argument("--output-file", required=True)
    opentargets_parser.add_argument("--disease-id")
    opentargets_parser.add_argument("--disease-query")
    opentargets_parser.add_argument("--page-size", type=int, default=500)
    opentargets_parser.add_argument("--max-pages", type=int)

    chembl_parser = subparsers.add_parser("fetch-chembl")
    chembl_parser.add_argument("--input-file", required=True)
    chembl_parser.add_argument("--output-file", required=True)
    chembl_parser.add_argument("--limit", type=int)

    pgc_parser = subparsers.add_parser("fetch-pgc-scz2022")
    pgc_parser.add_argument("--output-file", required=True)

    schema_parser = subparsers.add_parser("fetch-schema")
    schema_parser.add_argument("--input-file", required=True)
    schema_parser.add_argument("--output-file", required=True)
    schema_parser.add_argument("--limit", type=int)
    schema_parser.add_argument("--overrides-file")

    psychencode_parser = subparsers.add_parser("fetch-psychencode")
    psychencode_parser.add_argument("--input-file", required=True)
    psychencode_parser.add_argument("--output-file", required=True)
    psychencode_parser.add_argument("--limit", type=int)

    psychencode_modules_parser = subparsers.add_parser("fetch-psychencode-modules")
    psychencode_modules_parser.add_argument("--input-file", required=True)
    psychencode_modules_parser.add_argument("--output-file", required=True)
    psychencode_modules_parser.add_argument("--limit", type=int)

    registry_parser = subparsers.add_parser("build-candidate-registry")
    registry_parser.add_argument("--opentargets-file", required=True)
    registry_parser.add_argument("--output-file", required=True)
    registry_parser.add_argument("--pgc-file")

    registry_refresh_parser = subparsers.add_parser("refresh-candidate-registry")
    registry_refresh_parser.add_argument("--output-file")
    registry_refresh_parser.add_argument("--work-dir")
    registry_refresh_parser.add_argument("--disease-id")
    registry_refresh_parser.add_argument("--disease-query")
    registry_refresh_parser.add_argument("--skip-pgc", action="store_true")

    prepare_parser = subparsers.add_parser("prepare-gene-table")
    prepare_parser.add_argument("--seed-file", required=True)
    prepare_parser.add_argument("--output-file", required=True)
    prepare_parser.add_argument("--pgc-file")
    prepare_parser.add_argument("--schema-file")
    prepare_parser.add_argument("--psychencode-file")
    prepare_parser.add_argument("--opentargets-file")
    prepare_parser.add_argument("--chembl-file")

    refresh_parser = subparsers.add_parser("refresh-example-gene-table")
    refresh_parser.add_argument("--seed-file")
    refresh_parser.add_argument("--output-file")
    refresh_parser.add_argument("--work-dir")
    refresh_parser.add_argument("--disease-id")
    refresh_parser.add_argument("--disease-query")
    refresh_parser.add_argument("--overrides-file")

    refresh_module_parser = subparsers.add_parser("refresh-example-module-table")
    refresh_module_parser.add_argument("--gene-file")
    refresh_module_parser.add_argument("--output-file")
    refresh_module_parser.add_argument("--work-dir")

    refresh_inputs_parser = subparsers.add_parser("refresh-example-inputs")
    refresh_inputs_parser.add_argument("--seed-file")
    refresh_inputs_parser.add_argument("--gene-output-file")
    refresh_inputs_parser.add_argument("--module-output-file")
    refresh_inputs_parser.add_argument("--gene-work-dir")
    refresh_inputs_parser.add_argument("--module-work-dir")
    refresh_inputs_parser.add_argument("--disease-id")
    refresh_inputs_parser.add_argument("--disease-query")
    refresh_inputs_parser.add_argument("--overrides-file")

    benchmark_snapshot_parser = subparsers.add_parser("build-benchmark-snapshot")
    benchmark_snapshot_parser.add_argument("--request-file", required=True)
    benchmark_snapshot_parser.add_argument("--archive-index-file", required=True)
    benchmark_snapshot_parser.add_argument("--output-file", required=True)
    benchmark_snapshot_parser.add_argument("--materialized-at", required=True)

    benchmark_cohort_parser = subparsers.add_parser("build-benchmark-cohort")
    benchmark_cohort_parser.add_argument("--manifest-file", required=True)
    benchmark_cohort_parser.add_argument("--cohort-members-file", required=True)
    benchmark_cohort_parser.add_argument("--future-outcomes-file", required=True)
    benchmark_cohort_parser.add_argument("--output-file", required=True)

    benchmark_run_parser = subparsers.add_parser("run-benchmark")
    benchmark_run_parser.add_argument("--manifest-file", required=True)
    benchmark_run_parser.add_argument("--cohort-labels-file", required=True)
    benchmark_run_parser.add_argument("--archive-index-file", required=True)
    benchmark_run_parser.add_argument("--output-dir", required=True)
    benchmark_run_parser.add_argument("--config")
    benchmark_run_parser.add_argument("--bootstrap-iterations", type=int)
    benchmark_run_parser.add_argument(
        "--bootstrap-confidence-level",
        type=float,
        default=0.95,
    )
    benchmark_run_parser.add_argument("--random-seed", type=int, default=17)
    benchmark_run_parser.add_argument(
        "--deterministic-test-mode",
        action="store_true",
    )

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
    config_dir = config.config_path.parent
    input_dir = (
        Path(args.input_dir).resolve()
        if args.input_dir
        else (config_dir.parent / "examples" / "v0" / "input").resolve()
    )

    if args.command == "validate":
        result = validate_inputs(config, input_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (config_dir.parent / config.build.output_dir).resolve()
    )
    result = build_outputs(config, input_dir, output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
