from pathlib import Path

from scz_target_engine.cli import build_parser, main


def test_cli_validate_runs() -> None:
    exit_code = main(
        [
            "validate",
            "--config",
            str(Path("config/v0.toml").resolve()),
            "--input-dir",
            str(Path("examples/v0/input").resolve()),
        ]
    )
    assert exit_code == 0


def test_cli_prepare_parser_accepts_pgc_file() -> None:
    args = build_parser().parse_args(
        [
            "prepare-gene-table",
            "--seed-file",
            "seed.csv",
            "--output-file",
            "prepared.csv",
            "--pgc-file",
            "pgc.csv",
            "--schema-file",
            "schema.csv",
            "--psychencode-file",
            "psychencode.csv",
        ]
    )
    assert args.command == "prepare-gene-table"
    assert args.pgc_file == "pgc.csv"
    assert args.schema_file == "schema.csv"
    assert args.psychencode_file == "psychencode.csv"


def test_cli_fetch_pgc_parser_accepts_output_file() -> None:
    args = build_parser().parse_args(
        [
            "fetch-pgc-scz2022",
            "--output-file",
            "pgc.csv",
        ]
    )
    assert args.command == "fetch-pgc-scz2022"
    assert args.output_file == "pgc.csv"


def test_cli_fetch_schema_parser_accepts_input_and_output_files() -> None:
    args = build_parser().parse_args(
        [
            "fetch-schema",
            "--input-file",
            "seed.csv",
            "--output-file",
            "schema.csv",
            "--overrides-file",
            "overrides.csv",
        ]
    )
    assert args.command == "fetch-schema"
    assert args.input_file == "seed.csv"
    assert args.output_file == "schema.csv"
    assert args.overrides_file == "overrides.csv"


def test_cli_fetch_psychencode_parser_accepts_input_and_output_files() -> None:
    args = build_parser().parse_args(
        [
            "fetch-psychencode",
            "--input-file",
            "seed.csv",
            "--output-file",
            "psychencode.csv",
        ]
    )
    assert args.command == "fetch-psychencode"
    assert args.input_file == "seed.csv"
    assert args.output_file == "psychencode.csv"


def test_cli_fetch_psychencode_modules_parser_accepts_input_and_output_files() -> None:
    args = build_parser().parse_args(
        [
            "fetch-psychencode-modules",
            "--input-file",
            "gene_evidence.csv",
            "--output-file",
            "module_evidence.csv",
        ]
    )
    assert args.command == "fetch-psychencode-modules"
    assert args.input_file == "gene_evidence.csv"
    assert args.output_file == "module_evidence.csv"


def test_cli_build_candidate_registry_parser_accepts_source_files() -> None:
    args = build_parser().parse_args(
        [
            "build-candidate-registry",
            "--opentargets-file",
            "opentargets.csv",
            "--pgc-file",
            "pgc.csv",
            "--output-file",
            "candidate_gene_registry.csv",
        ]
    )
    assert args.command == "build-candidate-registry"
    assert args.opentargets_file == "opentargets.csv"
    assert args.pgc_file == "pgc.csv"
    assert args.output_file == "candidate_gene_registry.csv"


def test_cli_refresh_candidate_registry_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "refresh-candidate-registry",
            "--output-file",
            "candidate_gene_registry.csv",
            "--work-dir",
            "data/processed/full_universe_ingest",
            "--disease-id",
            "MONDO_0005090",
            "--skip-pgc",
        ]
    )
    assert args.command == "refresh-candidate-registry"
    assert args.output_file == "candidate_gene_registry.csv"
    assert args.work_dir == "data/processed/full_universe_ingest"
    assert args.disease_id == "MONDO_0005090"
    assert args.skip_pgc is True


def test_cli_refresh_candidate_registry_runs(monkeypatch, tmp_path: Path) -> None:
    output_file = tmp_path / "candidate_gene_registry.csv"
    work_dir = tmp_path / "work"
    calls: dict[str, object] = {}

    def fake_refresh_candidate_registry(
        *,
        output_file: Path | None,
        work_dir: Path | None,
        disease_id: str | None,
        disease_query: str | None,
        include_pgc: bool,
    ) -> dict[str, object]:
        calls["output_file"] = output_file
        calls["work_dir"] = work_dir
        calls["disease_id"] = disease_id
        calls["disease_query"] = disease_query
        calls["include_pgc"] = include_pgc
        return {"published_output_file": str(output_file)}

    monkeypatch.setattr(
        "scz_target_engine.cli.refresh_candidate_registry",
        fake_refresh_candidate_registry,
    )

    exit_code = main(
        [
            "refresh-candidate-registry",
            "--output-file",
            str(output_file),
            "--work-dir",
            str(work_dir),
            "--disease-query",
            "schizophrenia",
        ]
    )

    assert exit_code == 0
    assert calls["output_file"] == output_file.resolve()
    assert calls["work_dir"] == work_dir.resolve()
    assert calls["disease_id"] is None
    assert calls["disease_query"] == "schizophrenia"
    assert calls["include_pgc"] is True


def test_cli_refresh_candidate_registry_runs_without_pgc(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "candidate_gene_registry.csv"
    work_dir = tmp_path / "work"
    calls: dict[str, object] = {}

    def fake_refresh_candidate_registry(
        *,
        output_file: Path | None,
        work_dir: Path | None,
        disease_id: str | None,
        disease_query: str | None,
        include_pgc: bool,
    ) -> dict[str, object]:
        calls["output_file"] = output_file
        calls["work_dir"] = work_dir
        calls["disease_id"] = disease_id
        calls["disease_query"] = disease_query
        calls["include_pgc"] = include_pgc
        return {"published_output_file": str(output_file)}

    monkeypatch.setattr(
        "scz_target_engine.cli.refresh_candidate_registry",
        fake_refresh_candidate_registry,
    )

    exit_code = main(
        [
            "refresh-candidate-registry",
            "--output-file",
            str(output_file),
            "--work-dir",
            str(work_dir),
            "--disease-query",
            "schizophrenia",
            "--skip-pgc",
        ]
    )

    assert exit_code == 0
    assert calls["output_file"] == output_file.resolve()
    assert calls["work_dir"] == work_dir.resolve()
    assert calls["disease_id"] is None
    assert calls["disease_query"] == "schizophrenia"
    assert calls["include_pgc"] is False


def test_cli_refresh_example_gene_table_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "refresh-example-gene-table",
            "--seed-file",
            "seed.csv",
            "--output-file",
            "gene_evidence.csv",
            "--work-dir",
            "data/processed/example",
            "--disease-id",
            "MONDO_0005090",
            "--overrides-file",
            "overrides.csv",
        ]
    )
    assert args.command == "refresh-example-gene-table"
    assert args.seed_file == "seed.csv"
    assert args.output_file == "gene_evidence.csv"
    assert args.work_dir == "data/processed/example"
    assert args.disease_id == "MONDO_0005090"
    assert args.overrides_file == "overrides.csv"


def test_cli_refresh_example_module_table_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "refresh-example-module-table",
            "--gene-file",
            "gene_evidence.csv",
            "--output-file",
            "module_evidence.csv",
            "--work-dir",
            "data/processed/module-example",
        ]
    )
    assert args.command == "refresh-example-module-table"
    assert args.gene_file == "gene_evidence.csv"
    assert args.output_file == "module_evidence.csv"
    assert args.work_dir == "data/processed/module-example"


def test_cli_refresh_example_inputs_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "refresh-example-inputs",
            "--seed-file",
            "gene_seed.csv",
            "--gene-output-file",
            "gene_evidence.csv",
            "--module-output-file",
            "module_evidence.csv",
            "--gene-work-dir",
            "data/processed/example-gene",
            "--module-work-dir",
            "data/processed/example-module",
            "--disease-query",
            "schizophrenia",
        ]
    )
    assert args.command == "refresh-example-inputs"
    assert args.seed_file == "gene_seed.csv"
    assert args.gene_output_file == "gene_evidence.csv"
    assert args.module_output_file == "module_evidence.csv"
    assert args.gene_work_dir == "data/processed/example-gene"
    assert args.module_work_dir == "data/processed/example-module"
    assert args.disease_query == "schizophrenia"


def test_cli_build_benchmark_snapshot_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "build-benchmark-snapshot",
            "--request-file",
            "snapshot_request.json",
            "--archive-index-file",
            "source_archives.json",
            "--output-file",
            "snapshot_manifest.json",
            "--materialized-at",
            "2026-03-28",
        ]
    )
    assert args.command == "build-benchmark-snapshot"
    assert args.request_file == "snapshot_request.json"
    assert args.archive_index_file == "source_archives.json"
    assert args.output_file == "snapshot_manifest.json"
    assert args.materialized_at == "2026-03-28"


def test_cli_build_benchmark_cohort_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "build-benchmark-cohort",
            "--manifest-file",
            "snapshot_manifest.json",
            "--cohort-members-file",
            "cohort_members.csv",
            "--future-outcomes-file",
            "future_outcomes.csv",
            "--output-file",
            "cohort_labels.csv",
        ]
    )
    assert args.command == "build-benchmark-cohort"
    assert args.manifest_file == "snapshot_manifest.json"
    assert args.cohort_members_file == "cohort_members.csv"
    assert args.future_outcomes_file == "future_outcomes.csv"
    assert args.output_file == "cohort_labels.csv"
