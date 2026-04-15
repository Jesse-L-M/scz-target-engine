import json
import importlib
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.cli import build_parser, main
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs


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


@pytest.mark.parametrize(
    ("argv", "expected_command", "expected_command_path"),
    [
        (
            [
                "engine",
                "validate",
                "--config",
                "config/v0.toml",
            ],
            "validate",
            ("engine", "validate"),
        ),
        (
            [
                "hypothesis-lab",
                "build-packets",
                "--policy-artifact",
                "policy_decision_vectors_v2.json",
                "--ledger-artifact",
                "gene_target_ledgers.json",
                "--output-file",
                "hypothesis_packets_v1.json",
            ],
            "build-hypothesis-packets",
            ("hypothesis-lab", "build-packets"),
        ),
        (
            [
                "hypothesis-lab",
                "build-expert-review",
                "--hypothesis-artifact",
                "hypothesis_packets_v1.json",
                "--output-dir",
                "examples/expert_review",
            ],
            "build-expert-review-packets",
            ("hypothesis-lab", "build-expert-review"),
        ),
        (
            [
                "challenge",
                "prospective",
                "register",
                "--hypothesis-artifact",
                "hypothesis_packets_v1.json",
                "--packet-id",
                "ENSG00000180720__acute_translation_guardrails_v1",
                "--output-file",
                "prospective_prediction_registration.json",
                "--registered-at",
                "2026-03-31T00:00:00Z",
                "--registered-by",
                "repo_checked_in_example",
                "--predicted-outcome",
                "advance",
                "--option-probability",
                "advance=0.58",
                "--option-probability",
                "hold=0.32",
                "--option-probability",
                "kill=0.10",
                "--outcome-window-closes-on",
                "2027-12-31",
                "--rationale",
                "Explicit offsetting evidence keeps the packet live.",
            ],
            "register-prospective-prediction",
            ("challenge", "prospective", "register"),
        ),
        (
            [
                "program-memory",
                "harvest",
                "--input-file",
                "raw_harvest.json",
                "--output-file",
                "harvest.json",
                "--harvest-id",
                "example-curation",
                "--harvester",
                "llm-assist",
            ],
            "program-memory-harvest",
            ("program-memory", "harvest"),
        ),
        (
            [
                "program-memory",
                "adjudicate",
                "--harvest-file",
                "harvest.json",
                "--decisions-file",
                "decisions.json",
                "--output-dir",
                "adjudicated",
                "--adjudication-id",
                "example-curation-review",
                "--reviewer",
                "curator@example.com",
            ],
            "program-memory-adjudicate",
            ("program-memory", "adjudicate"),
        ),
        (
            [
                "program-memory",
                "coverage-audit",
                "--output-dir",
                "coverage",
            ],
            "program-memory-coverage-audit",
            ("program-memory", "coverage-audit"),
        ),
        (
            [
                "program-memory",
                "harvest-program",
                "--program-id",
                "karxt",
                "--output-dir",
                "harvest_bundle",
            ],
            "program-memory-harvest-program",
            ("program-memory", "harvest-program"),
        ),
        (
            [
                "program-memory",
                "adjudicate-program",
                "--harvest-dir",
                "harvest_bundle",
                "--output-dir",
                "adjudicated_bundle",
                "--adjudication-id",
                "karxt_review_v1",
                "--reviewer",
                "reviewer@example.com",
            ],
            "program-memory-adjudicate-program",
            ("program-memory", "adjudicate-program"),
        ),
        (
            [
                "program-memory",
                "build-insight-packet",
                "--program-dir",
                "adjudicated_bundle",
                "--output-file",
                "packet.json",
                "--packet-id",
                "karxt_packet_v1",
            ],
            "program-memory-build-insight-packet",
            ("program-memory", "build-insight-packet"),
        ),
        (
            [
                "benchmark",
                "backfill",
                "public-slices",
                "--output-dir",
                "data/benchmark/public_slices",
            ],
            "backfill-benchmark-public-slices",
            ("benchmark", "backfill", "public-slices"),
        ),
        (
            [
                "benchmark",
                "snapshot",
                "--request-file",
                "snapshot_request.json",
                "--archive-index-file",
                "source_archives.json",
                "--output-file",
                "snapshot_manifest.json",
                "--materialized-at",
                "2026-03-28",
            ],
            "build-benchmark-snapshot",
            ("benchmark", "snapshot"),
        ),
        (
            [
                "benchmark",
                "reporting",
                "--manifest-file",
                "snapshot_manifest.json",
                "--cohort-labels-file",
                "cohort_labels.csv",
                "--runner-output-dir",
                "runner_outputs",
                "--output-dir",
                "public_payloads",
            ],
            "build-benchmark-reporting",
            ("benchmark", "reporting"),
        ),
        (
            [
                "sources",
                "psychencode",
                "modules",
                "--input-file",
                "gene_evidence.csv",
                "--output-file",
                "module_evidence.csv",
            ],
            "fetch-psychencode-modules",
            ("sources", "psychencode", "modules"),
        ),
        (
            [
                "registry",
                "refresh",
                "--output-file",
                "candidate_gene_registry.csv",
            ],
            "refresh-candidate-registry",
            ("registry", "refresh"),
        ),
        (
            [
                "atlas",
                "ingest",
                "candidate-registry",
                "--output-file",
                "candidate_gene_registry.csv",
            ],
            "atlas-refresh-candidate-registry",
            ("atlas", "ingest", "candidate-registry"),
        ),
        (
            [
                "atlas",
                "build",
                "taxonomy",
                "--ingest-manifest-file",
                "atlas_manifest.json",
            ],
            "atlas-build-taxonomy",
            ("atlas", "build", "taxonomy"),
        ),
        (
            [
                "atlas",
                "build",
                "tensor",
                "--ingest-manifest-file",
                "atlas_manifest.json",
            ],
            "atlas-build-tensor",
            ("atlas", "build", "tensor"),
        ),
        (
            [
                "atlas",
                "build",
                "mechanistic-axes",
                "--tensor-manifest-file",
                "tensor_manifest.json",
            ],
            "atlas-build-mechanistic-axes",
            ("atlas", "build", "mechanistic-axes"),
        ),
        (
            [
                "atlas",
                "build",
                "convergence-hubs",
                "--tensor-manifest-file",
                "tensor_manifest.json",
            ],
            "atlas-build-convergence-hubs",
            ("atlas", "build", "convergence-hubs"),
        ),
        (
            [
                "rescue",
                "run",
                "glutamatergic-convergence",
                "--output-dir",
                "rescue_outputs",
            ],
            "rescue-run-glutamatergic-convergence",
            ("rescue", "run", "glutamatergic-convergence"),
        ),
        (
            [
                "rescue",
                "compare",
                "baselines",
                "--output-dir",
                "rescue_baselines",
            ],
            "rescue-compare-baselines",
            ("rescue", "compare", "baselines"),
        ),
        (
            [
                "hidden-eval",
                "task-package",
                "--task-id",
                "glutamatergic_convergence_rescue_task",
                "--output-dir",
                "hidden_eval_task",
            ],
            "hidden-eval-task-package",
            ("hidden-eval", "task-package"),
        ),
        (
            [
                "hidden-eval",
                "pack-submission",
                "--task-package-dir",
                "hidden_eval_task",
                "--predictions-file",
                "ranked_predictions.csv",
                "--output-file",
                "submission.tar.gz",
                "--submitter-id",
                "partner-demo",
                "--submission-id",
                "demo-v1",
                "--scorer-id",
                "demo-model",
            ],
            "hidden-eval-pack-submission",
            ("hidden-eval", "pack-submission"),
        ),
        (
            [
                "hidden-eval",
                "simulate",
                "--task-package-dir",
                "hidden_eval_task",
                "--submission-file",
                "submission.tar.gz",
                "--output-dir",
                "hidden_eval_run",
            ],
            "hidden-eval-simulate",
            ("hidden-eval", "simulate"),
        ),
    ],
)
def test_cli_namespaced_routes_map_to_legacy_commands(
    argv: list[str],
    expected_command: str,
    expected_command_path: tuple[str, ...],
) -> None:
    args = build_parser().parse_args(argv)
    assert args.command == expected_command
    assert args.command_path == expected_command_path


def test_cli_namespaced_validate_runs_with_mirrored_config() -> None:
    exit_code = main(
        [
            "engine",
            "validate",
            "--config",
            str(Path("config/engine/v0.toml").resolve()),
        ]
    )
    assert exit_code == 0


def test_cli_build_hypothesis_packets_runs(tmp_path: Path) -> None:
    config = load_config(Path("config/v0.toml"))
    build_dir = tmp_path / "build"
    build_outputs(
        config,
        Path("examples/v0/input").resolve(),
        build_dir,
    )

    exit_code = main(
        [
            "build-hypothesis-packets",
            "--policy-artifact",
            str((build_dir / "policy_decision_vectors_v2.json").resolve()),
            "--ledger-artifact",
            str((build_dir / "gene_target_ledgers.json").resolve()),
            "--output-file",
            str((tmp_path / "cli_hypothesis_packets_v1.json").resolve()),
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "cli_hypothesis_packets_v1.json").exists()


def test_cli_build_expert_review_packets_runs(tmp_path: Path) -> None:
    exit_code = main(
        [
            "build-expert-review-packets",
            "--hypothesis-artifact",
            str(Path("examples/v0/output/hypothesis_packets_v1.json").resolve()),
            "--output-dir",
            str((tmp_path / "expert_review").resolve()),
            "--rubric-file",
            str(
                Path(
                    "docs/review_rubrics/blinded_expert_review_rubric.json"
                ).resolve()
            ),
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "expert_review" / "blinded_expert_review_packets_v1.json").exists()


def test_cli_register_prospective_prediction_runs(tmp_path: Path) -> None:
    output_file = tmp_path / "prospective_prediction_registration.json"

    exit_code = main(
        [
            "register-prospective-prediction",
            "--hypothesis-artifact",
            str(Path("examples/v0/output/hypothesis_packets_v1.json").resolve()),
            "--packet-id",
            "ENSG00000180720__acute_translation_guardrails_v1",
            "--output-file",
            str(output_file.resolve()),
            "--registered-at",
            "2026-03-31T00:00:00Z",
            "--registered-by",
            "repo_checked_in_example",
            "--predicted-outcome",
            "advance",
            "--option-probability",
            "advance=0.58",
            "--option-probability",
            "hold=0.32",
            "--option-probability",
            "kill=0.10",
            "--outcome-window-closes-on",
            "2027-12-31",
            "--rationale",
            "The packet carries a scoreable available policy signal and explicit nonfailure offsetting evidence.",
            "--rationale",
            "Contradiction and replay uncertainty remain live, so the forecast still assigns substantial hold and kill mass.",
            "--registration-id",
            "forecast_chrm4_acute_translation_guardrails_2026_03_31",
        ]
    )

    assert exit_code == 0
    artifact = load_artifact(
        output_file,
        artifact_name="prospective_prediction_registration",
    )
    assert artifact.artifact_name == "prospective_prediction_registration"
    assert artifact.payload.registration_id == (
        "forecast_chrm4_acute_translation_guardrails_2026_03_31"
    )


def test_cli_register_prospective_prediction_rejects_existing_output_file(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "prospective_prediction_registration.json"

    exit_code = main(
        [
            "register-prospective-prediction",
            "--hypothesis-artifact",
            str(Path("examples/v0/output/hypothesis_packets_v1.json").resolve()),
            "--packet-id",
            "ENSG00000180720__acute_translation_guardrails_v1",
            "--output-file",
            str(output_file.resolve()),
            "--registered-at",
            "2026-03-31T00:00:00Z",
            "--registered-by",
            "repo_checked_in_example",
            "--predicted-outcome",
            "advance",
            "--option-probability",
            "advance=0.58",
            "--option-probability",
            "hold=0.32",
            "--option-probability",
            "kill=0.10",
            "--outcome-window-closes-on",
            "2027-12-31",
            "--rationale",
            "The packet carries a scoreable available policy signal and explicit nonfailure offsetting evidence.",
            "--rationale",
            "Contradiction and replay uncertainty remain live, so the forecast still assigns substantial hold and kill mass.",
            "--registration-id",
            "forecast_chrm4_acute_translation_guardrails_2026_03_31",
        ]
    )
    assert exit_code == 0

    with pytest.raises(ValueError, match="prospective registrations are immutable"):
        main(
            [
                "register-prospective-prediction",
                "--hypothesis-artifact",
                str(Path("examples/v0/output/hypothesis_packets_v1.json").resolve()),
                "--packet-id",
                "ENSG00000180720__acute_translation_guardrails_v1",
                "--output-file",
                str(output_file.resolve()),
                "--registered-at",
                "2026-03-31T00:00:00Z",
                "--registered-by",
                "repo_checked_in_example",
                "--predicted-outcome",
                "advance",
                "--option-probability",
                "advance=0.58",
                "--option-probability",
                "hold=0.32",
                "--option-probability",
                "kill=0.10",
                "--outcome-window-closes-on",
                "2027-12-31",
                "--rationale",
                "The packet carries a scoreable available policy signal and explicit nonfailure offsetting evidence.",
                "--rationale",
                "Contradiction and replay uncertainty remain live, so the forecast still assigns substantial hold and kill mass.",
                "--registration-id",
                "forecast_chrm4_acute_translation_guardrails_2026_03_31",
            ]
        )


def test_cli_program_memory_v3_karxt_workflow_runs(tmp_path: Path) -> None:
    harvest_dir = tmp_path / "harvest"
    adjudicated_dir = tmp_path / "adjudicated"
    insight_packet_path = tmp_path / "packet" / "insight_packet.json"

    harvest_exit_code = main(
        [
            "program-memory",
            "harvest-program",
            "--program-id",
            "karxt",
            "--output-dir",
            str(harvest_dir.resolve()),
            "--program-label",
            "KarXT",
            "--materialized-at",
            "2026-04-12",
            "--corpus-tier",
            "A",
        ]
    )
    assert harvest_exit_code == 0
    assert (harvest_dir / "source_manifest.json").exists()

    adjudicate_exit_code = main(
        [
            "program-memory",
            "adjudicate-program",
            "--harvest-dir",
            str(harvest_dir.resolve()),
            "--output-dir",
            str(adjudicated_dir.resolve()),
            "--adjudication-id",
            "karxt_review_v1",
            "--reviewer",
            "reviewer@example.com",
            "--reviewed-at",
            "2026-04-12",
        ]
    )
    assert adjudicate_exit_code == 0
    assert (adjudicated_dir / "program_card.json").exists()
    assert (adjudicated_dir / "contradictions.csv").exists()

    packet_exit_code = main(
        [
            "program-memory",
            "build-insight-packet",
            "--program-dir",
            str(adjudicated_dir.resolve()),
            "--output-file",
            str(insight_packet_path.resolve()),
            "--packet-id",
            "karxt-acute-efficacy-tolerability",
            "--packet-question",
            (
                "What should the public KarXT schizophrenia evidence update about "
                "acute efficacy, tolerability burden, and what is molecule-specific "
                "vs mechanism-general?"
            ),
            "--scope-summary",
            "Single-program KarXT schizophrenia pilot packet.",
            "--generated-at",
            "2026-04-12",
        ]
    )
    assert packet_exit_code == 0
    assert insight_packet_path.exists()

    source_manifest = json.loads(
        (harvest_dir / "source_manifest.json").read_text(encoding="utf-8")
    )
    study_index_lines = (
        harvest_dir / "study_index.csv"
    ).read_text(encoding="utf-8").strip().splitlines()
    claims_lines = (
        adjudicated_dir / "claims.csv"
    ).read_text(encoding="utf-8").strip().splitlines()
    packet_payload = json.loads(insight_packet_path.read_text(encoding="utf-8"))

    assert source_manifest["program_id"] == "xanomeline-trospium-schizophrenia"
    assert source_manifest["source_document_count"] >= 10
    assert any(
        document["capture_method"] == "clinicaltrials_gov_api_v2_json"
        for document in source_manifest["source_documents"]
    )
    assert any(
        document["capture_method"] == "pubmed_efetch_xml"
        for document in source_manifest["source_documents"]
    )
    assert any(
        document["capture_method"] == "url_seed_record"
        for document in source_manifest["source_documents"]
    )
    assert all(
        document["captured_at"] != source_manifest["materialized_at"]
        for document in source_manifest["source_documents"]
    )
    assert len(study_index_lines) > 1
    assert len(claims_lines) > 1
    assert packet_payload["candidate_insights"]


def test_cli_program_memory_v3_unknown_program_fails_closed_without_seed_mode(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="curated pilot registry"):
        main(
            [
                "program-memory",
                "harvest-program",
                "--program-id",
                "future-muscarinic-program",
                "--program-label",
                "Future Muscarinic Program",
                "--output-dir",
                str((tmp_path / "harvest").resolve()),
                "--source-url",
                "https://example.com/future-muscarinic-program",
                "--materialized-at",
                "2026-04-12",
            ]
        )


def test_cli_program_memory_v3_seed_mode_allows_unknown_program_bootstrap(
    tmp_path: Path,
) -> None:
    harvest_dir = tmp_path / "harvest"

    exit_code = main(
        [
            "program-memory",
            "harvest-program",
            "--program-id",
            "future-muscarinic-program",
            "--program-label",
            "Future Muscarinic Program",
            "--output-dir",
            str(harvest_dir.resolve()),
            "--source-url",
            "https://example.com/future-muscarinic-program",
            "--materialized-at",
            "2026-04-12",
            "--seed-mode",
        ]
    )

    source_manifest = json.loads(
        (harvest_dir / "source_manifest.json").read_text(encoding="utf-8")
    )

    assert exit_code == 0
    assert source_manifest["seed_mode"] is True
    assert source_manifest["source_documents"][0]["capture_method"] == "url_seed_record"


def test_cli_build_expert_review_packets_rejects_reserved_required_finding(
    tmp_path: Path,
) -> None:
    rubric_path = tmp_path / "reserved_required_finding_rubric.json"
    rubric_path.write_text(
        json.dumps(
            {
                "rubric_id": "cli_reserved_required_finding_v1",
                "review_goal": "Fail when CLI receives a reserved response-template field.",
                "comparison_prompt": "This rubric should fail validation.",
                "dimensions": [
                    {
                        "dimension_id": "clarity",
                        "label": "Clarity",
                        "question": "Is the packet easy to read?",
                        "scale_min": 1,
                        "scale_max": 5,
                        "low_anchor": "No.",
                        "high_anchor": "Yes.",
                    }
                ],
                "required_findings": ["comparison_id"],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"reserved response-template fields: comparison_id",
    ):
        main(
            [
                "build-expert-review-packets",
                "--hypothesis-artifact",
                str(Path("examples/v0/output/hypothesis_packets_v1.json").resolve()),
                "--output-dir",
                str((tmp_path / "expert_review").resolve()),
                "--rubric-file",
                str(rubric_path.resolve()),
            ]
        )


@pytest.mark.parametrize(
    ("namespaced_module_name", "legacy_module_name", "symbol_names"),
    [
        (
            "scz_target_engine.app.cli",
            "scz_target_engine.cli",
            ("build_parser", "main"),
        ),
        (
            "scz_target_engine.benchmark.backfill",
            "scz_target_engine.benchmark_backfill",
            ("materialize_public_benchmark_slices", "plan_public_benchmark_slices"),
        ),
        (
            "scz_target_engine.benchmark.leaderboard",
            "scz_target_engine.benchmark_leaderboard",
            (
                "materialize_benchmark_reporting",
                "read_benchmark_leaderboard_payload",
            ),
        ),
        (
            "scz_target_engine.benchmark.labels",
            "scz_target_engine.benchmark_labels",
            ("materialize_benchmark_cohort_labels", "read_benchmark_cohort_labels"),
        ),
        (
            "scz_target_engine.benchmark.metrics",
            "scz_target_engine.benchmark_metrics",
            ("calculate_metric_values", "read_benchmark_metric_output_payload"),
        ),
        (
            "scz_target_engine.benchmark.protocol",
            "scz_target_engine.benchmark_protocol",
            ("BenchmarkSnapshotManifest", "FROZEN_BENCHMARK_PROTOCOL"),
        ),
        (
            "scz_target_engine.benchmark.registry",
            "scz_target_engine.benchmark_registry",
            ("load_benchmark_task_contracts", "resolve_benchmark_task_contract"),
        ),
        (
            "scz_target_engine.benchmark.runner",
            "scz_target_engine.benchmark_runner",
            ("materialize_benchmark_run", "_deterministic_random_score"),
        ),
        (
            "scz_target_engine.benchmark.snapshots",
            "scz_target_engine.benchmark_snapshots",
            ("materialize_benchmark_snapshot_manifest", "read_benchmark_snapshot_manifest"),
        ),
        (
            "scz_target_engine.core.config",
            "scz_target_engine.config",
            ("EngineConfig", "load_config"),
        ),
        (
            "scz_target_engine.core.identity",
            "scz_target_engine.identity",
            ("SourceIdentityMatch", "build_gene_identity_fields"),
        ),
        (
            "scz_target_engine.core.io",
            "scz_target_engine.io",
            ("read_csv_rows", "write_json"),
        ),
        (
            "scz_target_engine.domain.decision_vector",
            "scz_target_engine.decision_vector",
            ("build_decision_vector", "serialize_decision_vector"),
        ),
        (
            "scz_target_engine.domain.ledger",
            "scz_target_engine.ledger",
            ("TargetLedger", "build_target_ledgers"),
        ),
        (
            "scz_target_engine.domain.reporting",
            "scz_target_engine.reporting",
            ("build_cards_markdown", "ranked_entities_to_rows"),
        ),
        (
            "scz_target_engine.domain.scoring",
            "scz_target_engine.scoring",
            ("RankedEntity", "rank_records"),
        ),
        (
            "scz_target_engine.workflows.engine",
            "scz_target_engine.engine",
            ("build_outputs", "validate_inputs"),
        ),
        (
            "scz_target_engine.workflows.ingest",
            "scz_target_engine.ingest",
            ("refresh_candidate_gene_registry", "refresh_candidate_registry"),
        ),
        (
            "scz_target_engine.workflows.prepare",
            "scz_target_engine.prepare",
            ("prepare_gene_table", "refresh_example_input_tables"),
        ),
        (
            "scz_target_engine.workflows.registry",
            "scz_target_engine.registry",
            ("build_candidate_gene_registry", "build_candidate_registry"),
        ),
    ],
)
def test_namespaced_modules_reexport_legacy_symbols(
    namespaced_module_name: str,
    legacy_module_name: str,
    symbol_names: tuple[str, ...],
) -> None:
    namespaced_module = importlib.import_module(namespaced_module_name)
    legacy_module = importlib.import_module(legacy_module_name)

    for symbol_name in symbol_names:
        assert getattr(namespaced_module, symbol_name) is getattr(
            legacy_module,
            symbol_name,
        )


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


def test_cli_atlas_refresh_candidate_registry_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "atlas",
            "ingest",
            "candidate-registry",
            "--output-file",
            "candidate_gene_registry.csv",
            "--work-dir",
            "data/processed/atlas/full_universe_ingest",
            "--raw-dir",
            "data/raw/sources",
            "--materialized-at",
            "2026-03-30",
            "--disease-id",
            "MONDO_0005090",
            "--skip-pgc",
        ]
    )
    assert args.command == "atlas-refresh-candidate-registry"
    assert args.output_file == "candidate_gene_registry.csv"
    assert args.work_dir == "data/processed/atlas/full_universe_ingest"
    assert args.raw_dir == "data/raw/sources"
    assert args.materialized_at == "2026-03-30"
    assert args.disease_id == "MONDO_0005090"
    assert args.skip_pgc is True


def test_cli_atlas_refresh_candidate_registry_runs(monkeypatch, tmp_path: Path) -> None:
    output_file = tmp_path / "candidate_gene_registry.csv"
    work_dir = tmp_path / "work"
    raw_dir = tmp_path / "raw"
    calls: dict[str, object] = {}

    def fake_refresh_atlas_candidate_registry(
        *,
        output_file: Path | None,
        work_dir: Path | None,
        raw_dir: Path | None,
        materialized_at: str | None,
        disease_id: str | None,
        disease_query: str | None,
        include_pgc: bool,
    ) -> dict[str, object]:
        calls["output_file"] = output_file
        calls["work_dir"] = work_dir
        calls["raw_dir"] = raw_dir
        calls["materialized_at"] = materialized_at
        calls["disease_id"] = disease_id
        calls["disease_query"] = disease_query
        calls["include_pgc"] = include_pgc
        return {"published_output_file": str(output_file)}

    monkeypatch.setattr(
        "scz_target_engine.cli.refresh_atlas_candidate_registry",
        fake_refresh_atlas_candidate_registry,
    )

    exit_code = main(
        [
            "atlas",
            "ingest",
            "candidate-registry",
            "--output-file",
            str(output_file),
            "--work-dir",
            str(work_dir),
            "--raw-dir",
            str(raw_dir),
            "--materialized-at",
            "2026-03-30",
            "--disease-query",
            "schizophrenia",
        ]
    )

    assert exit_code == 0
    assert calls["output_file"] == output_file.resolve()
    assert calls["work_dir"] == work_dir.resolve()
    assert calls["raw_dir"] == raw_dir.resolve()
    assert calls["materialized_at"] == "2026-03-30"
    assert calls["disease_id"] is None
    assert calls["disease_query"] == "schizophrenia"
    assert calls["include_pgc"] is True


def test_cli_atlas_build_taxonomy_parser_accepts_optional_output_dir() -> None:
    args = build_parser().parse_args(
        [
            "atlas",
            "build",
            "taxonomy",
            "--ingest-manifest-file",
            "data/curated/atlas/example_ingest_manifest.json",
            "--output-dir",
            "data/curated/atlas/taxonomy",
        ]
    )
    assert args.command == "atlas-build-taxonomy"
    assert args.ingest_manifest_file == "data/curated/atlas/example_ingest_manifest.json"
    assert args.output_dir == "data/curated/atlas/taxonomy"


def test_cli_atlas_build_tensor_parser_accepts_optional_taxonomy_dir() -> None:
    args = build_parser().parse_args(
        [
            "atlas",
            "build",
            "tensor",
            "--ingest-manifest-file",
            "data/curated/atlas/example_ingest_manifest.json",
            "--output-dir",
            "data/curated/atlas/tensor",
            "--taxonomy-dir",
            "data/curated/atlas/tensor/taxonomy",
        ]
    )
    assert args.command == "atlas-build-tensor"
    assert args.ingest_manifest_file == "data/curated/atlas/example_ingest_manifest.json"
    assert args.output_dir == "data/curated/atlas/tensor"
    assert args.taxonomy_dir == "data/curated/atlas/tensor/taxonomy"


def test_cli_atlas_build_taxonomy_runs(monkeypatch, tmp_path: Path) -> None:
    ingest_manifest_file = tmp_path / "atlas_manifest.json"
    output_dir = tmp_path / "taxonomy"
    calls: dict[str, object] = {}

    def fake_materialize_atlas_taxonomy(
        *,
        ingest_manifest_file: Path,
        output_dir: Path | None,
    ) -> dict[str, object]:
        calls["ingest_manifest_file"] = ingest_manifest_file
        calls["output_dir"] = output_dir
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_atlas_taxonomy",
        fake_materialize_atlas_taxonomy,
    )

    exit_code = main(
        [
            "atlas",
            "build",
            "taxonomy",
            "--ingest-manifest-file",
            str(ingest_manifest_file),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert calls["ingest_manifest_file"] == ingest_manifest_file.resolve()
    assert calls["output_dir"] == output_dir.resolve()


def test_cli_atlas_build_tensor_runs(monkeypatch, tmp_path: Path) -> None:
    ingest_manifest_file = tmp_path / "atlas_manifest.json"
    output_dir = tmp_path / "tensor"
    taxonomy_dir = tmp_path / "taxonomy"
    calls: dict[str, object] = {}

    def fake_materialize_atlas_tensor(
        *,
        ingest_manifest_file: Path,
        output_dir: Path | None,
        taxonomy_dir: Path | None,
    ) -> dict[str, object]:
        calls["ingest_manifest_file"] = ingest_manifest_file
        calls["output_dir"] = output_dir
        calls["taxonomy_dir"] = taxonomy_dir
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_atlas_tensor",
        fake_materialize_atlas_tensor,
    )

    exit_code = main(
        [
            "atlas",
            "build",
            "tensor",
            "--ingest-manifest-file",
            str(ingest_manifest_file),
            "--output-dir",
            str(output_dir),
            "--taxonomy-dir",
            str(taxonomy_dir),
        ]
    )

    assert exit_code == 0
    assert calls["ingest_manifest_file"] == ingest_manifest_file.resolve()
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["taxonomy_dir"] == taxonomy_dir.resolve()


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


def test_cli_run_benchmark_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "run-benchmark",
            "--manifest-file",
            "snapshot_manifest.json",
            "--cohort-labels-file",
            "cohort_labels.csv",
            "--archive-index-file",
            "source_archives.json",
            "--output-dir",
            "runner_outputs",
            "--bootstrap-iterations",
            "25",
            "--deterministic-test-mode",
        ]
    )
    assert args.command == "run-benchmark"
    assert args.manifest_file == "snapshot_manifest.json"
    assert args.cohort_labels_file == "cohort_labels.csv"
    assert args.archive_index_file == "source_archives.json"
    assert args.output_dir == "runner_outputs"
    assert args.bootstrap_iterations == 25
    assert args.deterministic_test_mode is True


def test_cli_rescue_run_glutamatergic_convergence_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "rescue-run-glutamatergic-convergence",
            "--output-dir",
            "rescue_outputs",
            "--task-card-path",
            "task_card.json",
            "--baseline-id",
            "example_baseline",
        ]
    )
    assert args.command == "rescue-run-glutamatergic-convergence"
    assert args.output_dir == "rescue_outputs"
    assert args.task_card_path == "task_card.json"
    assert args.baseline_id == "example_baseline"


def test_cli_rescue_compare_baselines_parser_accepts_output_dir() -> None:
    args = build_parser().parse_args(
        [
            "rescue-compare-baselines",
            "--output-dir",
            "rescue_baselines",
        ]
    )

    assert args.command == "rescue-compare-baselines"
    assert args.output_dir == "rescue_baselines"


def test_cli_hidden_eval_task_package_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "hidden-eval-task-package",
            "--task-id",
            "glutamatergic_convergence_rescue_task",
            "--output-dir",
            "hidden_eval_task",
            "--task-card-path",
            "task_card.json",
        ]
    )

    assert args.command == "hidden-eval-task-package"
    assert args.task_id == "glutamatergic_convergence_rescue_task"
    assert args.output_dir == "hidden_eval_task"
    assert args.task_card_path == "task_card.json"


def test_cli_hidden_eval_pack_submission_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "hidden-eval-pack-submission",
            "--task-package-dir",
            "hidden_eval_task",
            "--predictions-file",
            "ranked_predictions.csv",
            "--output-file",
            "submission.tar.gz",
            "--submitter-id",
            "partner-demo",
            "--submission-id",
            "demo-v1",
            "--scorer-id",
            "demo-model",
            "--notes",
            "fixture submission",
        ]
    )

    assert args.command == "hidden-eval-pack-submission"
    assert args.task_package_dir == "hidden_eval_task"
    assert args.predictions_file == "ranked_predictions.csv"
    assert args.output_file == "submission.tar.gz"
    assert args.submitter_id == "partner-demo"
    assert args.submission_id == "demo-v1"
    assert args.scorer_id == "demo-model"
    assert args.notes == "fixture submission"


def test_cli_hidden_eval_simulate_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "hidden-eval-simulate",
            "--task-package-dir",
            "hidden_eval_task",
            "--submission-file",
            "submission.tar.gz",
            "--output-dir",
            "hidden_eval_run",
        ]
    )

    assert args.command == "hidden-eval-simulate"
    assert args.task_package_dir == "hidden_eval_task"
    assert args.submission_file == "submission.tar.gz"
    assert args.output_dir == "hidden_eval_run"


def test_cli_backfill_benchmark_public_slices_parser_accepts_optional_paths() -> None:
    args = build_parser().parse_args(
        [
            "backfill-benchmark-public-slices",
            "--output-dir",
            "data/benchmark/public_slices",
            "--benchmark-task-id",
            "scz_translational_task",
            "--task-registry-path",
            "task_registry.csv",
        ]
    )
    assert args.command == "backfill-benchmark-public-slices"
    assert args.output_dir == "data/benchmark/public_slices"
    assert args.benchmark_task_id == "scz_translational_task"
    assert args.task_registry_path == "task_registry.csv"


def test_cli_run_benchmark_runs(monkeypatch, tmp_path: Path) -> None:
    manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    archive_index_file = tmp_path / "source_archives.json"
    output_dir = tmp_path / "runner_outputs"
    config_file = tmp_path / "config.toml"
    calls: dict[str, object] = {}

    def fake_materialize_benchmark_run(
        *,
        manifest_file: Path,
        cohort_labels_file: Path,
        archive_index_file: Path,
        output_dir: Path,
        config_file: Path | None,
        code_version: str | None = None,
        bootstrap_iterations: int | None = None,
        bootstrap_confidence_level: float = 0.95,
        random_seed: int = 17,
        deterministic_test_mode: bool = False,
        execution_timestamp: str | None = None,
    ) -> dict[str, object]:
        calls["manifest_file"] = manifest_file
        calls["cohort_labels_file"] = cohort_labels_file
        calls["archive_index_file"] = archive_index_file
        calls["output_dir"] = output_dir
        calls["config_file"] = config_file
        calls["bootstrap_iterations"] = bootstrap_iterations
        calls["bootstrap_confidence_level"] = bootstrap_confidence_level
        calls["random_seed"] = random_seed
        calls["deterministic_test_mode"] = deterministic_test_mode
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_benchmark_run",
        fake_materialize_benchmark_run,
    )

    exit_code = main(
        [
            "run-benchmark",
            "--manifest-file",
            str(manifest_file),
            "--cohort-labels-file",
            str(cohort_labels_file),
            "--archive-index-file",
            str(archive_index_file),
            "--output-dir",
            str(output_dir),
            "--config",
            str(config_file),
            "--bootstrap-iterations",
            "25",
            "--bootstrap-confidence-level",
            "0.9",
            "--random-seed",
            "23",
            "--deterministic-test-mode",
        ]
    )

    assert exit_code == 0
    assert calls["manifest_file"] == manifest_file.resolve()
    assert calls["cohort_labels_file"] == cohort_labels_file.resolve()
    assert calls["archive_index_file"] == archive_index_file.resolve()
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["config_file"] == config_file.resolve()
    assert calls["bootstrap_iterations"] == 25
    assert calls["bootstrap_confidence_level"] == 0.9
    assert calls["random_seed"] == 23
    assert calls["deterministic_test_mode"] is True


def test_cli_rescue_run_glutamatergic_convergence_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "rescue_outputs"
    task_card_path = tmp_path / "task_card.json"
    calls: dict[str, object] = {}

    def fake_materialize_glutamatergic_convergence_rescue_evaluation(
        *,
        output_dir: Path,
        task_card_path: Path | None,
        baseline_id: str,
    ) -> dict[str, object]:
        calls["output_dir"] = output_dir
        calls["task_card_path"] = task_card_path
        calls["baseline_id"] = baseline_id
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_glutamatergic_convergence_rescue_evaluation",
        fake_materialize_glutamatergic_convergence_rescue_evaluation,
    )

    exit_code = main(
        [
            "rescue-run-glutamatergic-convergence",
            "--output-dir",
            str(output_dir),
            "--task-card-path",
            str(task_card_path),
            "--baseline-id",
            "custom_baseline",
        ]
    )

    assert exit_code == 0
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["task_card_path"] == task_card_path.resolve()
    assert calls["baseline_id"] == "custom_baseline"


def test_cli_rescue_compare_baselines_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "rescue_baselines"
    calls: dict[str, object] = {}

    def fake_materialize_rescue_baseline_suite(
        *,
        output_dir: Path,
    ) -> dict[str, object]:
        calls["output_dir"] = output_dir
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_rescue_baseline_suite",
        fake_materialize_rescue_baseline_suite,
    )

    exit_code = main(
        [
            "rescue-compare-baselines",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert calls["output_dir"] == output_dir.resolve()


def test_cli_hidden_eval_task_package_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "hidden_eval_task"
    task_card_path = tmp_path / "task_card.json"
    calls: dict[str, object] = {}

    def fake_materialize_rescue_hidden_eval_task_package(
        *,
        task_id: str,
        output_dir: Path,
        task_card_path: Path | None,
    ) -> dict[str, object]:
        calls["task_id"] = task_id
        calls["output_dir"] = output_dir
        calls["task_card_path"] = task_card_path
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_rescue_hidden_eval_task_package",
        fake_materialize_rescue_hidden_eval_task_package,
    )

    exit_code = main(
        [
            "hidden-eval-task-package",
            "--task-id",
            "glutamatergic_convergence_rescue_task",
            "--output-dir",
            str(output_dir),
            "--task-card-path",
            str(task_card_path),
        ]
    )

    assert exit_code == 0
    assert calls["task_id"] == "glutamatergic_convergence_rescue_task"
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["task_card_path"] == task_card_path.resolve()


def test_cli_hidden_eval_pack_submission_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_package_dir = tmp_path / "hidden_eval_task"
    predictions_file = tmp_path / "ranked_predictions.csv"
    output_file = tmp_path / "submission.tar.gz"
    calls: dict[str, object] = {}

    def fake_materialize_hidden_eval_submission_archive(
        *,
        task_package_dir: Path,
        predictions_file: Path,
        output_file: Path,
        submitter_id: str,
        submission_id: str,
        scorer_id: str,
        notes: str = "",
    ) -> dict[str, object]:
        calls["task_package_dir"] = task_package_dir
        calls["predictions_file"] = predictions_file
        calls["output_file"] = output_file
        calls["submitter_id"] = submitter_id
        calls["submission_id"] = submission_id
        calls["scorer_id"] = scorer_id
        calls["notes"] = notes
        return {"output_file": str(output_file)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_hidden_eval_submission_archive",
        fake_materialize_hidden_eval_submission_archive,
    )

    exit_code = main(
        [
            "hidden-eval-pack-submission",
            "--task-package-dir",
            str(task_package_dir),
            "--predictions-file",
            str(predictions_file),
            "--output-file",
            str(output_file),
            "--submitter-id",
            "partner-demo",
            "--submission-id",
            "demo-v1",
            "--scorer-id",
            "demo-model",
            "--notes",
            "fixture submission",
        ]
    )

    assert exit_code == 0
    assert calls["task_package_dir"] == task_package_dir.resolve()
    assert calls["predictions_file"] == predictions_file.resolve()
    assert calls["output_file"] == output_file.resolve()
    assert calls["submitter_id"] == "partner-demo"
    assert calls["submission_id"] == "demo-v1"
    assert calls["scorer_id"] == "demo-model"
    assert calls["notes"] == "fixture submission"


def test_cli_hidden_eval_simulate_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_package_dir = tmp_path / "hidden_eval_task"
    submission_file = tmp_path / "submission.tar.gz"
    output_dir = tmp_path / "hidden_eval_run"
    calls: dict[str, object] = {}

    def fake_materialize_hidden_eval_simulation(
        *,
        task_package_dir: Path,
        submission_file: Path,
        output_dir: Path,
    ) -> dict[str, object]:
        calls["task_package_dir"] = task_package_dir
        calls["submission_file"] = submission_file
        calls["output_dir"] = output_dir
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_hidden_eval_simulation",
        fake_materialize_hidden_eval_simulation,
    )

    exit_code = main(
        [
            "hidden-eval-simulate",
            "--task-package-dir",
            str(task_package_dir),
            "--submission-file",
            str(submission_file),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert calls["task_package_dir"] == task_package_dir.resolve()
    assert calls["submission_file"] == submission_file.resolve()
    assert calls["output_dir"] == output_dir.resolve()


def test_cli_backfill_benchmark_public_slices_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "public_slices"
    task_registry_path = tmp_path / "task_registry.csv"
    calls: dict[str, object] = {}

    def fake_materialize_public_benchmark_slices(
        *,
        output_dir: Path | None,
        benchmark_task_id: str | None,
        task_registry_path: Path | None = None,
    ) -> dict[str, object]:
        calls["output_dir"] = output_dir
        calls["benchmark_task_id"] = benchmark_task_id
        calls["task_registry_path"] = task_registry_path
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_public_benchmark_slices",
        fake_materialize_public_benchmark_slices,
    )

    exit_code = main(
        [
            "backfill-benchmark-public-slices",
            "--output-dir",
            str(output_dir),
            "--benchmark-task-id",
            "scz_translational_task",
            "--task-registry-path",
            str(task_registry_path),
        ]
    )

    assert exit_code == 0
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["benchmark_task_id"] == "scz_translational_task"
    assert calls["task_registry_path"] == task_registry_path.resolve()


def test_cli_build_benchmark_reporting_parser_accepts_files() -> None:
    args = build_parser().parse_args(
        [
            "build-benchmark-reporting",
            "--manifest-file",
            "snapshot_manifest.json",
            "--cohort-labels-file",
            "cohort_labels.csv",
            "--runner-output-dir",
            "runner_outputs",
            "--output-dir",
            "public_payloads",
        ]
    )
    assert args.command == "build-benchmark-reporting"
    assert args.manifest_file == "snapshot_manifest.json"
    assert args.cohort_labels_file == "cohort_labels.csv"
    assert args.runner_output_dir == "runner_outputs"
    assert args.output_dir == "public_payloads"


def test_cli_build_benchmark_reporting_runs(monkeypatch, tmp_path: Path) -> None:
    manifest_file = tmp_path / "snapshot_manifest.json"
    cohort_labels_file = tmp_path / "cohort_labels.csv"
    runner_output_dir = tmp_path / "runner_outputs"
    output_dir = tmp_path / "public_payloads"
    calls: dict[str, object] = {}

    def fake_materialize_benchmark_reporting(
        *,
        manifest_file: Path,
        cohort_labels_file: Path,
        runner_output_dir: Path,
        output_dir: Path,
        generated_at: str | None = None,
    ) -> dict[str, object]:
        calls["manifest_file"] = manifest_file
        calls["cohort_labels_file"] = cohort_labels_file
        calls["runner_output_dir"] = runner_output_dir
        calls["output_dir"] = output_dir
        calls["generated_at"] = generated_at
        return {"output_dir": str(output_dir)}

    monkeypatch.setattr(
        "scz_target_engine.cli.materialize_benchmark_reporting",
        fake_materialize_benchmark_reporting,
    )

    exit_code = main(
        [
            "build-benchmark-reporting",
            "--manifest-file",
            str(manifest_file),
            "--cohort-labels-file",
            str(cohort_labels_file),
            "--runner-output-dir",
            str(runner_output_dir),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert calls["manifest_file"] == manifest_file.resolve()
    assert calls["cohort_labels_file"] == cohort_labels_file.resolve()
    assert calls["runner_output_dir"] == runner_output_dir.resolve()
    assert calls["output_dir"] == output_dir.resolve()
    assert calls["generated_at"] is None
