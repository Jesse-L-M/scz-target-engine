from pathlib import Path


def _assert_contains(path: str, snippets: list[str]) -> None:
    text = Path(path).read_text(encoding="utf-8").lower()
    missing = [snippet for snippet in snippets if snippet.lower() not in text]
    assert not missing, f"{path} is missing required release-doc snippets: {missing}"


def test_release_docs_cover_canonical_benchmark_workflow() -> None:
    workflow_snippets = [
        "uv run scz-target-engine build-benchmark-snapshot \\",
        "uv run scz-target-engine build-benchmark-cohort \\",
        "uv run scz-target-engine run-benchmark \\",
        "uv run scz-target-engine build-benchmark-reporting \\",
        "data/curated/rescue_tasks/task_registry.csv",
        "data/benchmark/generated/scz_small/snapshot_manifest.json",
        "data/benchmark/generated/scz_small/cohort_labels.csv",
        "data/benchmark/generated/scz_small/runner_outputs/run_manifests/",
        "data/benchmark/generated/scz_small/public_payloads/report_cards/",
        "data/benchmark/generated/scz_small/public_payloads/leaderboards/",
        "benchmark_snapshot_manifest",
        "benchmark_cohort_labels",
        "benchmark_model_run_manifest",
        "benchmark_metric_output_payload",
        "benchmark_confidence_interval_payload",
    ]
    _assert_contains("README.md", workflow_snippets)
    _assert_contains("docs/benchmarking.md", workflow_snippets)
    _assert_contains("data/benchmark/README.md", workflow_snippets)


def test_release_docs_call_out_current_benchmark_limitations() -> None:
    limitation_snippets = [
        "fixture-scale",
        "benchmark breadth is still limited",
        "calibration work",
    ]
    _assert_contains("README.md", limitation_snippets)
    _assert_contains("docs/claim.md", limitation_snippets)
    _assert_contains("docs/benchmarking.md", limitation_snippets)


def test_release_docs_cover_artifact_schema_registry() -> None:
    schema_snippets = [
        "schemas/artifact_schemas",
        "scz_target_engine.artifacts",
        "gene_target_ledgers",
        "decision_vectors_v1",
        "domain_head_rankings_v1",
        "benchmark_snapshot_manifest",
        "benchmark_task_id",
    ]
    _assert_contains("README.md", schema_snippets)
    _assert_contains("docs/artifact_schemas.md", schema_snippets)


def test_release_docs_cover_cli_namespace_aliases() -> None:
    readme_snippets = [
        "legacy flat commands remain supported",
        "engine validate",
        "engine build",
        "sources opentargets",
        "registry build",
        "prepare gene-table",
        "benchmark snapshot",
        "config/engine/v0.toml",
    ]
    benchmark_doc_snippets = [
        "benchmark snapshot",
        "benchmark cohort",
        "benchmark run",
        "benchmark reporting",
        "legacy flat commands remain supported",
    ]
    _assert_contains("README.md", readme_snippets)
    _assert_contains("docs/benchmarking.md", benchmark_doc_snippets)


def test_release_docs_cover_public_slice_backfill_and_replay() -> None:
    snippets = [
        "backfill-benchmark-public-slices",
        "benchmark backfill public-slices",
        "data/benchmark/public_slices/catalog.json",
        "evaluable on the principal `3y` horizon",
        "stop-go comparison remains blocked",
        "data/benchmark/generated/public_slices/scz_translational_2024_09_25/",
        "fall back to live source data",
    ]
    _assert_contains("README.md", snippets)
    _assert_contains("docs/benchmarking.md", snippets)
    _assert_contains("data/benchmark/README.md", snippets)
