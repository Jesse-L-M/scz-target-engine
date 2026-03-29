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
        "data/benchmark/generated/scz_small/snapshot_manifest.json",
        "data/benchmark/generated/scz_small/cohort_labels.csv",
        "data/benchmark/generated/scz_small/runner_outputs/run_manifests/",
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
