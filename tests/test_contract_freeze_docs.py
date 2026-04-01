from pathlib import Path


def _assert_contains(path: str, snippets: list[str]) -> None:
    text = Path(path).read_text(encoding="utf-8").lower()
    missing = [snippet for snippet in snippets if snippet.lower() not in text]
    assert not missing, f"{path} is missing required contract-freeze snippets: {missing}"


def test_contract_freeze_docs_pin_the_shared_smoke_path() -> None:
    smoke_snippets = [
        "scripts/run_contract_smoke_path.sh",
        "uv run scz-target-engine build --config config/v0.toml --input-dir examples/v0/input --output-dir examples/v0/output",
        "uv run scz-target-engine build-benchmark-snapshot --request-file data/benchmark/fixtures/scz_small/snapshot_request.json --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-file data/benchmark/generated/scz_small/snapshot_manifest.json --materialized-at 2026-03-28",
        "uv run scz-target-engine build-benchmark-cohort --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv --output-file data/benchmark/generated/scz_small/cohort_labels.csv",
        "uv run scz-target-engine run-benchmark --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json --output-dir data/benchmark/generated/scz_small/runner_outputs --config config/v0.toml --deterministic-test-mode",
        "uv run python -m scz_target_engine.cli rescue compare baselines --output-dir .context/rescue-baseline-suite",
        "pythonpath=src python3 -m scz_target_engine.cli build-hypothesis-packets --policy-artifact examples/v0/output/policy_decision_vectors_v2.json --ledger-artifact examples/v0/output/gene_target_ledgers.json --output-file .context/hypothesis_packets_v1.json",
    ]
    _assert_contains("README.md", smoke_snippets)
    _assert_contains("docs/designs/contracts-and-compat-v2.md", smoke_snippets)
    _assert_contains("scripts/run_contract_smoke_path.sh", smoke_snippets[1:])


def test_contract_freeze_docs_cover_compatibility_and_release_manifest_rules() -> None:
    compatibility_snippets = [
        "docs/intervention_object_compatibility.md",
        "projection multiplicity",
        "silent legacy-consumer collisions",
        "gene_target_ledgers",
        "decision_vectors_v1",
        "policy_decision_vectors_v2",
        "hypothesis_packets_v1",
        "fail closed",
    ]
    manifest_snippets = [
        "program_memory_release",
        "benchmark_release",
        "rescue_release",
        "variant_context_release",
        "policy_release",
        "hypothesis_release",
        "sha256",
        "expected_schema_version",
        "docs/decisions/0002-release-manifest-contract.md",
    ]
    _assert_contains("README.md", ["docs/intervention_object_compatibility.md"])
    _assert_contains("docs/claim.md", compatibility_snippets[:4])
    _assert_contains("docs/intervention_object_compatibility.md", compatibility_snippets[1:])
    _assert_contains("docs/artifact_schemas.md", manifest_snippets)
    _assert_contains(
        "docs/designs/contracts-and-compat-v2.md",
        manifest_snippets[:6] + ["docs/intervention_object_compatibility.md"],
    )


def test_contract_freeze_ci_runs_required_commands() -> None:
    ci_snippets = [
        "uv run --group dev pytest",
        "uv run --group dev pytest tests/test_artifacts.py",
        "./scripts/run_contract_smoke_path.sh",
    ]
    _assert_contains(".github/workflows/ci.yml", ci_snippets)
