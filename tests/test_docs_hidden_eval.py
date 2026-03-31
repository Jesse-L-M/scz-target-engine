from pathlib import Path


def _assert_contains(path: str, snippets: list[str]) -> None:
    text = Path(path).read_text(encoding="utf-8").lower()
    missing = [snippet for snippet in snippets if snippet.lower() not in text]
    assert not missing, f"{path} is missing hidden-eval doc snippets: {missing}"


def test_hidden_eval_docs_cover_public_and_hidden_boundaries() -> None:
    hidden_eval_snippets = [
        "hidden-eval-task-package",
        "hidden-eval-pack-submission",
        "hidden-eval-simulate",
        "public_scorecard.json",
        "internal_evaluation_rows.csv",
        "submitters receive the exported public task package",
        "repo itself still contains the checked-in evaluation-label csvs",
        "examples/benchmark_submissions/glutamatergic_convergence_hidden_eval/ranked_predictions.csv",
    ]
    rescue_doc_snippets = [
        "hidden eval packaging",
        "distribution separation",
        "docs/hidden_eval.md",
    ]
    _assert_contains("docs/hidden_eval.md", hidden_eval_snippets)
    _assert_contains("docs/rescue_tasks.md", rescue_doc_snippets)
