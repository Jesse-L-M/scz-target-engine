# Glutamatergic Hidden-Eval Example

`ranked_predictions.csv` is a concrete full-coverage example submission for the
shipped `glutamatergic_convergence_rescue_task`. It follows the governed candidate
universe from the real checked-in ranking artifact and matches the current shipped
glutamatergic baseline ordering.

Package it against an exported public task package with:

```bash
uv run scz-target-engine hidden-eval-pack-submission \
  --task-package-dir .context/glutamatergic_hidden_eval_task \
  --predictions-file examples/benchmark_submissions/glutamatergic_convergence_hidden_eval/ranked_predictions.csv \
  --submitter-id partner-demo \
  --submission-id glutamatergic-example-v1 \
  --scorer-id example-baseline \
  --output-file .context/glutamatergic_example_submission.tar.gz
```

Then simulate with:

```bash
uv run scz-target-engine hidden-eval-simulate \
  --task-package-dir .context/glutamatergic_hidden_eval_task \
  --submission-file .context/glutamatergic_example_submission.tar.gz \
  --output-dir .context/glutamatergic_hidden_eval_run
```

The simulator returns aggregate metrics in `public_scorecard.json` and keeps the
per-entity hidden-label join in `internal_evaluation_rows.csv`.
