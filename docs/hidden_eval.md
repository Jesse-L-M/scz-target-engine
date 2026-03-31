# Hidden Eval Protocol

This repo now ships a concrete hidden-eval packaging path for the real checked-in
glutamatergic rescue task artifacts. The public package copies the governed
pre-cutoff ranking surface exactly as shipped. The simulator keeps resolving the
real governed bundle from the repo and joins the held-out post-cutoff labels only
inside operator-only outputs.

Important: this is only a real hidden-eval boundary if submitters receive the
exported public task package instead of a checkout of this repository. The repo itself
still contains the checked-in evaluation-label CSVs for the shipped rescue tasks, so
submitters receive the exported public task package only.

In short: the repo itself still contains the checked-in evaluation-label CSVs.

## Public Vs Hidden Boundary

Submitter-visible package contents:

- `ranking_input.csv`: exact copy of the governed pre-cutoff ranking artifact
- `submission_template.csv`: blank full-coverage template keyed to the governed
  candidate universe
- `task_manifest.json`: protocol metadata, package id, frozen manifest ids, and
  explicit public-vs-hidden boundary notes
- `README.md`: package-local submission instructions

Hidden evaluator only:

- the governed `evaluation_target` CSV
- per-entity label joins written to `internal_evaluation_rows.csv`
- operator-side provenance in `simulation_manifest.json`

Safe output to share back with submitters:

- `public_scorecard.json`

## Canonical Workflow

Legacy flat commands remain supported. Namespaced aliases route to the same code:

- `uv run scz-target-engine hidden-eval task-package`
- `uv run scz-target-engine hidden-eval pack-submission`
- `uv run scz-target-engine hidden-eval simulate`

Materialize the public task package from the shipped glutamatergic rescue bundle:

```bash
uv run scz-target-engine hidden-eval-task-package \
  --task-id glutamatergic_convergence_rescue_task \
  --output-dir .context/glutamatergic_hidden_eval_task
```

Pack the checked-in example submission:

```bash
uv run scz-target-engine hidden-eval-pack-submission \
  --task-package-dir .context/glutamatergic_hidden_eval_task \
  --predictions-file examples/benchmark_submissions/glutamatergic_convergence_hidden_eval/ranked_predictions.csv \
  --submitter-id partner-demo \
  --submission-id glutamatergic-example-v1 \
  --scorer-id example-baseline \
  --output-file .context/glutamatergic_example_submission.tar.gz
```

Simulate hidden evaluation against the real checked-in held-out labels:

```bash
uv run scz-target-engine hidden-eval-simulate \
  --task-package-dir .context/glutamatergic_hidden_eval_task \
  --submission-file .context/glutamatergic_example_submission.tar.gz \
  --output-dir .context/glutamatergic_hidden_eval_run
```

That simulator writes:

- `public_scorecard.json`
- `internal_evaluation_rows.csv`
- `simulation_manifest.json`

`public_scorecard.json` is the shareable return payload. It keeps aggregate metrics
and split summaries but omits per-entity labels. `internal_evaluation_rows.csv`
contains the held-out join and stays operator-only.

## Using Existing Rescue Outputs As A Submission

The packer accepts existing glutamatergic `ranked_predictions.csv` files as long as
they preserve `task_id`, `gene_id`, and `rank`. That means a shipped rescue run can
be turned into a submission archive without rewriting the predictions file:

```bash
uv run scz-target-engine rescue run glutamatergic-convergence \
  --output-dir .context/glutamatergic_rescue_run

uv run scz-target-engine hidden-eval-pack-submission \
  --task-package-dir .context/glutamatergic_hidden_eval_task \
  --predictions-file .context/glutamatergic_rescue_run/ranked_predictions.csv \
  --submitter-id internal-baseline \
  --submission-id glutamatergic-rescue-baseline-v1 \
  --scorer-id convergence_state_baseline_v1 \
  --output-file .context/glutamatergic_rescue_baseline_submission.tar.gz
```

Do not use `rescue run glutamatergic-convergence` as a submitter-facing API. It also
writes `evaluation_rows.csv`, which contains hidden labels. The hidden-eval package
is the distribution boundary; the simulator is the operator-side join.
