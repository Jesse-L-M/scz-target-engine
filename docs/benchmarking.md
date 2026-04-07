# Benchmark Protocol

The benchmark path shipped on `main` is now real, not protocol-only. `PR9A` froze the
benchmark question and artifact contracts. `PR9B` added snapshot and cohort builders.
`PR9C` added the runner plus emitted run manifests, metric payloads, and percentile-bootstrap
confidence interval payloads.

This is a fixture-scale release path. It is useful for reproducible end-to-end evaluation,
artifact validation, and honest public slice backfill from the checked-in archive catalog.
It is not yet a production-scale historical replay system.

## Current Release Boundary

- historical benchmark archives are fixture-scale and currently checked in for the canonical fixture under `data/benchmark/fixtures/scz_small/` plus derived public slices under `data/benchmark/public_slices/`
- benchmark breadth is still limited to the frozen schizophrenia benchmark question, the ranking task plus one checked-in Track B structural replay task, small deterministic cohorts, and the current `available_now` baseline subset
- protocol-only baselines remain declared for comparability but are not executed unless later archived artifacts make them runnable
- calibration work, decision-threshold setting, and broader operating-point evaluation remain future work
- current benchmark outputs are generated locally under `data/benchmark/generated/`; only the fixture inputs under `data/benchmark/fixtures/` are checked in

## Frozen Benchmark Question

The benchmark asks:

- given a schizophrenia evidence snapshot as of date `T`
- rank admissible gene, module, or intervention-object entities using only evidence observable at or before `T`
- compare those ranks against later translational outcomes recorded on a separate label channel

Entity types:

- `gene`
- `module`
- `intervention_object`

Accepted translational outcome labels:

- `future_schizophrenia_program_started`
- `future_schizophrenia_program_advanced`
- `future_schizophrenia_positive_signal`
- `future_schizophrenia_negative_signal`
- `no_qualifying_future_outcome`

Evaluation horizons:

- `1y`
- `3y`
- `5y`

In-scope evidence for the ranking side:

- pre-cutoff schizophrenia genetics
- pre-cutoff schizophrenia transcriptomics and regulatory context
- pre-cutoff tractability and generic platform context
- pre-cutoff program-history denominator state and intervention-object lineage context when the replay slice is at intervention-object grain
- pre-cutoff scoring-neutral failure and directionality ledgers only when those artifacts were themselves archived before the snapshot

Not valid as benchmark labels:

- post-cutoff source evidence refreshes
- post-cutoff genetics or transcriptomics publications used as if they were outcomes
- any current-head internal state that was not already frozen into a pre-cutoff artifact

The benchmark is about ranking against later translational outcomes, not about replaying
later evidence as if it were ground truth.

## Snapshot Semantics And Leakage Controls

Every benchmark snapshot is described by a `benchmark_snapshot_manifest` with:

- `benchmark_suite_id`: optional suite contract id for the registry-backed benchmark path
- `benchmark_task_id`: optional task contract id for the registry-backed benchmark path
- `as_of_date`: the last date allowed for ranking evidence
- `outcome_observation_closed_at`: the last date used to adjudicate future labels
- `benchmark_question_id`: the frozen benchmark question id
- `source_snapshots`: one explicit cutoff or exclusion entry for every frozen evidence source
- `leakage_controls`: an explicit strict no-leakage block
- `baseline_ids`: the frozen comparison set to evaluate

The manifest lives in code as `BenchmarkSnapshotManifest` in
[src/scz_target_engine/benchmark_protocol.py](../src/scz_target_engine/benchmark_protocol.py).
The materializer lives in
[src/scz_target_engine/benchmark_snapshots.py](../src/scz_target_engine/benchmark_snapshots.py).

The leakage contract stays strict:

- a snapshot manifest is required
- future evidence cannot enter the ranking inputs
- future outcome labels cannot be reused as model inputs
- pre-cutoff evidence freezing is required for included evidence artifacts
- undated sources default to `exclude_source`
- missing cutoff definitions default to `reject_snapshot`
- benchmark execution cannot depend on current head internals that are not already present in frozen artifacts

`materialized_at` may be later than the benchmark `as_of_date` if the snapshot was
reconstructed from a pre-cutoff archived release or extract. The anti-leakage check is
about when the evidence was frozen, not when a later PR regenerated the manifest.

## Source Cutoff Behavior

The current evidence stack is frozen as release-scoped for benchmarking:

- `PGC`
- `SCHEMA`
- `PsychENCODE`
- `Open Targets`
- `ChEMBL`

All five currently use release or archived-extract semantics rather than row-level
timestamp semantics. That means:

- historical benchmark slices require a release or archived extract frozen on or before the snapshot date
- if such an archive does not exist, the source is excluded
- the protocol does not backfill or synthesize those historical archives

If a source does not expose reliable row-level dates, the protocol does not guess.
It either admits a valid archived descriptor or emits `included = false` with a concrete
`exclusion_reason`. There is no fallback from a missing historical archive to current
live source data.

## Frozen Baseline Matrix

The comparison set is frozen as:

1. `pgc_only`
2. `schema_only`
3. `opentargets_only`
4. `v0_current`
5. `v1_current`
6. `v1_pre_numeric_pr7_heads`
7. `v1_post_numeric_pr7_heads`
8. `chembl_only`
9. `random_with_coverage`

Notes:

- `v1_current` is the current additive `v1` output evaluated as shipped
- `v1_pre_numeric_pr7_heads` and `v1_post_numeric_pr7_heads` stay frozen as protocol comparison labels across the PR8.1 transition
- `chembl_only` applies only where tractability context exists and is not a module baseline
- `v0_current` and `v1_current` now also apply to `intervention_object` slices by writing explicit projection sidecars from archived gene/module baseline outputs through the checked-in compatibility contract
- `random_with_coverage` randomizes across the full admissible cohort and is evaluated with the same full-cohort semantics as the main baselines, including intervention-object public slices
- a snapshot may list a baseline only if that baseline applies to at least one entity type present in the snapshot manifest

## Registry-Driven Task Contract

The benchmark suite is now driven by the checked-in task registry at
`data/curated/rescue_tasks/task_registry.csv`.

That benchmark registry remains benchmark-specific. Rescue-task identity now lives in
the adjacent `data/curated/rescue_tasks/rescue_task_registry.csv` plus validated
`rescue_task_contract` JSON files, so benchmark lookups do not become ambiguous as
rescue tasks are added.

The current explicit task rows are:

- ranking task: suite `scz_translational_suite`, task `scz_translational_task`, question `scz_translational_ranking_v1`, fixture path `data/benchmark/fixtures/scz_small/`
- Track B task: suite `scz_translational_suite`, task `scz_failure_memory_track_b_task`, question `scz_failure_memory_track_b_v1`, protocol `track_b_structural_replay_protocol_v1`, fixture path `data/benchmark/fixtures/scz_failure_memory_2025_02_01/`
- both tasks emit the same benchmark artifact families: `benchmark_snapshot_manifest`, `benchmark_cohort_members`, `benchmark_source_cohort_members`, `benchmark_source_future_outcomes`, `benchmark_cohort_manifest`, `benchmark_cohort_labels`, `benchmark_model_run_manifest`, `benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload`

`snapshot_request.json` remains an operator input, but the fixture request now carries
the explicit suite/task ids from that registry row. The snapshot builder resolves the
task contract from the registry, then the cohort builder and runner continue from the
emitted snapshot manifest rather than a parallel benchmark configuration path.

## Track B Structural Replay Task

`scz_failure_memory_track_b_task` keeps Track B inside the shipped benchmark stack
instead of introducing a second runner or reporting pipeline.

Checked-in Track B inputs live under
`data/benchmark/fixtures/scz_failure_memory_2025_02_01/` and include:

- `track_b_casebook.csv`: frozen benchmark cases with gold analog ids, gold failure scope, gold replay status, and required-difference checklist items
- `program_universe.csv` and `events.csv`: slice-local program-memory denominator and event ledger used to freeze coverage-at-cutoff and pre-cutoff analog availability
- `assets.csv`, `event_provenance.csv`, and `directionality_hypotheses.csv`: pinned local program-memory substrate required by the structural replay baselines
- `cohort_members.csv`: the six admissible Track B proposal ids keyed to `track_b_casebook.csv` `proposal_entity_id`
- `future_outcomes.csv`: empty artifact-family placeholder kept for stack compatibility; Track B cohort labels are derived from the casebook, not future-outcome ranking labels
- `snapshot_request.json` and `source_archives.json`: the same benchmark entrypoints used by the rest of the stack, but the request now names the explicit structural replay question `scz_failure_memory_track_b_v1`

Track B cohort materialization is task-aware:

- `build-benchmark-cohort` loads the frozen casebook beside `cohort_members.csv`
- `benchmark_cohort_members.csv`, `benchmark_cohort_labels`, runner case outputs, and report-card denominators must all refer to the same six proposal ids
- `benchmark_cohort_labels` emits one true replay-status label per case on horizon `structural_replay`
- Track B fails closed if `cohort_members.csv` diverges from the casebook ids or labels

The registry-backed fixture contract now validates these archive-index sibling files up
front for Track B:

- `track_b_casebook.csv`
- `program_universe.csv`
- `events.csv`
- `assets.csv`
- `event_provenance.csv`
- `directionality_hypotheses.csv`

Track B executes these baselines only:

- `track_b_exact_target`
- `track_b_target_class`
- `track_b_nearest_history`
- `track_b_structural_current`

Track B emits the usual metric payloads and confidence intervals under the same
artifact families, but its executable metric bundle is structural:

- `analog_recall_at_3`
- `failure_scope_macro_f1`
- `what_must_differ_checklist_f1`
- `replay_status_exact_match`

Track B also writes derived sidecars from the same run:

- `runner_outputs/track_b_case_outputs/<run_id>.json`
- `runner_outputs/track_b_confusion_summaries/<run_id>.json`
- `public_payloads/error_analysis/scz_translational_suite/scz_failure_memory_track_b_task/<snapshot_id>/<track_b_public_id>.md`
- `public_payloads/error_analysis/scz_translational_suite/scz_failure_memory_track_b_task/<snapshot_id>/<track_b_public_id>.json`

Those sidecars are additive reporting aids, not new top-level benchmark schema
families. The strict no-fallback archive rule still applies: Track B does not fall
back to live source data or repo-head program-memory state when the checked-in slice
does not contain enough history.

Track B reporting now validates one complete reporting bundle before it writes
public payloads:

- the exact expected `available_now` Track B baseline set must be present, so a
  deleted baseline bundle cannot silently shrink the leaderboard
- run manifests, metric payloads, confidence-interval payloads, case-output
  payloads, and confusion summaries must all keep their canonical schema
  identity; reporting rejects tampered `schema_name` or `schema_version`
- run manifests, case outputs, confusion summaries, metric payloads, and
  confidence-interval payloads must all agree on the owning run, baseline,
  snapshot, suite/task/question surface, and Track B horizon
- Track B run manifests must keep the exact allowed `run_parameterization`
  shape, and public report cards republish only those validated fields
- public `evaluation_input_artifacts` are rebuilt from the validated
  `benchmark_snapshot_manifest`, materialized cohort bundle, and pinned Track B
  auxiliary source artifacts rather than copied from `run_manifest.input_artifacts`
- reporting copies those pinned `evaluation_input_artifacts` into
  `validated_track_b_runner_bundle/<track_b_public_id>/inputs/<basename>`, and
  public Track B revalidation rebuilds from that hashed public input set only
  instead of reopening sibling fixture files from the local workspace
- public Track B report cards and leaderboard entries publish
  `code_version = redacted_untrusted_runner_code_version`; reporting does not
  treat the runner-emitted full `code_version` as trustworthy public
  provenance because it has no immutable source outside the mutable runner
  bundle
- public Track B IDs and filenames are rebuilt from trusted contract inputs:
  `snapshot_id`, `baseline_id`, and the validated public
  `run_parameterization` digest. Public report cards, leaderboard entries,
  report-card paths, and error-analysis paths do not reuse the mutable runner
  `run_id`
- Track B `run_id` still validates the runner bundle's internal
  baseline/`code_version`/parameterization self-consistency, but public
  provenance does not rely on that mutable field and does not republish it
- Track B public report cards redact self-attested runner operational metadata:
  `started_at = redacted_untrusted_runner_started_at`,
  `completed_at = redacted_untrusted_runner_completed_at`, and
  `run_notes = redacted_untrusted_runner_notes`
- public `derived_from_artifacts[].artifact_path` values use stable logical
  paths rooted at `validated_track_b_runner_bundle/<track_b_public_id>/...`,
  and reporting materializes that published tree exactly as advertised
- the materialized `validated_track_b_runner_bundle/<track_b_public_id>/...`
  JSON files are rewritten into a public contract before publish:
  `run_id` becomes the stable public id, runner `code_version` and operational
  timestamps/notes are redacted, and copied input-artifact paths are rebased to
  relative public paths instead of absolute local filesystem paths
- public Track B readers fail closed on exact schema identity, the redacted
  Track B provenance contract, missing `run_parameterization`, and nested
  `SourceSnapshot.included` values that are missing or not literal booleans
- those readers also reject forged `source_snapshots`, rebuilt
  `evaluation_input_artifacts`, tampered `derived_from_artifacts.sha256`,
  tampered `derived_from_artifacts.notes`, missing materialized public bundle
  files, forged public `leaderboard_id`, and wrong per-metric public
  `metric_unit`
- those readers rebuild the expected Track B case outputs from the pinned
  casebook plus program-memory dataset, so a self-consistent forged public
  case-output bundle still fails closed
- those readers also recompute public report-card headline metrics and reopen
  referenced public report cards when validating leaderboards, so forged entry
  values, ranks, counts, or report-card paths fail closed
- public leaderboards also require the full expected `available_now` Track B
  baseline set for the pinned snapshot/task contract, so omitted, duplicate,
  or unexpected baselines fail closed
- public `evaluation_input_artifacts[].artifact_path`,
  `leaderboard.report_card_files[]`, and `leaderboard.entries[].report_card_path`
  must stay stable relative public paths that still resolve inside the Track B
  `public_payloads` root after normalization, so `../../` escapes and off-tree
  references fail closed
- Track B reporting and public validation derive suite/task/question identity
  and the complete expected `available_now` baseline set from the pinned
  snapshot manifest plus the frozen Track B protocol in code, not from a
  mutable `task_registry_path`
- interval provenance is bound to the run-manifest parameterization, including
  the deterministic per-baseline seed derived from the base seed plus
  `baseline_id` / `structural_replay`
- Track B metric payloads must include explicit `metric_unit` and keep the
  shipped metric-unit contract, currently `fraction` for all four structural
  replay metrics
- manifest-only provenance fields such as `track_b_case_count` and
  `track_b_casebook_sha256` must match the pinned casebook and emitted case set
- duplicate `artifact_name` entries in consumed Track B input-artifact
  provenance fail closed instead of being collapsed by name

Track B reporting still does not rerun model inference. It does revalidate the
full Track B bundle by recomputing structural metrics, confusion summaries, and
bootstrap intervals from the runner-emitted case outputs plus the pinned cohort
bundle before it publishes report cards, leaderboards, or error analysis.

Shared runtime readers were hardened in the same release:

- artifact schema loading now treats `required_fields` as required by position,
  not by embedded `required` metadata
- benchmark/protocol/runtime JSON readers reject malformed scalar and container
  types instead of coercing them into strings or iterables
- observatory packet loaders raise on malformed existing JSON and return `None`
  only for true absence or a valid hypothesis packet with no rescue augmentation

Bootstrap note:

- `analog_recall_at_3` intervals resample at unit `case` but skip resamples with zero evaluable analog cases instead of coercing them to `0.0`
- Track B mismatch review now includes pure analog-retrieval misses, so case-review markdown cannot silently omit a retrieval-only failure

Replay example on the checked-in Track B fixture:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json \
  --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --materialized-at 2026-04-05

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_failure_memory_2025_02_01/source_archives.json \
  --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_failure_memory_2025_02_01/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_failure_memory_2025_02_01/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs \
  --output-dir data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads
```

## Canonical End-To-End Workflow

The canonical benchmark workflow in this repo is the deterministic fixture under
`data/benchmark/fixtures/scz_small/`.

Legacy flat commands remain supported. The namespaced benchmark aliases route to the
same builders and flags:

- `uv run scz-target-engine benchmark snapshot`
- `uv run scz-target-engine benchmark cohort`
- `uv run scz-target-engine benchmark run`
- `uv run scz-target-engine benchmark backfill public-slices`
- `uv run scz-target-engine benchmark reporting`

Use the flat commands below for compatibility with existing docs and scripts. For new
automation, the namespaced aliases are equivalent. `config/v0.toml` remains the
canonical benchmark config path, and `config/engine/v0.toml` is the mirrored engine
namespace path.

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/fixtures/scz_small/snapshot_request.json \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --materialized-at 2026-03-28

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-members-file data/benchmark/fixtures/scz_small/cohort_members.csv \
  --future-outcomes-file data/benchmark/fixtures/scz_small/future_outcomes.csv \
  --output-file data/benchmark/generated/scz_small/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --archive-index-file data/benchmark/fixtures/scz_small/source_archives.json \
  --output-dir data/benchmark/generated/scz_small/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine backfill-benchmark-public-slices \
  --output-dir data/benchmark/public_slices \
  --benchmark-task-id scz_translational_task

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/scz_small/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/scz_small/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/scz_small/runner_outputs \
  --output-dir data/benchmark/generated/scz_small/public_payloads
```

The reporting stage is intentionally downstream of the runner. It derives public-facing
report cards and leaderboard payloads from `benchmark_model_run_manifest`,
`benchmark_metric_output_payload`, and `benchmark_confidence_interval_payload` files,
plus the supplied snapshot manifest and cohort labels.

For Track A and generic ranking tasks, reporting remains a read-only join over
the emitted runner artifacts. For Track B, reporting is stricter: it reloads the
pinned snapshot/cohort bundle, recomputes structural metric values,
confidence-interval summaries, and confusion outputs from the case-output sidecars,
rebuilds public provenance from trusted inputs instead of `run_manifest.input_artifacts`,
and materializes the advertised
`validated_track_b_runner_bundle/<track_b_public_id>/...` files before it writes
public payloads.

Supporting operator inputs:

- `snapshot_request.json`: suite/task ids, snapshot identity, dates, entity types, and requested baseline ids
- `source_archives.json`: archived source descriptors with archive paths and SHA256 digests
- `cohort_members.csv`: admissible ranking cohort membership
- `future_outcomes.csv`: post-cutoff label adjudication input
- `track_b_casebook.csv`: Track B-only frozen casebook with gold analog ids and structural replay labels
- `data/curated/program_history/v2/program_universe.csv` and `data/curated/program_history/v2/events.csv`: checked-in denominator inputs used to derive intervention-object public-slice cohorts and future outcomes
- `data/curated/rescue_tasks/task_registry.csv`: registry-backed suite/task contract source of truth
- `data/benchmark/public_slices/catalog.json`: checked-in catalog of derived public historical slice fixtures
- `data/curated/rescue_tasks/rescue_task_registry.csv`: rescue-task identity and contract index kept separate from the shipped benchmark registry

Snapshot materialization behavior:

1. find the latest descriptor whose `allowed_data_through` and `evidence_frozen_at` are both `<= as_of_date`
2. validate that the referenced archive file exists and matches the declared SHA256 digest
3. reject ambiguous ties on the newest eligible cutoff dates
4. emit an included `SourceSnapshot` when validation succeeds
5. emit an excluded `SourceSnapshot` with an explicit `exclusion_reason` when no valid pre-cutoff archive is available

Cohort materialization behavior:

- rows are emitted for every `(entity, horizon, label_name)` triple
- `no_qualifying_future_outcome` is computed by the builder, not supplied in the raw future-outcomes file
- `outcome_date` must be strictly after `as_of_date`
- `outcome_date` must be `<= outcome_observation_closed_at`
- intervention-object public slices derive `cohort_members.csv` and `future_outcomes.csv` from checked-in program-history tables rather than reusing the canonical `scz_small` gene/module fixture rows

## Artifact Families And Layout

The runner reads and writes these schema families exactly:

- `benchmark_snapshot_manifest`
- `benchmark_cohort_members`
- `benchmark_cohort_manifest`
- `benchmark_cohort_labels`
- `benchmark_model_run_manifest`
- `benchmark_metric_output_payload`
- `benchmark_confidence_interval_payload`

Those schemas are frozen in code as `BENCHMARK_ARTIFACT_SCHEMAS_V1`.
The supporting request files `snapshot_request.json` and `source_archives.json` are
operator inputs, not part of `BENCHMARK_ARTIFACT_SCHEMAS_V1`.
The public report-card and leaderboard JSON payloads are downstream derived outputs,
not additional runner-emitted schema families.

The matching registered schema files now live under
`schemas/artifact_schemas/benchmark_*.json`.
Runtime loading and validation for these artifacts is exposed through
`scz_target_engine.artifacts`.

Canonical generated locations:

- `data/benchmark/generated/scz_small/snapshot_manifest.json`: `benchmark_snapshot_manifest`
- `data/benchmark/generated/scz_small/benchmark_cohort_members.csv`: `benchmark_cohort_members`
- `data/benchmark/generated/scz_small/source_cohort_members.csv`: `benchmark_source_cohort_members`
- `data/benchmark/generated/scz_small/source_future_outcomes.csv`: `benchmark_source_future_outcomes`
- `data/benchmark/generated/scz_small/benchmark_cohort_manifest.json`: `benchmark_cohort_manifest`
- `data/benchmark/generated/scz_small/cohort_labels.csv`: `benchmark_cohort_labels`
- `data/benchmark/generated/scz_small/intervention_object_feature_bundle.parquet`: generated only when the snapshot request includes `intervention_object`
- `data/benchmark/generated/scz_small/runner_outputs/run_manifests/*.json`: `benchmark_model_run_manifest`
- `data/benchmark/generated/scz_small/runner_outputs/baseline_projections/<baseline_id>__intervention_object.json`: explicit intervention-object projection payload for projected baselines
- `data/benchmark/generated/scz_small/runner_outputs/metric_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_small/runner_outputs/confidence_interval_payloads/<run_id>/<entity_type>/<horizon>/<metric>.json`: `benchmark_confidence_interval_payload`
- `data/benchmark/generated/public_slices/<slice_id>/...`: local replay outputs for checked-in public slice inputs
- `data/benchmark/generated/scz_small/public_payloads/report_cards/scz_translational_suite/scz_translational_task/scz_fixture_2024_06_30/<run_id>.json`: public report card payload
- `data/benchmark/generated/scz_small/public_payloads/leaderboards/scz_translational_suite/scz_translational_task/scz_fixture_2024_06_30/<entity_type>/<horizon>/<metric>.json`: public leaderboard payload
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/metric_payloads/<run_id>/intervention_object/structural_replay/<metric>.json`: Track B `benchmark_metric_output_payload`
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/confidence_interval_payloads/<run_id>/intervention_object/structural_replay/<metric>.json`: Track B `benchmark_confidence_interval_payload`
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/track_b_case_outputs/<run_id>.json`: Track B per-case structural output sidecar
- `data/benchmark/generated/scz_failure_memory_2025_02_01/runner_outputs/track_b_confusion_summaries/<run_id>.json`: Track B confusion-summary sidecar
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/report_cards/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/<track_b_public_id>.json`: Track B public report card payload
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/leaderboards/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/intervention_object/structural_replay/<metric>.json`: Track B public leaderboard payload
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/error_analysis/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/<track_b_public_id>.md`: Track B markdown case review
- `data/benchmark/generated/scz_failure_memory_2025_02_01/public_payloads/error_analysis/scz_translational_suite/scz_failure_memory_track_b_task/scz_failure_memory_2025_02_01/<track_b_public_id>.json`: Track B public confusion-summary JSON
- `data/benchmark/generated/public_slices/<slice_id>/public_payloads/error_analysis/scz_translational_suite/scz_translational_task/<snapshot_id>/<run_id>.md`: markdown error analysis emitted for intervention-object runs only when the principal intervention-object slice is evaluable and the required bundle plus projection artifacts are present

What each generated artifact means:

- snapshot manifests freeze the suite/task contract identity, evidence boundary, leakage controls, requested baselines, and per-source inclusion or exclusion accounting
- cohort-member artifacts freeze the canonical denominator consumed by later runner and reporting validation
- source-copy cohort artifacts freeze the exact raw cohort inputs copied into the generated bundle so replay stays relocatable and self-contained
- cohort manifests freeze the digest-pinned bridge from snapshot manifest plus raw cohort inputs to the materialized cohort-member and cohort-label artifacts
- cohort label artifacts freeze future translational outcome labels over that canonical denominator
- intervention-object feature bundles freeze the replay-side program lineage, evidence availability, and compatibility inputs used to score one intervention-object snapshot
- run manifests record executed baseline, suite/task contract provenance, code version, parameterization, and input digests
- intervention-object baseline projection payloads freeze the explicit bridge from archived gene/module baseline outputs to intervention-object replay scores for one baseline and snapshot
- metric payloads record point estimates for one `(run_id, entity_type, horizon, metric_name)` slice
- confidence interval payloads record percentile-bootstrap intervals, bootstrap count, resample unit, and random seed for the same slice
- report cards join suite/task/snapshot provenance, source inclusion accounting,
  and per-slice metric summaries into one public payload per run; Track B public
  report cards rebuild `evaluation_input_artifacts` from the validated
  snapshot/cohort bundle, pin `source_snapshots` to the snapshot manifest, and
  point `derived_from_artifacts` at a materialized
  `validated_track_b_runner_bundle/<track_b_public_id>/...` tree
- leaderboard payloads rank the report-card slices by metric while preserving run-level provenance
- Track B case-output sidecars freeze per-case analog retrievals, structural predictions, and checklist predictions so reporting never has to rerun replay reasoning
- Track B confusion summaries freeze cross-case confusions and checklist misses for one run
- error-analysis markdown files explain misses and false positives on evaluable intervention-object replay slices using the frozen bundle metadata and explicit projection payloads

Runtime validation that now fails closed:

- runner and reporting require a materialized `benchmark_cohort_manifest.json` plus `benchmark_cohort_members.csv` beside the supplied `benchmark_cohort_labels.csv`
- cohort manifests must point at the canonical sibling `source_cohort_members.csv` and `source_future_outcomes.csv` copies emitted by `build-benchmark-cohort`
- cohort manifests must match the supplied snapshot manifest identity and digest
- cohort-label files must match the canonical cohort-member denominator, including exact entity set, stable labels, and full `horizon x label_name` coverage
- intervention-object replay bundles must match the frozen bundle schema name/version and the manifest `as_of_date`
- intervention-object replay bundles must align with the manifest source inclusion set and the exact intervention-object cohort rows

## Public Historical Slices

Public slices are checked-in fixture bundles under `data/benchmark/public_slices/`.
They are derived from the registry-backed `scz_translational_task`. They keep the frozen
benchmark question and leakage controls, but the shipped Track A replay path now
materializes them at `entity_type = intervention_object` with the executable baseline
subset `v0_current`, `v1_current`, and `random_with_coverage`.

Current honest public slices from the checked-in archive catalog:

- `scz_translational_2024_06_15`
- `scz_translational_2024_06_18`
- `scz_translational_2024_06_20`
- As of April 2, 2026, none are evaluable on the principal `3y` horizon because each checked-in slice yields zero positive intervention-object outcomes after strict pre-cutoff denominator filtering.

Each slice directory contains:

- `snapshot_request.json`: slice-specific cutoff, snapshot id, and suite/task provenance
- `source_archives.json`: copied archived source descriptor index with SHA256 digests
- `archives/`: copied archived source extracts referenced by the slice index
- `program_universe.csv` and `events.csv`: pinned program-history replay inputs used for intervention-object cohort and bundle regeneration
- `cohort_members.csv` and `future_outcomes.csv`: checked-in intervention-object cohort and label inputs derived from the denominator plus program-history event ledger
- checked-in intervention-object `entity_id` values now use the full replay grain
  `asset_lineage_id / target_class_lineage_id / modality / domain / population /
  regimen / stage_bucket`, not the shorter human-readable denominator slug

The canonical `scz_small` fixture remains the regression path for gene/module
benchmarking. Its checked-in archive contents were restored to the minimal
pre-Track-A two-gene/one-module surface so public-slice replay work does not
mutate the legacy gene/module regression path.

The slice catalog at `data/benchmark/public_slices/catalog.json` records the exact
included and excluded sources for each cutoff. Missing historical archives stay explicit
exclusions; the backfill path does not fall back to live source data.

Replay example on a checked-in public slice such as `scz_translational_2024_06_20`:

```bash
uv run scz-target-engine build-benchmark-snapshot \
  --request-file data/benchmark/public_slices/scz_translational_2024_06_20/snapshot_request.json \
  --archive-index-file data/benchmark/public_slices/scz_translational_2024_06_20/source_archives.json \
  --output-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json \
  --materialized-at 2026-03-30

uv run scz-target-engine build-benchmark-cohort \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json \
  --cohort-members-file data/benchmark/public_slices/scz_translational_2024_06_20/cohort_members.csv \
  --future-outcomes-file data/benchmark/public_slices/scz_translational_2024_06_20/future_outcomes.csv \
  --output-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv

uv run scz-target-engine run-benchmark \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv \
  --archive-index-file data/benchmark/public_slices/scz_translational_2024_06_20/source_archives.json \
  --output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/runner_outputs \
  --config config/v0.toml \
  --deterministic-test-mode

uv run scz-target-engine build-benchmark-reporting \
  --manifest-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/snapshot_manifest.json \
  --cohort-labels-file data/benchmark/generated/public_slices/scz_translational_2024_06_20/cohort_labels.csv \
  --runner-output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/runner_outputs \
  --output-dir data/benchmark/generated/public_slices/scz_translational_2024_06_20/public_payloads
```

That replay writes the intervention-object feature bundle beside the generated
snapshot manifest, explicit projected baseline payloads under
`runner_outputs/baseline_projections/`, and intervention-object leaderboard
outputs under `public_payloads/`. Because the shipped checked-in public slices
have zero principal-horizon positives, the reporting step currently produces no
error-analysis markdown for them.

## Current Runner Coverage

The runner executes only baselines whose frozen protocol status is `available_now`
and that are explicitly listed in the snapshot manifest's `baseline_ids`.

Implemented executable baselines:

1. `pgc_only`
2. `schema_only`
3. `opentargets_only`
4. `v0_current`
5. `v1_current`
6. `chembl_only`
7. `random_with_coverage`
8. `track_b_exact_target`
9. `track_b_target_class`
10. `track_b_nearest_history`
11. `track_b_structural_current`

Explicit protocol-only baselines that remain declared but non-executed:

1. `v1_pre_numeric_pr7_heads`
2. `v1_post_numeric_pr7_heads`

The runner consumes:

- a built `benchmark_snapshot_manifest`
- a built `benchmark_cohort_labels` artifact
- the archived source descriptor index used to resolve the actual archived source files

The checked-in deterministic fixture intentionally executes only:

- `pgc_only`
- `opentargets_only`
- `v0_current`
- `v1_current`
- `random_with_coverage`

That fixture includes archived `PGC`, `Open Targets`, and `PsychENCODE` inputs, while
`SCHEMA` and `ChEMBL` remain explicit exclusions at the `2024-06-30` cutoff.

The checked-in public slices intentionally execute only:

- `v0_current`
- `v1_current`
- `random_with_coverage`

Those slices are intervention-object replay tasks, so `pgc_only`, `schema_only`,
`opentargets_only`, and `chembl_only` stay outside the current Track A public replay
surface.

The checked-in Track B fixture intentionally executes only:

- `track_b_exact_target`
- `track_b_target_class`
- `track_b_nearest_history`
- `track_b_structural_current`

## Metric Bundle And Interval Method

Current metric payloads treat
`future_schizophrenia_program_started`,
`future_schizophrenia_program_advanced`, and
`future_schizophrenia_positive_signal`
as the positive retrieval target for each `(entity_type, horizon)` slice.

Primary metrics are computed on the full admissible cohort for that slice. If a baseline
cannot score an admissible entity, that entity stays in evaluation as a deterministic
bottom-tier row and the emitted `notes` keep coverage explicit as
`covered_entities=<covered>/<admissible>`.

The runner emits:

- `average_precision_any_positive_outcome`
- `mean_reciprocal_rank_any_positive_outcome`
- `precision_at_1_any_positive_outcome`
- `precision_at_3_any_positive_outcome`
- `precision_at_5_any_positive_outcome`
- `recall_at_1_any_positive_outcome`
- `recall_at_3_any_positive_outcome`
- `recall_at_5_any_positive_outcome`

`future_schizophrenia_negative_signal` and `no_qualifying_future_outcome` remain explicit
in the label artifact but are treated as non-relevant for the current metric bundle.

The `v1_current` comparator resolves the current additive `v1` output by taking the mean
available `domain_head_score_v1` across domain profiles for each entity.

Current confidence interval payloads use percentile bootstrap with:

- resample unit `entity`, sampled with replacement and replayed in original rank order within each replicate
- explicit bootstrap iteration count on every payload
- explicit random seed on every payload
- deterministic test mode via a fixed seed and reduced iteration count

Track B uses the same confidence-interval artifact family, but with the structural
replay metric bundle:

- `analog_recall_at_3`
- `failure_scope_macro_f1`
- `what_must_differ_checklist_f1`
- `replay_status_exact_match`

Track B bootstrap payloads resample at unit `case`. The corresponding runner output
also writes per-case structural sidecars and a confusion summary so the reporting step
can emit case-review markdown without rerunning replay logic.

## Operator Flow

- Re-run the three canonical commands in order whenever the snapshot request, archive descriptors, future-outcome labels, code version, or benchmark parameters change.
- Re-running the snapshot or cohort command overwrites the manifest and label file at the same path.
- Re-running `run-benchmark` writes run-id keyed payload directories. Changing code version or benchmark parameters changes the run id. Identical inputs overwrite the same run-id files.
- If you want scratch outputs without touching the canonical generated path, run the same commands with `.context/...` output paths.

## What This Still Does Not Claim

- historical source backfills
- synthetic historical snapshot generation
- protocol-only baseline execution without the required archived artifacts
- calibration, threshold selection, or production deployment readiness

The goal is a stable benchmark contract that later PRs can widen without changing what
the benchmark means.
