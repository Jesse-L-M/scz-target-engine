# Target Ledger Contract

`PR7` adds a scoring-neutral target-ledger artifact to `v0`.

The ledger exists so later PRs can consume structural failure history, directionality hypotheses, and source primitives without reverse-engineering markdown cards. `PR8.1` now consumes these target-ledger fields into numeric `v1` heads while keeping shared `v0` score, rank, and stability outputs unchanged.

## Outputs

Each `build` now emits:

- `gene_target_ledgers.json`: nested target-level ledger payload for every gene target in the build input, including unranked or ineligible rows
- `gene_rankings.csv`: the existing flat ranking file plus compact ledger summary columns

These outputs are structural only. They do not change composite scores, ranks, eligibility, or `heuristic_stable`.

## `gene_target_ledgers.json` Shape

Top-level payload fields:

- `schema_version`: currently `pr7-target-ledger-v1`
- `scoring_neutral`: always `true` in `v0`
- `data_sources`: checked-in curated substrate inputs used to build the ledgers
- `target_count`
- `targets_with_program_history`
- `targets_with_curated_directionality`
- `targets`: one ledger object per gene target

Per-target ledger fields:

- `entity_id`
- `entity_label`
- `v0_snapshot`
- `source_primitives`
- `subgroup_domain_relevance`
- `structural_failure_history`
- `directionality_hypothesis`
- `falsification_conditions`
- `open_risks`

## Structural Failure History

`structural_failure_history` makes failure evidence machine-readable.

Summary fields:

- `matched_event_count`
- `failure_event_count`
- `nonfailure_event_count`
- `event_count_by_scope`
- `failure_taxonomy_counts`
- `failure_scopes`
- `failure_taxonomies`
- `events`

Each event carries:

- `failure_reason_taxonomy`: curated label from `failure_taxonomy.md`
- `failure_scope`: normalized structural scope such as `target_class`, `molecule`, `endpoint`, `population`, `target`, `unresolved`, or `nonfailure`
- `what_failed`: the specific object currently judged to have failed at that scope. When the scope is `unresolved`, this stays `undetermined` rather than asserting a molecule or class failure.
- `where`: structured location of the event via `domain`, `population`, `phase`, and `mono_or_adjunct`
- `evidence_strength`: `strong`, `moderate`, or `provisional`, derived from curator confidence
- `source_tier` and `source_url`

That contract is the PR7 boundary for "what failed", "where it failed", and "how strong the evidence is".

## Directionality / Modality Hypothesis

`directionality_hypothesis` carries an explicit inspectable substrate for later scoring work.

Fields:

- `status`: `curated` or `undetermined`
- `desired_perturbation_direction`
- `modality_hypothesis`
- `preferred_modalities`
- `confidence`
- `ambiguity`
- `evidence_basis`
- `supporting_program_ids`
- `contradiction_conditions`
- `falsification_conditions`
- `open_risks`

`undetermined` is a valid state. Missing curation should stay explicit rather than being hidden in prose.

## Flat Ranking Summary Columns

`gene_rankings.csv` now also includes:

- `program_history_event_count`
- `failure_event_count`
- `failure_scopes`
- `failure_taxonomies`
- `program_history_domains`
- `program_history_populations`
- `directionality_status`
- `desired_perturbation_direction`
- `modality_hypothesis`
- `directionality_confidence`

These are summary fields only. Full nested details remain in `gene_target_ledgers.json`.
