# variant-to-context-v1

Status: draft
Owner branch: Jesse-L-M/calibrate-review
Depends on: docs/designs/contracts-and-compat-v2.md, docs/designs/scz-rescue-1-v1.md
Blocked by: concrete replay and rescue feature questions selected from prior milestones
Supersedes: -
Last updated: 2026-04-01

## Objective

Turn the atlas from a provenance-bearing evidence substrate into a usable
variant-to-context feature store for replay and rescue.

The key idea is narrow:
extend the existing atlas ingest, tensor, mechanistic-axis, and convergence-hub
layers so replay and rescue can ask concrete questions like:

- which variant-backed genes matter in this cell context?
- which developmental window matters?
- which intervention object or rescue candidate is supported by that context?

This is not a scoring rewrite and not a UI project.

## Success Condition

- Primary success metric:
  every admitted rescue model and every intervention-object replay run can point to
  a frozen context-feature manifest describing which variant-to-gene, cell-context,
  and developmental features were used
- Secondary success metric:
  context-feature outputs preserve explicit missingness, conflict, and uncertainty
  instead of flattening them away
- Stop/go gate:
  do not expand the atlas into new sources or abstractions that are not consumed by
  replay or rescue within the same milestone window

## Scope

- extend atlas source ingest and tensor support for SCHEMA and PsychENCODE
- add developmental and regulatory context joins where public, queryable, and
  provenance-preserving
- materialize feature-store outputs that map:
  variant -> gene -> cell context -> developmental window -> rescue / intervention object
- preserve current tensor, mechanistic-axis, and convergence-hub semantics as
  additive layers
- emit a context-feature manifest and feature tables consumable by replay and rescue

## Not in Scope

- replacing current scoring inputs
- raw consortium-dump parsing marathons
- graph database work
- atlas UI
- chemistry-first modeling or de novo molecule generation

## Existing Surfaces To Reuse

- atlas source ingest:
  keep the additive staged raw-source contract on top of current source adapters
- atlas tensor:
  keep `observed`, `missingness`, `conflict`, and `uncertainty` channels explicit
- mechanistic axes and convergence hubs:
  reuse them as auditable summaries, not scalar scores
- current rescue and replay feature consumers:
  make the feature store serve those workstreams instead of inventing a standalone
  atlas product

## Inputs

- current atlas ingest manifest and staged raw-source manifests
- processed source tables for `Open Targets` and `PGC`
- new adapter-backed processed inputs for SCHEMA and PsychENCODE
- public developmental and regulatory context resources that can be staged with
  the same provenance contract
- intervention-object compatibility mapping
- rescue and replay feature requests defined by earlier milestone specs

## Outputs And Artifact Contracts

- New or changed artifact:
  `variant_gene_context_links.csv`
  mapping variant-backed evidence to genes, context dimensions, and provenance bundles
- New or changed artifact:
  `context_feature_rows.csv`
  normalized replay/rescue feature rows keyed by consumer entity and context feature id
- New or changed artifact:
  `context_feature_manifest.json`
  describing source manifests, tensor manifests, feature definitions, and consumer scope
- New or changed artifact:
  additive `variant_effect_priors` table if external priors are used, with explicit
  provenance and availability boundary
- Backward-compatibility rule:
  current atlas taxonomy, tensor, mechanistic-axis, and convergence-hub outputs stay
  valid and remain additive

### Proposed Consumer Keys

The feature store should support at least these consumer keys:

- `gene_id`
- `intervention_object_id`
- `rescue_task_id + entity_id`

The same underlying context feature may project to multiple consumers, but those
projections must stay explicit instead of being silently flattened.

## Data Flow

```text
ATLAS SOURCE ADAPTERS + STAGED RAW MANIFESTS
    -> INGEST MANIFEST
    -> TAXONOMY + TENSOR
    -> MECHANISTIC AXES + CONVERGENCE HUBS
    -> VARIANT/GENE/CONTEXT LINKS
    -> CONTEXT FEATURE ROWS + MANIFEST
    -> REPLAY / RESCUE FEATURE CONSUMERS
```

- Source adapters stage provenance-bearing raw artifacts and preserve processed outputs.
- Tensor builders preserve structural uncertainty and conflicts.
- Mechanistic axes and convergence hubs summarize auditable context without becoming
  ranks.
- Variant-to-context links and context-feature rows are the new feature-store layer.
- Replay and rescue consume the feature store through explicit manifests.

## Implementation Plan

1. Define the context-feature manifest and consumer-key contract.
2. Extend atlas source ingest for SCHEMA and PsychENCODE with the same staged raw
   provenance rules.
3. Add variant/gene/context linking tables on top of tensor and mechanistic-axis outputs.
4. Materialize context-feature rows scoped to replay and rescue consumers.
5. Add manifest validation and consumer-side load helpers.

## Acceptance Tests

- Unit:
  add tests for context-feature manifest validation, consumer-key projection, and
  preservation of conflict / uncertainty channels
- Integration:
  run atlas ingest -> tensor -> mechanistic axes -> convergence hubs -> context
  feature build on a checked-in fixture
- Regression:
  add a test that fails if an `id_conflict` alignment is silently reduced to one
  winner in the feature-store outputs
- E2E, if relevant:
  build context features from a fixture manifest and load them into one replay or
  rescue consumer without reopening raw source or post-cutoff task data

## Failure Modes

- Failure mode:
  context features quietly consume post-cutoff rescue labels or current-head state;
  consumer manifests must pin allowed inputs and forbid evaluation-label joins
- Failure mode:
  atlas alignments with structural conflict are flattened into one clean feature;
  conflict state must survive into the feature-store layer
- Failure mode:
  the feature store grows into a decorative abstraction with no real consumer;
  every emitted feature family must name its replay or rescue consumer scope

## Rollout / Compatibility

- current atlas CLI routes and current tensor outputs remain valid
- replay and rescue may consume context-feature manifests additively
- a breaking change is any implementation that repurposes the atlas as the sole
  scoring source or drops current tensor/manifest outputs without a compatibility path

## Open Questions

- Should context features be emitted in CSV first to match current atlas builders,
  with Parquet reserved for release bundling, or should the internal build surfaces
  switch to Parquet now?
- Which developmental / regulatory resources are worth adapter work first, given the
  rule that the atlas must answer replay or rescue questions within the same window?

## Decision Log Links

- `docs/decisions/0001-planning-contract.md`

## Commands

```bash
uv run scz-target-engine atlas ingest candidate-registry --output-file .context/atlas/candidate_gene_registry.csv --work-dir .context/atlas/work --raw-dir .context/atlas/raw
uv run scz-target-engine atlas build taxonomy --ingest-manifest-file data/curated/atlas/example_ingest_manifest.json --output-dir .context/atlas/taxonomy
uv run scz-target-engine atlas build tensor --ingest-manifest-file data/curated/atlas/example_ingest_manifest.json --output-dir .context/atlas/tensor
uv run scz-target-engine atlas build mechanistic-axes --tensor-manifest-file .context/atlas/tensor/tensor_manifest.json --output-dir .context/atlas/mechanistic_axes
uv run scz-target-engine atlas build convergence-hubs --tensor-manifest-file .context/atlas/tensor/tensor_manifest.json --output-dir .context/atlas/convergence
```
