# Package Architecture

`PR-00` introduces namespaced subpackages without changing the current flat-module
behavior on `main`.

## Boundary

- `scz_target_engine.app`: user-facing entrypoints such as the CLI
- `scz_target_engine.artifacts`: artifact schema registration and runtime validation helpers
- `scz_target_engine.benchmark`: benchmark protocol, snapshot/cohort builders, metrics, and runner
- `scz_target_engine.core`: shared support code such as config loading, identity helpers, and file IO
- `scz_target_engine.domain`: ranking, ledgers, decision vectors, and reporting helpers
- `scz_target_engine.workflows`: orchestration layers for build, ingest, prep, and registry flows
- `scz_target_engine.sources`: upstream source adapters

## Compatibility Strategy

This PR keeps the legacy flat modules as the implementation source of truth:

- `scz_target_engine.cli`
- `scz_target_engine.benchmark_*`
- `scz_target_engine.config`
- `scz_target_engine.decision_vector`
- `scz_target_engine.engine`
- `scz_target_engine.identity`
- `scz_target_engine.ingest`
- `scz_target_engine.io`
- `scz_target_engine.ledger`
- `scz_target_engine.prepare`
- `scz_target_engine.registry`
- `scz_target_engine.reporting`
- `scz_target_engine.scoring`

The new subpackage modules are narrow wrappers that re-export those legacy modules.
That means:

- current imports keep working unchanged
- current CLI entrypoint stays `scz_target_engine.cli:main`
- monkeypatching and existing tests still bind against the legacy module objects
- later PRs can migrate one boundary at a time into the new subpackages

## Old-To-New Mapping

- `scz_target_engine.cli` -> `scz_target_engine.app.cli`
- `scz_target_engine.benchmark_labels` -> `scz_target_engine.benchmark.labels`
- `scz_target_engine.benchmark_metrics` -> `scz_target_engine.benchmark.metrics`
- `scz_target_engine.benchmark_protocol` -> `scz_target_engine.benchmark.protocol`
- `scz_target_engine.benchmark_runner` -> `scz_target_engine.benchmark.runner`
- `scz_target_engine.benchmark_snapshots` -> `scz_target_engine.benchmark.snapshots`
- `scz_target_engine.config` -> `scz_target_engine.core.config`
- `scz_target_engine.identity` -> `scz_target_engine.core.identity`
- `scz_target_engine.io` -> `scz_target_engine.core.io`
- `scz_target_engine.decision_vector` -> `scz_target_engine.domain.decision_vector`
- `scz_target_engine.ledger` -> `scz_target_engine.domain.ledger`
- `scz_target_engine.reporting` -> `scz_target_engine.domain.reporting`
- `scz_target_engine.scoring` -> `scz_target_engine.domain.scoring`
- `scz_target_engine.engine` -> `scz_target_engine.workflows.engine`
- `scz_target_engine.ingest` -> `scz_target_engine.workflows.ingest`
- `scz_target_engine.prepare` -> `scz_target_engine.workflows.prepare`
- `scz_target_engine.registry` -> `scz_target_engine.workflows.registry`

## Follow-On Work

`PR-01` and `PR-02` can now move implementation module-by-module under the namespaced
packages and flip the legacy flat modules into reverse-direction shims only where that
migration is complete.
