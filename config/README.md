## Config Tree

`config/v0.toml` remains the canonical runtime path for the current repo.

`config/engine/v0.toml` is an additive mirror for the new CLI namespace tree. It
exists so namespaced commands can point at a namespaced config path without changing
current behavior.

Current posture:

- Keep `config/v0.toml` stable for existing scripts, docs, and benchmark workflows.
- Mirror the active engine config under `config/engine/`.
- Add future module-specific config families under their own namespace directories
  instead of overloading the root `config/` path.

Nothing in the repo requires the namespaced mirror today. It is scaffolding for the
CLI migration, not a new source of truth.
