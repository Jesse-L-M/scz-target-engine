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

Policy posture:

- `config/policies/*.toml` is the additive policy family for `PR-33`.
- Those files change only the new policy-aware outputs; they must not mutate `v0`
  rank, score, stability, benchmark, or rescue semantics.
- Keep policy assumptions reviewable in checked-in TOML rather than hard-coding
  policy weights or uncertainty penalties in engine code.

Nothing in the repo requires the namespaced mirror today. It is scaffolding for the
CLI migration, not a new source of truth.
