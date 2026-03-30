# Program History Data

This directory holds curated, checked-in landmark schizophrenia program events.

Files:

- `v2/`: normalized source-of-truth tables for assets, events, provenance, and target-level directionality hypotheses
- `programs.csv`: compatibility event view used by current ledger consumers
- `failure_taxonomy.md`: maintained definitions for the shared failure vocabulary
- `directionality_hypotheses.csv`: compatibility target-level directionality view used by current ledgers

See [docs/program_history.md](../../../docs/program_history.md) for schema, curation standards, and maintainer rules.
See [docs/ledger_contract.md](../../../docs/ledger_contract.md) for the emitted target-ledger contract.

This data is substrate only for shared `v0` scores. The normalized v2 tables now back
the compatibility views used by current ledger outputs and downstream `v1` gene heads,
while shared `v0` numeric ranking remains unchanged.
