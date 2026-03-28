# Program History Data

This directory holds curated, checked-in landmark schizophrenia program events.

Files:

- `programs.csv`: seed event-level table with source URLs, ontology mapping, and first-pass failure-taxonomy labels
- `failure_taxonomy.md`: maintained definitions for the shared failure vocabulary
- `directionality_hypotheses.csv`: small explicit target-level directionality and modality hypotheses used for PR7 ledgers

See [docs/program_history.md](../../../docs/program_history.md) for schema, curation standards, and maintainer rules.
See [docs/ledger_contract.md](../../../docs/ledger_contract.md) for the emitted target-ledger contract.

This data is substrate only in `v0`. It now feeds structural ledger outputs, but it is still not wired into numeric ranking.
