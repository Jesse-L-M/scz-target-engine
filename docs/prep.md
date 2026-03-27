# Gene Table Prep

The engine does not want raw source exports forever. It wants a curated gene evidence table with clear ownership of each column.

## Current Command

`prepare-gene-table` joins:

- a seed gene list or draft gene evidence table
- optional `PGC scz2022` prioritized-gene output
- optional `SCHEMA` rare-variant output
- optional `Open Targets` baseline output
- optional `ChEMBL` tractability output

It emits an engine-ready CSV with:

- required engine layer columns always present
- source-owned columns merged in
- `canonical_entity_id`
- source match keys
- source presence flags
- provenance JSON

## Join Rules

For each source:

1. match by `entity_id` first
2. if that fails, match by `entity_label` exactly, case-insensitive
3. if `PGC` matches, use its `entity_id` as `canonical_entity_id`
4. if `SCHEMA` matches with a confirmed source match, overwrite `canonical_entity_id` with its `entity_id`
5. if `Open Targets` matches, overwrite `canonical_entity_id` with its `entity_id`
6. keep the seed row as the row driver, do not expand or drop rows

## Why This Matters

This keeps source fetchers honest. `Open Targets` and `ChEMBL` stay as upstream adapters. The prep layer is where they become engine input.
