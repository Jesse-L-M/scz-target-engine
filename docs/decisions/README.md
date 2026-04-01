# Decisions

This directory holds small, durable decisions that affect multiple specs or
multiple future PRs.

Use this when a choice would otherwise get repeated in chat, buried in a PR
thread, or drift across workspaces.

## Keep It Small

Write a decision record only when the choice changes more than one workstream.

Examples:

- intervention-object compatibility policy
- replay leakage rules
- rescue model admission policy
- planning-contract rules for specs and supersession

Do not log every local implementation detail.

## Format

Use numbered files:

- `0001-...`
- `0002-...`

Each decision should include:

- Status
- Date
- Context
- Decision
- Consequences
- Affected specs
