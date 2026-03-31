# Hypothesis Packets

`PR-50` adds schema-validated hypothesis packets that sit on top of the shipped
`policy_decision_vectors_v2.json` and `gene_target_ledgers.json` artifacts.

The packet layer is intentionally downstream-only:

- it does not change policy scores
- it does not change scoring-neutral failure-memory ledgers
- it does not add rescue augmentation or review-pilot logic

Instead it packages policy-scoped, traceable hypotheses for targets whose
directionality substrate is already concrete enough to avoid vague stubs.

## Output

Each packet artifact is written as `hypothesis_packets_v1.json`.

The current generator only emits packets for:

- `gene` entities
- targets whose ledger directionality status is `curated`
- policy rows with a concrete scored policy signal

That gate is explicit in the artifact under `packet_generation_criteria` so
consumers can see why uncurated or scoreless targets were excluded.

`packet_count = 0` with `packets = []` is now a valid outcome.

That means:

- otherwise-valid builds do not fail just because no target passes the packet gate
- a curated target with only null policy scores is skipped instead of aborting the artifact

## Packet Shape

Top-level fields:

- `schema_version`
- `source_artifacts`
- `packet_generation_criteria`
- `packet_count`
- `packets`

Each packet is one `(gene target, policy)` pair and carries:

- a concrete `hypothesis` statement plus desired direction and modality
- the full `policy_signal` copied from the shipped policy artifact
- explicit `contradiction_handling`
- `failure_memory` with the full structural failure history slice and replay-risk slice
- `failure_escape_logic` with current escape routes and next falsification evidence
- `traceability` pointers back to the originating policy and ledger locations

## Explicit Guardrails

The packet contract preserves the parts the upstream artifacts already make explicit:

- contradiction handling stays machine-readable through
  `contradiction_handling.contradiction_conditions`
- failure escape logic stays explicit through
  `failure_escape_logic.status`, `escape_routes`, and `next_evidence`
- traceability stays explicit through artifact-path refs, JSON-pointer-style paths,
  supporting program ids, structural failure program ids, and replay-reason event ids
- packet validation now dereferences those pointers against the referenced
  `policy_decision_vectors_v2.json` and `gene_target_ledgers.json` artifacts instead of
  only checking that the pointer strings are non-empty; the score pointer must also
  belong to the same resolved policy entity and policy context as the packet payload

The validator rejects vague packet stubs by requiring:

- curated directionality-backed hypotheses
- non-`undetermined` perturbation direction and modality fields
- non-empty preferred modalities
- a non-placeholder hypothesis statement
- a scored policy signal for every emitted packet, including non-null score fields and
  an `available` or `partial` policy status, while allowing the artifact itself to be empty

## Generation Paths

`build_outputs(...)` now emits `hypothesis_packets_v1.json` automatically after it
writes `policy_decision_vectors_v2.json` and `gene_target_ledgers.json`.

For already-built artifacts, the standalone generator path is:

```bash
PYTHONPATH=src python3 -m scz_target_engine.cli build-hypothesis-packets \
  --policy-artifact path/to/policy_decision_vectors_v2.json \
  --ledger-artifact path/to/gene_target_ledgers.json \
  --output-file path/to/hypothesis_packets_v1.json
```

That path validates the source artifacts before materializing packets.
