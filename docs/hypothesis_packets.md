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
- policy rows with a fully scored policy signal:
  non-null `score`, `base_score`, and `score_before_clamp`, plus `status` in
  `{available, partial}`

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
It now also validates the generated `hypothesis_packets_v1` payload against the
repo artifact contract before returning or writing output.

## Blinded Expert Review Pilot

`PR-61` adds a downstream review-materials path that consumes shipped
`hypothesis_packets_v1.json` artifacts and generates a blinded packet-comparison
pilot.

The generator intentionally does not mutate the shipped packet artifact.
It produces:

- `blinded_expert_review_packets_v1.json`:
  reviewer-facing blinded packet comparisons
- `blinded_expert_review_key_v1.json`:
  the internal mapping back to source packet ids, source field paths, and
  shipped-artifact traceability
- `blinded_expert_review_response_template_v1.json`:
  the structured response template aligned to the rubric under
  `docs/review_rubrics/blinded_expert_review_rubric.*`

Run it from an already-built hypothesis packet artifact:

```bash
PYTHONPATH=src python3 -m scz_target_engine.cli build-expert-review-packets \
  --hypothesis-artifact examples/v0/output/hypothesis_packets_v1.json \
  --output-dir examples/expert_review
```

The checked-in pilot preserves two hard boundaries:

- every blinded comparison includes the richer expert packet plus a simpler baseline
  packet derived from the same shipped source packet
- the admin key records enough traceability to map every reviewed packet variant
  back to the shipped packet artifact and the exact source packet fields it used

The completed pilot outputs under `examples/expert_review/` are intended to feed
the next packet-contract revision rather than to replace the shipped packet
artifact directly.
