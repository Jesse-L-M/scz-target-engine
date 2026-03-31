# Blinded Expert Review Rubric

This rubric is the contract for the blinded packet-review pilot generated from
`hypothesis_packets_v1.json`.

## Review Goal

Choose the blinded packet variant you would rather send into an expert packet
review, then record the contract changes the comparison exposed.

The comparison is not about target identity. It is about packet style:

- one variant is a traceable expert packet derived from the shipped hypothesis packet
- one variant is a simpler baseline packet derived from the same source packet
- the style labels stay hidden during review

## Scored Dimensions

Score each blinded packet on a 1-5 scale.

Each `dimensions[].dimension_id` becomes a key in the generated `blind_scores`
schema, so those ids must be unique within a rubric.

### `decision_readiness`

- `1`: the packet is too vague to support an advance, hold, or kill decision
- `5`: the decision path is explicit and reviewable

Question:
Could an expert decide advance, hold, or kill from this packet without hunting
through upstream artifacts?

### `evidence_traceability`

- `1`: claims require guesswork or extra lookup
- `5`: claims stay attached to specific artifact anchors

Question:
Can the packet's claims be traced back to shipped packet artifacts or concrete
program-history anchors?

### `falsifiability`

- `1`: no clear kill conditions or reversal path
- `5`: kill conditions and missing evidence are explicit

Question:
Does the packet make contradiction conditions or change-my-mind evidence explicit
enough to challenge the hypothesis?

### `schema_change_signal`

- `1`: the packet hides what needs to change
- `5`: the packet clearly exposes contract pressure points

Question:
Does the packet make missing contract fields obvious enough to feed the next
schema revision?

## Required Findings

Each comparison must record:

- `winner_reason`
- `loser_reason`
- `missing_fields`
- `traceability_gaps`
- `schema_change_requests`
- `generator_revision_requests`

Those fields are not decorative docs. The generated response template derives its
comparison-level finding fields directly from `required_findings`.

That means:

- adding a new required finding adds that field to every generated comparison entry
- removing a required finding removes that field from the generated template
- legacy findings keep their current empty defaults, while genuinely new finding
  names are emitted as `null` placeholders
- reserved comparison-template fields like `comparison_id`, `available_blind_ids`,
  `preferred_blind_id`, and `blind_scores` are invalid rubric finding names and are
  rejected instead of being silently overwritten

## Pilot Success Condition

The pilot is only useful if it produces contract pressure, not just style
preference.

That means the completed review should surface both:

- which blinded packet style experts preferred
- which packet fields and generator behaviors should change in the next schema revision

## PR-53 Translation

`PR-53` lands the concrete schema/generator translation the pilot was meant to
surface:

- `decision_readiness` now lands as packet-level `decision_focus`
- `evidence_traceability` now lands as first-class `evidence_anchors`
- anchor absence now lands as `evidence_anchor_gap_status` and
  `program_history_gap_status`
- the requested generator-first summaries now land as `risk_digest` and
  `evidence_needed_next`

The pilot rubric itself stays stable. The response-template contract is still
driven by `required_findings`; the new packet fields land in the upstream packet
contract and the traceable expert-packet renderer.
