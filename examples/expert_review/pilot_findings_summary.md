# Blinded Expert Review Pilot Findings

This directory contains the completed blinded packet-review pilot generated from
`examples/v0/output/hypothesis_packets_v1.json`.

## Outcome

- reviewed comparisons: `8`
- preferred richer packet style: `8 / 8`
- preferred baseline style: `0 / 8`
- `PR-53` status: `unblocked`

The richer blinded packet consistently beat the simpler baseline because it kept
three things in one place:

- the review ask
- the evidence or evidence-gap anchors
- the change-my-mind conditions

## Design-Changing Evidence

The blinded comparison did more than show style preference. It exposed four
contract changes needed for the next packet revision:

1. Add `decision_focus` or `review_call`.
The review packet should say what decision the expert is being asked to make and
where their verdict belongs.

2. Add first-class `evidence_anchors`.
Reviewers wanted event rows with `role`, `event_id`, `event_type`, `outcome`,
and `why_it_matters` instead of raw program ids or prose-only references.

3. Add structured gap states.
For `SLC39A8` and `SLC6A1`, the winning packet made the absence of anchors
useful. The packet contract should explicitly emit `evidence_anchor_gap_status`
and, where relevant, `program_history_gap_status`.

4. Revise the generator to emit digests first.
Reviewers wanted a short `risk_digest` and `evidence_needed_next` summary ahead
of the full contradiction and open-risk blocks.

## Implemented In PR-53

`PR-53` translates the pilot findings into a narrow packet-contract revision:

- finding: reviewers needed the review ask and disposition slot to be explicit
  change: add `decision_focus.review_question`, `decision_options`, and
  `current_readout`
- finding: reviewers preferred anchor rows over raw ids or prose-only mentions
  change: add first-class `evidence_anchors` with `role`, `event_id`,
  `event_type`, `outcome`, and `why_it_matters`
- finding: anchor absence was useful only when made explicit
  change: add `evidence_anchor_gap_status` and `program_history_gap_status`
- finding: short summaries should appear before long risk blocks
  change: add `risk_digest` and `evidence_needed_next` while preserving the full
  `contradiction_handling` and `failure_escape_logic` payloads

The historical blinded review materials referenced by `pilot_results_v1.json`
now live under `examples/expert_review/pilot_v1/`. The root
`examples/expert_review/*.json` artifacts are the current generator outputs from
the revised packet contract.
