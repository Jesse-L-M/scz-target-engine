#!/usr/bin/env python3
"""Example: run the hypothesis co-scientist against checked-in artifacts.

This script demonstrates the end-to-end drafting flow:
  1. Build hypothesis packets from shipped v0 input artifacts.
  2. Augment with synthetic rescue decisions for CHRM4.
  3. Load the checked-in prospective forecast registration.
  4. Build grounded hypothesis drafts for all packets.
  5. Write the draft payload to JSON for human review.

Usage
-----
  uv run python examples/hypothesis_co_scientist/run_hypothesis_draft.py

The draft is written to examples/hypothesis_co_scientist/output/hypothesis_drafts.json.
Every section requires explicit human review -- no decisions are made autonomously.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from scz_target_engine.agents.hypothesis_agent import (
    build_hypothesis_drafts,
    write_hypothesis_drafts,
)
from scz_target_engine.config import load_config
from scz_target_engine.engine import build_outputs
from scz_target_engine.hypothesis_lab.rescue_sections import (
    augment_packets_with_rescue,
)


def main() -> None:
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Build hypothesis packets from v0 inputs
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        config = load_config(_project_root / "config" / "v0.toml")
        build_outputs(config, (_project_root / "examples" / "v0" / "input").resolve(), tmp_path)
        payload = json.loads((tmp_path / "hypothesis_packets_v1.json").read_text())

    print(f"Built {payload['packet_count']} hypothesis packets.")

    # Step 2: Augment with synthetic rescue decisions for CHRM4
    chrm4_id = next(
        p["entity_id"]
        for p in payload["packets"]
        if p["entity_label"] == "CHRM4"
    )
    rescue_decisions = {
        chrm4_id: {
            "task_id": "glutamatergic_convergence_rescue_task",
            "task_label": "Glutamatergic convergence rescue task",
            "entity_id": chrm4_id,
            "decision": "advance",
        }
    }
    augmented = augment_packets_with_rescue(
        payload,
        rescue_entity_decisions=rescue_decisions,
        baseline_comparison_summaries=[],
    )
    match_count = augmented["rescue_augmentation"]["rescue_match_count"]
    print(f"Rescue augmentation: {match_count} matched, "
          f"{augmented['rescue_augmentation']['rescue_unmatched_count']} unmatched.")

    # Step 3: Load checked-in prospective registration
    registration_path = (
        _project_root / "data" / "prospective_registry" / "registrations"
        / "forecast_chrm4_acute_translation_guardrails_2026_03_31.json"
    )
    registration = json.loads(registration_path.read_text(encoding="utf-8"))
    print(f"Loaded prospective registration: {registration['registration_id']}")

    # Step 4: Build drafts
    draft_payload = build_hypothesis_drafts(
        augmented,
        prospective_registrations=[registration],
        source_artifacts={
            "hypothesis_packets_v1": "examples/v0/output/hypothesis_packets_v1.json",
            "prospective_registration": str(registration_path.relative_to(_project_root)),
        },
    )

    # Step 5: Write output
    output_path = output_dir / "hypothesis_drafts.json"
    write_hypothesis_drafts(output_path, draft_payload)
    print(f"\nWrote {len(draft_payload.drafts)} hypothesis drafts to {output_path}")
    print(f"Schema version: {draft_payload.schema_version}")
    print(f"Trust boundary: {draft_payload.trust_boundary}")

    # Summary
    print("\nDraft summary:")
    for draft in draft_payload.drafts:
        credibility = "yes" if any(
            s.section_id == "credibility_grounding"
            and "No prospective" not in s.lines[0]
            for s in draft.sections
        ) else "no"
        print(
            f"  {draft.entity_label} / {draft.policy_id}: "
            f"contradiction={draft.contradiction_status}, "
            f"replay={draft.replay_status}, "
            f"rescue_alignment={draft.rescue_policy_alignment}, "
            f"credibility_grounding={credibility}"
        )


if __name__ == "__main__":
    main()
