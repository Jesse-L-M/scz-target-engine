#!/usr/bin/env python3
"""Example: run the curation assistant against checked-in program-memory artifacts.

This script demonstrates the end-to-end assistant flow:
  1. Load the checked-in v2 program-memory dataset.
  2. Optionally attach a harvest batch with machine suggestions.
  3. Build a curation draft (scoped or unscoped).
  4. Write the draft to a JSON file for human review.

Usage
-----
  uv run python examples/curation_assistant/run_curation_draft.py

The draft is written to examples/curation_assistant/output/curation_draft.json.
Every item in the draft requires explicit human adjudication -- no changes
are applied to the canonical dataset.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure the project is importable when run from the repo root.
_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from scz_target_engine.agents.program_memory_agent import (
    CurationDraftRequest,
    build_curation_draft,
    write_curation_draft,
)
from scz_target_engine.program_memory import build_program_memory_harvest_batch
from tests.program_memory_fixtures import (
    make_directionality_suggestion,
    make_event_suggestion,
    make_source_document,
)


def main() -> None:
    v2_dir = _project_root / "data" / "curated" / "program_history" / "v2"
    output_dir = Path(__file__).resolve().parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    draft_path = output_dir / "curation_draft.json"

    # --- full unscoped draft ---
    print("Building unscoped curation draft from checked-in v2 data ...")
    draft = build_curation_draft(v2_dir)
    write_curation_draft(draft_path, draft)
    print(f"  Draft written to {draft_path}")
    print(f"  Items: {len(draft.items)}")
    print(f"  Trust boundary: {draft.to_dict()['trust_boundary']}")
    print()

    # --- scoped draft for CHRM4 ---
    print("Building CHRM4-scoped draft ...")
    scoped_request = CurationDraftRequest(target="CHRM4")
    scoped_draft = build_curation_draft(v2_dir, request=scoped_request)
    scoped_path = output_dir / "curation_draft_chrm4.json"
    write_curation_draft(scoped_path, scoped_draft)
    print(f"  Draft written to {scoped_path}")
    print(f"  Items: {len(scoped_draft.items)}")
    print()

    # --- draft with harvest batch ---
    print("Building draft with harvest batch attached ...")
    harvest = build_program_memory_harvest_batch(
        harvest_id="example-harvest",
        harvester="example-curation-assistant",
        created_at="2026-03-31",
        source_document_payloads=[make_source_document()],
        suggestion_payloads=[
            make_event_suggestion(),
            make_directionality_suggestion(),
        ],
    )
    harvest_draft = build_curation_draft(v2_dir, harvest=harvest)
    harvest_path = output_dir / "curation_draft_with_harvest.json"
    write_curation_draft(harvest_path, harvest_draft)
    print(f"  Draft written to {harvest_path}")
    print(f"  Items: {len(harvest_draft.items)}")
    harvest_items = [i for i in harvest_draft.items if i.harvest_suggestion_ids]
    print(f"  Harvest-sourced items: {len(harvest_items)}")
    print()

    # --- summary ---
    print("=== Curation Draft Summary ===")
    payload = draft.to_dict()
    print(f"Schema: {payload['schema_version']}")
    print(f"Audit: {json.dumps(payload['audit_summary'], indent=2)}")
    print()
    for item_dict in payload["items"]:
        print(
            f"  [{item_dict['action']}] {item_dict['dimension']}/"
            f"{item_dict['scope_value']}: {item_dict['rationale'][:80]}..."
        )
    print()
    print("All items require human review before merge.")


if __name__ == "__main__":
    main()
