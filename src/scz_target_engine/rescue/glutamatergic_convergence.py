from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import read_csv_rows
from scz_target_engine.rescue.governance import (
    RescueGovernanceBundle,
    validate_rescue_governance_bundle,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH = (
    REPO_ROOT
    / "data"
    / "curated"
    / "rescue_tasks"
    / "glutamatergic_convergence"
    / "task_card.json"
)
GLUTAMATERGIC_CONVERGENCE_RANKING_INPUTS_PATH = (
    REPO_ROOT
    / "data"
    / "curated"
    / "rescue_tasks"
    / "glutamatergic_convergence"
    / "frozen"
    / "glutamatergic_convergence_ranking_inputs_2025_01_15.csv"
)
GLUTAMATERGIC_CONVERGENCE_EVALUATION_LABELS_PATH = (
    REPO_ROOT
    / "data"
    / "curated"
    / "rescue_tasks"
    / "glutamatergic_convergence"
    / "frozen"
    / "glutamatergic_convergence_evaluation_labels_2025_06_30.csv"
)
GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH = (
    REPO_ROOT
    / "data"
    / "curated"
    / "atlas"
    / "glutamatergic_convergence_fixture"
    / "example_ingest_manifest.json"
)


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


@dataclass(frozen=True)
class GlutamatergicConvergenceRescueBundle:
    governance_bundle: RescueGovernanceBundle
    ranking_input_rows: tuple[dict[str, str], ...]
    evaluation_label_rows: tuple[dict[str, str], ...]


def load_glutamatergic_convergence_rescue_bundle() -> (
    GlutamatergicConvergenceRescueBundle
):
    governance_bundle = validate_rescue_governance_bundle(
        GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH
    )
    dataset_cards = {
        dataset.dataset_id: dataset for dataset in governance_bundle.dataset_cards
    }

    ranking_card = dataset_cards["glutamatergic_convergence_ranking_inputs_2025_01_15"]
    evaluation_card = dataset_cards[
        "glutamatergic_convergence_evaluation_labels_2025_06_30"
    ]

    ranking_inputs_path = _resolve_repo_relative_path(ranking_card.expected_output_path)
    evaluation_labels_path = _resolve_repo_relative_path(
        evaluation_card.expected_output_path
    )
    if ranking_inputs_path != GLUTAMATERGIC_CONVERGENCE_RANKING_INPUTS_PATH:
        raise ValueError(
            "ranking input dataset card must point to the checked-in glutamatergic "
            "convergence frozen CSV"
        )
    if evaluation_labels_path != GLUTAMATERGIC_CONVERGENCE_EVALUATION_LABELS_PATH:
        raise ValueError(
            "evaluation label dataset card must point to the checked-in glutamatergic "
            "convergence frozen CSV"
        )

    ranking_input_rows = tuple(read_csv_rows(ranking_inputs_path))
    evaluation_label_rows = tuple(read_csv_rows(evaluation_labels_path))
    if not ranking_input_rows:
        raise ValueError("glutamatergic convergence ranking inputs must not be empty")
    if not evaluation_label_rows:
        raise ValueError("glutamatergic convergence evaluation labels must not be empty")

    return GlutamatergicConvergenceRescueBundle(
        governance_bundle=governance_bundle,
        ranking_input_rows=ranking_input_rows,
        evaluation_label_rows=evaluation_label_rows,
    )


__all__ = [
    "GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH",
    "GLUTAMATERGIC_CONVERGENCE_EVALUATION_LABELS_PATH",
    "GLUTAMATERGIC_CONVERGENCE_RANKING_INPUTS_PATH",
    "GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH",
    "GlutamatergicConvergenceRescueBundle",
    "load_glutamatergic_convergence_rescue_bundle",
]
