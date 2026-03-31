from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from scz_target_engine.atlas.contracts import ATLAS_CONVERGENCE_CONTRACT_VERSION
from scz_target_engine.benchmark_metrics import (
    build_ranked_evaluation_rows,
    calculate_metric_values,
    count_relevant,
)
from scz_target_engine.io import write_csv, write_json
from scz_target_engine.rescue.frozen import (
    FrozenRescueTaskBundle,
    load_frozen_rescue_task_bundle,
)


DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID = (
    "glutamatergic_convergence_rescue_task"
)
DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID = (
    "convergence_state_baseline_v1"
)
DEFAULT_GLUTAMATERGIC_EVALUATION_LABEL_NAME = "follow_up_priority"

REQUIRED_GLUTAMATERGIC_RANKING_COLUMNS = (
    "gene_id",
    "gene_symbol",
    "approved_name",
    "hub_id",
    "alignment_id",
    "convergence_contract_version",
    "source_coverage_state",
    "axis_coverage_state",
    "missingness_state",
    "conflict_state",
    "uncertainty_max_level",
    "observed_axis_count",
    "partial_axis_count",
    "unobserved_axis_count",
    "observed_source_count",
    "missing_source_count",
    "clinical_translation_state",
    "clinical_translation_uncertainty_max_level",
    "disease_association_state",
    "disease_association_missingness_state",
    "disease_association_uncertainty_max_level",
    "variant_to_gene_state",
    "variant_to_gene_missingness_state",
    "variant_to_gene_uncertainty_max_level",
    "split_name",
)
REQUIRED_GLUTAMATERGIC_EVALUATION_COLUMNS = (
    "gene_id",
    "gene_symbol",
    "evaluation_label",
    "evaluation_label_name",
    "decision",
    "adjudicated_at",
    "decision_owner",
    "label_rationale",
    "split_name",
)

_SUPPORT_STATE_WEIGHTS = {
    "observed": 1.0,
    "partial_observed": 0.5,
    "unobserved": 0.0,
}
_SOURCE_COVERAGE_WEIGHTS = {
    "cross_source": 1.0,
    "single_source": 0.5,
    "none": 0.0,
}
_MISSINGNESS_WEIGHTS = {
    "none": 1.0,
    "mixed": 0.5,
    "source_absent": 0.0,
}
_UNCERTAINTY_WEIGHTS = {
    "low": 1.0,
    "medium": 0.5,
    "high": 0.0,
}
_SPLIT_ORDER = ("train", "validation", "test")

_PREDICTION_FIELDNAMES = [
    "task_id",
    "baseline_id",
    "rank",
    "gene_id",
    "gene_symbol",
    "approved_name",
    "hub_id",
    "alignment_id",
    "split_name",
    "rescue_score",
    "priority_tier",
    "convergence_contract_version",
    "source_coverage_state",
    "axis_coverage_state",
    "missingness_state",
    "conflict_state",
    "uncertainty_max_level",
    "observed_axis_count",
    "partial_axis_count",
    "unobserved_axis_count",
    "observed_source_count",
    "missing_source_count",
    "clinical_translation_state",
    "disease_association_state",
    "variant_to_gene_state",
]
_EVALUATION_ROW_FIELDNAMES = _PREDICTION_FIELDNAMES + [
    "evaluation_label",
    "evaluation_label_name",
    "decision",
    "adjudicated_at",
    "decision_owner",
    "label_rationale",
    "evaluation_relevant",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_default_task_card_path() -> Path:
    from scz_target_engine.rescue.registry import load_rescue_task_registrations

    for registration in load_rescue_task_registrations():
        if registration.task_id == DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID:
            return registration.task_card_file.resolve()
    raise KeyError(
        "unknown rescue task id: "
        f"{DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID}"
    )


def _require_columns(
    available_columns: tuple[str, ...],
    *,
    required_columns: tuple[str, ...],
    dataset_label: str,
) -> None:
    missing = [
        column_name
        for column_name in required_columns
        if column_name not in available_columns
    ]
    if missing:
        raise ValueError(
            f"{dataset_label} is missing required columns: "
            + ", ".join(missing)
        )


def _require_int(row: dict[str, str], field_name: str) -> int:
    value = row.get(field_name, "")
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer: {value}") from exc


def _require_weight(
    value: str,
    *,
    field_name: str,
    weights: dict[str, float],
) -> float:
    if value not in weights:
        raise ValueError(f"unsupported {field_name}: {value}")
    return weights[value]


def load_glutamatergic_convergence_rescue_task_bundle(
    *,
    task_card_path: Path | None = None,
) -> FrozenRescueTaskBundle:
    bundle = load_frozen_rescue_task_bundle(
        rescue_task_id=(
            DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID
            if task_card_path is None
            else None
        ),
        task_card_path=task_card_path,
    )
    if bundle.governance.task_card.task_id != DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID:
        raise ValueError(
            "glutamatergic convergence rescue task loader only supports "
            f"{DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID}"
        )
    _require_columns(
        bundle.ranking_input.columns,
        required_columns=REQUIRED_GLUTAMATERGIC_RANKING_COLUMNS,
        dataset_label=bundle.ranking_input.card.dataset_id,
    )
    _require_columns(
        bundle.evaluation_target.columns,
        required_columns=REQUIRED_GLUTAMATERGIC_EVALUATION_COLUMNS,
        dataset_label=bundle.evaluation_target.card.dataset_id,
    )
    contract_versions = {
        row["convergence_contract_version"] for row in bundle.ranking_input.rows
    }
    if contract_versions != {ATLAS_CONVERGENCE_CONTRACT_VERSION}:
        raise ValueError(
            "ranking_input must remain pinned to the shipped atlas convergence "
            f"contract version {ATLAS_CONVERGENCE_CONTRACT_VERSION}"
        )
    evaluation_label_names = {
        row["evaluation_label_name"] for row in bundle.evaluation_target.rows
    }
    if evaluation_label_names != {DEFAULT_GLUTAMATERGIC_EVALUATION_LABEL_NAME}:
        raise ValueError(
            "evaluation_target must expose only the held-out "
            f"{DEFAULT_GLUTAMATERGIC_EVALUATION_LABEL_NAME} label"
        )
    if bundle.governance.contract.leakage_boundary.forbid_evaluation_labels_in_ranking is not True:
        raise ValueError(
            "rescue leakage boundary must continue to forbid evaluation labels "
            "from ranking features"
        )
    return bundle


def _priority_tier(row: dict[str, str]) -> str:
    if (
        row["source_coverage_state"] == "cross_source"
        and _require_int(row, "observed_axis_count") >= 3
        and row["uncertainty_max_level"] == "low"
    ):
        return "full_convergence"
    if _require_int(row, "observed_axis_count") >= 1:
        return "partial_convergence"
    return "coverage_gap"


def _convergence_score(row: dict[str, str]) -> float:
    return round(
        (
            (_require_int(row, "observed_axis_count") * 4.0)
            + (_require_int(row, "partial_axis_count") * 1.5)
            - (_require_int(row, "unobserved_axis_count") * 0.25)
            + (_require_int(row, "observed_source_count") * 2.0)
            - (_require_int(row, "missing_source_count") * 0.5)
            + (
                _require_weight(
                    row["source_coverage_state"],
                    field_name="source_coverage_state",
                    weights=_SOURCE_COVERAGE_WEIGHTS,
                )
                * 2.0
            )
            + _require_weight(
                row["missingness_state"],
                field_name="missingness_state",
                weights=_MISSINGNESS_WEIGHTS,
            )
            + _require_weight(
                row["uncertainty_max_level"],
                field_name="uncertainty_max_level",
                weights=_UNCERTAINTY_WEIGHTS,
            )
            + _require_weight(
                row["clinical_translation_state"],
                field_name="clinical_translation_state",
                weights=_SUPPORT_STATE_WEIGHTS,
            )
            + (
                _require_weight(
                    row["disease_association_state"],
                    field_name="disease_association_state",
                    weights=_SUPPORT_STATE_WEIGHTS,
                )
                * 2.0
            )
            + (
                _require_weight(
                    row["variant_to_gene_state"],
                    field_name="variant_to_gene_state",
                    weights=_SUPPORT_STATE_WEIGHTS,
                )
                * 3.0
            )
            + (
                _require_weight(
                    row["clinical_translation_uncertainty_max_level"],
                    field_name="clinical_translation_uncertainty_max_level",
                    weights=_UNCERTAINTY_WEIGHTS,
                )
                * 0.5
            )
            + (
                _require_weight(
                    row["disease_association_missingness_state"],
                    field_name="disease_association_missingness_state",
                    weights=_MISSINGNESS_WEIGHTS,
                )
                * 0.75
            )
            + (
                _require_weight(
                    row["disease_association_uncertainty_max_level"],
                    field_name="disease_association_uncertainty_max_level",
                    weights=_UNCERTAINTY_WEIGHTS,
                )
                * 0.75
            )
            + (
                _require_weight(
                    row["variant_to_gene_missingness_state"],
                    field_name="variant_to_gene_missingness_state",
                    weights=_MISSINGNESS_WEIGHTS,
                )
                * 1.0
            )
            + (
                _require_weight(
                    row["variant_to_gene_uncertainty_max_level"],
                    field_name="variant_to_gene_uncertainty_max_level",
                    weights=_UNCERTAINTY_WEIGHTS,
                )
                * 1.0
            )
        ),
        6,
    )


def build_glutamatergic_convergence_ranked_predictions(
    bundle: FrozenRescueTaskBundle,
    *,
    baseline_id: str = DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
) -> list[dict[str, object]]:
    scored_rows: list[dict[str, object]] = []
    for row in bundle.ranking_input.rows:
        scored_rows.append(
            {
                **row,
                "task_id": bundle.governance.task_card.task_id,
                "baseline_id": baseline_id,
                "rescue_score": _convergence_score(row),
                "priority_tier": _priority_tier(row),
            }
        )
    scored_rows.sort(
        key=lambda row: (
            -float(row["rescue_score"]),
            -int(str(row["observed_axis_count"])),
            -int(str(row["observed_source_count"])),
            str(row["gene_symbol"]),
            str(row["gene_id"]),
        )
    )

    ranked_predictions: list[dict[str, object]] = []
    for rank, row in enumerate(scored_rows, start=1):
        ranked_predictions.append(
            {
                "task_id": row["task_id"],
                "baseline_id": row["baseline_id"],
                "rank": rank,
                "gene_id": row["gene_id"],
                "gene_symbol": row["gene_symbol"],
                "approved_name": row["approved_name"],
                "hub_id": row["hub_id"],
                "alignment_id": row["alignment_id"],
                "split_name": row["split_name"],
                "rescue_score": row["rescue_score"],
                "priority_tier": row["priority_tier"],
                "convergence_contract_version": row["convergence_contract_version"],
                "source_coverage_state": row["source_coverage_state"],
                "axis_coverage_state": row["axis_coverage_state"],
                "missingness_state": row["missingness_state"],
                "conflict_state": row["conflict_state"],
                "uncertainty_max_level": row["uncertainty_max_level"],
                "observed_axis_count": row["observed_axis_count"],
                "partial_axis_count": row["partial_axis_count"],
                "unobserved_axis_count": row["unobserved_axis_count"],
                "observed_source_count": row["observed_source_count"],
                "missing_source_count": row["missing_source_count"],
                "clinical_translation_state": row["clinical_translation_state"],
                "disease_association_state": row["disease_association_state"],
                "variant_to_gene_state": row["variant_to_gene_state"],
            }
        )
    return ranked_predictions


def evaluate_glutamatergic_convergence_ranked_predictions(
    *,
    predictions: list[dict[str, object]],
    bundle: FrozenRescueTaskBundle,
) -> dict[str, object]:
    label_rows_by_gene_id = {
        row["gene_id"]: row for row in bundle.evaluation_target.rows
    }
    admissible_entity_ids = tuple(
        row["gene_id"] for row in bundle.evaluation_target.rows
    )
    ranked_entity_ids = tuple(
        str(row["gene_id"]) for row in predictions
    )
    relevance_index = {
        row["gene_id"]: row["evaluation_label"] == "1"
        for row in bundle.evaluation_target.rows
    }
    ranked_rows = build_ranked_evaluation_rows(
        admissible_entity_ids,
        ranked_entity_ids,
        relevance_index,
    )

    evaluation_rows: list[dict[str, object]] = []
    for prediction in predictions:
        label_row = label_rows_by_gene_id[str(prediction["gene_id"])]
        evaluation_rows.append(
            {
                **prediction,
                "evaluation_label": label_row["evaluation_label"],
                "evaluation_label_name": label_row["evaluation_label_name"],
                "decision": label_row["decision"],
                "adjudicated_at": label_row["adjudicated_at"],
                "decision_owner": label_row["decision_owner"],
                "label_rationale": label_row["label_rationale"],
                "evaluation_relevant": str(
                    label_row["evaluation_label"] == "1"
                ).lower(),
            }
        )

    split_summaries: dict[str, dict[str, object]] = {}
    for split_name in ("all",) + _SPLIT_ORDER:
        split_predictions = (
            predictions
            if split_name == "all"
            else [
                row for row in predictions if row["split_name"] == split_name
            ]
        )
        split_admissible_ids = (
            admissible_entity_ids
            if split_name == "all"
            else tuple(
                row["gene_id"]
                for row in bundle.evaluation_target.rows
                if row["split_name"] == split_name
            )
        )
        split_ranked_rows = build_ranked_evaluation_rows(
            split_admissible_ids,
            tuple(str(row["gene_id"]) for row in split_predictions),
            {gene_id: relevance_index[gene_id] for gene_id in split_admissible_ids},
        )
        split_summaries[split_name] = {
            "candidate_count": len(split_ranked_rows),
            "positive_label_count": count_relevant(split_ranked_rows),
            "metric_values": calculate_metric_values(split_ranked_rows),
        }

    return {
        "evaluation_rows": evaluation_rows,
        "summary": {
            "task_id": bundle.governance.task_card.task_id,
            "baseline_id": predictions[0]["baseline_id"] if predictions else "",
            "ranking_dataset_id": bundle.ranking_input.card.dataset_id,
            "evaluation_dataset_id": bundle.evaluation_target.card.dataset_id,
            "candidate_count": len(ranked_rows),
            "positive_label_count": count_relevant(ranked_rows),
            "convergence_contract_version": ATLAS_CONVERGENCE_CONTRACT_VERSION,
            "top_ranked_gene_symbols": [
                str(row["gene_symbol"]) for row in predictions[:3]
            ],
            "split_counts": dict(
                Counter(str(row["split_name"]) for row in predictions)
            ),
            "metric_values": calculate_metric_values(ranked_rows),
            "split_summaries": split_summaries,
        },
    }


def materialize_glutamatergic_convergence_rescue_evaluation(
    *,
    output_dir: Path,
    task_card_path: Path | None = None,
    baseline_id: str = DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
) -> dict[str, object]:
    started_at = _utc_now()
    bundle = load_glutamatergic_convergence_rescue_task_bundle(
        task_card_path=task_card_path,
    )
    predictions = build_glutamatergic_convergence_ranked_predictions(
        bundle,
        baseline_id=baseline_id,
    )
    evaluation = evaluate_glutamatergic_convergence_ranked_predictions(
        predictions=predictions,
        bundle=bundle,
    )
    completed_at = _utc_now()

    resolved_output_dir = output_dir.resolve()
    predictions_file = resolved_output_dir / "ranked_predictions.csv"
    evaluation_rows_file = resolved_output_dir / "evaluation_rows.csv"
    evaluation_summary_file = resolved_output_dir / "evaluation_summary.json"
    run_manifest_file = resolved_output_dir / "run_manifest.json"

    write_csv(predictions_file, predictions, fieldnames=_PREDICTION_FIELDNAMES)
    write_csv(
        evaluation_rows_file,
        evaluation["evaluation_rows"],
        fieldnames=_EVALUATION_ROW_FIELDNAMES,
    )
    write_json(evaluation_summary_file, evaluation["summary"])

    run_manifest = {
        "task_id": bundle.governance.task_card.task_id,
        "baseline_id": baseline_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "task_card_file": str(
            (
                task_card_path.resolve()
                if task_card_path is not None
                else _resolve_default_task_card_path()
            )
        ),
        "ranking_input_file": str(bundle.ranking_input.path),
        "evaluation_target_file": str(bundle.evaluation_target.path),
        "freeze_manifest_ids": [
            manifest.freeze_manifest_id for manifest in bundle.governance.freeze_manifests
        ],
        "split_manifest_ids": [
            manifest.split_manifest_id for manifest in bundle.governance.split_manifests
        ],
        "lineage_ids": [
            lineage.lineage_id for lineage in bundle.governance.lineages
        ],
        "predictions_file": str(predictions_file),
        "evaluation_rows_file": str(evaluation_rows_file),
        "evaluation_summary_file": str(evaluation_summary_file),
    }
    write_json(run_manifest_file, run_manifest)

    return {
        "task_id": bundle.governance.task_card.task_id,
        "baseline_id": baseline_id,
        "output_dir": str(resolved_output_dir),
        "predictions_file": str(predictions_file),
        "evaluation_rows_file": str(evaluation_rows_file),
        "evaluation_summary_file": str(evaluation_summary_file),
        "run_manifest_file": str(run_manifest_file),
        "ranking_input_file": str(bundle.ranking_input.path),
        "evaluation_target_file": str(bundle.evaluation_target.path),
        "candidate_count": evaluation["summary"]["candidate_count"],
        "positive_label_count": evaluation["summary"]["positive_label_count"],
        "metric_values": evaluation["summary"]["metric_values"],
        "top_ranked_gene_symbols": evaluation["summary"]["top_ranked_gene_symbols"],
    }


__all__ = [
    "DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID",
    "DEFAULT_GLUTAMATERGIC_CONVERGENCE_TASK_ID",
    "build_glutamatergic_convergence_ranked_predictions",
    "evaluate_glutamatergic_convergence_ranked_predictions",
    "load_glutamatergic_convergence_rescue_task_bundle",
    "materialize_glutamatergic_convergence_rescue_evaluation",
]
