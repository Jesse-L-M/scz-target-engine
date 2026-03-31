from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import write_csv, write_json
from scz_target_engine.rescue.frozen import (
    FrozenRescueTaskBundle,
    load_frozen_rescue_task_bundle,
)


NPC_SIGNATURE_REVERSAL_TASK_ID = "scz_npc_signature_reversal_rescue_task"
NPC_SIGNATURE_REVERSAL_TASK_LABEL = "Schizophrenia NPC signature-reversal rescue task"
NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID = "npc_signature_reversal_priority_v1"
NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_LABEL = "NPC signature-reversal priority v1"
PRIMARY_SCORE_SIGNATURE_WEIGHT = 0.7
PRIMARY_SCORE_ABS_LOG_FC_WEIGHT = 0.3
EVALUATION_SPLITS = ("all", "train", "validation", "test")
EVALUATION_K_VALUES = (50, 100)
PREDICTIONS_FILE_NAME = "ranked_predictions.csv"
SUMMARY_FILE_NAME = "run_summary.json"


def _require_text(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_float(row: dict[str, str], field_name: str) -> float:
    value = row.get(field_name)
    if value is None or not str(value).strip():
        raise ValueError(f"ranking row is missing {field_name}")
    return float(value)


def _round_metric(value: float) -> float:
    return round(value, 6)


@dataclass(frozen=True)
class NpcSignatureReversalScorerDefinition:
    scorer_id: str
    scorer_label: str
    scorer_role: str
    input_fields: tuple[str, ...]
    description: str
    tie_break_input_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.scorer_id, "scorer_id")
        _require_text(self.scorer_label, "scorer_label")
        _require_text(self.scorer_role, "scorer_role")
        if self.scorer_role not in {"model", "baseline"}:
            raise ValueError("scorer_role must be model or baseline")
        if not self.input_fields:
            raise ValueError("input_fields must contain at least one field")
        for field_name in self.input_fields:
            _require_text(field_name, "input_fields")
        for field_name in self.tie_break_input_fields:
            _require_text(field_name, "tie_break_input_fields")
        _require_text(self.description, "description")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "scorer_id": self.scorer_id,
            "scorer_label": self.scorer_label,
            "scorer_role": self.scorer_role,
            "input_fields": list(self.input_fields),
            "description": self.description,
        }
        if self.tie_break_input_fields:
            payload["tie_break_input_fields"] = list(self.tie_break_input_fields)
        return payload


DEFAULT_NPC_SIGNATURE_REVERSAL_SCORERS = (
    NpcSignatureReversalScorerDefinition(
        scorer_id=NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID,
        scorer_label=NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_LABEL,
        scorer_role="model",
        input_fields=(
            "signature_weight",
            "npc_log_fc",
            "reversal_fraction",
            "max_abs_reversal_rzs",
            "reversal_drug_count",
        ),
        tie_break_input_fields=(
            "reversal_fraction",
            "max_abs_reversal_rzs",
            "reversal_drug_count",
        ),
        description=(
            "Weighted percentile score across frozen disease-signature magnitude fields "
            "with deterministic reversal-feature tie-breaks. Evaluation labels remain "
            "offline-only and are never joined back into ranked predictions."
        ),
    ),
    NpcSignatureReversalScorerDefinition(
        scorer_id="signature_weight_only",
        scorer_label="Signature weight only",
        scorer_role="baseline",
        input_fields=("signature_weight",),
        description=(
            "Ranks genes only by the frozen pre-cutoff NPC disease-signature weight."
        ),
    ),
    NpcSignatureReversalScorerDefinition(
        scorer_id="absolute_npc_log_fc_only",
        scorer_label="Absolute NPC log fold-change only",
        scorer_role="baseline",
        input_fields=("npc_log_fc",),
        description=(
            "Ranks genes only by absolute NPC disease log fold-change magnitude."
        ),
    ),
    NpcSignatureReversalScorerDefinition(
        scorer_id="reversal_fraction_only",
        scorer_label="Reversal fraction only",
        scorer_role="baseline",
        input_fields=("reversal_fraction",),
        description=(
            "Ranks genes only by the frozen fraction of perturbations that reverse the "
            "NPC disease direction."
        ),
    ),
    NpcSignatureReversalScorerDefinition(
        scorer_id="max_abs_reversal_rzs_only",
        scorer_label="Max absolute reversal RZS only",
        scorer_role="baseline",
        input_fields=("max_abs_reversal_rzs",),
        description=(
            "Ranks genes only by the largest absolute reversal perturbation z-score in "
            "the frozen pre-cutoff perturbation aggregate."
        ),
    ),
    NpcSignatureReversalScorerDefinition(
        scorer_id="reversal_drug_count_only",
        scorer_label="Reversal drug count only",
        scorer_role="baseline",
        input_fields=("reversal_drug_count",),
        description=(
            "Ranks genes only by the number of frozen perturbations that reverse the "
            "NPC disease direction."
        ),
    ),
)


def _absolute_npc_log_fc(row: dict[str, str]) -> float:
    return abs(_parse_float(row, "npc_log_fc"))


def _field_value(row: dict[str, str], field_name: str) -> float:
    if field_name == "npc_log_fc":
        return _absolute_npc_log_fc(row)
    return _parse_float(row, field_name)


def _ensure_expected_columns(
    bundle: FrozenRescueTaskBundle,
    *,
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
) -> None:
    ranking_columns = set(bundle.ranking_input.columns)
    required_fields = {
        "gene_id",
        "gene_symbol",
        "split_name",
        "disease_direction",
        "best_reversal_compound",
    }
    for scorer in scorers:
        required_fields.update(scorer.input_fields)
        required_fields.update(scorer.tie_break_input_fields)
    missing = sorted(required_fields.difference(ranking_columns))
    if missing:
        raise ValueError(
            "frozen NPC ranking input is missing required columns: "
            + ", ".join(missing)
        )


def _resolve_default_task_card_path() -> Path:
    from scz_target_engine.rescue.registry import load_rescue_task_registrations

    for registration in load_rescue_task_registrations():
        if registration.task_id == NPC_SIGNATURE_REVERSAL_TASK_ID:
            return registration.task_card_file.resolve()
    raise KeyError(f"unknown rescue_task_id: {NPC_SIGNATURE_REVERSAL_TASK_ID}")


def _load_bundle(task_card_path: Path) -> FrozenRescueTaskBundle:
    return load_frozen_rescue_task_bundle(task_card_path=task_card_path)


def _validate_bundle_identity(bundle: FrozenRescueTaskBundle) -> None:
    task_id = bundle.governance.task_card.task_id
    if task_id != NPC_SIGNATURE_REVERSAL_TASK_ID:
        raise ValueError(
            "npc signature-reversal runner expected task_id "
            f"{NPC_SIGNATURE_REVERSAL_TASK_ID}, found {task_id}"
        )


def _build_descending_percentile_map(
    ranking_rows: tuple[dict[str, str], ...],
    *,
    field_name: str,
) -> dict[str, float]:
    ordered_rows = sorted(
        ranking_rows,
        key=lambda row: (-_field_value(row, field_name), row["gene_id"]),
    )
    denominator = max(len(ordered_rows) - 1, 1)
    return {
        row["gene_id"]: 1.0 - (position / denominator)
        for position, row in enumerate(ordered_rows)
    }


def _build_primary_score_map(
    ranking_rows: tuple[dict[str, str], ...],
) -> dict[str, float]:
    signature_weight_percentiles = _build_descending_percentile_map(
        ranking_rows,
        field_name="signature_weight",
    )
    absolute_log_fc_percentiles = _build_descending_percentile_map(
        ranking_rows,
        field_name="npc_log_fc",
    )
    return {
        row["gene_id"]: (
            PRIMARY_SCORE_SIGNATURE_WEIGHT
            * signature_weight_percentiles[row["gene_id"]]
            + PRIMARY_SCORE_ABS_LOG_FC_WEIGHT
            * absolute_log_fc_percentiles[row["gene_id"]]
        )
        for row in ranking_rows
    }


def _rank_primary_predictions(
    ranking_rows: tuple[dict[str, str], ...],
    *,
    primary_score_map: dict[str, float],
) -> tuple[dict[str, str], ...]:
    return tuple(
        sorted(
            ranking_rows,
            key=lambda row: (
                -primary_score_map[row["gene_id"]],
                -_parse_float(row, "reversal_fraction"),
                -_parse_float(row, "max_abs_reversal_rzs"),
                -_parse_float(row, "reversal_drug_count"),
                row["gene_id"],
            ),
        )
    )


def _rank_baseline_predictions(
    ranking_rows: tuple[dict[str, str], ...],
    *,
    field_name: str,
) -> tuple[str, ...]:
    return tuple(
        row["gene_id"]
        for row in sorted(
            ranking_rows,
            key=lambda row: (-_field_value(row, field_name), row["gene_id"]),
        )
    )


def _build_prediction_rows(
    ranked_rows: tuple[dict[str, str], ...],
    *,
    primary_score_map: dict[str, float],
) -> list[dict[str, object]]:
    prediction_rows: list[dict[str, object]] = []
    for rank, row in enumerate(ranked_rows, start=1):
        prediction_rows.append(
            {
                "rank": rank,
                "gene_id": row["gene_id"],
                "gene_symbol": row["gene_symbol"],
                "split_name": row["split_name"],
                "npc_signature_reversal_priority_score": round(
                    primary_score_map[row["gene_id"]],
                    12,
                ),
                "signature_weight_baseline_score": _parse_float(
                    row,
                    "signature_weight",
                ),
                "absolute_npc_log_fc_baseline_score": _absolute_npc_log_fc(row),
                "reversal_fraction_baseline_score": _parse_float(
                    row,
                    "reversal_fraction",
                ),
                "max_abs_reversal_rzs_baseline_score": _parse_float(
                    row,
                    "max_abs_reversal_rzs",
                ),
                "reversal_drug_count_baseline_score": _parse_float(
                    row,
                    "reversal_drug_count",
                ),
                "disease_direction": row["disease_direction"],
                "best_reversal_compound": row["best_reversal_compound"],
            }
        )
    return prediction_rows


def _split_to_gene_ids(
    evaluation_rows: tuple[dict[str, str], ...],
) -> dict[str, tuple[str, ...]]:
    split_to_gene_ids: dict[str, list[str]] = {split_name: [] for split_name in EVALUATION_SPLITS}
    for row in evaluation_rows:
        gene_id = row["gene_id"]
        split_name = row["split_name"]
        split_to_gene_ids["all"].append(gene_id)
        split_to_gene_ids.setdefault(split_name, []).append(gene_id)
    return {
        split_name: tuple(gene_ids)
        for split_name, gene_ids in split_to_gene_ids.items()
    }


def _split_to_positive_gene_ids(
    evaluation_rows: tuple[dict[str, str], ...],
) -> dict[str, set[str]]:
    split_to_positive_gene_ids: dict[str, set[str]] = {
        split_name: set() for split_name in EVALUATION_SPLITS
    }
    for row in evaluation_rows:
        if row["rescue_positive_label"] != "1":
            continue
        gene_id = row["gene_id"]
        split_name = row["split_name"]
        split_to_positive_gene_ids["all"].add(gene_id)
        split_to_positive_gene_ids.setdefault(split_name, set()).add(gene_id)
    return split_to_positive_gene_ids


def _ordered_relevance_rows(
    *,
    admissible_gene_ids: tuple[str, ...],
    ranked_gene_ids: tuple[str, ...],
    positive_gene_ids: set[str],
) -> tuple[tuple[str, bool], ...]:
    admissible_gene_id_set = set(admissible_gene_ids)
    seen_gene_ids: set[str] = set()
    ordered_gene_ids: list[str] = []

    for gene_id in ranked_gene_ids:
        if gene_id not in admissible_gene_id_set or gene_id in seen_gene_ids:
            continue
        seen_gene_ids.add(gene_id)
        ordered_gene_ids.append(gene_id)

    for gene_id in admissible_gene_ids:
        if gene_id in seen_gene_ids:
            continue
        seen_gene_ids.add(gene_id)
        ordered_gene_ids.append(gene_id)

    return tuple(
        (gene_id, gene_id in positive_gene_ids)
        for gene_id in ordered_gene_ids
    )


def _precision_at_k(
    ordered_rows: tuple[tuple[str, bool], ...],
    *,
    k: int,
) -> float:
    if not ordered_rows:
        return 0.0
    window = ordered_rows[:k]
    denominator = min(k, len(ordered_rows))
    if denominator == 0:
        return 0.0
    return sum(1 for _, relevant in window if relevant) / denominator


def _recall_at_k(
    ordered_rows: tuple[tuple[str, bool], ...],
    *,
    k: int,
) -> float:
    relevant_total = sum(1 for _, relevant in ordered_rows if relevant)
    if relevant_total == 0:
        return 0.0
    window = ordered_rows[:k]
    return sum(1 for _, relevant in window if relevant) / relevant_total


def _calculate_metrics(
    ordered_rows: tuple[tuple[str, bool], ...],
) -> dict[str, float | int | None]:
    relevant_total = sum(1 for _, relevant in ordered_rows if relevant)
    hit_count = 0
    precision_sum = 0.0
    reciprocal_rank = 0.0
    first_positive_rank: int | None = None

    for index, (_, relevant) in enumerate(ordered_rows, start=1):
        if not relevant:
            continue
        hit_count += 1
        precision_sum += hit_count / index
        if first_positive_rank is None:
            first_positive_rank = index
            reciprocal_rank = 1.0 / index

    average_precision = (
        precision_sum / relevant_total if relevant_total else 0.0
    )
    metrics: dict[str, float | int | None] = {
        "average_precision": _round_metric(average_precision),
        "mean_reciprocal_rank": _round_metric(reciprocal_rank),
        "first_positive_rank": first_positive_rank,
    }
    for k_value in EVALUATION_K_VALUES:
        metrics[f"precision_at_{k_value}"] = _round_metric(
            _precision_at_k(ordered_rows, k=k_value)
        )
        metrics[f"recall_at_{k_value}"] = _round_metric(
            _recall_at_k(ordered_rows, k=k_value)
        )
    return metrics


def _build_ranked_gene_ids_by_scorer(
    ranking_rows: tuple[dict[str, str], ...],
    *,
    primary_ranked_rows: tuple[dict[str, str], ...],
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
) -> dict[str, tuple[str, ...]]:
    ranked_gene_ids_by_scorer: dict[str, tuple[str, ...]] = {
        NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID: tuple(
            row["gene_id"] for row in primary_ranked_rows
        )
    }
    for scorer in scorers:
        if scorer.scorer_id == NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID:
            continue
        ranked_gene_ids_by_scorer[scorer.scorer_id] = _rank_baseline_predictions(
            ranking_rows,
            field_name=scorer.input_fields[0],
        )
    return ranked_gene_ids_by_scorer


def _build_evaluation_payload(
    *,
    ranking_rows: tuple[dict[str, str], ...],
    evaluation_rows: tuple[dict[str, str], ...],
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
    ranked_gene_ids_by_scorer: dict[str, tuple[str, ...]],
) -> dict[str, object]:
    split_to_gene_ids = _split_to_gene_ids(evaluation_rows)
    split_to_positive_gene_ids = _split_to_positive_gene_ids(evaluation_rows)

    slices: dict[str, object] = {}
    for split_name in EVALUATION_SPLITS:
        admissible_gene_ids = split_to_gene_ids[split_name]
        positive_gene_ids = split_to_positive_gene_ids[split_name]
        scorers_payload: dict[str, object] = {}
        for scorer in scorers:
            ordered_rows = _ordered_relevance_rows(
                admissible_gene_ids=admissible_gene_ids,
                ranked_gene_ids=ranked_gene_ids_by_scorer[scorer.scorer_id],
                positive_gene_ids=positive_gene_ids,
            )
            scorers_payload[scorer.scorer_id] = {
                "scorer_label": scorer.scorer_label,
                "scorer_role": scorer.scorer_role,
                "metrics": _calculate_metrics(ordered_rows),
            }
        slices[split_name] = {
            "entity_count": len(admissible_gene_ids),
            "positive_count": len(positive_gene_ids),
            "scorers": scorers_payload,
        }
    return {
        "principal_split": "test",
        "reported_k_values": list(EVALUATION_K_VALUES),
        "slices": slices,
    }


def _artifact_reference(
    *,
    dataset_id: str,
    path: Path,
    row_count: int,
) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "path": str(path),
        "row_count": row_count,
        "sha256": _sha256_path(path),
    }


def _task_summary_payload(
    *,
    output_dir: Path,
    task_card_path: Path | None,
    bundle: FrozenRescueTaskBundle,
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
    evaluation_payload: dict[str, object],
) -> dict[str, object]:
    baseline_ids = [
        scorer.scorer_id for scorer in scorers if scorer.scorer_role == "baseline"
    ]
    return {
        "task_id": bundle.governance.task_card.task_id,
        "task_label": bundle.governance.contract.task_label,
        "task_card_id": bundle.governance.task_card.task_card_id,
        "contract_path": bundle.governance.task_card.contract_path,
        "governance_task_card_path": str(task_card_path) if task_card_path else "",
        "governance_status": bundle.governance.task_card.governance_status,
        "freeze_manifest_ids": [
            manifest.freeze_manifest_id for manifest in bundle.governance.freeze_manifests
        ],
        "split_manifest_ids": [
            manifest.split_manifest_id for manifest in bundle.governance.split_manifests
        ],
        "lineage_ids": [
            lineage.lineage_id for lineage in bundle.governance.lineages
        ],
        "input_artifacts": {
            "ranking_input": _artifact_reference(
                dataset_id=bundle.ranking_input.card.dataset_id,
                path=bundle.ranking_input.path,
                row_count=len(bundle.ranking_input.rows),
            ),
            "evaluation_target": _artifact_reference(
                dataset_id=bundle.evaluation_target.card.dataset_id,
                path=bundle.evaluation_target.path,
                row_count=len(bundle.evaluation_target.rows),
            ),
        },
        "leakage_boundary": {
            "policy_id": bundle.governance.contract.leakage_boundary.policy_id,
            "raw_runtime_ingestion_enabled": False,
            "ranking_inputs_require_frozen_artifacts": True,
            "evaluation_labels_used_only_for_offline_metrics": True,
            "evaluation_labels_emitted_in_predictions": False,
            "principal_split": "test",
        },
        "model_id": NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID,
        "baseline_ids": baseline_ids,
        "scorers": [scorer.to_dict() for scorer in scorers],
        "outputs": {
            "output_dir": str(output_dir),
            "predictions_file": str(output_dir / PREDICTIONS_FILE_NAME),
            "summary_file": str(output_dir / SUMMARY_FILE_NAME),
        },
        "evaluation": evaluation_payload,
    }


def materialize_npc_signature_reversal_run(
    *,
    output_dir: Path,
    task_card_path: Path | None = None,
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...] = (
        DEFAULT_NPC_SIGNATURE_REVERSAL_SCORERS
    ),
) -> dict[str, object]:
    resolved_output_dir = output_dir.resolve()
    resolved_task_card_path = (
        task_card_path.resolve() if task_card_path else _resolve_default_task_card_path()
    )
    bundle = _load_bundle(resolved_task_card_path)
    _validate_bundle_identity(bundle)
    _ensure_expected_columns(bundle, scorers=scorers)

    primary_score_map = _build_primary_score_map(bundle.ranking_input.rows)
    primary_ranked_rows = _rank_primary_predictions(
        bundle.ranking_input.rows,
        primary_score_map=primary_score_map,
    )
    prediction_rows = _build_prediction_rows(
        primary_ranked_rows,
        primary_score_map=primary_score_map,
    )

    predictions_file = resolved_output_dir / PREDICTIONS_FILE_NAME
    write_csv(
        predictions_file,
        prediction_rows,
        fieldnames=list(prediction_rows[0].keys()) if prediction_rows else [],
    )

    ranked_gene_ids_by_scorer = _build_ranked_gene_ids_by_scorer(
        bundle.ranking_input.rows,
        primary_ranked_rows=primary_ranked_rows,
        scorers=scorers,
    )
    evaluation_payload = _build_evaluation_payload(
        ranking_rows=bundle.ranking_input.rows,
        evaluation_rows=bundle.evaluation_target.rows,
        scorers=scorers,
        ranked_gene_ids_by_scorer=ranked_gene_ids_by_scorer,
    )
    summary_payload = _task_summary_payload(
        output_dir=resolved_output_dir,
        task_card_path=resolved_task_card_path,
        bundle=bundle,
        scorers=scorers,
        evaluation_payload=evaluation_payload,
    )
    summary_file = resolved_output_dir / SUMMARY_FILE_NAME
    write_json(summary_file, summary_payload)
    return summary_payload


__all__ = [
    "DEFAULT_NPC_SIGNATURE_REVERSAL_SCORERS",
    "NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID",
    "NPC_SIGNATURE_REVERSAL_TASK_ID",
    "NPC_SIGNATURE_REVERSAL_TASK_LABEL",
    "NpcSignatureReversalScorerDefinition",
    "materialize_npc_signature_reversal_run",
]
