from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import write_csv, write_json
from scz_target_engine.rescue.baselines.reporting import (
    RescueComparisonRow,
    materialize_rescue_comparison_report,
)
from scz_target_engine.rescue.frozen import (
    FrozenRescueTaskBundle,
    load_frozen_rescue_task_bundle,
)
from scz_target_engine.rescue.models import (
    RescueModelInput,
    build_rescue_model_admission_summary,
    list_rescue_model_plugins,
    resolve_rescue_model_plugin,
)


NPC_SIGNATURE_REVERSAL_TASK_ID = "scz_npc_signature_reversal_rescue_task"
NPC_SIGNATURE_REVERSAL_TASK_LABEL = "Schizophrenia NPC signature-reversal rescue task"
NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID = "npc_abs_log_fc_priority_v1"
NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_LABEL = (
    "NPC absolute log fold-change priority v1"
)
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


NPC_SIGNATURE_REVERSAL_BASELINE_SCORERS = (
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


def _plugin_to_scorer_definition(
    plugin,
) -> NpcSignatureReversalScorerDefinition:
    definition = plugin.definition
    return NpcSignatureReversalScorerDefinition(
        scorer_id=definition.model_id,
        scorer_label=definition.label,
        scorer_role="model",
        input_fields=definition.input_fields,
        tie_break_input_fields=definition.tie_break_input_fields,
        description=definition.description,
    )


def _default_npc_signature_reversal_scorers(
) -> tuple[NpcSignatureReversalScorerDefinition, ...]:
    return tuple(
        _plugin_to_scorer_definition(plugin)
        for plugin in list_rescue_model_plugins(NPC_SIGNATURE_REVERSAL_TASK_ID)
    ) + NPC_SIGNATURE_REVERSAL_BASELINE_SCORERS


DEFAULT_NPC_SIGNATURE_REVERSAL_SCORERS = _default_npc_signature_reversal_scorers()


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


def _build_primary_score_map(
    ranking_rows: tuple[dict[str, str], ...],
) -> dict[str, float]:
    return {
        row["gene_id"]: _absolute_npc_log_fc(row)
        for row in ranking_rows
    }


def _build_model_input(
    bundle: FrozenRescueTaskBundle,
) -> RescueModelInput:
    return RescueModelInput(
        task_id=bundle.governance.task_card.task_id,
        task_label=bundle.governance.contract.task_label,
        ranking_dataset_id=bundle.ranking_input.card.dataset_id,
        ranking_rows=bundle.ranking_input.rows,
        ranking_columns=tuple(bundle.ranking_input.columns),
        principal_split="test",
    )


def _rank_primary_predictions(
    ranking_rows_by_gene_id: dict[str, dict[str, str]],
    *,
    ranked_gene_ids: tuple[str, ...],
) -> tuple[dict[str, str], ...]:
    return tuple(
        ranking_rows_by_gene_id[gene_id]
        for gene_id in ranked_gene_ids
    )


def _rank_predictions_for_scorer(
    ranking_rows: tuple[dict[str, str], ...],
    *,
    scorer: NpcSignatureReversalScorerDefinition,
) -> tuple[str, ...]:
    return tuple(
        row["gene_id"]
        for row in sorted(
            ranking_rows,
            key=lambda row: tuple(
                [-_field_value(row, field_name) for field_name in scorer.input_fields]
                + [
                    -_field_value(row, field_name)
                    for field_name in scorer.tie_break_input_fields
                ]
                + [row["gene_id"]]
            ),
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
                "npc_abs_log_fc_priority_score": round(
                    primary_score_map[row["gene_id"]],
                    12,
                ),
                "signature_weight_baseline_score": _parse_float(
                    row,
                    "signature_weight",
                ),
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
    primary_ranked_gene_ids: tuple[str, ...],
    model_input: RescueModelInput,
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
) -> dict[str, tuple[str, ...]]:
    ranked_gene_ids_by_scorer: dict[str, tuple[str, ...]] = {}
    for scorer in scorers:
        if scorer.scorer_role == "model":
            if scorer.scorer_id == NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID:
                ranked_gene_ids_by_scorer[scorer.scorer_id] = primary_ranked_gene_ids
                continue
            ranked_gene_ids_by_scorer[scorer.scorer_id] = resolve_rescue_model_plugin(
                NPC_SIGNATURE_REVERSAL_TASK_ID,
                scorer.scorer_id,
            ).rank_entities(model_input)
            continue
        ranked_gene_ids_by_scorer[scorer.scorer_id] = _rank_predictions_for_scorer(
            ranking_rows,
            scorer=scorer,
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
    model_admission_summary: dict[str, object],
    model_plugins: tuple[object, ...],
) -> dict[str, object]:
    baseline_ids = [
        scorer.scorer_id for scorer in scorers if scorer.scorer_role == "baseline"
    ]
    model_definitions = tuple(
        plugin.definition for plugin in model_plugins
    )
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
        "model_plugin_ids": [
            model_definition.model_id for model_definition in model_definitions
        ],
        "baseline_ids": baseline_ids,
        "scorers": [
            (
                next(
                    (
                        model_definition.to_scorer_definition()
                        for model_definition in model_definitions
                        if model_definition.model_id == scorer.scorer_id
                    ),
                    scorer.to_dict(),
                )
            )
            for scorer in scorers
        ],
        "model_plugins": [
            model_definition.to_dict() for model_definition in model_definitions
        ],
        "model_admission": model_admission_summary,
        "outputs": {
            "output_dir": str(output_dir),
            "predictions_file": str(output_dir / PREDICTIONS_FILE_NAME),
            "summary_file": str(output_dir / SUMMARY_FILE_NAME),
        },
        "evaluation": evaluation_payload,
    }


def _build_npc_comparison_rows(
    *,
    task_id: str,
    task_label: str,
    evaluation_payload: dict[str, object],
    scorers: tuple[NpcSignatureReversalScorerDefinition, ...],
) -> tuple[RescueComparisonRow, ...]:
    slices = evaluation_payload["slices"]
    if not isinstance(slices, dict):
        raise ValueError("evaluation payload must expose slice metrics")
    comparison_rows: list[RescueComparisonRow] = []
    for split_name in EVALUATION_SPLITS:
        split_payload = slices[split_name]
        scorers_payload = split_payload["scorers"]
        for scorer in scorers:
            scorer_payload = scorers_payload[scorer.scorer_id]
            comparison_rows.append(
                RescueComparisonRow(
                    task_id=task_id,
                    task_label=task_label,
                    evaluation_split=split_name,
                    scorer_id=scorer.scorer_id,
                    scorer_label=scorer.scorer_label,
                    scorer_role=scorer.scorer_role,
                    candidate_count=int(split_payload["entity_count"]),
                    positive_count=int(split_payload["positive_count"]),
                    metrics=scorer_payload["metrics"],
                )
            )
    return tuple(comparison_rows)


def _materialize_npc_signature_reversal_result(
    *,
    output_dir: Path,
    task_card_path: Path | None = None,
) -> tuple[dict[str, object], tuple[RescueComparisonRow, ...]]:
    model_plugins = list_rescue_model_plugins(NPC_SIGNATURE_REVERSAL_TASK_ID)
    scorers = tuple(
        _plugin_to_scorer_definition(plugin) for plugin in model_plugins
    ) + NPC_SIGNATURE_REVERSAL_BASELINE_SCORERS
    resolved_output_dir = output_dir.resolve()
    resolved_task_card_path = (
        task_card_path.resolve() if task_card_path else _resolve_default_task_card_path()
    )
    bundle = _load_bundle(resolved_task_card_path)
    _validate_bundle_identity(bundle)
    _ensure_expected_columns(bundle, scorers=scorers)

    model_input = _build_model_input(bundle)
    primary_score_map = _build_primary_score_map(bundle.ranking_input.rows)
    primary_ranked_gene_ids = resolve_rescue_model_plugin(
        NPC_SIGNATURE_REVERSAL_TASK_ID,
        NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID,
    ).rank_entities(model_input)
    primary_ranked_rows = _rank_primary_predictions(
        {
            row["gene_id"]: row
            for row in bundle.ranking_input.rows
        },
        ranked_gene_ids=primary_ranked_gene_ids,
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
        primary_ranked_gene_ids=primary_ranked_gene_ids,
        model_input=model_input,
        scorers=scorers,
    )
    evaluation_payload = _build_evaluation_payload(
        ranking_rows=bundle.ranking_input.rows,
        evaluation_rows=bundle.evaluation_target.rows,
        scorers=scorers,
        ranked_gene_ids_by_scorer=ranked_gene_ids_by_scorer,
    )
    comparison_rows = _build_npc_comparison_rows(
        task_id=bundle.governance.task_card.task_id,
        task_label=bundle.governance.contract.task_label,
        evaluation_payload=evaluation_payload,
        scorers=scorers,
    )
    model_admission_summary = build_rescue_model_admission_summary(
        comparison_rows=comparison_rows,
        model_definitions=tuple(
            plugin.definition for plugin in model_plugins
        ),
        principal_split="test",
        baseline_scorer_ids=tuple(
            scorer.scorer_id
            for scorer in scorers
            if scorer.scorer_role == "baseline"
        ),
    )
    summary_payload = _task_summary_payload(
        output_dir=resolved_output_dir,
        task_card_path=resolved_task_card_path,
        bundle=bundle,
        scorers=scorers,
        evaluation_payload=evaluation_payload,
        model_admission_summary=model_admission_summary,
        model_plugins=model_plugins,
    )
    summary_file = resolved_output_dir / SUMMARY_FILE_NAME
    write_json(summary_file, summary_payload)
    comparison_outputs = materialize_rescue_comparison_report(
        resolved_output_dir,
        task_id=bundle.governance.task_card.task_id,
        task_label=bundle.governance.contract.task_label,
        principal_split="test",
        comparison_rows=comparison_rows,
        scorer_definitions=tuple(
            next(
                (
                    plugin.definition.to_scorer_definition()
                    for plugin in model_plugins
                    if plugin.definition.model_id == scorer.scorer_id
                ),
                {
                    **scorer.to_dict(),
                    "comparison_id": scorer.scorer_id,
                },
            )
            for scorer in scorers
        ),
        notes=(
            "The NPC rescue report compares shipped model plugins against the fixed "
            "simple baselines on the frozen rescue bundle. Predictions remain "
            "label-free and evaluation stays aggregate-only in the comparison "
            "outputs."
        ),
    )
    return (
        {
            **summary_payload,
            "comparison_outputs": comparison_outputs,
        },
        comparison_rows,
    )


def materialize_npc_signature_reversal_run(
    *,
    output_dir: Path,
    task_card_path: Path | None = None,
) -> dict[str, object]:
    result, comparison_rows = _materialize_npc_signature_reversal_result(
        output_dir=output_dir,
        task_card_path=task_card_path,
    )
    return {
        **result,
        "comparison_rows": [row.to_dict() for row in comparison_rows],
    }


def materialize_npc_signature_reversal_baseline_pack(
    *,
    output_dir: Path,
    task_card_path: Path | None = None,
) -> dict[str, object]:
    result, comparison_rows = _materialize_npc_signature_reversal_result(
        output_dir=output_dir,
        task_card_path=task_card_path,
    )
    return {
        **result,
        "comparison_rows": comparison_rows,
    }


__all__ = [
    "DEFAULT_NPC_SIGNATURE_REVERSAL_SCORERS",
    "NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID",
    "NPC_SIGNATURE_REVERSAL_TASK_ID",
    "NPC_SIGNATURE_REVERSAL_TASK_LABEL",
    "NpcSignatureReversalScorerDefinition",
    "materialize_npc_signature_reversal_baseline_pack",
    "materialize_npc_signature_reversal_run",
]
