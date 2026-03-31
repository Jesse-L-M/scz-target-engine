from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
import hashlib
from pathlib import Path
import random

from scz_target_engine.benchmark_metrics import (
    build_ranked_evaluation_rows,
    calculate_metric_values,
)
from scz_target_engine.io import write_csv, write_json
from scz_target_engine.rescue.frozen import (
    FrozenRescueDataset,
    FrozenRescueGovernedTaskBundle,
    load_frozen_rescue_governance_bundle,
)
from scz_target_engine.rescue.governance import (
    RescueFrozenDatasetReference,
    RescueSplitManifest,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
INTERNEURON_TASK_CARD_PATH = (
    REPO_ROOT
    / "data"
    / "curated"
    / "rescue_tasks"
    / "governance"
    / "interneuron_gene_rescue_task"
    / "task_card.json"
)
INTERNEURON_TASK_ID = "interneuron_gene_rescue_task"
INTERNEURON_EVALUATION_DATASET_ID = "interneuron_followup_labels_2026_03_31"
INTERNEURON_AXIS_DATASET_IDS = {
    "interneuron_synapse": "interneuron_synapse_ranking_inputs_2023_12_31",
    "interneuron_arbor": "interneuron_arbor_ranking_inputs_2023_12_31",
}
VALID_INTERNEURON_AXIS_IDS = tuple(INTERNEURON_AXIS_DATASET_IDS)
DEFAULT_INTERNEURON_BASELINE_IDS = (
    "frozen_priority_rank",
    "recent_publication_first",
    "alphabetical_gene_symbol",
)
PREDICTION_FIELDNAMES = [
    "task_id",
    "axis_id",
    "baseline_id",
    "gene_id",
    "hgnc_id",
    "gene_symbol",
    "split_name",
    "candidate_tier",
    "priority_rank",
    "published_at",
    "score",
    "rank",
]
FORBIDDEN_RANKING_COLUMNS = {
    "followup_support_label",
    "label_window_start",
    "label_window_end",
    "label_source_pmid",
    "label_source_title",
    "label_source_url",
    "label_rationale",
}


@dataclass(frozen=True)
class InterneuronBaselineDefinition:
    baseline_id: str
    label: str
    description: str
    leakage_rule: str


@dataclass(frozen=True)
class InterneuronSplitAssignment:
    gene_id: str
    split_name: str
    purpose: str


@dataclass(frozen=True)
class InterneuronAxisTaskData:
    task_id: str
    axis_id: str
    frozen_bundle: FrozenRescueGovernedTaskBundle
    ranking_input: FrozenRescueDataset
    evaluation_target: FrozenRescueDataset
    split_manifest: RescueSplitManifest
    split_assignments: tuple[InterneuronSplitAssignment, ...]

    @property
    def split_name_by_gene_id(self) -> dict[str, str]:
        return {
            assignment.gene_id: assignment.split_name
            for assignment in self.split_assignments
        }

    @property
    def split_counts(self) -> dict[str, int]:
        counts = Counter(
            assignment.split_name for assignment in self.split_assignments
        )
        return {
            split_name: counts.get(split_name, 0)
            for split_name in ("train", "validation", "test")
        }

    @property
    def evaluation_row_by_gene_id(self) -> dict[str, dict[str, str]]:
        return {
            row["gene_id"]: row
            for row in self.evaluation_target.rows
            if row["gene_id"] in self.split_name_by_gene_id
        }


INTERNEURON_BASELINE_DEFINITIONS = (
    InterneuronBaselineDefinition(
        baseline_id="frozen_priority_rank",
        label="Frozen priority rank",
        description=(
            "Use the governed frozen priority_rank ordering exactly as shipped in the "
            "ranking artifact."
        ),
        leakage_rule=(
            "Consumes only the pre-cutoff ranking_input CSV and ignores all "
            "post-cutoff follow-up labels until offline evaluation."
        ),
    ),
    InterneuronBaselineDefinition(
        baseline_id="recent_publication_first",
        label="Most recent publication first",
        description=(
            "Rank frozen candidates by published_at descending, then priority_rank "
            "ascending to preserve deterministic tie-breaking."
        ),
        leakage_rule=(
            "Uses only pre-cutoff published_at values already frozen into the "
            "ranking artifact."
        ),
    ),
    InterneuronBaselineDefinition(
        baseline_id="alphabetical_gene_symbol",
        label="Alphabetical gene symbol",
        description=(
            "Rank frozen candidates alphabetically by gene_symbol as a leakage-safe "
            "non-informative control."
        ),
        leakage_rule=(
            "Uses only gene identifiers already present in the governed ranking "
            "artifact and does not inspect evaluation labels."
        ),
    ),
)


def _require_axis_id(axis_id: str) -> str:
    if axis_id not in INTERNEURON_AXIS_DATASET_IDS:
        valid_axis_ids = ", ".join(VALID_INTERNEURON_AXIS_IDS)
        raise ValueError(f"axis_id must be one of: {valid_axis_ids}")
    return axis_id


def _parse_published_at(value: str) -> date:
    return date.fromisoformat(value)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def list_interneuron_baselines() -> tuple[InterneuronBaselineDefinition, ...]:
    return INTERNEURON_BASELINE_DEFINITIONS


def resolve_interneuron_baseline(
    baseline_id: str,
) -> InterneuronBaselineDefinition:
    for baseline in INTERNEURON_BASELINE_DEFINITIONS:
        if baseline.baseline_id == baseline_id:
            return baseline
    valid_baselines = ", ".join(DEFAULT_INTERNEURON_BASELINE_IDS)
    raise ValueError(f"unknown baseline_id: {baseline_id}; expected one of {valid_baselines}")


def _freeze_reference_for_dataset(
    task_data: InterneuronAxisTaskData,
    dataset: FrozenRescueDataset,
) -> RescueFrozenDatasetReference:
    freeze_manifest_by_path = {
        freeze_manifest_path: freeze_manifest
        for freeze_manifest_path, freeze_manifest in zip(
            task_data.frozen_bundle.governance.task_card.freeze_manifest_paths,
            task_data.frozen_bundle.governance.freeze_manifests,
        )
    }
    freeze_manifest = freeze_manifest_by_path[dataset.card.freeze_manifest_path]
    for frozen_dataset in freeze_manifest.frozen_datasets:
        if frozen_dataset.dataset_id == dataset.card.dataset_id:
            return frozen_dataset
    raise KeyError(f"missing frozen dataset reference: {dataset.card.dataset_id}")


def _resolve_split_counts(
    total_rows: int,
    split_manifest: RescueSplitManifest,
) -> list[int]:
    partitions = list(split_manifest.partitions)
    if total_rows < len(partitions):
        raise ValueError(
            "ranking_input must contain at least one row per split partition for "
            f"{split_manifest.split_manifest_id}"
        )

    raw_counts = [
        partition.expected_fraction * total_rows
        for partition in partitions
    ]
    counts = [int(raw_count) for raw_count in raw_counts]
    for index in range(len(counts)):
        if counts[index] == 0:
            counts[index] = 1

    difference = total_rows - sum(counts)
    remainders = [
        (raw_count - int(raw_count), index)
        for index, raw_count in enumerate(raw_counts)
    ]

    if difference > 0:
        for _, index in sorted(remainders, reverse=True):
            if difference == 0:
                break
            counts[index] += 1
            difference -= 1
    elif difference < 0:
        for _, index in sorted(remainders):
            while difference < 0 and counts[index] > 1:
                counts[index] -= 1
                difference += 1
            if difference == 0:
                break

    if difference != 0:
        raise ValueError(
            f"unable to reconcile split counts for {split_manifest.split_manifest_id}"
        )
    return counts


def _build_split_assignments(
    ranking_input: FrozenRescueDataset,
    split_manifest: RescueSplitManifest,
) -> tuple[InterneuronSplitAssignment, ...]:
    gene_ids = sorted(row["gene_id"] for row in ranking_input.rows)
    if len(gene_ids) != len(set(gene_ids)):
        raise ValueError(
            f"{ranking_input.card.dataset_id} must not repeat gene_id values"
        )

    shuffled_gene_ids = list(gene_ids)
    random.Random(split_manifest.split_seed).shuffle(shuffled_gene_ids)
    split_counts = _resolve_split_counts(len(shuffled_gene_ids), split_manifest)

    assignments: list[InterneuronSplitAssignment] = []
    cursor = 0
    for partition, count in zip(split_manifest.partitions, split_counts, strict=True):
        partition_gene_ids = shuffled_gene_ids[cursor : cursor + count]
        assignments.extend(
            InterneuronSplitAssignment(
                gene_id=gene_id,
                split_name=partition.split_name,
                purpose=partition.purpose,
            )
            for gene_id in partition_gene_ids
        )
        cursor += count

    if cursor != len(shuffled_gene_ids):
        raise ValueError(
            f"split assignments did not cover every gene for {split_manifest.split_manifest_id}"
        )

    return tuple(
        sorted(assignments, key=lambda assignment: assignment.gene_id)
    )


def _require_ranking_dataset_boundary(dataset: FrozenRescueDataset) -> None:
    leaked_columns = sorted(set(dataset.columns).intersection(FORBIDDEN_RANKING_COLUMNS))
    if leaked_columns:
        raise ValueError(
            f"{dataset.card.dataset_id} exposed evaluation-only columns in ranking input: "
            + ", ".join(leaked_columns)
        )
    if dataset.card.dataset_role != "ranking_input":
        raise ValueError("expected a ranking_input dataset")
    if dataset.card.availability != "pre_cutoff":
        raise ValueError("ranking_input dataset must remain pre_cutoff")


def _require_evaluation_dataset_boundary(dataset: FrozenRescueDataset) -> None:
    if dataset.card.dataset_role != "evaluation_target":
        raise ValueError("expected an evaluation_target dataset")
    if dataset.card.availability != "post_cutoff":
        raise ValueError("evaluation_target dataset must remain post_cutoff")
    if "followup_support_label" not in dataset.columns:
        raise ValueError(
            f"{dataset.card.dataset_id} must contain followup_support_label"
        )


def load_interneuron_axis_task_data(axis_id: str) -> InterneuronAxisTaskData:
    resolved_axis_id = _require_axis_id(axis_id)
    frozen_bundle = load_frozen_rescue_governance_bundle(
        task_card_path=INTERNEURON_TASK_CARD_PATH
    )
    dataset_index = frozen_bundle.dataset_index
    ranking_input = dataset_index[INTERNEURON_AXIS_DATASET_IDS[resolved_axis_id]]
    evaluation_target = dataset_index[INTERNEURON_EVALUATION_DATASET_ID]
    split_manifest = next(
        split_manifest
        for split_manifest in frozen_bundle.governance.split_manifests
        if split_manifest.source_dataset_id == ranking_input.card.dataset_id
    )

    _require_ranking_dataset_boundary(ranking_input)
    _require_evaluation_dataset_boundary(evaluation_target)

    split_assignments = _build_split_assignments(ranking_input, split_manifest)
    split_gene_ids = {assignment.gene_id for assignment in split_assignments}
    evaluation_gene_ids = {
        row["gene_id"]
        for row in evaluation_target.rows
        if row["gene_id"] in split_gene_ids
    }
    if evaluation_gene_ids != split_gene_ids:
        missing_gene_ids = sorted(split_gene_ids.difference(evaluation_gene_ids))
        raise ValueError(
            f"{evaluation_target.card.dataset_id} is missing labels for ranking genes: "
            + ", ".join(missing_gene_ids)
        )

    return InterneuronAxisTaskData(
        task_id=INTERNEURON_TASK_ID,
        axis_id=resolved_axis_id,
        frozen_bundle=frozen_bundle,
        ranking_input=ranking_input,
        evaluation_target=evaluation_target,
        split_manifest=split_manifest,
        split_assignments=split_assignments,
    )


def _sorted_ranking_rows(
    task_data: InterneuronAxisTaskData,
    *,
    baseline_id: str,
) -> tuple[dict[str, str], ...]:
    resolve_interneuron_baseline(baseline_id)

    if baseline_id == "frozen_priority_rank":
        key_fn = lambda row: (
            int(row["priority_rank"]),
            row["gene_symbol"],
        )
    elif baseline_id == "recent_publication_first":
        key_fn = lambda row: (
            -_parse_published_at(row["published_at"]).toordinal(),
            int(row["priority_rank"]),
            row["gene_symbol"],
        )
    elif baseline_id == "alphabetical_gene_symbol":
        key_fn = lambda row: (
            row["gene_symbol"],
            int(row["priority_rank"]),
        )
    else:
        raise ValueError(f"unsupported baseline_id: {baseline_id}")

    return tuple(sorted(task_data.ranking_input.rows, key=key_fn))


def build_interneuron_axis_predictions(
    task_data: InterneuronAxisTaskData,
    *,
    baseline_id: str,
) -> tuple[dict[str, str], ...]:
    sorted_rows = _sorted_ranking_rows(task_data, baseline_id=baseline_id)
    total_rows = len(sorted_rows)
    split_name_by_gene_id = task_data.split_name_by_gene_id

    predictions: list[dict[str, str]] = []
    for rank, row in enumerate(sorted_rows, start=1):
        score = (total_rows - rank + 1) / total_rows
        predictions.append(
            {
                "task_id": task_data.task_id,
                "axis_id": task_data.axis_id,
                "baseline_id": baseline_id,
                "gene_id": row["gene_id"],
                "hgnc_id": row["hgnc_id"],
                "gene_symbol": row["gene_symbol"],
                "split_name": split_name_by_gene_id[row["gene_id"]],
                "candidate_tier": row["candidate_tier"],
                "priority_rank": row["priority_rank"],
                "published_at": row["published_at"],
                "score": f"{score:.6f}",
                "rank": str(rank),
            }
        )
    return tuple(predictions)


def _prediction_rows_for_evaluation_split(
    prediction_rows: tuple[dict[str, str], ...],
    *,
    evaluation_split: str,
) -> tuple[dict[str, str], ...]:
    if evaluation_split == "heldout":
        allowed_splits = {"validation", "test"}
    elif evaluation_split in {"validation", "test"}:
        allowed_splits = {evaluation_split}
    else:
        raise ValueError(f"unsupported evaluation_split: {evaluation_split}")
    return tuple(
        row
        for row in prediction_rows
        if row["split_name"] in allowed_splits
    )


def evaluate_interneuron_axis_predictions(
    task_data: InterneuronAxisTaskData,
    *,
    prediction_rows: tuple[dict[str, str], ...],
) -> dict[str, object]:
    evaluation_rows_by_gene_id = task_data.evaluation_row_by_gene_id
    expected_gene_ids = {
        row["gene_id"] for row in prediction_rows
    }
    if set(evaluation_rows_by_gene_id) != expected_gene_ids:
        raise ValueError(
            "prediction rows must cover exactly the ranking genes for offline evaluation"
        )

    evaluation_summaries: dict[str, object] = {}
    for evaluation_split in ("validation", "test", "heldout"):
        split_prediction_rows = _prediction_rows_for_evaluation_split(
            prediction_rows,
            evaluation_split=evaluation_split,
        )
        ranked_gene_ids = tuple(
            row["gene_id"] for row in split_prediction_rows
        )
        relevance_index = {
            gene_id: evaluation_rows_by_gene_id[gene_id]["followup_support_label"] == "1"
            for gene_id in ranked_gene_ids
        }
        ranked_rows = build_ranked_evaluation_rows(
            ranked_gene_ids,
            ranked_gene_ids,
            relevance_index,
        )
        evaluation_summaries[evaluation_split] = {
            "cohort_size": len(ranked_rows),
            "positive_count": sum(relevance_index.values()),
            "ranked_gene_ids": list(ranked_gene_ids),
            "positive_gene_ids": [
                gene_id
                for gene_id in ranked_gene_ids
                if relevance_index[gene_id]
            ],
            "metric_values": calculate_metric_values(ranked_rows),
        }
    return evaluation_summaries


def _build_emitted_offline_evaluation_metadata() -> dict[str, object]:
    return {
        "executed": True,
        "output_policy": (
            "Label-derived evaluation summaries are withheld from emitted outputs. "
            "Use the in-memory evaluation helpers for offline analysis."
        ),
    }


def _build_input_artifact_summary(
    task_data: InterneuronAxisTaskData,
    dataset: FrozenRescueDataset,
) -> dict[str, object]:
    freeze_reference = _freeze_reference_for_dataset(task_data, dataset)
    return {
        "artifact_contract_id": dataset.card.artifact_contract_id,
        "dataset_id": dataset.card.dataset_id,
        "dataset_role": dataset.card.dataset_role,
        "availability": dataset.card.availability,
        "path": str(dataset.path),
        "row_count": len(dataset.rows),
        "sha256": _sha256_path(dataset.path),
        "governed_row_count": freeze_reference.expected_row_count,
        "governed_sha256": freeze_reference.expected_sha256,
    }


def materialize_interneuron_axis_rescue_runs(
    axis_id: str,
    *,
    output_dir: Path,
    baseline_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    task_data = load_interneuron_axis_task_data(axis_id)
    resolved_baseline_ids = (
        DEFAULT_INTERNEURON_BASELINE_IDS
        if baseline_ids is None
        else tuple(baseline_ids)
    )

    axis_output_dir = output_dir.resolve() / axis_id
    run_summaries: list[dict[str, object]] = []
    for baseline_id in resolved_baseline_ids:
        baseline = resolve_interneuron_baseline(baseline_id)
        prediction_rows = build_interneuron_axis_predictions(
            task_data,
            baseline_id=baseline_id,
        )
        evaluate_interneuron_axis_predictions(
            task_data,
            prediction_rows=prediction_rows,
        )

        baseline_output_dir = axis_output_dir / baseline_id
        prediction_file = baseline_output_dir / "predictions.csv"
        summary_file = baseline_output_dir / "summary.json"
        write_csv(
            prediction_file,
            list(prediction_rows),
            fieldnames=PREDICTION_FIELDNAMES,
        )

        summary_payload = {
            "task_id": task_data.task_id,
            "axis_id": task_data.axis_id,
            "baseline_id": baseline.baseline_id,
            "baseline_label": baseline.label,
            "baseline_description": baseline.description,
            "baseline_leakage_rule": baseline.leakage_rule,
            "freeze_manifest_id": (
                task_data.frozen_bundle.governance.freeze_manifests[0].freeze_manifest_id
            ),
            "cutoff_date": (
                task_data.frozen_bundle.governance.freeze_manifests[0].cutoff_date
            ),
            "split_manifest_id": task_data.split_manifest.split_manifest_id,
            "leakage_boundary_policy_id": (
                task_data.frozen_bundle.governance.contract.leakage_boundary.policy_id
            ),
            "ranking_dataset_id": task_data.ranking_input.card.dataset_id,
            "evaluation_dataset_id": task_data.evaluation_target.card.dataset_id,
            "prediction_file": str(prediction_file),
            "prediction_count": len(prediction_rows),
            "split_counts": task_data.split_counts,
            "input_artifacts": [
                _build_input_artifact_summary(task_data, task_data.ranking_input),
                _build_input_artifact_summary(task_data, task_data.evaluation_target),
            ],
            "offline_evaluation": _build_emitted_offline_evaluation_metadata(),
            "notes": (
                "Predictions were built from the governed pre-cutoff ranking artifact "
                "only. Held-out post-cutoff follow-up labels were consumed only during "
                "offline evaluation."
            ),
        }
        write_json(summary_file, summary_payload)
        run_summaries.append(summary_payload)

    axis_summary = {
        "task_id": task_data.task_id,
        "axis_id": task_data.axis_id,
        "output_dir": str(axis_output_dir),
        "baseline_ids": list(resolved_baseline_ids),
        "runs": run_summaries,
    }
    write_json(axis_output_dir / "axis_summary.json", axis_summary)
    return axis_summary


def materialize_interneuron_rescue_lane(
    output_dir: Path,
    *,
    axis_ids: tuple[str, ...] = VALID_INTERNEURON_AXIS_IDS,
    baseline_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    resolved_axis_ids = tuple(_require_axis_id(axis_id) for axis_id in axis_ids)
    lane_output_dir = output_dir.resolve()
    axis_summaries = [
        materialize_interneuron_axis_rescue_runs(
            axis_id,
            output_dir=lane_output_dir,
            baseline_ids=baseline_ids,
        )
        for axis_id in resolved_axis_ids
    ]
    lane_summary = {
        "task_id": INTERNEURON_TASK_ID,
        "task_card_path": str(INTERNEURON_TASK_CARD_PATH),
        "axis_ids": list(resolved_axis_ids),
        "baseline_ids": list(
            DEFAULT_INTERNEURON_BASELINE_IDS
            if baseline_ids is None
            else baseline_ids
        ),
        "output_dir": str(lane_output_dir),
        "axis_runs": axis_summaries,
    }
    write_json(lane_output_dir / "lane_summary.json", lane_summary)
    return lane_summary


__all__ = [
    "DEFAULT_INTERNEURON_BASELINE_IDS",
    "INTERNEURON_TASK_CARD_PATH",
    "INTERNEURON_TASK_ID",
    "InterneuronAxisTaskData",
    "InterneuronBaselineDefinition",
    "InterneuronSplitAssignment",
    "VALID_INTERNEURON_AXIS_IDS",
    "build_interneuron_axis_predictions",
    "evaluate_interneuron_axis_predictions",
    "list_interneuron_baselines",
    "load_interneuron_axis_task_data",
    "materialize_interneuron_axis_rescue_runs",
    "materialize_interneuron_rescue_lane",
    "resolve_interneuron_baseline",
]
