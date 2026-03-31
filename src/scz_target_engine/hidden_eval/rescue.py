from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from scz_target_engine.hidden_eval.protocol import (
    HIDDEN_EVAL_PROTOCOL_ID,
    HIDDEN_EVAL_PUBLIC_SCORECARD_SCHEMA_NAME,
    HIDDEN_EVAL_SCHEMA_VERSION,
    HIDDEN_EVAL_SIMULATION_SCHEMA_NAME,
    HIDDEN_EVAL_SUBMISSION_SCHEMA_NAME,
    HIDDEN_EVAL_TASK_PACKAGE_SCHEMA_NAME,
    RECOMMENDED_SUBMISSION_COLUMNS,
    REQUIRED_SUBMISSION_COLUMNS,
    read_hidden_eval_task_package_manifest,
    validate_hidden_eval_submission_manifest,
)
from scz_target_engine.io import read_csv_table, write_csv, write_json, write_text
from scz_target_engine.rescue.frozen import FrozenRescueTaskBundle
from scz_target_engine.rescue.tasks import (
    DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
    evaluate_glutamatergic_convergence_ranked_predictions,
    load_glutamatergic_convergence_rescue_task_bundle,
)

TASK_MANIFEST_FILE_NAME = "task_manifest.json"
TASK_README_FILE_NAME = "README.md"
RANKING_INPUT_FILE_NAME = "ranking_input.csv"
SUBMISSION_TEMPLATE_FILE_NAME = "submission_template.csv"
SUBMISSION_MANIFEST_FILE_NAME = "submission_manifest.json"
SUBMISSION_PREDICTIONS_FILE_NAME = "ranked_predictions.csv"
PUBLIC_SCORECARD_FILE_NAME = "public_scorecard.json"
INTERNAL_EVALUATION_ROWS_FILE_NAME = "internal_evaluation_rows.csv"
SIMULATION_MANIFEST_FILE_NAME = "simulation_manifest.json"


BundleLoader = Callable[[Path | None], FrozenRescueTaskBundle]
PredictionEvaluator = Callable[
    [list[dict[str, object]], FrozenRescueTaskBundle],
    dict[str, object],
]


@dataclass(frozen=True)
class HiddenEvalRescueTaskAdapter:
    task_id: str
    default_scorer_id: str
    compatibility_notes: tuple[str, ...]
    load_bundle: BundleLoader
    evaluate_predictions: PredictionEvaluator


def _load_glutamatergic_bundle(task_card_path: Path | None) -> FrozenRescueTaskBundle:
    return load_glutamatergic_convergence_rescue_task_bundle(
        task_card_path=task_card_path,
    )


def _evaluate_glutamatergic_predictions(
    predictions: list[dict[str, object]],
    bundle: FrozenRescueTaskBundle,
) -> dict[str, object]:
    return evaluate_glutamatergic_convergence_ranked_predictions(
        predictions=predictions,
        bundle=bundle,
    )


HIDDEN_EVAL_RESCUE_TASK_ADAPTERS = {
    "glutamatergic_convergence_rescue_task": HiddenEvalRescueTaskAdapter(
        task_id="glutamatergic_convergence_rescue_task",
        default_scorer_id=DEFAULT_GLUTAMATERGIC_CONVERGENCE_BASELINE_ID,
        compatibility_notes=(
            "The public package copies the exact governed pre_cutoff ranking_input CSV "
            "and excludes the held-out evaluation_target CSV.",
            "Existing glutamatergic ranked_predictions.csv outputs can be packed "
            "directly as submissions as long as they retain task_id, gene_id, and rank.",
            "This protocol is only genuinely hidden if submitters receive the exported "
            "task package instead of a checkout of this repository, because the repo "
            "itself also contains the held-out evaluation labels.",
        ),
        load_bundle=_load_glutamatergic_bundle,
        evaluate_predictions=_evaluate_glutamatergic_predictions,
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_int(value: object, field_name: str) -> int:
    text = _require_text(value, field_name)
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer: {text}") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return parsed


def _fieldnames_from_rows(rows: list[dict[str, object]]) -> list[str]:
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for field_name in row:
            if field_name in seen:
                continue
            fieldnames.append(field_name)
            seen.add(field_name)
    return fieldnames


def _serialize_csv_bytes(
    rows: list[dict[str, object]],
    *,
    fieldnames: list[str],
) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _serialize_json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _read_csv_table_from_bytes(
    payload: bytes,
) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    text_buffer = io.StringIO(payload.decode("utf-8"))
    reader = csv.DictReader(text_buffer)
    rows = list(reader)
    return tuple(reader.fieldnames or ()), rows


def _add_bytes_to_archive(
    archive: tarfile.TarFile,
    *,
    member_name: str,
    payload: bytes,
) -> None:
    info = tarfile.TarInfo(name=member_name)
    info.size = len(payload)
    archive.addfile(info, io.BytesIO(payload))


def _read_archive_member_bytes(
    archive: tarfile.TarFile,
    *,
    member_name: str,
) -> bytes:
    try:
        member = archive.getmember(member_name)
    except KeyError as exc:
        raise FileNotFoundError(
            f"submission archive is missing required member: {member_name}"
        ) from exc
    if not member.isfile():
        raise ValueError(f"archive member must be a regular file: {member_name}")
    extracted = archive.extractfile(member)
    if extracted is None:
        raise ValueError(f"archive member could not be read: {member_name}")
    return extracted.read()


def _resolve_hidden_eval_task_adapter(task_id: str) -> HiddenEvalRescueTaskAdapter:
    try:
        return HIDDEN_EVAL_RESCUE_TASK_ADAPTERS[task_id]
    except KeyError as exc:
        supported = ", ".join(sorted(HIDDEN_EVAL_RESCUE_TASK_ADAPTERS))
        raise ValueError(
            f"unsupported hidden-eval rescue task: {task_id}; supported tasks: {supported}"
        ) from exc


def _resolve_entity_id_field(bundle: FrozenRescueTaskBundle) -> str:
    primary_key_fields = bundle.ranking_input.card.primary_key_fields
    if primary_key_fields != ("gene_id",):
        raise ValueError(
            "hidden-eval rescue packaging currently supports only gene_id-keyed "
            f"ranking inputs, found {primary_key_fields!r}"
        )
    return "gene_id"


def _task_package_id(bundle: FrozenRescueTaskBundle) -> str:
    freeze_manifest_id = bundle.governance.freeze_manifests[0].freeze_manifest_id
    return f"{bundle.governance.task_card.task_id}_{freeze_manifest_id}_hidden_eval"


def _task_package_readme(
    *,
    task_id: str,
    task_label: str,
    package_id: str,
) -> str:
    return "\n".join(
        [
            f"# {task_label}",
            "",
            f"- `task_id`: `{task_id}`",
            f"- `package_id`: `{package_id}`",
            f"- `protocol_id`: `{HIDDEN_EVAL_PROTOCOL_ID}`",
            "",
            "This directory is the public submitter package. It intentionally includes",
            "only the governed pre-cutoff ranking surface plus a blank submission",
            "template. The held-out post-cutoff evaluation labels stay out of this",
            "package and are resolved only by the operator-side simulator.",
            "",
            "Important: this is only a real hidden-eval boundary if submitters receive",
            "this exported package instead of a checkout of the repository, because the",
            "repository still contains the checked-in evaluation-target CSVs.",
            "",
            "Submission expectations:",
            f"- required columns: {', '.join(REQUIRED_SUBMISSION_COLUMNS)}",
            f"- recommended columns: {', '.join(RECOMMENDED_SUBMISSION_COLUMNS)}",
            "- predictions must cover the full candidate universe exactly once",
            "- ranks must be contiguous integers from 1..N",
            "",
            "Canonical flow:",
            "",
            "```bash",
            "uv run scz-target-engine hidden-eval-pack-submission \\",
            "  --task-package-dir <this-directory> \\",
            "  --predictions-file path/to/ranked_predictions.csv \\",
            "  --submitter-id partner-demo \\",
            "  --submission-id demo-submission-v1 \\",
            "  --scorer-id demo-model \\",
            "  --output-file .context/demo-submission.tar.gz",
            "",
            "uv run scz-target-engine hidden-eval-simulate \\",
            "  --task-package-dir <this-directory> \\",
            "  --submission-file .context/demo-submission.tar.gz \\",
            "  --output-dir .context/demo-hidden-eval",
            "```",
            "",
            "Simulator outputs:",
            "- `public_scorecard.json`: safe aggregate metrics to share back",
            "- `internal_evaluation_rows.csv`: operator-only per-entity label join",
            "- `simulation_manifest.json`: operator-side provenance and file pointers",
            "",
        ]
    )


def _load_task_package_manifest_from_dir(
    task_package_dir: Path,
) -> tuple[dict[str, Any], Path]:
    manifest_path = task_package_dir.resolve() / TASK_MANIFEST_FILE_NAME
    return read_hidden_eval_task_package_manifest(manifest_path), manifest_path


def _load_public_ranking_rows(
    *,
    task_package_dir: Path,
    task_manifest: dict[str, Any],
) -> tuple[tuple[str, ...], list[dict[str, str]], Path]:
    public_artifacts = task_manifest["public_artifacts"]
    ranking_input_path = task_package_dir / str(public_artifacts["ranking_input_file"])
    if not ranking_input_path.exists():
        raise FileNotFoundError(
            f"public ranking_input file does not exist: {ranking_input_path}"
        )
    actual_sha256 = _sha256_path(ranking_input_path)
    expected_sha256 = str(public_artifacts["ranking_dataset_sha256"])
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "public ranking_input.csv drift detected: "
            f"expected {expected_sha256}, found {actual_sha256}"
        )
    fieldnames, rows = read_csv_table(ranking_input_path)
    return tuple(fieldnames), rows, ranking_input_path


def _normalize_submission_rows(
    *,
    task_manifest: dict[str, Any],
    ranking_rows: list[dict[str, str]],
    prediction_fieldnames: tuple[str, ...],
    prediction_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[str]]:
    submission_contract = task_manifest["submission_contract"]
    required_columns = tuple(submission_contract["required_columns"])
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in prediction_fieldnames
    ]
    if missing_columns:
        raise ValueError(
            "predictions file is missing required submission columns: "
            + ", ".join(missing_columns)
        )

    entity_id_field = str(submission_contract["entity_id_field"])
    candidate_rows_by_id = {row[entity_id_field]: row for row in ranking_rows}
    expected_entity_ids = set(candidate_rows_by_id)
    expected_row_count = int(submission_contract["expected_row_count"])
    if len(expected_entity_ids) != expected_row_count:
        raise ValueError(
            "task package candidate_count does not match the public ranking_input entity set"
        )

    normalized_rows: list[dict[str, str]] = []
    seen_entity_ids: set[str] = set()
    seen_ranks: set[int] = set()
    expected_task_id = str(task_manifest["task_id"])

    for row in prediction_rows:
        task_id = _require_text(row.get("task_id", ""), "task_id")
        if task_id != expected_task_id:
            raise ValueError(
                f"submission task_id must be {expected_task_id}, found {task_id}"
            )
        entity_id = _require_text(row.get(entity_id_field, ""), entity_id_field)
        if entity_id not in candidate_rows_by_id:
            raise ValueError(
                f"submission referenced an unknown {entity_id_field}: {entity_id}"
            )
        if entity_id in seen_entity_ids:
            raise ValueError(
                f"submission repeated {entity_id_field}: {entity_id}"
            )
        rank = _require_positive_int(row.get("rank", ""), "rank")
        if rank in seen_ranks:
            raise ValueError(f"submission repeated rank: {rank}")

        public_row = candidate_rows_by_id[entity_id]
        if row.get("gene_symbol", "").strip():
            expected_symbol = public_row.get("gene_symbol", "")
            if expected_symbol and row["gene_symbol"].strip() != expected_symbol:
                raise ValueError(
                    f"submission gene_symbol drift detected for {entity_id}: "
                    f"expected {expected_symbol}, found {row['gene_symbol'].strip()}"
                )
        if row.get("split_name", "").strip():
            expected_split = public_row.get("split_name", "")
            if expected_split and row["split_name"].strip() != expected_split:
                raise ValueError(
                    f"submission split_name drift detected for {entity_id}: "
                    f"expected {expected_split}, found {row['split_name'].strip()}"
                )

        normalized_row = dict(row)
        normalized_row["task_id"] = task_id
        normalized_row[entity_id_field] = entity_id
        normalized_row["rank"] = str(rank)
        if public_row.get("gene_symbol") and not normalized_row.get("gene_symbol", "").strip():
            normalized_row["gene_symbol"] = public_row["gene_symbol"]
        if public_row.get("split_name") and not normalized_row.get("split_name", "").strip():
            normalized_row["split_name"] = public_row["split_name"]
        normalized_rows.append(normalized_row)
        seen_entity_ids.add(entity_id)
        seen_ranks.add(rank)

    missing_entity_ids = sorted(expected_entity_ids.difference(seen_entity_ids))
    if missing_entity_ids:
        preview = ", ".join(missing_entity_ids[:5])
        suffix = ", ..." if len(missing_entity_ids) > 5 else ""
        raise ValueError(
            "submission must cover the full candidate universe; missing entity ids: "
            f"{preview}{suffix}"
        )

    expected_ranks = set(range(1, expected_row_count + 1))
    if seen_ranks != expected_ranks:
        missing_ranks = sorted(expected_ranks.difference(seen_ranks))
        preview = ", ".join(str(rank) for rank in missing_ranks[:5])
        suffix = ", ..." if len(missing_ranks) > 5 else ""
        raise ValueError(
            "submission ranks must be contiguous from 1..N; missing ranks: "
            f"{preview}{suffix}"
        )

    normalized_rows.sort(key=lambda row: int(row["rank"]))
    serialized_fieldnames = list(prediction_fieldnames)
    for supplemental_field in ("gene_symbol", "split_name"):
        if any(row.get(supplemental_field) for row in normalized_rows):
            if supplemental_field not in serialized_fieldnames:
                serialized_fieldnames.append(supplemental_field)
    return normalized_rows, serialized_fieldnames


def _build_operator_prediction_rows(
    *,
    bundle: FrozenRescueTaskBundle,
    normalized_rows: list[dict[str, str]],
    scorer_id: str,
) -> list[dict[str, object]]:
    ranking_rows_by_gene_id = {
        row["gene_id"]: row for row in bundle.ranking_input.rows
    }
    operator_rows: list[dict[str, object]] = []
    for row in normalized_rows:
        ranking_row = ranking_rows_by_gene_id[row["gene_id"]]
        operator_row: dict[str, object] = dict(row)
        operator_row["baseline_id"] = scorer_id
        operator_row["gene_symbol"] = row.get("gene_symbol", "") or ranking_row["gene_symbol"]
        operator_row["split_name"] = row.get("split_name", "") or ranking_row["split_name"]
        operator_rows.append(operator_row)
    return operator_rows


def _build_public_scorecard(
    *,
    task_manifest: dict[str, Any],
    submission_manifest: dict[str, Any],
    evaluation_summary: dict[str, object],
    generated_at: str,
) -> dict[str, object]:
    raw_split_summaries = evaluation_summary["split_summaries"]
    if not isinstance(raw_split_summaries, dict):
        raise ValueError("evaluation summary must include split_summaries")
    public_split_summaries = {
        split_name: {
            "candidate_count": int(split_summary["candidate_count"]),
            "metric_values": split_summary["metric_values"],
        }
        for split_name, split_summary in raw_split_summaries.items()
    }
    return {
        "schema_name": HIDDEN_EVAL_PUBLIC_SCORECARD_SCHEMA_NAME,
        "schema_version": HIDDEN_EVAL_SCHEMA_VERSION,
        "protocol_id": HIDDEN_EVAL_PROTOCOL_ID,
        "generated_at": generated_at,
        "task_id": task_manifest["task_id"],
        "package_id": task_manifest["package_id"],
        "submission_id": submission_manifest["submission_id"],
        "scorer_id": submission_manifest["scorer_id"],
        "candidate_count": int(evaluation_summary["candidate_count"]),
        "split_counts": evaluation_summary["split_counts"],
        "metric_values": evaluation_summary["metric_values"],
        "split_summaries": public_split_summaries,
        "top_ranked_gene_symbols": evaluation_summary["top_ranked_gene_symbols"],
        "notes": (
            "Aggregate metrics only. The held-out per-entity labels remain in the "
            "operator-only internal_evaluation_rows.csv output."
        ),
    }


def materialize_rescue_hidden_eval_task_package(
    *,
    task_id: str,
    output_dir: Path,
    task_card_path: Path | None = None,
) -> dict[str, object]:
    adapter = _resolve_hidden_eval_task_adapter(task_id)
    resolved_task_card_path = task_card_path.resolve() if task_card_path else None
    bundle = adapter.load_bundle(resolved_task_card_path)
    if bundle.governance.task_card.task_id != task_id:
        raise ValueError(
            f"task_card_path resolved task_id {bundle.governance.task_card.task_id}, "
            f"expected {task_id}"
        )

    entity_id_field = _resolve_entity_id_field(bundle)
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    ranking_input_copy = resolved_output_dir / RANKING_INPUT_FILE_NAME
    shutil.copyfile(bundle.ranking_input.path, ranking_input_copy)

    submission_template_rows = [
        {
            "task_id": task_id,
            "gene_id": row["gene_id"],
            "gene_symbol": row.get("gene_symbol", ""),
            "split_name": row.get("split_name", ""),
            "rank": "",
            "score": "",
        }
        for row in bundle.ranking_input.rows
    ]
    submission_template_file = resolved_output_dir / SUBMISSION_TEMPLATE_FILE_NAME
    write_csv(
        submission_template_file,
        submission_template_rows,
        fieldnames=[
            "task_id",
            "gene_id",
            "gene_symbol",
            "split_name",
            "rank",
            "score",
        ],
    )

    package_id = _task_package_id(bundle)
    manifest = {
        "schema_name": HIDDEN_EVAL_TASK_PACKAGE_SCHEMA_NAME,
        "schema_version": HIDDEN_EVAL_SCHEMA_VERSION,
        "protocol_id": HIDDEN_EVAL_PROTOCOL_ID,
        "package_id": package_id,
        "task_id": task_id,
        "task_label": bundle.governance.contract.task_label,
        "task_type": bundle.governance.contract.task_type,
        "entity_type": bundle.governance.contract.entity_type,
        "materialized_at": _utc_now(),
        "cutoff_date": bundle.governance.freeze_manifests[0].cutoff_date,
        "frozen_at": bundle.governance.freeze_manifests[0].frozen_at,
        "public_artifacts": {
            "ranking_input_file": RANKING_INPUT_FILE_NAME,
            "submission_template_file": SUBMISSION_TEMPLATE_FILE_NAME,
            "ranking_dataset_id": bundle.ranking_input.card.dataset_id,
            "ranking_dataset_sha256": _sha256_path(ranking_input_copy),
            "candidate_count": len(bundle.ranking_input.rows),
            "primary_key_fields": list(bundle.ranking_input.card.primary_key_fields),
        },
        "submission_contract": {
            "required_columns": list(REQUIRED_SUBMISSION_COLUMNS),
            "recommended_columns": list(RECOMMENDED_SUBMISSION_COLUMNS),
            "ignored_additional_columns_allowed": True,
            "entity_id_field": entity_id_field,
            "expected_row_count": len(bundle.ranking_input.rows),
        },
        "protocol_boundaries": {
            "submitter_visible": [
                "ranking_input.csv is an exact copy of the governed pre_cutoff ranking_input artifact",
                "submission_template.csv is a blank full-coverage submission template keyed to the governed candidate universe",
                "task_manifest.json names the frozen contract boundary without exposing hidden evaluation bytes",
            ],
            "hidden_evaluator_only": [
                "The governed post_cutoff evaluation_target dataset remains sealed and is not copied into this package",
                "Simulator-only outputs may materialize per-entity labels internally; do not share internal_evaluation_rows.csv back to submitters",
            ],
            "frozen_by_contract": {
                "task_card_id": bundle.governance.task_card.task_card_id,
                "freeze_manifest_ids": [
                    manifest.freeze_manifest_id
                    for manifest in bundle.governance.freeze_manifests
                ],
                "split_manifest_ids": [
                    manifest.split_manifest_id
                    for manifest in bundle.governance.split_manifests
                ],
                "lineage_ids": [
                    lineage.lineage_id for lineage in bundle.governance.lineages
                ],
                "leakage_boundary_policy_id": (
                    bundle.governance.contract.leakage_boundary.policy_id
                ),
            },
        },
        "compatibility_notes": list(adapter.compatibility_notes),
        "notes": (
            "This package is generated from the checked-in rescue governance bundle and "
            "is meant to be the submitter-facing distribution boundary. The operator-side "
            "simulator continues to resolve the real frozen bundle from the repository."
        ),
    }
    task_manifest_file = resolved_output_dir / TASK_MANIFEST_FILE_NAME
    write_json(task_manifest_file, manifest)

    readme_file = resolved_output_dir / TASK_README_FILE_NAME
    write_text(
        readme_file,
        _task_package_readme(
            task_id=task_id,
            task_label=bundle.governance.contract.task_label,
            package_id=package_id,
        ),
    )

    return {
        "task_id": task_id,
        "package_id": package_id,
        "output_dir": str(resolved_output_dir),
        "task_manifest_file": str(task_manifest_file),
        "ranking_input_file": str(ranking_input_copy),
        "submission_template_file": str(submission_template_file),
        "readme_file": str(readme_file),
        "candidate_count": len(bundle.ranking_input.rows),
    }


def materialize_hidden_eval_submission_archive(
    *,
    task_package_dir: Path,
    predictions_file: Path,
    output_file: Path,
    submitter_id: str,
    submission_id: str,
    scorer_id: str,
    notes: str = "",
) -> dict[str, object]:
    resolved_task_package_dir = task_package_dir.resolve()
    resolved_predictions_file = predictions_file.resolve()
    task_manifest, manifest_path = _load_task_package_manifest_from_dir(
        resolved_task_package_dir
    )
    prediction_fieldnames, prediction_rows = read_csv_table(resolved_predictions_file)
    _, ranking_rows, _ = _load_public_ranking_rows(
        task_package_dir=resolved_task_package_dir,
        task_manifest=task_manifest,
    )
    normalized_rows, serialized_fieldnames = _normalize_submission_rows(
        task_manifest=task_manifest,
        ranking_rows=ranking_rows,
        prediction_fieldnames=tuple(prediction_fieldnames),
        prediction_rows=prediction_rows,
    )

    predictions_payload = _serialize_csv_bytes(
        [{field_name: row.get(field_name, "") for field_name in serialized_fieldnames} for row in normalized_rows],
        fieldnames=serialized_fieldnames,
    )
    submission_manifest = {
        "schema_name": HIDDEN_EVAL_SUBMISSION_SCHEMA_NAME,
        "schema_version": HIDDEN_EVAL_SCHEMA_VERSION,
        "protocol_id": HIDDEN_EVAL_PROTOCOL_ID,
        "submission_id": _require_text(submission_id, "submission_id"),
        "task_id": task_manifest["task_id"],
        "package_id": task_manifest["package_id"],
        "submitter_id": _require_text(submitter_id, "submitter_id"),
        "scorer_id": _require_text(scorer_id, "scorer_id"),
        "created_at": _utc_now(),
        "predictions_file": SUBMISSION_PREDICTIONS_FILE_NAME,
        "prediction_sha256": _sha256_bytes(predictions_payload),
        "prediction_row_count": len(normalized_rows),
        "package_manifest_sha256": _sha256_path(manifest_path),
        "submitted_columns": serialized_fieldnames,
        "notes": notes,
    }

    resolved_output_file = output_file.resolve()
    resolved_output_file.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(resolved_output_file, "w:gz") as archive:
        _add_bytes_to_archive(
            archive,
            member_name=SUBMISSION_MANIFEST_FILE_NAME,
            payload=_serialize_json_bytes(submission_manifest),
        )
        _add_bytes_to_archive(
            archive,
            member_name=SUBMISSION_PREDICTIONS_FILE_NAME,
            payload=predictions_payload,
        )

    return {
        "task_id": task_manifest["task_id"],
        "package_id": task_manifest["package_id"],
        "submission_id": submission_manifest["submission_id"],
        "scorer_id": submission_manifest["scorer_id"],
        "output_file": str(resolved_output_file),
        "prediction_row_count": len(normalized_rows),
        "submitted_columns": serialized_fieldnames,
    }


def materialize_hidden_eval_simulation(
    *,
    task_package_dir: Path,
    submission_file: Path,
    output_dir: Path,
) -> dict[str, object]:
    started_at = _utc_now()
    resolved_task_package_dir = task_package_dir.resolve()
    resolved_submission_file = submission_file.resolve()
    resolved_output_dir = output_dir.resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    task_manifest, manifest_path = _load_task_package_manifest_from_dir(
        resolved_task_package_dir
    )
    _, ranking_rows, _ = _load_public_ranking_rows(
        task_package_dir=resolved_task_package_dir,
        task_manifest=task_manifest,
    )

    with tarfile.open(resolved_submission_file, "r:gz") as archive:
        submission_manifest_payload = json.loads(
            _read_archive_member_bytes(
                archive,
                member_name=SUBMISSION_MANIFEST_FILE_NAME,
            ).decode("utf-8")
        )
        submission_manifest = validate_hidden_eval_submission_manifest(
            submission_manifest_payload
        )
        predictions_payload = _read_archive_member_bytes(
            archive,
            member_name=SUBMISSION_PREDICTIONS_FILE_NAME,
        )

    actual_predictions_sha256 = _sha256_bytes(predictions_payload)
    expected_predictions_sha256 = str(submission_manifest["prediction_sha256"])
    if actual_predictions_sha256 != expected_predictions_sha256:
        raise ValueError(
            "submission archive predictions drift detected: "
            f"expected {expected_predictions_sha256}, found {actual_predictions_sha256}"
        )
    if submission_manifest["package_id"] != task_manifest["package_id"]:
        raise ValueError(
            "submission package_id does not match the supplied task package"
        )
    actual_manifest_sha256 = _sha256_path(manifest_path)
    if submission_manifest["package_manifest_sha256"] != actual_manifest_sha256:
        raise ValueError(
            "submission package_manifest_sha256 does not match the supplied task package"
        )

    prediction_fieldnames, prediction_rows = _read_csv_table_from_bytes(
        predictions_payload
    )
    normalized_rows, _ = _normalize_submission_rows(
        task_manifest=task_manifest,
        ranking_rows=ranking_rows,
        prediction_fieldnames=prediction_fieldnames,
        prediction_rows=prediction_rows,
    )

    adapter = _resolve_hidden_eval_task_adapter(str(task_manifest["task_id"]))
    bundle = adapter.load_bundle(None)
    if bundle.governance.task_card.task_id != task_manifest["task_id"]:
        raise ValueError(
            "operator bundle task_id does not match the supplied public task package"
        )
    if bundle.ranking_input.card.dataset_id != task_manifest["public_artifacts"]["ranking_dataset_id"]:
        raise ValueError(
            "operator bundle ranking dataset id does not match the supplied public task package"
        )

    operator_rows = _build_operator_prediction_rows(
        bundle=bundle,
        normalized_rows=normalized_rows,
        scorer_id=str(submission_manifest["scorer_id"]),
    )
    evaluation = adapter.evaluate_predictions(operator_rows, bundle)
    evaluation_rows = evaluation["evaluation_rows"]
    if not isinstance(evaluation_rows, list):
        raise ValueError("evaluation must return evaluation_rows as a list")
    evaluation_summary = evaluation["summary"]
    if not isinstance(evaluation_summary, dict):
        raise ValueError("evaluation must return summary as a dict")

    completed_at = _utc_now()
    internal_evaluation_rows_file = resolved_output_dir / INTERNAL_EVALUATION_ROWS_FILE_NAME
    write_csv(
        internal_evaluation_rows_file,
        evaluation_rows,
        fieldnames=_fieldnames_from_rows(evaluation_rows),
    )

    public_scorecard = _build_public_scorecard(
        task_manifest=task_manifest,
        submission_manifest=submission_manifest,
        evaluation_summary=evaluation_summary,
        generated_at=completed_at,
    )
    public_scorecard_file = resolved_output_dir / PUBLIC_SCORECARD_FILE_NAME
    write_json(public_scorecard_file, public_scorecard)

    simulation_manifest = {
        "schema_name": HIDDEN_EVAL_SIMULATION_SCHEMA_NAME,
        "schema_version": HIDDEN_EVAL_SCHEMA_VERSION,
        "protocol_id": HIDDEN_EVAL_PROTOCOL_ID,
        "task_id": task_manifest["task_id"],
        "package_id": task_manifest["package_id"],
        "submission_id": submission_manifest["submission_id"],
        "scorer_id": submission_manifest["scorer_id"],
        "started_at": started_at,
        "completed_at": completed_at,
        "task_package_dir": str(resolved_task_package_dir),
        "submission_file": str(resolved_submission_file),
        "public_scorecard_file": str(public_scorecard_file),
        "internal_evaluation_rows_file": str(internal_evaluation_rows_file),
        "governed_task_card_id": bundle.governance.task_card.task_card_id,
        "freeze_manifest_ids": [
            manifest.freeze_manifest_id for manifest in bundle.governance.freeze_manifests
        ],
        "split_manifest_ids": [
            manifest.split_manifest_id for manifest in bundle.governance.split_manifests
        ],
        "ranking_input_file": str(bundle.ranking_input.path),
        "evaluation_target_file": str(bundle.evaluation_target.path),
    }
    simulation_manifest_file = resolved_output_dir / SIMULATION_MANIFEST_FILE_NAME
    write_json(simulation_manifest_file, simulation_manifest)

    return {
        "task_id": task_manifest["task_id"],
        "package_id": task_manifest["package_id"],
        "submission_id": submission_manifest["submission_id"],
        "scorer_id": submission_manifest["scorer_id"],
        "output_dir": str(resolved_output_dir),
        "public_scorecard_file": str(public_scorecard_file),
        "internal_evaluation_rows_file": str(internal_evaluation_rows_file),
        "simulation_manifest_file": str(simulation_manifest_file),
        "metric_values": public_scorecard["metric_values"],
    }


__all__ = [
    "materialize_hidden_eval_simulation",
    "materialize_hidden_eval_submission_archive",
    "materialize_rescue_hidden_eval_task_package",
]
