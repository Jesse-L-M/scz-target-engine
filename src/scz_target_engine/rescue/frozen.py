from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import read_csv_table
from scz_target_engine.rescue.governance import (
    REPO_ROOT,
    RescueDatasetCard,
    RescueFrozenDatasetReference,
    RescueGovernanceBundle,
    validate_rescue_governance_bundle,
)


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _primary_key_tuple(
    row: dict[str, str],
    *,
    primary_key_fields: tuple[str, ...],
    dataset_id: str,
) -> tuple[str, ...]:
    missing_fields = [field for field in primary_key_fields if field not in row]
    if missing_fields:
        raise ValueError(
            f"{dataset_id} is missing declared primary key fields: "
            + ", ".join(missing_fields)
        )
    return tuple(row[field] for field in primary_key_fields)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class FrozenRescueDataset:
    card: RescueDatasetCard
    path: Path
    columns: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class FrozenRescueTaskBundle:
    governance: RescueGovernanceBundle
    ranking_input: FrozenRescueDataset
    evaluation_target: FrozenRescueDataset


@dataclass(frozen=True)
class FrozenRescueGovernedTaskBundle:
    governance: RescueGovernanceBundle
    datasets: tuple[FrozenRescueDataset, ...]

    @property
    def dataset_index(self) -> dict[str, FrozenRescueDataset]:
        return {
            dataset.card.dataset_id: dataset
            for dataset in self.datasets
        }


def _freeze_reference_by_dataset_id(
    bundle: RescueGovernanceBundle,
    *,
    card: RescueDatasetCard,
) -> RescueFrozenDatasetReference:
    freeze_manifest_by_path = {
        path_text: freeze_manifest
        for path_text, freeze_manifest in zip(
            bundle.task_card.freeze_manifest_paths,
            bundle.freeze_manifests,
        )
    }
    freeze_manifest = freeze_manifest_by_path.get(card.freeze_manifest_path)
    if freeze_manifest is None:
        raise ValueError(
            f"{card.dataset_id} referenced an unknown freeze_manifest_path: "
            f"{card.freeze_manifest_path}"
        )

    matches = [
        dataset
        for dataset in freeze_manifest.frozen_datasets
        if dataset.dataset_id == card.dataset_id
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one frozen dataset reference for {card.dataset_id} "
            f"inside {card.freeze_manifest_path}, found {len(matches)}"
        )
    return matches[0]


def _load_dataset(
    card: RescueDatasetCard,
    *,
    freeze_reference: RescueFrozenDatasetReference,
) -> FrozenRescueDataset:
    if card.file_format != "csv":
        raise ValueError(f"{card.dataset_id} must point to a csv frozen artifact")

    path = _resolve_repo_relative_path(card.expected_output_path)
    if not path.exists():
        raise FileNotFoundError(f"frozen dataset does not exist: {path}")

    if not freeze_reference.expected_sha256:
        raise ValueError(
            f"{card.dataset_id} is missing expected_sha256 in the governing freeze manifest"
        )
    actual_sha256 = _sha256_path(path)
    if actual_sha256 != freeze_reference.expected_sha256:
        raise ValueError(
            f"{card.dataset_id} failed checksum validation for {path}: "
            "governed frozen artifact drift detected"
        )

    columns, rows = read_csv_table(path)
    if freeze_reference.expected_row_count is None:
        raise ValueError(
            f"{card.dataset_id} is missing expected_row_count in the governing freeze manifest"
        )
    if len(rows) != freeze_reference.expected_row_count:
        raise ValueError(
            f"{card.dataset_id} row count drift detected for {path}: "
            f"expected {freeze_reference.expected_row_count}, found {len(rows)}"
        )

    seen_primary_keys: set[tuple[str, ...]] = set()
    for row in rows:
        primary_key = _primary_key_tuple(
            row,
            primary_key_fields=card.primary_key_fields,
            dataset_id=card.dataset_id,
        )
        if primary_key in seen_primary_keys:
            raise ValueError(
                f"{card.dataset_id} repeated a declared primary key: {primary_key}"
            )
        seen_primary_keys.add(primary_key)

    return FrozenRescueDataset(
        card=card,
        path=path,
        columns=tuple(columns),
        rows=tuple(rows),
    )


def _require_split_consistency(
    *,
    ranking_input: FrozenRescueDataset,
    evaluation_target: FrozenRescueDataset,
) -> None:
    if ranking_input.card.primary_key_fields != evaluation_target.card.primary_key_fields:
        raise ValueError(
            "ranking_input and evaluation_target must share primary_key_fields"
        )

    for dataset in (ranking_input, evaluation_target):
        if "split_name" not in dataset.columns:
            raise ValueError(
                f"{dataset.card.dataset_id} must expose split_name for governed split validation"
            )

    ranking_splits: dict[tuple[str, ...], str] = {}
    for row in ranking_input.rows:
        primary_key = _primary_key_tuple(
            row,
            primary_key_fields=ranking_input.card.primary_key_fields,
            dataset_id=ranking_input.card.dataset_id,
        )
        ranking_splits[primary_key] = row["split_name"]

    for row in evaluation_target.rows:
        primary_key = _primary_key_tuple(
            row,
            primary_key_fields=evaluation_target.card.primary_key_fields,
            dataset_id=evaluation_target.card.dataset_id,
        )
        ranking_split = ranking_splits.get(primary_key)
        if ranking_split is None:
            raise ValueError(
                f"{evaluation_target.card.dataset_id} contains an entity not present in "
                f"{ranking_input.card.dataset_id}: {primary_key}"
            )
        evaluation_split = row["split_name"]
        if evaluation_split != ranking_split:
            raise ValueError(
                "split_name drift detected between ranking_input and evaluation_target "
                f"for {primary_key}: expected {ranking_split}, found {evaluation_split}"
            )


def _resolve_task_card_path(
    *,
    rescue_task_id: str | None,
    task_card_path: Path | None,
) -> Path:
    if (rescue_task_id is None) == (task_card_path is None):
        raise ValueError("provide exactly one of rescue_task_id or task_card_path")

    if task_card_path is not None:
        return _resolve_repo_relative_path(str(task_card_path))

    from scz_target_engine.rescue.registry import load_rescue_task_registrations

    registrations = load_rescue_task_registrations()
    for registration in registrations:
        if registration.task_id == rescue_task_id:
            return registration.task_card_file.resolve()

    raise KeyError(f"unknown rescue_task_id: {rescue_task_id}")


def _select_dataset(
    bundle: RescueGovernanceBundle,
    *,
    dataset_role: str,
) -> RescueDatasetCard:
    matches = tuple(
        dataset for dataset in bundle.dataset_cards if dataset.dataset_role == dataset_role
    )
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one {dataset_role} dataset card, found {len(matches)}"
        )
    return matches[0]


def load_frozen_rescue_task_bundle(
    *,
    rescue_task_id: str | None = None,
    task_card_path: Path | None = None,
) -> FrozenRescueTaskBundle:
    governed_bundle = load_frozen_rescue_governance_bundle(
        rescue_task_id=rescue_task_id,
        task_card_path=task_card_path,
    )
    governance = governed_bundle.governance
    ranking_card = _select_dataset(governance, dataset_role="ranking_input")
    evaluation_card = _select_dataset(governance, dataset_role="evaluation_target")
    dataset_index = governed_bundle.dataset_index
    ranking_input = dataset_index[ranking_card.dataset_id]
    evaluation_target = dataset_index[evaluation_card.dataset_id]
    _require_split_consistency(
        ranking_input=ranking_input,
        evaluation_target=evaluation_target,
    )
    return FrozenRescueTaskBundle(
        governance=governance,
        ranking_input=ranking_input,
        evaluation_target=evaluation_target,
    )


def load_frozen_rescue_governance_bundle(
    *,
    rescue_task_id: str | None = None,
    task_card_path: Path | None = None,
) -> FrozenRescueGovernedTaskBundle:
    resolved_task_card = _resolve_task_card_path(
        rescue_task_id=rescue_task_id,
        task_card_path=task_card_path,
    )
    governance = validate_rescue_governance_bundle(resolved_task_card)
    datasets = tuple(
        _load_dataset(
            dataset_card,
            freeze_reference=_freeze_reference_by_dataset_id(
                governance,
                card=dataset_card,
            ),
        )
        for dataset_card in governance.dataset_cards
    )
    return FrozenRescueGovernedTaskBundle(
        governance=governance,
        datasets=datasets,
    )
