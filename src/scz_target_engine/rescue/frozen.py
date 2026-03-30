from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.io import read_csv_table
from scz_target_engine.rescue.governance import (
    REPO_ROOT,
    RescueDatasetCard,
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


def _load_dataset(card: RescueDatasetCard) -> FrozenRescueDataset:
    if card.file_format != "csv":
        raise ValueError(f"{card.dataset_id} must point to a csv frozen artifact")

    path = _resolve_repo_relative_path(card.expected_output_path)
    if not path.exists():
        raise FileNotFoundError(f"frozen dataset does not exist: {path}")

    columns, rows = read_csv_table(path)
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


def _resolve_task_card_path(
    *,
    rescue_task_id: str | None,
    task_card_path: Path | None,
) -> Path:
    if (rescue_task_id is None) == (task_card_path is None):
        raise ValueError("provide exactly one of rescue_task_id or task_card_path")

    if task_card_path is not None:
        return task_card_path.resolve()

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
    resolved_task_card = _resolve_task_card_path(
        rescue_task_id=rescue_task_id,
        task_card_path=task_card_path,
    )
    governance = validate_rescue_governance_bundle(resolved_task_card)
    ranking_card = _select_dataset(governance, dataset_role="ranking_input")
    evaluation_card = _select_dataset(governance, dataset_role="evaluation_target")
    ranking_input = _load_dataset(ranking_card)
    evaluation_target = _load_dataset(evaluation_card)
    return FrozenRescueTaskBundle(
        governance=governance,
        ranking_input=ranking_input,
        evaluation_target=evaluation_target,
    )
