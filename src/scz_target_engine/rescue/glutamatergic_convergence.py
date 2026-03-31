from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scz_target_engine.atlas.substrate import load_atlas_source_bundles, resolve_manifest_path
from scz_target_engine.io import read_csv_rows
from scz_target_engine.io import read_json
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
GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH = (
    REPO_ROOT
    / "data"
    / "raw"
    / "rescue"
    / "glutamatergic_convergence"
    / "2025-01-15"
    / "raw_snapshot_manifest.json"
)
GLUTAMATERGIC_CONVERGENCE_RAW_TENSOR_MANIFEST_PATH = (
    REPO_ROOT
    / "data"
    / "raw"
    / "rescue"
    / "glutamatergic_convergence"
    / "2025-01-15"
    / "tensor"
    / "tensor_manifest.json"
)
GLUTAMATERGIC_CONVERGENCE_RAW_CONVERGENCE_MANIFEST_PATH = (
    REPO_ROOT
    / "data"
    / "raw"
    / "rescue"
    / "glutamatergic_convergence"
    / "2025-01-15"
    / "convergence_manifest.json"
)


def _resolve_repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _read_manifest_dict(manifest_file: Path, *, label: str) -> dict[str, object]:
    payload = read_json(manifest_file)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {manifest_file}")
    return payload


def _ensure_checked_in_path(path: Path, *, label: str, field_name: str) -> Path:
    resolved_path = path.resolve()
    if not resolved_path.exists():
        raise ValueError(f"{label}.{field_name} does not exist: {resolved_path}")
    try:
        resolved_path.relative_to(REPO_ROOT)
    except ValueError as exc:
        raise ValueError(
            f"{label}.{field_name} must resolve inside the checked-in repository: "
            f"{resolved_path}"
        ) from exc
    return resolved_path


def _validate_portable_path_text(path_text: str, *, label: str, field_name: str) -> None:
    if Path(path_text).is_absolute():
        raise ValueError(f"{label}.{field_name} must not be an absolute path: {path_text}")
    if ".context" in Path(path_text).parts:
        raise ValueError(f"{label}.{field_name} must not point into .context: {path_text}")


def _resolve_portable_manifest_path(
    manifest_file: Path,
    path_text: str,
    *,
    label: str,
    field_name: str,
) -> Path:
    _validate_portable_path_text(path_text, label=label, field_name=field_name)
    return _ensure_checked_in_path(
        resolve_manifest_path(manifest_file, path_text),
        label=label,
        field_name=field_name,
    )


def _resolve_required_manifest_field(
    manifest_file: Path,
    manifest: dict[str, object],
    field_name: str,
    *,
    label: str,
) -> Path:
    path_text = manifest.get(field_name)
    if not isinstance(path_text, str) or not path_text.strip():
        raise ValueError(f"{label} is missing {field_name}.")
    return _resolve_portable_manifest_path(
        manifest_file,
        path_text,
        label=label,
        field_name=field_name,
    )


def _resolve_required_artifact_map(
    manifest_file: Path,
    manifest: dict[str, object],
    *,
    label: str,
    field_names: tuple[str, ...],
) -> dict[str, Path]:
    emitted_artifacts = manifest.get("emitted_artifacts")
    if not isinstance(emitted_artifacts, dict):
        raise ValueError(f"{label} is missing an emitted_artifacts mapping.")

    resolved_artifacts: dict[str, Path] = {}
    for field_name in field_names:
        path_text = emitted_artifacts.get(field_name)
        if not isinstance(path_text, str) or not path_text.strip():
            raise ValueError(f"{label}.emitted_artifacts is missing {field_name}.")
        resolved_artifacts[field_name] = _resolve_portable_manifest_path(
            manifest_file,
            path_text,
            label=f"{label}.emitted_artifacts",
            field_name=field_name,
        )
    return resolved_artifacts


def _resolve_provenance_bundle_path(path_text: str, *, field_name: str) -> Path:
    _validate_portable_path_text(
        path_text,
        label="tensor.provenance_bundles",
        field_name=field_name,
    )
    return _ensure_checked_in_path(
        _resolve_repo_relative_path(path_text),
        label="tensor.provenance_bundles",
        field_name=field_name,
    )


@dataclass(frozen=True)
class GlutamatergicConvergenceRescueBundle:
    governance_bundle: RescueGovernanceBundle
    ranking_input_rows: tuple[dict[str, str], ...]
    evaluation_label_rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class GlutamatergicConvergenceRawSnapshotBundle:
    raw_snapshot_manifest_file: Path
    atlas_ingest_manifest_file: Path
    tensor_manifest_file: Path
    tensor_artifact_files: dict[str, Path]
    taxonomy_manifest_file: Path
    taxonomy_artifact_files: dict[str, Path]
    convergence_manifest_file: Path
    convergence_artifact_files: dict[str, Path]
    provenance_bundle_rows: tuple[dict[str, str], ...]


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


def validate_glutamatergic_convergence_raw_snapshot_bundle() -> (
    GlutamatergicConvergenceRawSnapshotBundle
):
    raw_snapshot_manifest = _read_manifest_dict(
        GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
        label="glutamatergic raw snapshot manifest",
    )
    atlas_ingest_manifest_file = _resolve_required_manifest_field(
        GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
        raw_snapshot_manifest,
        "atlas_ingest_manifest_file",
        label="glutamatergic raw snapshot manifest",
    )
    tensor_manifest_file = _resolve_required_manifest_field(
        GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
        raw_snapshot_manifest,
        "tensor_manifest_file",
        label="glutamatergic raw snapshot manifest",
    )
    convergence_manifest_file = _resolve_required_manifest_field(
        GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
        raw_snapshot_manifest,
        "convergence_manifest_file",
        label="glutamatergic raw snapshot manifest",
    )
    if tensor_manifest_file != GLUTAMATERGIC_CONVERGENCE_RAW_TENSOR_MANIFEST_PATH:
        raise ValueError(
            "glutamatergic raw snapshot must point to the checked-in tensor manifest"
        )
    if (
        convergence_manifest_file
        != GLUTAMATERGIC_CONVERGENCE_RAW_CONVERGENCE_MANIFEST_PATH
    ):
        raise ValueError(
            "glutamatergic raw snapshot must point to the checked-in convergence manifest"
        )

    load_atlas_source_bundles(atlas_ingest_manifest_file)

    tensor_manifest = _read_manifest_dict(
        tensor_manifest_file,
        label="glutamatergic raw tensor manifest",
    )
    tensor_artifact_files = _resolve_required_artifact_map(
        tensor_manifest_file,
        tensor_manifest,
        label="glutamatergic raw tensor manifest",
        field_names=(
            "entity_alignments_file",
            "evidence_tensor_file",
            "provenance_bundles_file",
            "taxonomy_manifest_file",
        ),
    )
    tensor_ingest_manifest_file = _resolve_required_manifest_field(
        tensor_manifest_file,
        tensor_manifest,
        "ingest_manifest_file",
        label="glutamatergic raw tensor manifest",
    )
    if tensor_ingest_manifest_file != atlas_ingest_manifest_file:
        raise ValueError(
            "glutamatergic raw tensor manifest must point to the checked-in atlas "
            "fixture ingest manifest"
        )
    _resolve_required_manifest_field(
        tensor_manifest_file,
        tensor_manifest,
        "output_dir",
        label="glutamatergic raw tensor manifest",
    )
    _resolve_required_manifest_field(
        tensor_manifest_file,
        tensor_manifest,
        "taxonomy_output_dir",
        label="glutamatergic raw tensor manifest",
    )

    taxonomy_manifest_file = tensor_artifact_files["taxonomy_manifest_file"]
    taxonomy_manifest = _read_manifest_dict(
        taxonomy_manifest_file,
        label="glutamatergic raw taxonomy manifest",
    )
    taxonomy_artifact_files = _resolve_required_artifact_map(
        taxonomy_manifest_file,
        taxonomy_manifest,
        label="glutamatergic raw taxonomy manifest",
        field_names=(
            "context_dimensions_file",
            "context_members_file",
            "feature_taxonomy_file",
        ),
    )
    taxonomy_ingest_manifest_file = _resolve_required_manifest_field(
        taxonomy_manifest_file,
        taxonomy_manifest,
        "ingest_manifest_file",
        label="glutamatergic raw taxonomy manifest",
    )
    if taxonomy_ingest_manifest_file != atlas_ingest_manifest_file:
        raise ValueError(
            "glutamatergic raw taxonomy manifest must point to the checked-in atlas "
            "fixture ingest manifest"
        )
    _resolve_required_manifest_field(
        taxonomy_manifest_file,
        taxonomy_manifest,
        "output_dir",
        label="glutamatergic raw taxonomy manifest",
    )

    convergence_manifest = _read_manifest_dict(
        convergence_manifest_file,
        label="glutamatergic raw convergence manifest",
    )
    convergence_artifact_files = _resolve_required_artifact_map(
        convergence_manifest_file,
        convergence_manifest,
        label="glutamatergic raw convergence manifest",
        field_names=(
            "convergence_hubs_file",
            "hub_axis_members_file",
            "hub_evidence_links_file",
        ),
    )
    convergence_tensor_manifest_file = _resolve_required_manifest_field(
        convergence_manifest_file,
        convergence_manifest,
        "tensor_manifest_file",
        label="glutamatergic raw convergence manifest",
    )
    if convergence_tensor_manifest_file != tensor_manifest_file:
        raise ValueError(
            "glutamatergic raw convergence manifest must point to the checked-in tensor "
            "manifest"
        )
    convergence_evidence_tensor_file = _resolve_required_manifest_field(
        convergence_manifest_file,
        convergence_manifest,
        "evidence_tensor_file",
        label="glutamatergic raw convergence manifest",
    )
    if convergence_evidence_tensor_file != tensor_artifact_files["evidence_tensor_file"]:
        raise ValueError(
            "glutamatergic raw convergence manifest must point to the checked-in tensor "
            "evidence CSV"
        )
    _resolve_required_manifest_field(
        convergence_manifest_file,
        convergence_manifest,
        "output_dir",
        label="glutamatergic raw convergence manifest",
    )

    provenance_bundle_rows = tuple(
        read_csv_rows(tensor_artifact_files["provenance_bundles_file"])
    )
    for row in provenance_bundle_rows:
        for field_name in (
            "processed_output_file",
            "processed_metadata_file",
            "raw_manifest_file",
        ):
            path_text = row.get(field_name, "").strip()
            if not path_text:
                continue
            _resolve_provenance_bundle_path(path_text, field_name=field_name)

    return GlutamatergicConvergenceRawSnapshotBundle(
        raw_snapshot_manifest_file=GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
        atlas_ingest_manifest_file=atlas_ingest_manifest_file,
        tensor_manifest_file=tensor_manifest_file,
        tensor_artifact_files=tensor_artifact_files,
        taxonomy_manifest_file=taxonomy_manifest_file,
        taxonomy_artifact_files=taxonomy_artifact_files,
        convergence_manifest_file=convergence_manifest_file,
        convergence_artifact_files=convergence_artifact_files,
        provenance_bundle_rows=provenance_bundle_rows,
    )


__all__ = [
    "GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH",
    "GLUTAMATERGIC_CONVERGENCE_EVALUATION_LABELS_PATH",
    "GLUTAMATERGIC_CONVERGENCE_RAW_CONVERGENCE_MANIFEST_PATH",
    "GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH",
    "GLUTAMATERGIC_CONVERGENCE_RAW_TENSOR_MANIFEST_PATH",
    "GLUTAMATERGIC_CONVERGENCE_RANKING_INPUTS_PATH",
    "GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH",
    "GlutamatergicConvergenceRawSnapshotBundle",
    "GlutamatergicConvergenceRescueBundle",
    "load_glutamatergic_convergence_rescue_bundle",
    "validate_glutamatergic_convergence_raw_snapshot_bundle",
]
