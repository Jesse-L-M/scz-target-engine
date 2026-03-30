from __future__ import annotations

import json
from pathlib import Path

from scz_target_engine.atlas.contracts import ATLAS_TAXONOMY_CONTRACT_VERSION
from scz_target_engine.atlas.substrate import (
    AtlasSourceBundle,
    build_atlas_feature_specs,
    load_atlas_source_bundles,
    taxonomy_member_id,
)
from scz_target_engine.io import write_csv, write_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_CURATED_DIR = REPO_ROOT / "data" / "curated" / "atlas"
DEFAULT_ATLAS_TAXONOMY_OUTPUT_DIR = DEFAULT_ATLAS_CURATED_DIR / "taxonomy"

_DIMENSION_DEFINITIONS = {
    "channel": (
        "Channel",
        "Separates observed evidence from explicit missingness, conflict, and uncertainty rows.",
    ),
    "source": (
        "Source",
        "Names the raw-source adapter or atlas-native structural producer associated with a tensor row.",
    ),
    "dataset": (
        "Dataset",
        "Names the processed dataset or atlas-native structural dataset associated with a tensor row.",
    ),
    "alignment_status": (
        "Alignment Status",
        "Describes whether a cross-source alignment is ID-consistent, label-only, or ID-conflicted.",
    ),
    "missingness_kind": (
        "Missingness Kind",
        "Explains whether a missing tensor slice comes from source absence or a blank source field.",
    ),
    "conflict_kind": (
        "Conflict Kind",
        "Explains the structural conflict captured by an atlas-native conflict slice.",
    ),
    "uncertainty_level": (
        "Uncertainty Level",
        "Qualitative level attached to structural uncertainty rows.",
    ),
    "disease": (
        "Disease",
        "Disease context carried through from source-level disease-scoped pulls.",
    ),
    "datatype": (
        "Datatype",
        "Open Targets datatype subscore context.",
    ),
    "study": (
        "Study",
        "Study or release context carried through from the processed source table.",
    ),
    "criterion": (
        "Criterion",
        "Binary prioritization criterion context carried through from the processed source table.",
    ),
}


def _register_member(
    member_rows: dict[str, dict[str, object]],
    *,
    dimension_id: str,
    member_id: str,
    member_label: str,
    member_description: str,
    source_name: str = "",
    dataset_name: str = "",
    attributes: dict[str, object] | None = None,
) -> None:
    member_rows[member_id] = {
        "dimension_id": dimension_id,
        "member_id": member_id,
        "member_label": member_label,
        "member_description": member_description,
        "source_name": source_name,
        "dataset_name": dataset_name,
        "attributes_json": json.dumps(attributes or {}, sort_keys=True),
    }


def _disease_context(bundle: AtlasSourceBundle) -> tuple[str, str]:
    disease_id = ""
    disease_name = ""
    if bundle.processed_metadata is not None:
        disease = bundle.processed_metadata.get("disease")
        if isinstance(disease, dict):
            disease_id = str(disease.get("id") or "")
            disease_name = str(disease.get("name") or "")
    if not disease_id and bundle.rows:
        disease_id = bundle.rows[0].get("opentargets_disease_id", "").strip()
        disease_name = bundle.rows[0].get("opentargets_disease_name", "").strip()
    return disease_id, disease_name


def materialize_atlas_taxonomy(
    ingest_manifest_file: Path,
    output_dir: Path | None = None,
) -> dict[str, object]:
    resolved_manifest_file = ingest_manifest_file.resolve()
    resolved_output_dir = (output_dir or DEFAULT_ATLAS_TAXONOMY_OUTPUT_DIR).resolve()
    bundles = load_atlas_source_bundles(resolved_manifest_file)
    feature_specs = build_atlas_feature_specs(bundles)

    member_rows: dict[str, dict[str, object]] = {}

    for channel in ("observed", "missingness", "conflict", "uncertainty"):
        _register_member(
            member_rows,
            dimension_id="channel",
            member_id=taxonomy_member_id("channel", channel),
            member_label=channel,
            member_description=f"Tensor rows in the {channel} channel.",
        )

    for source_name, description in (
        ("atlas", "Atlas-native structural slices emitted by the tensor builder."),
        ("opentargets", "Processed Open Targets baseline rows staged through atlas ingest."),
        ("pgc", "Processed PGC prioritized-gene rows staged through atlas ingest."),
    ):
        _register_member(
            member_rows,
            dimension_id="source",
            member_id=taxonomy_member_id("source", source_name),
            member_label=source_name,
            member_description=description,
        )

    for dataset_name, description in (
        ("alignment", "Atlas-native structural dataset for cross-source alignments."),
        *[
            (
                bundle.dataset_name,
                f"Processed dataset for source {bundle.source_name}.",
            )
            for bundle in bundles
        ],
    ):
        _register_member(
            member_rows,
            dimension_id="dataset",
            member_id=taxonomy_member_id("dataset", dataset_name),
            member_label=dataset_name,
            member_description=description,
            dataset_name=dataset_name,
        )

    for status in ("id_consistent", "id_conflict", "label_only"):
        _register_member(
            member_rows,
            dimension_id="alignment_status",
            member_id=taxonomy_member_id("alignment_status", status),
            member_label=status,
            member_description=f"Alignment rows classified as {status}.",
        )

    for missingness_kind in ("source_absent", "source_field_blank"):
        _register_member(
            member_rows,
            dimension_id="missingness_kind",
            member_id=taxonomy_member_id("missingness_kind", missingness_kind),
            member_label=missingness_kind,
            member_description=f"Missingness rows with kind {missingness_kind}.",
        )

    _register_member(
        member_rows,
        dimension_id="conflict_kind",
        member_id=taxonomy_member_id("conflict_kind", "id_conflict"),
        member_label="id_conflict",
        member_description="Cross-source alignments that carried multiple non-empty entity IDs.",
    )

    for level in ("low", "medium", "high"):
        _register_member(
            member_rows,
            dimension_id="uncertainty_level",
            member_id=taxonomy_member_id("uncertainty_level", level),
            member_label=level,
            member_description=f"Structural uncertainty rows with {level} uncertainty.",
        )

    for bundle in bundles:
        if bundle.source_name == "opentargets":
            disease_id, disease_name = _disease_context(bundle)
            disease_key = disease_id or disease_name or "unknown-disease"
            _register_member(
                member_rows,
                dimension_id="disease",
                member_id=taxonomy_member_id("disease", disease_key),
                member_label=disease_name or disease_id or "unknown disease",
                member_description="Disease context sourced from the Open Targets baseline pull.",
                source_name=bundle.source_name,
                dataset_name=bundle.dataset_name,
                attributes={
                    "disease_id": disease_id or None,
                    "disease_name": disease_name or None,
                },
            )
            for field_name in bundle.fieldnames:
                if not field_name.startswith("opentargets_datatype_"):
                    continue
                if field_name == "opentargets_datatype_scores_json":
                    continue
                datatype_name = field_name.removeprefix("opentargets_datatype_")
                _register_member(
                    member_rows,
                    dimension_id="datatype",
                    member_id=taxonomy_member_id("datatype", datatype_name),
                    member_label=datatype_name,
                    member_description="Open Targets datatype score context.",
                    source_name=bundle.source_name,
                    dataset_name=bundle.dataset_name,
                )
            continue

        if bundle.source_name == "pgc":
            _register_member(
                member_rows,
                dimension_id="study",
                member_id=taxonomy_member_id("study", "pgc_scz2022"),
                member_label="pgc_scz2022",
                member_description="PGC schizophrenia 2022 prioritized-gene release context.",
                source_name=bundle.source_name,
                dataset_name=bundle.dataset_name,
            )
            for spec in feature_specs:
                if spec.source_name != "pgc":
                    continue
                if spec.feature_group != "prioritization_criterion":
                    continue
                criterion_name = spec.field_name.removeprefix("pgc_scz2022_")
                _register_member(
                    member_rows,
                    dimension_id="criterion",
                    member_id=taxonomy_member_id("criterion", criterion_name),
                    member_label=criterion_name,
                    member_description="PGC binary prioritization criterion.",
                    source_name=bundle.source_name,
                    dataset_name=bundle.dataset_name,
                )

    dimension_counts = {
        dimension_id: 0
        for dimension_id in _DIMENSION_DEFINITIONS
    }
    for row in member_rows.values():
        dimension_counts[str(row["dimension_id"])] += 1

    dimension_rows = [
        {
            "dimension_id": dimension_id,
            "dimension_label": label,
            "description": description,
            "member_count": dimension_counts[dimension_id],
        }
        for dimension_id, (label, description) in _DIMENSION_DEFINITIONS.items()
    ]
    member_output_rows = sorted(
        member_rows.values(),
        key=lambda row: (
            str(row["dimension_id"]),
            str(row["member_id"]),
        ),
    )
    feature_rows = [
        {
            "feature_id": spec.feature_id,
            "feature_label": spec.feature_label,
            "source_name": spec.source_name,
            "dataset_name": spec.dataset_name,
            "feature_group": spec.feature_group,
            "field_name": spec.field_name,
            "value_type": spec.value_type,
            "channels_json": json.dumps(list(spec.channels)),
            "context_member_ids_json": json.dumps(list(spec.context_member_ids)),
            "description": spec.description,
        }
        for spec in feature_specs
    ]

    dimensions_file = resolved_output_dir / "context_dimensions.csv"
    members_file = resolved_output_dir / "context_members.csv"
    features_file = resolved_output_dir / "feature_taxonomy.csv"
    manifest_file = resolved_output_dir / "taxonomy_manifest.json"

    write_csv(
        dimensions_file,
        dimension_rows,
        ["dimension_id", "dimension_label", "description", "member_count"],
    )
    write_csv(
        members_file,
        member_output_rows,
        [
            "dimension_id",
            "member_id",
            "member_label",
            "member_description",
            "source_name",
            "dataset_name",
            "attributes_json",
        ],
    )
    write_csv(
        features_file,
        feature_rows,
        [
            "feature_id",
            "feature_label",
            "source_name",
            "dataset_name",
            "feature_group",
            "field_name",
            "value_type",
            "channels_json",
            "context_member_ids_json",
            "description",
        ],
    )

    write_json(
        manifest_file,
        {
            "contract_version": ATLAS_TAXONOMY_CONTRACT_VERSION,
            "ingest_manifest_file": str(resolved_manifest_file),
            "output_dir": str(resolved_output_dir),
            "dimension_count": len(dimension_rows),
            "member_count": len(member_output_rows),
            "feature_count": len(feature_rows),
            "emitted_artifacts": {
                "context_dimensions_file": str(dimensions_file),
                "context_members_file": str(members_file),
                "feature_taxonomy_file": str(features_file),
            },
        },
    )

    return {
        "contract_version": ATLAS_TAXONOMY_CONTRACT_VERSION,
        "ingest_manifest_file": str(resolved_manifest_file),
        "output_dir": str(resolved_output_dir),
        "context_dimensions_file": str(dimensions_file),
        "context_members_file": str(members_file),
        "feature_taxonomy_file": str(features_file),
        "manifest_file": str(manifest_file),
        "dimension_count": len(dimension_rows),
        "member_count": len(member_output_rows),
        "feature_count": len(feature_rows),
    }
