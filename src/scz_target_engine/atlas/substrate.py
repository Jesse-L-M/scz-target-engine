from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from scz_target_engine.atlas.staging import slugify
from scz_target_engine.io import read_csv_table, read_json


REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class AtlasSourceBundle:
    provenance_bundle_id: str
    source_name: str
    dataset_name: str
    processed_output_file: Path
    processed_metadata_file: Path | None
    raw_manifest_file: Path | None
    source_contract: dict[str, object]
    processed_metadata: dict[str, object] | None
    raw_manifest: dict[str, object] | None
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class AtlasFeatureSpec:
    feature_id: str
    feature_label: str
    source_name: str
    dataset_name: str
    field_name: str
    feature_group: str
    value_type: str
    description: str
    context_member_ids: tuple[str, ...]
    channels: tuple[str, ...]


def taxonomy_member_id(dimension_id: str, raw_value: str) -> str:
    return f"{dimension_id}:{slugify(raw_value)}"


def normalize_alignment_label(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.strip().upper().split())


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def resolve_manifest_path(reference_file: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path.resolve()

    resolved_reference_dir = reference_file.parent.resolve()
    reference_candidate = (resolved_reference_dir / path).resolve()
    if reference_candidate.exists():
        return reference_candidate

    repo_candidate = (REPO_ROOT / path).resolve()
    if _is_within(repo_candidate, REPO_ROOT) and repo_candidate.exists():
        return repo_candidate

    return reference_candidate


def serialize_manifest_path(reference_file: Path, path: Path) -> str:
    resolved_reference_dir = reference_file.parent.resolve()
    resolved_path = path.resolve()
    reference_in_repo = _is_within(resolved_reference_dir, REPO_ROOT)
    path_in_repo = _is_within(resolved_path, REPO_ROOT)

    if reference_in_repo == path_in_repo:
        return Path(os.path.relpath(resolved_path, resolved_reference_dir)).as_posix()
    if path_in_repo:
        return resolved_path.relative_to(REPO_ROOT).as_posix()
    return str(resolved_path)


def serialize_provenance_path(path: Path) -> str:
    resolved_path = path.resolve()
    if _is_within(resolved_path, REPO_ROOT):
        return resolved_path.relative_to(REPO_ROOT).as_posix()
    return str(resolved_path)


def resolve_optional_manifest_path(
    reference_file: Path,
    raw_path: object | None,
) -> Path | None:
    if raw_path is None:
        return None
    if not isinstance(raw_path, str):
        raise ValueError(f"Expected a string path in {reference_file}, got {raw_path!r}")
    return resolve_manifest_path(reference_file, raw_path)


def load_atlas_source_bundles(ingest_manifest_file: Path) -> list[AtlasSourceBundle]:
    resolved_manifest_file = ingest_manifest_file.resolve()
    payload = read_json(resolved_manifest_file)
    if not isinstance(payload, dict):
        raise ValueError(
            f"Atlas ingest manifest must be a JSON object: {resolved_manifest_file}"
        )

    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, dict):
        raise ValueError(
            "Atlas ingest manifest must include a sources mapping for tensor materialization."
        )

    bundles: list[AtlasSourceBundle] = []
    for source_name in sorted(raw_sources):
        source_payload = raw_sources[source_name]
        if source_payload is None:
            continue
        if not isinstance(source_payload, dict):
            raise ValueError(
                f"Atlas source payload for {source_name!r} must be a JSON object."
            )

        processed_output_file = resolve_optional_manifest_path(
            resolved_manifest_file,
            source_payload.get("processed_output_file"),
        )
        if processed_output_file is None:
            raise ValueError(
                f"Atlas source payload for {source_name!r} is missing processed_output_file."
            )
        processed_metadata_file = resolve_optional_manifest_path(
            resolved_manifest_file,
            source_payload.get("processed_metadata_file"),
        )
        raw_manifest_file = resolve_optional_manifest_path(
            resolved_manifest_file,
            source_payload.get("raw_manifest_file"),
        )

        fieldnames, rows = read_csv_table(processed_output_file)
        processed_metadata = None
        if processed_metadata_file is not None and processed_metadata_file.exists():
            processed_metadata_payload = read_json(processed_metadata_file)
            if isinstance(processed_metadata_payload, dict):
                processed_metadata = processed_metadata_payload
        raw_manifest = None
        if raw_manifest_file is not None and raw_manifest_file.exists():
            raw_manifest_payload = read_json(raw_manifest_file)
            if isinstance(raw_manifest_payload, dict):
                raw_manifest = raw_manifest_payload

        source_contract = source_payload.get("source_contract")
        if not isinstance(source_contract, dict):
            source_contract = {}
        dataset_name = str(source_contract.get("dataset_name") or source_name)
        bundles.append(
            AtlasSourceBundle(
                provenance_bundle_id=f"{source_name}:{slugify(dataset_name)}",
                source_name=str(source_name),
                dataset_name=dataset_name,
                processed_output_file=processed_output_file,
                processed_metadata_file=processed_metadata_file,
                raw_manifest_file=raw_manifest_file,
                source_contract=source_contract,
                processed_metadata=processed_metadata,
                raw_manifest=raw_manifest,
                fieldnames=tuple(fieldnames),
                rows=tuple(rows),
            )
        )

    return bundles


def build_atlas_feature_specs(
    bundles: list[AtlasSourceBundle],
) -> list[AtlasFeatureSpec]:
    feature_specs: list[AtlasFeatureSpec] = [
        AtlasFeatureSpec(
            feature_id="atlas.alignment_entity_id_conflict",
            feature_label="Alignment entity ID conflict",
            source_name="atlas",
            dataset_name="alignment",
            field_name="",
            feature_group="alignment_conflict",
            value_type="json",
            description=(
                "Cross-source entity identifier disagreement observed inside one "
                "alignment group."
            ),
            context_member_ids=(
                taxonomy_member_id("source", "atlas"),
                taxonomy_member_id("dataset", "alignment"),
            ),
            channels=("conflict", "uncertainty"),
        )
    ]

    for bundle in bundles:
        source_member = taxonomy_member_id("source", bundle.source_name)
        dataset_member = taxonomy_member_id("dataset", bundle.dataset_name)

        if bundle.source_name == "opentargets":
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
            disease_key = disease_id or disease_name or "unknown-disease"
            disease_member = taxonomy_member_id("disease", disease_key)
            feature_specs.append(
                AtlasFeatureSpec(
                    feature_id="opentargets.generic_platform_baseline",
                    feature_label="Open Targets generic platform baseline",
                    source_name=bundle.source_name,
                    dataset_name=bundle.dataset_name,
                    field_name="generic_platform_baseline",
                    feature_group="association_score",
                    value_type="number",
                    description=(
                        "Disease-level Open Targets association score carried through "
                        "from the processed baseline table."
                    ),
                    context_member_ids=(source_member, dataset_member, disease_member),
                    channels=("observed", "missingness", "uncertainty"),
                )
            )
            for field_name in bundle.fieldnames:
                if not field_name.startswith("opentargets_datatype_"):
                    continue
                if field_name == "opentargets_datatype_scores_json":
                    continue
                datatype_name = field_name.removeprefix("opentargets_datatype_")
                datatype_member = taxonomy_member_id("datatype", datatype_name)
                feature_specs.append(
                    AtlasFeatureSpec(
                        feature_id=f"opentargets.datatype.{slugify(datatype_name)}",
                        feature_label=f"Open Targets datatype score: {datatype_name}",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name=field_name,
                        feature_group="datatype_score",
                        value_type="number",
                        description=(
                            "Disease-level Open Targets datatype score materialized as "
                            "one factorized feature column."
                        ),
                        context_member_ids=(
                            source_member,
                            dataset_member,
                            disease_member,
                            datatype_member,
                        ),
                        channels=("observed", "missingness", "uncertainty"),
                    )
                )
            continue

        if bundle.source_name == "pgc":
            study_member = taxonomy_member_id("study", "pgc_scz2022")
            feature_specs.extend(
                [
                    AtlasFeatureSpec(
                        feature_id="pgc.common_variant_support",
                        feature_label="PGC common variant support",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name="common_variant_support",
                        feature_group="association_score",
                        value_type="number",
                        description=(
                            "Aggregated common-variant support score from the PGC "
                            "schizophrenia prioritized-gene release."
                        ),
                        context_member_ids=(source_member, dataset_member, study_member),
                        channels=("observed", "missingness", "uncertainty"),
                    ),
                    AtlasFeatureSpec(
                        feature_id="pgc.prioritised",
                        feature_label="PGC prioritised flag",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name="pgc_scz2022_prioritised",
                        feature_group="prioritization_summary",
                        value_type="boolean",
                        description=(
                            "Binary prioritized flag from the PGC schizophrenia release."
                        ),
                        context_member_ids=(source_member, dataset_member, study_member),
                        channels=("observed", "missingness", "uncertainty"),
                    ),
                    AtlasFeatureSpec(
                        feature_id="pgc.priority_index_snp_count",
                        feature_label="PGC priority index SNP count",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name="pgc_scz2022_priority_index_snp_count",
                        feature_group="index_snp_summary",
                        value_type="integer",
                        description=(
                            "Number of priority-index SNPs supporting the PGC gene row."
                        ),
                        context_member_ids=(source_member, dataset_member, study_member),
                        channels=("observed", "missingness", "uncertainty"),
                    ),
                    AtlasFeatureSpec(
                        feature_id="pgc.priority_index_snps",
                        feature_label="PGC priority index SNP list",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name="pgc_scz2022_priority_index_snps_json",
                        feature_group="index_snp_summary",
                        value_type="json",
                        description=(
                            "JSON list of priority-index SNP IDs supporting the PGC gene row."
                        ),
                        context_member_ids=(source_member, dataset_member, study_member),
                        channels=("observed", "missingness", "uncertainty"),
                    ),
                ]
            )

            excluded_fields = {
                "pgc_scz2022_prioritised",
                "pgc_scz2022_priority_index_snp_count",
                "pgc_scz2022_priority_index_snps_json",
            }
            for field_name in bundle.fieldnames:
                if not field_name.startswith("pgc_scz2022_"):
                    continue
                if field_name in excluded_fields:
                    continue
                criterion_name = field_name.removeprefix("pgc_scz2022_")
                criterion_member = taxonomy_member_id("criterion", criterion_name)
                feature_specs.append(
                    AtlasFeatureSpec(
                        feature_id=f"pgc.criterion.{slugify(criterion_name)}",
                        feature_label=f"PGC criterion: {criterion_name}",
                        source_name=bundle.source_name,
                        dataset_name=bundle.dataset_name,
                        field_name=field_name,
                        feature_group="prioritization_criterion",
                        value_type="boolean",
                        description=(
                            "One binary prioritization criterion carried through from "
                            "the PGC schizophrenia release."
                        ),
                        context_member_ids=(
                            source_member,
                            dataset_member,
                            study_member,
                            criterion_member,
                        ),
                        channels=("observed", "missingness", "uncertainty"),
                    )
                )

    feature_specs.sort(
        key=lambda spec: (
            spec.source_name,
            spec.dataset_name,
            spec.feature_group,
            spec.feature_id,
        )
    )
    return feature_specs
