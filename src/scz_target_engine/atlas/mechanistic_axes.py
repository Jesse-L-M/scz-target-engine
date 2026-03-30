from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from scz_target_engine.atlas.contracts import (
    ATLAS_MECHANISTIC_AXES_CONTRACT_VERSION,
    ATLAS_TENSOR_CONTRACT_VERSION,
)
from scz_target_engine.atlas.substrate import resolve_manifest_path
from scz_target_engine.io import read_csv_rows, read_json, write_csv, write_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_MECHANISTIC_AXES_OUTPUT_DIR = (
    REPO_ROOT / "data" / "curated" / "atlas" / "mechanistic_axes"
)
ATLAS_STRUCTURAL_CONFLICT_FEATURE_ID = "atlas.alignment_entity_id_conflict"

_CHANNEL_SORT_ORDER = {
    "observed": 0,
    "missingness": 1,
    "conflict": 2,
    "uncertainty": 3,
}
_UNCERTAINTY_LEVEL_ORDER = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


@dataclass(frozen=True)
class AtlasTensorRow:
    slice_id: str
    alignment_id: str
    alignment_label: str
    alignment_status: str
    source_name: str
    dataset_name: str
    feature_id: str
    feature_group: str
    channel: str
    context_member_ids: tuple[str, ...]
    value_type: str
    numeric_value: str
    text_value: str
    json_value: str
    provenance_bundle_id: str
    source_row_index: str
    source_entity_id: str
    source_entity_label: str

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "AtlasTensorRow":
        context_member_ids = _parse_context_member_ids(row.get("context_member_ids_json", ""))
        return cls(
            slice_id=row.get("slice_id", ""),
            alignment_id=row.get("alignment_id", ""),
            alignment_label=row.get("alignment_label", ""),
            alignment_status=row.get("alignment_status", ""),
            source_name=row.get("source_name", ""),
            dataset_name=row.get("dataset_name", ""),
            feature_id=row.get("feature_id", ""),
            feature_group=row.get("feature_group", ""),
            channel=row.get("channel", ""),
            context_member_ids=context_member_ids,
            value_type=row.get("value_type", ""),
            numeric_value=row.get("numeric_value", ""),
            text_value=row.get("text_value", ""),
            json_value=row.get("json_value", ""),
            provenance_bundle_id=row.get("provenance_bundle_id", ""),
            source_row_index=row.get("source_row_index", ""),
            source_entity_id=row.get("source_entity_id", ""),
            source_entity_label=row.get("source_entity_label", ""),
        )


@dataclass(frozen=True)
class AtlasTensorBundle:
    manifest_file: Path
    evidence_tensor_file: Path
    manifest: dict[str, object]
    tensor_rows: tuple[AtlasTensorRow, ...]


@dataclass(frozen=True)
class MechanisticAxisSpec:
    axis_id: str
    axis_label: str
    axis_description: str
    feature_ids: tuple[str, ...]
    feature_prefixes: tuple[str, ...] = ()

    def matches_feature(self, feature_id: str) -> bool:
        if feature_id in self.feature_ids:
            return True
        return any(feature_id.startswith(prefix) for prefix in self.feature_prefixes)


@dataclass(frozen=True)
class MechanisticAxisProfile:
    axis_id: str
    axis_label: str
    axis_description: str
    alignment_id: str
    alignment_label: str
    alignment_status: str
    expected_feature_ids: tuple[str, ...]
    observed_feature_ids: tuple[str, ...]
    missing_feature_ids: tuple[str, ...]
    observed_source_names: tuple[str, ...]
    source_coverage_state: str
    support_state: str
    missingness_state: str
    conflict_state: str
    uncertainty_max_level: str
    observed_slice_ids: tuple[str, ...]
    missingness_slice_ids: tuple[str, ...]
    conflict_slice_ids: tuple[str, ...]
    uncertainty_slice_ids: tuple[str, ...]
    missingness_kinds: tuple[str, ...]
    uncertainty_reasons: tuple[str, ...]
    conflict_reasons: tuple[str, ...]
    resolved_provenance_bundle_ids: tuple[str, ...]

    def to_row(self) -> dict[str, object]:
        return {
            "axis_id": self.axis_id,
            "axis_label": self.axis_label,
            "axis_description": self.axis_description,
            "alignment_id": self.alignment_id,
            "alignment_label": self.alignment_label,
            "alignment_status": self.alignment_status,
            "expected_feature_ids_json": json.dumps(list(self.expected_feature_ids)),
            "observed_feature_ids_json": json.dumps(list(self.observed_feature_ids)),
            "missing_feature_ids_json": json.dumps(list(self.missing_feature_ids)),
            "observed_source_names_json": json.dumps(list(self.observed_source_names)),
            "source_coverage_state": self.source_coverage_state,
            "support_state": self.support_state,
            "missingness_state": self.missingness_state,
            "conflict_state": self.conflict_state,
            "uncertainty_max_level": self.uncertainty_max_level,
            "observed_slice_ids_json": json.dumps(list(self.observed_slice_ids)),
            "missingness_slice_ids_json": json.dumps(list(self.missingness_slice_ids)),
            "conflict_slice_ids_json": json.dumps(list(self.conflict_slice_ids)),
            "uncertainty_slice_ids_json": json.dumps(list(self.uncertainty_slice_ids)),
            "expected_feature_count": len(self.expected_feature_ids),
            "observed_feature_count": len(self.observed_feature_ids),
            "missing_feature_count": len(self.missing_feature_ids),
            "observed_source_count": len(self.observed_source_names),
            "observed_slice_count": len(self.observed_slice_ids),
            "missingness_slice_count": len(self.missingness_slice_ids),
            "conflict_slice_count": len(self.conflict_slice_ids),
            "uncertainty_slice_count": len(self.uncertainty_slice_ids),
            "missingness_kinds_json": json.dumps(list(self.missingness_kinds)),
            "uncertainty_reasons_json": json.dumps(list(self.uncertainty_reasons)),
            "conflict_reasons_json": json.dumps(list(self.conflict_reasons)),
            "resolved_provenance_bundle_ids_json": json.dumps(
                list(self.resolved_provenance_bundle_ids)
            ),
        }


@dataclass(frozen=True)
class MechanisticAxisEvidenceLink:
    axis_id: str
    alignment_id: str
    alignment_label: str
    axis_relation: str
    tensor_row: AtlasTensorRow
    resolved_provenance_bundle_ids: tuple[str, ...]
    resolved_source_row_indices: tuple[str, ...]

    def to_row(self) -> dict[str, object]:
        return {
            "axis_id": self.axis_id,
            "alignment_id": self.alignment_id,
            "alignment_label": self.alignment_label,
            "axis_relation": self.axis_relation,
            "tensor_slice_id": self.tensor_row.slice_id,
            "tensor_channel": self.tensor_row.channel,
            "feature_id": self.tensor_row.feature_id,
            "feature_group": self.tensor_row.feature_group,
            "source_name": self.tensor_row.source_name,
            "dataset_name": self.tensor_row.dataset_name,
            "context_member_ids_json": json.dumps(list(self.tensor_row.context_member_ids)),
            "provenance_bundle_id": self.tensor_row.provenance_bundle_id,
            "source_row_index": self.tensor_row.source_row_index,
            "source_entity_id": self.tensor_row.source_entity_id,
            "source_entity_label": self.tensor_row.source_entity_label,
            "resolved_provenance_bundle_ids_json": json.dumps(
                list(self.resolved_provenance_bundle_ids)
            ),
            "resolved_source_row_indices_json": json.dumps(
                list(self.resolved_source_row_indices)
            ),
            "numeric_value": self.tensor_row.numeric_value,
            "text_value": self.tensor_row.text_value,
            "json_value": self.tensor_row.json_value,
        }


def load_atlas_tensor_bundle(tensor_manifest_file: Path) -> AtlasTensorBundle:
    resolved_manifest_file = tensor_manifest_file.resolve()
    payload = read_json(resolved_manifest_file)
    if not isinstance(payload, dict):
        raise ValueError(f"Atlas tensor manifest must be a JSON object: {resolved_manifest_file}")

    contract_version = str(payload.get("contract_version") or "")
    if contract_version != ATLAS_TENSOR_CONTRACT_VERSION:
        raise ValueError(
            "Atlas mechanistic axes require an atlas evidence tensor manifest "
            f"({ATLAS_TENSOR_CONTRACT_VERSION}), got {contract_version!r}."
        )

    emitted_artifacts = payload.get("emitted_artifacts")
    if not isinstance(emitted_artifacts, dict):
        raise ValueError("Atlas tensor manifest is missing an emitted_artifacts mapping.")

    evidence_tensor_file = emitted_artifacts.get("evidence_tensor_file")
    if not isinstance(evidence_tensor_file, str):
        raise ValueError("Atlas tensor manifest is missing emitted_artifacts.evidence_tensor_file.")

    resolved_evidence_tensor_file = resolve_manifest_path(
        resolved_manifest_file,
        evidence_tensor_file,
    )
    tensor_rows = tuple(
        AtlasTensorRow.from_csv_row(row)
        for row in read_csv_rows(resolved_evidence_tensor_file)
    )
    return AtlasTensorBundle(
        manifest_file=resolved_manifest_file,
        evidence_tensor_file=resolved_evidence_tensor_file,
        manifest=payload,
        tensor_rows=tensor_rows,
    )


def resolved_provenance_bundle_ids(row: AtlasTensorRow) -> tuple[str, ...]:
    bundle_ids: list[str] = []
    if row.provenance_bundle_id:
        bundle_ids.append(row.provenance_bundle_id)

    payload = _parse_json_payload(row.json_value)
    if isinstance(payload, dict):
        row_refs = payload.get("row_refs")
        if isinstance(row_refs, list):
            for ref in row_refs:
                if not isinstance(ref, dict):
                    continue
                provenance_bundle_id = ref.get("provenance_bundle_id")
                if isinstance(provenance_bundle_id, str) and provenance_bundle_id.strip():
                    bundle_ids.append(provenance_bundle_id.strip())

    return _sorted_unique(bundle_ids)


def resolved_source_row_indices(row: AtlasTensorRow) -> tuple[str, ...]:
    row_indices: list[str] = []
    if row.source_row_index:
        row_indices.append(row.source_row_index)

    payload = _parse_json_payload(row.json_value)
    if isinstance(payload, dict):
        row_refs = payload.get("row_refs")
        if isinstance(row_refs, list):
            for ref in row_refs:
                if not isinstance(ref, dict):
                    continue
                row_index = ref.get("row_index")
                if row_index in (None, ""):
                    continue
                row_indices.append(str(row_index))

    return tuple(sorted(set(row_indices), key=_row_index_sort_key))


def build_mechanistic_axis_profiles(
    tensor_bundle: AtlasTensorBundle,
) -> tuple[list[MechanisticAxisProfile], list[MechanisticAxisEvidenceLink]]:
    axis_specs = _mechanistic_axis_specs()
    available_feature_ids = {row.feature_id for row in tensor_bundle.tensor_rows}
    for axis_spec in axis_specs:
        if any(axis_spec.matches_feature(feature_id) for feature_id in available_feature_ids):
            continue
        raise ValueError(
            f"Mechanistic axis {axis_spec.axis_id!r} does not match any tensor features."
        )

    rows_by_alignment = _group_rows_by_alignment(tensor_bundle.tensor_rows)
    profiles: list[MechanisticAxisProfile] = []
    evidence_links: list[MechanisticAxisEvidenceLink] = []

    ordered_alignments = sorted(
        rows_by_alignment.items(),
        key=lambda item: (
            item[1][0].alignment_label,
            item[0],
        ),
    )

    for _, alignment_rows in ordered_alignments:
        alignment_id = alignment_rows[0].alignment_id
        alignment_label = alignment_rows[0].alignment_label
        alignment_status = alignment_rows[0].alignment_status
        structural_rows = [
            row
            for row in alignment_rows
            if row.feature_id == ATLAS_STRUCTURAL_CONFLICT_FEATURE_ID
        ]
        structural_conflict_rows = [row for row in structural_rows if row.channel == "conflict"]
        structural_uncertainty_rows = [
            row for row in structural_rows if row.channel == "uncertainty"
        ]

        for axis_spec in axis_specs:
            matched_rows = [
                row for row in alignment_rows if axis_spec.matches_feature(row.feature_id)
            ]
            observed_rows = [row for row in matched_rows if row.channel == "observed"]
            missingness_rows = [row for row in matched_rows if row.channel == "missingness"]
            uncertainty_rows = [row for row in matched_rows if row.channel == "uncertainty"]
            if structural_conflict_rows:
                uncertainty_rows = [*uncertainty_rows, *structural_uncertainty_rows]

            expected_feature_ids = _sorted_unique(
                row.feature_id
                for row in matched_rows
                if row.channel != "uncertainty"
            )
            observed_feature_ids = _sorted_unique(row.feature_id for row in observed_rows)
            missing_feature_ids = _sorted_unique(row.feature_id for row in missingness_rows)
            observed_source_names = _sorted_unique(row.source_name for row in observed_rows)
            source_coverage_state = _source_coverage_state(len(observed_source_names))
            support_state = _support_state(
                expected_feature_count=len(expected_feature_ids),
                observed_feature_count=len(observed_feature_ids),
            )
            missingness_state = _missingness_state(missingness_rows)
            conflict_state = _conflict_state(structural_conflict_rows)
            uncertainty_max_level = _max_uncertainty_level(uncertainty_rows)

            observed_slice_ids = tuple(sorted(row.slice_id for row in observed_rows))
            missingness_slice_ids = tuple(sorted(row.slice_id for row in missingness_rows))
            conflict_slice_ids = tuple(sorted(row.slice_id for row in structural_conflict_rows))
            uncertainty_slice_ids = tuple(sorted(row.slice_id for row in uncertainty_rows))

            missingness_kinds = _sorted_unique(row.text_value for row in missingness_rows)
            uncertainty_reasons = _sorted_unique(
                _reason_code(row) for row in uncertainty_rows if _reason_code(row)
            )
            conflict_reasons = _sorted_unique(
                _reason_code(row) or row.text_value for row in structural_conflict_rows
            )

            relevant_rows = _dedupe_rows(
                [
                    *matched_rows,
                    *structural_conflict_rows,
                    *structural_uncertainty_rows,
                ]
            )
            provenance_bundle_ids = _sorted_unique(
                bundle_id
                for row in relevant_rows
                for bundle_id in resolved_provenance_bundle_ids(row)
            )

            profiles.append(
                MechanisticAxisProfile(
                    axis_id=axis_spec.axis_id,
                    axis_label=axis_spec.axis_label,
                    axis_description=axis_spec.axis_description,
                    alignment_id=alignment_id,
                    alignment_label=alignment_label,
                    alignment_status=alignment_status,
                    expected_feature_ids=expected_feature_ids,
                    observed_feature_ids=observed_feature_ids,
                    missing_feature_ids=missing_feature_ids,
                    observed_source_names=observed_source_names,
                    source_coverage_state=source_coverage_state,
                    support_state=support_state,
                    missingness_state=missingness_state,
                    conflict_state=conflict_state,
                    uncertainty_max_level=uncertainty_max_level,
                    observed_slice_ids=observed_slice_ids,
                    missingness_slice_ids=missingness_slice_ids,
                    conflict_slice_ids=conflict_slice_ids,
                    uncertainty_slice_ids=uncertainty_slice_ids,
                    missingness_kinds=missingness_kinds,
                    uncertainty_reasons=uncertainty_reasons,
                    conflict_reasons=conflict_reasons,
                    resolved_provenance_bundle_ids=provenance_bundle_ids,
                )
            )

            for row in relevant_rows:
                evidence_links.append(
                    MechanisticAxisEvidenceLink(
                        axis_id=axis_spec.axis_id,
                        alignment_id=alignment_id,
                        alignment_label=alignment_label,
                        axis_relation=(
                            "alignment_structural"
                            if row.feature_id == ATLAS_STRUCTURAL_CONFLICT_FEATURE_ID
                            else "axis_feature"
                        ),
                        tensor_row=row,
                        resolved_provenance_bundle_ids=resolved_provenance_bundle_ids(row),
                        resolved_source_row_indices=resolved_source_row_indices(row),
                    )
                )

    profiles.sort(key=lambda profile: (profile.alignment_label, profile.axis_id))
    evidence_links.sort(
        key=lambda link: (
            link.alignment_label,
            link.axis_id,
            _CHANNEL_SORT_ORDER.get(link.tensor_row.channel, 99),
            link.tensor_row.feature_id,
            _row_index_sort_key(link.tensor_row.source_row_index or "0"),
            link.tensor_row.slice_id,
        )
    )
    return profiles, evidence_links


def materialize_mechanistic_axes(
    tensor_manifest_file: Path,
    output_dir: Path | None = None,
) -> dict[str, object]:
    tensor_bundle = load_atlas_tensor_bundle(tensor_manifest_file)
    resolved_output_dir = (
        output_dir or DEFAULT_ATLAS_MECHANISTIC_AXES_OUTPUT_DIR
    ).resolve()

    profiles, evidence_links = build_mechanistic_axis_profiles(tensor_bundle)
    axes_file = resolved_output_dir / "mechanistic_axes.csv"
    evidence_links_file = resolved_output_dir / "mechanistic_axis_evidence_links.csv"
    manifest_file = resolved_output_dir / "mechanistic_axes_manifest.json"

    write_csv(
        axes_file,
        [profile.to_row() for profile in profiles],
        [
            "axis_id",
            "axis_label",
            "axis_description",
            "alignment_id",
            "alignment_label",
            "alignment_status",
            "expected_feature_ids_json",
            "observed_feature_ids_json",
            "missing_feature_ids_json",
            "observed_source_names_json",
            "source_coverage_state",
            "support_state",
            "missingness_state",
            "conflict_state",
            "uncertainty_max_level",
            "observed_slice_ids_json",
            "missingness_slice_ids_json",
            "conflict_slice_ids_json",
            "uncertainty_slice_ids_json",
            "expected_feature_count",
            "observed_feature_count",
            "missing_feature_count",
            "observed_source_count",
            "observed_slice_count",
            "missingness_slice_count",
            "conflict_slice_count",
            "uncertainty_slice_count",
            "missingness_kinds_json",
            "uncertainty_reasons_json",
            "conflict_reasons_json",
            "resolved_provenance_bundle_ids_json",
        ],
    )
    write_csv(
        evidence_links_file,
        [link.to_row() for link in evidence_links],
        [
            "axis_id",
            "alignment_id",
            "alignment_label",
            "axis_relation",
            "tensor_slice_id",
            "tensor_channel",
            "feature_id",
            "feature_group",
            "source_name",
            "dataset_name",
            "context_member_ids_json",
            "provenance_bundle_id",
            "source_row_index",
            "source_entity_id",
            "source_entity_label",
            "resolved_provenance_bundle_ids_json",
            "resolved_source_row_indices_json",
            "numeric_value",
            "text_value",
            "json_value",
        ],
    )
    write_json(
        manifest_file,
        {
            "contract_version": ATLAS_MECHANISTIC_AXES_CONTRACT_VERSION,
            "tensor_manifest_file": str(tensor_bundle.manifest_file),
            "evidence_tensor_file": str(tensor_bundle.evidence_tensor_file),
            "output_dir": str(resolved_output_dir),
            "axis_definition_count": len(_mechanistic_axis_specs()),
            "axis_profile_count": len(profiles),
            "evidence_link_count": len(evidence_links),
            "emitted_artifacts": {
                "mechanistic_axes_file": str(axes_file),
                "mechanistic_axis_evidence_links_file": str(evidence_links_file),
            },
        },
    )

    return {
        "contract_version": ATLAS_MECHANISTIC_AXES_CONTRACT_VERSION,
        "tensor_manifest_file": str(tensor_bundle.manifest_file),
        "evidence_tensor_file": str(tensor_bundle.evidence_tensor_file),
        "output_dir": str(resolved_output_dir),
        "mechanistic_axes_file": str(axes_file),
        "mechanistic_axis_evidence_links_file": str(evidence_links_file),
        "manifest_file": str(manifest_file),
        "axis_definition_count": len(_mechanistic_axis_specs()),
        "axis_profile_count": len(profiles),
        "evidence_link_count": len(evidence_links),
    }


def _mechanistic_axis_specs() -> tuple[MechanisticAxisSpec, ...]:
    return (
        MechanisticAxisSpec(
            axis_id="mechanistic_axis:disease-association",
            axis_label="Disease association",
            axis_description=(
                "Cross-source disease-association surface spanning Open Targets "
                "baseline and genetic-association slices plus PGC common-variant support."
            ),
            feature_ids=(
                "opentargets.generic_platform_baseline",
                "opentargets.datatype.genetic-association",
                "pgc.common_variant_support",
            ),
        ),
        MechanisticAxisSpec(
            axis_id="mechanistic_axis:clinical-translation",
            axis_label="Clinical translation",
            axis_description=(
                "Clinical-support surface grounded in the Open Targets clinical datatype slice."
            ),
            feature_ids=("opentargets.datatype.clinical",),
        ),
        MechanisticAxisSpec(
            axis_id="mechanistic_axis:variant-to-gene",
            axis_label="Variant-to-gene prioritization",
            axis_description=(
                "Variant-to-gene prioritization surface from PGC prioritization "
                "flags, criteria, and index-SNP summaries."
            ),
            feature_ids=(
                "pgc.prioritised",
                "pgc.priority_index_snp_count",
                "pgc.priority_index_snps",
            ),
            feature_prefixes=("pgc.criterion.",),
        ),
    )


def _parse_context_member_ids(raw_json: str) -> tuple[str, ...]:
    payload = _parse_json_payload(raw_json)
    if not isinstance(payload, list):
        return ()
    context_member_ids: list[str] = []
    for item in payload:
        if isinstance(item, str):
            context_member_ids.append(item)
    return tuple(context_member_ids)


def _parse_json_payload(raw_json: str) -> object | None:
    if not raw_json.strip():
        return None
    return json.loads(raw_json)


def _group_rows_by_alignment(
    tensor_rows: tuple[AtlasTensorRow, ...],
) -> dict[str, tuple[AtlasTensorRow, ...]]:
    grouped_rows: dict[str, list[AtlasTensorRow]] = {}
    for row in tensor_rows:
        grouped_rows.setdefault(row.alignment_id, []).append(row)

    return {
        alignment_id: tuple(sorted(rows, key=_tensor_row_sort_key))
        for alignment_id, rows in grouped_rows.items()
    }


def _tensor_row_sort_key(row: AtlasTensorRow) -> tuple[object, ...]:
    return (
        row.alignment_label,
        _CHANNEL_SORT_ORDER.get(row.channel, 99),
        row.feature_id,
        row.source_name,
        _row_index_sort_key(row.source_row_index or "0"),
        row.slice_id,
    )


def _sorted_unique(values: object) -> tuple[str, ...]:
    deduped = {
        str(value).strip()
        for value in values
        if str(value).strip()
    }
    return tuple(sorted(deduped))


def _source_coverage_state(observed_source_count: int) -> str:
    if observed_source_count == 0:
        return "none"
    if observed_source_count == 1:
        return "single_source"
    return "cross_source"


def _support_state(
    *,
    expected_feature_count: int,
    observed_feature_count: int,
) -> str:
    if expected_feature_count == 0:
        return "unmapped"
    if observed_feature_count == 0:
        return "unobserved"
    if observed_feature_count == expected_feature_count:
        return "observed"
    return "partial_observed"


def _missingness_state(rows: list[AtlasTensorRow]) -> str:
    missingness_kinds = {row.text_value for row in rows if row.text_value}
    if not missingness_kinds:
        return "none"
    if missingness_kinds == {"source_absent"}:
        return "source_absent"
    if missingness_kinds == {"source_field_blank"}:
        return "field_blank"
    return "mixed"


def _conflict_state(rows: list[AtlasTensorRow]) -> str:
    if not rows:
        return "none"
    return "alignment_id_conflict"


def _max_uncertainty_level(rows: list[AtlasTensorRow]) -> str:
    max_level = "none"
    for row in rows:
        level = row.text_value or "none"
        if _UNCERTAINTY_LEVEL_ORDER.get(level, -1) > _UNCERTAINTY_LEVEL_ORDER[max_level]:
            max_level = level
    return max_level


def _reason_code(row: AtlasTensorRow) -> str:
    payload = _parse_json_payload(row.json_value)
    if not isinstance(payload, dict):
        return ""
    reason = payload.get("reason")
    if isinstance(reason, str):
        return reason
    return ""


def _dedupe_rows(rows: list[AtlasTensorRow]) -> list[AtlasTensorRow]:
    deduped: dict[str, AtlasTensorRow] = {}
    for row in rows:
        deduped.setdefault(row.slice_id, row)
    return sorted(deduped.values(), key=_tensor_row_sort_key)


def _row_index_sort_key(value: str) -> tuple[int, int | str]:
    if value.isdigit():
        return (0, int(value))
    return (1, value)
