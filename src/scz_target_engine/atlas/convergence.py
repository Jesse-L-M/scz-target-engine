from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from scz_target_engine.atlas.contracts import ATLAS_CONVERGENCE_CONTRACT_VERSION
from scz_target_engine.atlas.mechanistic_axes import (
    AtlasTensorRow,
    MechanisticAxisProfile,
    build_mechanistic_axis_profiles,
    load_atlas_tensor_bundle,
    resolved_provenance_bundle_ids,
    resolved_source_row_indices,
)
from scz_target_engine.io import write_csv, write_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_CONVERGENCE_OUTPUT_DIR = REPO_ROOT / "data" / "curated" / "atlas" / "convergence"

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
class ConvergenceHub:
    hub_id: str
    alignment_id: str
    alignment_label: str
    alignment_status: str
    hub_kind: str
    source_coverage_state: str
    axis_coverage_state: str
    missingness_state: str
    conflict_state: str
    uncertainty_max_level: str
    axis_ids: tuple[str, ...]
    supported_axis_ids: tuple[str, ...]
    partial_axis_ids: tuple[str, ...]
    unobserved_axis_ids: tuple[str, ...]
    conflicted_axis_ids: tuple[str, ...]
    observed_source_names: tuple[str, ...]
    missing_source_names: tuple[str, ...]
    observed_feature_groups: tuple[str, ...]
    observed_slice_ids: tuple[str, ...]
    missingness_slice_ids: tuple[str, ...]
    conflict_slice_ids: tuple[str, ...]
    uncertainty_slice_ids: tuple[str, ...]
    resolved_provenance_bundle_ids: tuple[str, ...]

    def to_row(self) -> dict[str, object]:
        observed_axis_count = len(self.supported_axis_ids)
        partial_axis_count = len(self.partial_axis_ids)
        return {
            "hub_id": self.hub_id,
            "alignment_id": self.alignment_id,
            "alignment_label": self.alignment_label,
            "alignment_status": self.alignment_status,
            "hub_kind": self.hub_kind,
            "source_coverage_state": self.source_coverage_state,
            "axis_coverage_state": self.axis_coverage_state,
            "missingness_state": self.missingness_state,
            "conflict_state": self.conflict_state,
            "uncertainty_max_level": self.uncertainty_max_level,
            "axis_ids_json": json.dumps(list(self.axis_ids)),
            "supported_axis_ids_json": json.dumps(list(self.supported_axis_ids)),
            "partial_axis_ids_json": json.dumps(list(self.partial_axis_ids)),
            "unobserved_axis_ids_json": json.dumps(list(self.unobserved_axis_ids)),
            "conflicted_axis_ids_json": json.dumps(list(self.conflicted_axis_ids)),
            "observed_source_names_json": json.dumps(list(self.observed_source_names)),
            "missing_source_names_json": json.dumps(list(self.missing_source_names)),
            "observed_feature_groups_json": json.dumps(list(self.observed_feature_groups)),
            "observed_axis_count": observed_axis_count,
            "partial_axis_count": partial_axis_count,
            "unobserved_axis_count": len(self.unobserved_axis_ids),
            "observed_source_count": len(self.observed_source_names),
            "missing_source_count": len(self.missing_source_names),
            "observed_slice_count": len(self.observed_slice_ids),
            "missingness_slice_count": len(self.missingness_slice_ids),
            "conflict_slice_count": len(self.conflict_slice_ids),
            "uncertainty_slice_count": len(self.uncertainty_slice_ids),
            "observed_slice_ids_json": json.dumps(list(self.observed_slice_ids)),
            "missingness_slice_ids_json": json.dumps(list(self.missingness_slice_ids)),
            "conflict_slice_ids_json": json.dumps(list(self.conflict_slice_ids)),
            "uncertainty_slice_ids_json": json.dumps(list(self.uncertainty_slice_ids)),
            "resolved_provenance_bundle_ids_json": json.dumps(
                list(self.resolved_provenance_bundle_ids)
            ),
        }


@dataclass(frozen=True)
class HubAxisMembership:
    hub_id: str
    alignment_id: str
    axis_profile: MechanisticAxisProfile

    def to_row(self) -> dict[str, object]:
        return {
            "hub_id": self.hub_id,
            "alignment_id": self.alignment_id,
            "alignment_label": self.axis_profile.alignment_label,
            "axis_id": self.axis_profile.axis_id,
            "axis_label": self.axis_profile.axis_label,
            "support_state": self.axis_profile.support_state,
            "source_coverage_state": self.axis_profile.source_coverage_state,
            "missingness_state": self.axis_profile.missingness_state,
            "conflict_state": self.axis_profile.conflict_state,
            "uncertainty_max_level": self.axis_profile.uncertainty_max_level,
            "observed_slice_ids_json": json.dumps(list(self.axis_profile.observed_slice_ids)),
            "missingness_slice_ids_json": json.dumps(
                list(self.axis_profile.missingness_slice_ids)
            ),
            "conflict_slice_ids_json": json.dumps(list(self.axis_profile.conflict_slice_ids)),
            "uncertainty_slice_ids_json": json.dumps(
                list(self.axis_profile.uncertainty_slice_ids)
            ),
        }


@dataclass(frozen=True)
class ConvergenceHubEvidenceLink:
    hub_id: str
    alignment_id: str
    alignment_label: str
    linked_axis_ids: tuple[str, ...]
    tensor_row: AtlasTensorRow
    resolved_provenance_bundle_ids: tuple[str, ...]
    resolved_source_row_indices: tuple[str, ...]

    def to_row(self) -> dict[str, object]:
        return {
            "hub_id": self.hub_id,
            "alignment_id": self.alignment_id,
            "alignment_label": self.alignment_label,
            "linked_axis_ids_json": json.dumps(list(self.linked_axis_ids)),
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


def materialize_convergence_hubs(
    tensor_manifest_file: Path,
    output_dir: Path | None = None,
) -> dict[str, object]:
    tensor_bundle = load_atlas_tensor_bundle(tensor_manifest_file)
    axis_profiles, _ = build_mechanistic_axis_profiles(tensor_bundle)
    resolved_output_dir = (output_dir or DEFAULT_ATLAS_CONVERGENCE_OUTPUT_DIR).resolve()

    axis_profiles_by_alignment: dict[str, list[MechanisticAxisProfile]] = {}
    for axis_profile in axis_profiles:
        axis_profiles_by_alignment.setdefault(axis_profile.alignment_id, []).append(axis_profile)

    rows_by_alignment: dict[str, list[AtlasTensorRow]] = {}
    for tensor_row in tensor_bundle.tensor_rows:
        rows_by_alignment.setdefault(tensor_row.alignment_id, []).append(tensor_row)

    hubs: list[ConvergenceHub] = []
    hub_axis_memberships: list[HubAxisMembership] = []
    evidence_links: list[ConvergenceHubEvidenceLink] = []

    ordered_alignments = sorted(
        rows_by_alignment.items(),
        key=lambda item: (
            item[1][0].alignment_label,
            item[0],
        ),
    )

    for alignment_id, alignment_rows in ordered_alignments:
        sorted_rows = sorted(alignment_rows, key=_tensor_row_sort_key)
        alignment_label = sorted_rows[0].alignment_label
        alignment_status = sorted_rows[0].alignment_status
        hub_id = alignment_id.replace("alignment:", "hub:", 1)
        profiles = sorted(
            axis_profiles_by_alignment.get(alignment_id, []),
            key=lambda profile: profile.axis_id,
        )

        observed_rows = [row for row in sorted_rows if row.channel == "observed"]
        missingness_rows = [row for row in sorted_rows if row.channel == "missingness"]
        conflict_rows = [row for row in sorted_rows if row.channel == "conflict"]
        uncertainty_rows = [row for row in sorted_rows if row.channel == "uncertainty"]

        observed_source_names = _sorted_unique(row.source_name for row in observed_rows)
        missing_source_names = _sorted_unique(
            row.source_name
            for row in missingness_rows
            if row.text_value == "source_absent"
        )
        observed_feature_groups = _sorted_unique(
            row.feature_group for row in observed_rows if row.feature_group
        )

        supported_axis_ids = tuple(
            profile.axis_id
            for profile in profiles
            if profile.support_state == "observed"
        )
        partial_axis_ids = tuple(
            profile.axis_id
            for profile in profiles
            if profile.support_state == "partial_observed"
        )
        unobserved_axis_ids = tuple(
            profile.axis_id
            for profile in profiles
            if profile.support_state == "unobserved"
        )
        conflicted_axis_ids = tuple(
            profile.axis_id
            for profile in profiles
            if profile.conflict_state != "none"
        )

        slice_id_to_axis_ids: dict[str, set[str]] = {}
        for profile in profiles:
            hub_axis_memberships.append(
                HubAxisMembership(
                    hub_id=hub_id,
                    alignment_id=alignment_id,
                    axis_profile=profile,
                )
            )
            for slice_id in (
                *profile.observed_slice_ids,
                *profile.missingness_slice_ids,
                *profile.conflict_slice_ids,
                *profile.uncertainty_slice_ids,
            ):
                slice_id_to_axis_ids.setdefault(slice_id, set()).add(profile.axis_id)

        hubs.append(
            ConvergenceHub(
                hub_id=hub_id,
                alignment_id=alignment_id,
                alignment_label=alignment_label,
                alignment_status=alignment_status,
                hub_kind="gene_alignment",
                source_coverage_state=_source_coverage_state(len(observed_source_names)),
                axis_coverage_state=_axis_coverage_state(
                    observed_axis_count=len(supported_axis_ids),
                    partial_axis_count=len(partial_axis_ids),
                ),
                missingness_state=_missingness_state(missingness_rows),
                conflict_state=_conflict_state(conflict_rows),
                uncertainty_max_level=_max_uncertainty_level(uncertainty_rows),
                axis_ids=tuple(profile.axis_id for profile in profiles),
                supported_axis_ids=supported_axis_ids,
                partial_axis_ids=partial_axis_ids,
                unobserved_axis_ids=unobserved_axis_ids,
                conflicted_axis_ids=conflicted_axis_ids,
                observed_source_names=observed_source_names,
                missing_source_names=missing_source_names,
                observed_feature_groups=observed_feature_groups,
                observed_slice_ids=tuple(row.slice_id for row in observed_rows),
                missingness_slice_ids=tuple(row.slice_id for row in missingness_rows),
                conflict_slice_ids=tuple(row.slice_id for row in conflict_rows),
                uncertainty_slice_ids=tuple(row.slice_id for row in uncertainty_rows),
                resolved_provenance_bundle_ids=_sorted_unique(
                    bundle_id
                    for row in sorted_rows
                    for bundle_id in resolved_provenance_bundle_ids(row)
                ),
            )
        )

        for row in sorted_rows:
            evidence_links.append(
                ConvergenceHubEvidenceLink(
                    hub_id=hub_id,
                    alignment_id=alignment_id,
                    alignment_label=alignment_label,
                    linked_axis_ids=tuple(sorted(slice_id_to_axis_ids.get(row.slice_id, set()))),
                    tensor_row=row,
                    resolved_provenance_bundle_ids=resolved_provenance_bundle_ids(row),
                    resolved_source_row_indices=resolved_source_row_indices(row),
                )
            )

    hubs.sort(key=lambda hub: (hub.alignment_label, hub.hub_id))
    hub_axis_memberships.sort(
        key=lambda member: (
            member.axis_profile.alignment_label,
            member.axis_profile.axis_id,
        )
    )
    evidence_links.sort(
        key=lambda link: (
            link.alignment_label,
            _CHANNEL_SORT_ORDER.get(link.tensor_row.channel, 99),
            link.tensor_row.feature_id,
            _row_index_sort_key(link.tensor_row.source_row_index or "0"),
            link.tensor_row.slice_id,
        )
    )

    hubs_file = resolved_output_dir / "convergence_hubs.csv"
    hub_axis_members_file = resolved_output_dir / "hub_axis_members.csv"
    evidence_links_file = resolved_output_dir / "hub_evidence_links.csv"
    manifest_file = resolved_output_dir / "convergence_manifest.json"

    write_csv(
        hubs_file,
        [hub.to_row() for hub in hubs],
        [
            "hub_id",
            "alignment_id",
            "alignment_label",
            "alignment_status",
            "hub_kind",
            "source_coverage_state",
            "axis_coverage_state",
            "missingness_state",
            "conflict_state",
            "uncertainty_max_level",
            "axis_ids_json",
            "supported_axis_ids_json",
            "partial_axis_ids_json",
            "unobserved_axis_ids_json",
            "conflicted_axis_ids_json",
            "observed_source_names_json",
            "missing_source_names_json",
            "observed_feature_groups_json",
            "observed_axis_count",
            "partial_axis_count",
            "unobserved_axis_count",
            "observed_source_count",
            "missing_source_count",
            "observed_slice_count",
            "missingness_slice_count",
            "conflict_slice_count",
            "uncertainty_slice_count",
            "observed_slice_ids_json",
            "missingness_slice_ids_json",
            "conflict_slice_ids_json",
            "uncertainty_slice_ids_json",
            "resolved_provenance_bundle_ids_json",
        ],
    )
    write_csv(
        hub_axis_members_file,
        [member.to_row() for member in hub_axis_memberships],
        [
            "hub_id",
            "alignment_id",
            "alignment_label",
            "axis_id",
            "axis_label",
            "support_state",
            "source_coverage_state",
            "missingness_state",
            "conflict_state",
            "uncertainty_max_level",
            "observed_slice_ids_json",
            "missingness_slice_ids_json",
            "conflict_slice_ids_json",
            "uncertainty_slice_ids_json",
        ],
    )
    write_csv(
        evidence_links_file,
        [link.to_row() for link in evidence_links],
        [
            "hub_id",
            "alignment_id",
            "alignment_label",
            "linked_axis_ids_json",
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
            "contract_version": ATLAS_CONVERGENCE_CONTRACT_VERSION,
            "tensor_manifest_file": str(tensor_bundle.manifest_file),
            "evidence_tensor_file": str(tensor_bundle.evidence_tensor_file),
            "output_dir": str(resolved_output_dir),
            "hub_count": len(hubs),
            "hub_axis_member_count": len(hub_axis_memberships),
            "evidence_link_count": len(evidence_links),
            "emitted_artifacts": {
                "convergence_hubs_file": str(hubs_file),
                "hub_axis_members_file": str(hub_axis_members_file),
                "hub_evidence_links_file": str(evidence_links_file),
            },
        },
    )

    return {
        "contract_version": ATLAS_CONVERGENCE_CONTRACT_VERSION,
        "tensor_manifest_file": str(tensor_bundle.manifest_file),
        "evidence_tensor_file": str(tensor_bundle.evidence_tensor_file),
        "output_dir": str(resolved_output_dir),
        "convergence_hubs_file": str(hubs_file),
        "hub_axis_members_file": str(hub_axis_members_file),
        "hub_evidence_links_file": str(evidence_links_file),
        "manifest_file": str(manifest_file),
        "hub_count": len(hubs),
        "hub_axis_member_count": len(hub_axis_memberships),
        "evidence_link_count": len(evidence_links),
    }


def _source_coverage_state(observed_source_count: int) -> str:
    if observed_source_count == 0:
        return "none"
    if observed_source_count == 1:
        return "single_source"
    return "cross_source"


def _axis_coverage_state(*, observed_axis_count: int, partial_axis_count: int) -> str:
    covered_axis_count = observed_axis_count + partial_axis_count
    if covered_axis_count == 0:
        return "none"
    if covered_axis_count == 1:
        return "single_axis"
    return "multi_axis"


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


def _sorted_unique(values: object) -> tuple[str, ...]:
    deduped = {
        str(value).strip()
        for value in values
        if str(value).strip()
    }
    return tuple(sorted(deduped))


def _tensor_row_sort_key(row: AtlasTensorRow) -> tuple[object, ...]:
    return (
        row.alignment_label,
        _CHANNEL_SORT_ORDER.get(row.channel, 99),
        row.feature_id,
        row.source_name,
        _row_index_sort_key(row.source_row_index or "0"),
        row.slice_id,
    )


def _row_index_sort_key(value: str) -> tuple[int, int | str]:
    if value.isdigit():
        return (0, int(value))
    return (1, value)
