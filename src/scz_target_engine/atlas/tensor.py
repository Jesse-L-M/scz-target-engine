from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path

from scz_target_engine.atlas.contracts import ATLAS_TENSOR_CONTRACT_VERSION
from scz_target_engine.atlas.staging import slugify
from scz_target_engine.atlas.substrate import (
    AtlasFeatureSpec,
    AtlasSourceBundle,
    build_atlas_feature_specs,
    load_atlas_source_bundles,
    normalize_alignment_label,
    taxonomy_member_id,
)
from scz_target_engine.atlas.taxonomy import materialize_atlas_taxonomy
from scz_target_engine.io import write_csv, write_json


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ATLAS_TENSOR_OUTPUT_DIR = REPO_ROOT / "data" / "curated" / "atlas" / "tensor"


@dataclass(frozen=True)
class AtlasSourceRowRef:
    provenance_bundle_id: str
    source_name: str
    dataset_name: str
    row_index: int
    row: dict[str, str]


@dataclass
class AtlasAlignment:
    alignment_id: str
    alignment_label: str
    normalized_label: str
    alignment_status: str
    row_refs_by_source: dict[str, list[AtlasSourceRowRef]]

    @property
    def row_refs(self) -> list[AtlasSourceRowRef]:
        flattened: list[AtlasSourceRowRef] = []
        for source_name in sorted(self.row_refs_by_source):
            flattened.extend(
                sorted(
                    self.row_refs_by_source[source_name],
                    key=lambda ref: ref.row_index,
                )
            )
        return flattened

    @property
    def source_names(self) -> list[str]:
        return sorted(self.row_refs_by_source)

    @property
    def source_count(self) -> int:
        return len(self.row_refs_by_source)

    @property
    def entity_ids(self) -> list[str]:
        entity_ids = {
            ref.row.get("entity_id", "").strip()
            for ref in self.row_refs
            if ref.row.get("entity_id", "").strip()
        }
        return sorted(entity_ids)


def _unique_alignment_id(base_label: str, seen: set[str]) -> str:
    base_id = f"alignment:{slugify(base_label)}"
    if base_id not in seen:
        seen.add(base_id)
        return base_id
    suffix = 2
    while True:
        candidate = f"{base_id}-{suffix}"
        if candidate not in seen:
            seen.add(candidate)
            return candidate
        suffix += 1


def _classify_alignment_status(row_refs: list[AtlasSourceRowRef]) -> str:
    entity_ids = {
        ref.row.get("entity_id", "").strip()
        for ref in row_refs
        if ref.row.get("entity_id", "").strip()
    }
    if len(entity_ids) > 1:
        return "id_conflict"
    if entity_ids:
        return "id_consistent"
    return "label_only"


def _build_alignments(bundles: list[AtlasSourceBundle]) -> list[AtlasAlignment]:
    grouped_refs: dict[str, list[AtlasSourceRowRef]] = {}
    for bundle in bundles:
        for row_index, row in enumerate(bundle.rows, start=1):
            raw_label = row.get("entity_label", "").strip() or row.get("entity_id", "").strip()
            if not raw_label:
                raw_label = f"{bundle.source_name}-row-{row_index}"
            normalized_label = normalize_alignment_label(raw_label) or raw_label
            grouped_refs.setdefault(normalized_label, []).append(
                AtlasSourceRowRef(
                    provenance_bundle_id=bundle.provenance_bundle_id,
                    source_name=bundle.source_name,
                    dataset_name=bundle.dataset_name,
                    row_index=row_index,
                    row=row,
                )
            )

    seen_alignment_ids: set[str] = set()
    alignments: list[AtlasAlignment] = []
    for normalized_label in sorted(grouped_refs):
        row_refs = grouped_refs[normalized_label]
        row_refs_by_source: dict[str, list[AtlasSourceRowRef]] = {}
        for row_ref in row_refs:
            row_refs_by_source.setdefault(row_ref.source_name, []).append(row_ref)
        alignment_label = next(
            (
                ref.row.get("entity_label", "").strip()
                or ref.row.get("entity_id", "").strip()
                for ref in row_refs
                if ref.row.get("entity_label", "").strip()
                or ref.row.get("entity_id", "").strip()
            ),
            normalized_label,
        )
        alignments.append(
            AtlasAlignment(
                alignment_id=_unique_alignment_id(alignment_label or normalized_label, seen_alignment_ids),
                alignment_label=alignment_label,
                normalized_label=normalized_label,
                alignment_status=_classify_alignment_status(row_refs),
                row_refs_by_source=row_refs_by_source,
            )
        )
    return alignments


def _normalize_feature_value(
    raw_value: str,
    value_type: str,
) -> tuple[object, str, str]:
    value = raw_value.strip()
    if value_type == "number":
        return float(value), "", ""
    if value_type == "integer":
        return int(value), "", ""
    if value_type == "boolean":
        bool_value = value.upper() in {"1", "TRUE", "YES"}
        return (1 if bool_value else 0), ("true" if bool_value else "false"), ""
    if value_type == "json":
        return "", "", json.dumps(json.loads(value), sort_keys=True)
    return "", value, ""


def _ordered_context_member_ids(
    spec: AtlasFeatureSpec,
    *,
    channel: str,
    alignment_status: str,
    extra_members: list[str] | None = None,
) -> list[str]:
    member_ids = [
        taxonomy_member_id("channel", channel),
        *spec.context_member_ids,
        taxonomy_member_id("alignment_status", alignment_status),
    ]
    if extra_members:
        member_ids.extend(extra_members)
    ordered: list[str] = []
    seen: set[str] = set()
    for member_id in member_ids:
        if member_id in seen:
            continue
        seen.add(member_id)
        ordered.append(member_id)
    return ordered


def _observed_uncertainty(
    alignment: AtlasAlignment,
) -> tuple[str, float, str]:
    if alignment.alignment_status == "id_conflict":
        return "high", 0.9, "cross_source_entity_id_conflict"
    if alignment.alignment_status == "label_only":
        return "high", 0.75, "label_only_alignment"
    if alignment.source_count == 1:
        return "medium", 0.5, "single_source_alignment"
    return "low", 0.2, "cross_source_entity_id_consistent"


def _missingness_uncertainty(missingness_kind: str) -> tuple[str, float, str]:
    if missingness_kind == "source_absent":
        return "high", 0.8, "source_absent_for_alignment"
    return "medium", 0.6, "source_field_blank"


def _row_payload(
    *,
    slice_id: str,
    alignment: AtlasAlignment,
    spec: AtlasFeatureSpec,
    channel: str,
    context_member_ids: list[str],
    value_type: str,
    numeric_value: object = "",
    text_value: str = "",
    json_value: str = "",
    provenance_bundle_id: str = "",
    source_row_index: object = "",
    source_entity_id: str = "",
    source_entity_label: str = "",
) -> dict[str, object]:
    return {
        "slice_id": slice_id,
        "alignment_id": alignment.alignment_id,
        "alignment_label": alignment.alignment_label,
        "alignment_status": alignment.alignment_status,
        "source_name": spec.source_name,
        "dataset_name": spec.dataset_name,
        "feature_id": spec.feature_id,
        "feature_group": spec.feature_group,
        "channel": channel,
        "context_member_ids_json": json.dumps(context_member_ids),
        "value_type": value_type,
        "numeric_value": numeric_value,
        "text_value": text_value,
        "json_value": json_value,
        "provenance_bundle_id": provenance_bundle_id,
        "source_row_index": source_row_index,
        "source_entity_id": source_entity_id,
        "source_entity_label": source_entity_label,
    }


def materialize_atlas_tensor(
    ingest_manifest_file: Path,
    output_dir: Path | None = None,
    taxonomy_dir: Path | None = None,
) -> dict[str, object]:
    resolved_manifest_file = ingest_manifest_file.resolve()
    resolved_output_dir = (output_dir or DEFAULT_ATLAS_TENSOR_OUTPUT_DIR).resolve()
    resolved_taxonomy_dir = (
        taxonomy_dir.resolve()
        if taxonomy_dir is not None
        else (resolved_output_dir / "taxonomy").resolve()
    )

    taxonomy_result = materialize_atlas_taxonomy(
        ingest_manifest_file=resolved_manifest_file,
        output_dir=resolved_taxonomy_dir,
    )
    bundles = load_atlas_source_bundles(resolved_manifest_file)
    feature_specs = build_atlas_feature_specs(bundles)
    alignments = _build_alignments(bundles)

    provenance_rows = []
    for bundle in bundles:
        raw_artifacts: list[object] = []
        raw_status = ""
        if bundle.raw_manifest is not None:
            raw_status = str(bundle.raw_manifest.get("status") or "")
            raw_artifacts_payload = bundle.raw_manifest.get("artifacts")
            if isinstance(raw_artifacts_payload, list):
                raw_artifacts = raw_artifacts_payload
        provenance_rows.append(
            {
                "provenance_bundle_id": bundle.provenance_bundle_id,
                "source_name": bundle.source_name,
                "dataset_name": bundle.dataset_name,
                "processed_output_file": str(bundle.processed_output_file),
                "processed_metadata_file": (
                    str(bundle.processed_metadata_file)
                    if bundle.processed_metadata_file is not None
                    else ""
                ),
                "raw_manifest_file": (
                    str(bundle.raw_manifest_file) if bundle.raw_manifest_file is not None else ""
                ),
                "raw_status": raw_status,
                "raw_artifact_count": len(raw_artifacts),
                "staged_artifacts_json": json.dumps(raw_artifacts, sort_keys=True),
                "source_contract_json": json.dumps(bundle.source_contract, sort_keys=True),
                "row_count": len(bundle.rows),
            }
        )

    alignment_rows = []
    for alignment in alignments:
        alignment_rows.append(
            {
                "alignment_id": alignment.alignment_id,
                "alignment_label": alignment.alignment_label,
                "normalized_label": alignment.normalized_label,
                "alignment_status": alignment.alignment_status,
                "source_count": alignment.source_count,
                "row_count": len(alignment.row_refs),
                "source_names_json": json.dumps(alignment.source_names),
                "entity_ids_json": json.dumps(alignment.entity_ids),
                "source_entity_ids_json": json.dumps(
                    {
                        source_name: sorted(
                            {
                                row_ref.row.get("entity_id", "").strip()
                                for row_ref in row_refs
                                if row_ref.row.get("entity_id", "").strip()
                            }
                        )
                        for source_name, row_refs in sorted(alignment.row_refs_by_source.items())
                    },
                    sort_keys=True,
                ),
                "source_row_refs_json": json.dumps(
                    [
                        {
                            "provenance_bundle_id": row_ref.provenance_bundle_id,
                            "source_name": row_ref.source_name,
                            "dataset_name": row_ref.dataset_name,
                            "row_index": row_ref.row_index,
                            "entity_id": row_ref.row.get("entity_id", "").strip() or None,
                            "entity_label": row_ref.row.get("entity_label", "").strip() or None,
                        }
                        for row_ref in alignment.row_refs
                    ],
                    sort_keys=True,
                ),
            }
        )

    feature_specs_by_source: dict[str, list[AtlasFeatureSpec]] = {}
    atlas_conflict_spec: AtlasFeatureSpec | None = None
    for spec in feature_specs:
        if spec.source_name == "atlas":
            atlas_conflict_spec = spec
            continue
        feature_specs_by_source.setdefault(spec.source_name, []).append(spec)

    if atlas_conflict_spec is None:
        raise RuntimeError("Atlas conflict feature spec was not generated.")

    bundle_by_source = {bundle.source_name: bundle for bundle in bundles}
    tensor_rows: list[dict[str, object]] = []
    channel_counts: Counter[str] = Counter()

    for alignment in alignments:
        for source_name in sorted(feature_specs_by_source):
            specs = feature_specs_by_source[source_name]
            bundle = bundle_by_source[source_name]
            row_refs = alignment.row_refs_by_source.get(source_name, [])
            if not row_refs:
                for spec in specs:
                    missingness_context = _ordered_context_member_ids(
                        spec,
                        channel="missingness",
                        alignment_status=alignment.alignment_status,
                        extra_members=[
                            taxonomy_member_id("missingness_kind", "source_absent"),
                        ],
                    )
                    missingness_slice_id = (
                        f"{alignment.alignment_id}|{spec.source_name}|{spec.feature_id}|missingness|absent"
                    )
                    tensor_rows.append(
                        _row_payload(
                            slice_id=missingness_slice_id,
                            alignment=alignment,
                            spec=spec,
                            channel="missingness",
                            context_member_ids=missingness_context,
                            value_type="string",
                            text_value="source_absent",
                            json_value=json.dumps(
                                {
                                    "reason": "source_absent",
                                    "expected_source": source_name,
                                },
                                sort_keys=True,
                            ),
                            provenance_bundle_id=bundle.provenance_bundle_id,
                        )
                    )
                    level, score, reason = _missingness_uncertainty("source_absent")
                    uncertainty_context = _ordered_context_member_ids(
                        spec,
                        channel="uncertainty",
                        alignment_status=alignment.alignment_status,
                        extra_members=[taxonomy_member_id("uncertainty_level", level)],
                    )
                    tensor_rows.append(
                        _row_payload(
                            slice_id=(
                                f"{alignment.alignment_id}|{spec.source_name}|"
                                f"{spec.feature_id}|uncertainty|absent"
                            ),
                            alignment=alignment,
                            spec=spec,
                            channel="uncertainty",
                            context_member_ids=uncertainty_context,
                            value_type="number",
                            numeric_value=score,
                            text_value=level,
                            json_value=json.dumps(
                                {
                                    "reason": reason,
                                    "related_channel": "missingness",
                                    "missingness_kind": "source_absent",
                                },
                                sort_keys=True,
                            ),
                            provenance_bundle_id=bundle.provenance_bundle_id,
                        )
                    )
                continue

            for row_ref in row_refs:
                for spec in specs:
                    raw_value = row_ref.row.get(spec.field_name, "")
                    if not raw_value.strip():
                        missingness_context = _ordered_context_member_ids(
                            spec,
                            channel="missingness",
                            alignment_status=alignment.alignment_status,
                            extra_members=[
                                taxonomy_member_id("missingness_kind", "source_field_blank"),
                            ],
                        )
                        tensor_rows.append(
                            _row_payload(
                                slice_id=(
                                    f"{alignment.alignment_id}|{spec.source_name}|"
                                    f"{spec.feature_id}|missingness|{row_ref.row_index}"
                                ),
                                alignment=alignment,
                                spec=spec,
                                channel="missingness",
                                context_member_ids=missingness_context,
                                value_type="string",
                                text_value="source_field_blank",
                                json_value=json.dumps(
                                    {
                                        "reason": "source_field_blank",
                                        "field_name": spec.field_name,
                                    },
                                    sort_keys=True,
                                ),
                                provenance_bundle_id=row_ref.provenance_bundle_id,
                                source_row_index=row_ref.row_index,
                                source_entity_id=row_ref.row.get("entity_id", "").strip(),
                                source_entity_label=row_ref.row.get("entity_label", "").strip(),
                            )
                        )
                        level, score, reason = _missingness_uncertainty("source_field_blank")
                        uncertainty_context = _ordered_context_member_ids(
                            spec,
                            channel="uncertainty",
                            alignment_status=alignment.alignment_status,
                            extra_members=[taxonomy_member_id("uncertainty_level", level)],
                        )
                        tensor_rows.append(
                            _row_payload(
                                slice_id=(
                                    f"{alignment.alignment_id}|{spec.source_name}|"
                                    f"{spec.feature_id}|uncertainty|blank-{row_ref.row_index}"
                                ),
                                alignment=alignment,
                                spec=spec,
                                channel="uncertainty",
                                context_member_ids=uncertainty_context,
                                value_type="number",
                                numeric_value=score,
                                text_value=level,
                                json_value=json.dumps(
                                    {
                                        "reason": reason,
                                        "related_channel": "missingness",
                                        "missingness_kind": "source_field_blank",
                                        "field_name": spec.field_name,
                                    },
                                    sort_keys=True,
                                ),
                                provenance_bundle_id=row_ref.provenance_bundle_id,
                                source_row_index=row_ref.row_index,
                                source_entity_id=row_ref.row.get("entity_id", "").strip(),
                                source_entity_label=row_ref.row.get("entity_label", "").strip(),
                            )
                        )
                        continue

                    numeric_value, text_value, json_value = _normalize_feature_value(
                        raw_value,
                        spec.value_type,
                    )
                    observed_context = _ordered_context_member_ids(
                        spec,
                        channel="observed",
                        alignment_status=alignment.alignment_status,
                    )
                    tensor_rows.append(
                        _row_payload(
                            slice_id=(
                                f"{alignment.alignment_id}|{spec.source_name}|"
                                f"{spec.feature_id}|observed|{row_ref.row_index}"
                            ),
                            alignment=alignment,
                            spec=spec,
                            channel="observed",
                            context_member_ids=observed_context,
                            value_type=spec.value_type,
                            numeric_value=numeric_value,
                            text_value=text_value,
                            json_value=json_value,
                            provenance_bundle_id=row_ref.provenance_bundle_id,
                            source_row_index=row_ref.row_index,
                            source_entity_id=row_ref.row.get("entity_id", "").strip(),
                            source_entity_label=row_ref.row.get("entity_label", "").strip(),
                        )
                    )
                    level, score, reason = _observed_uncertainty(alignment)
                    uncertainty_context = _ordered_context_member_ids(
                        spec,
                        channel="uncertainty",
                        alignment_status=alignment.alignment_status,
                        extra_members=[taxonomy_member_id("uncertainty_level", level)],
                    )
                    tensor_rows.append(
                        _row_payload(
                            slice_id=(
                                f"{alignment.alignment_id}|{spec.source_name}|"
                                f"{spec.feature_id}|uncertainty|{row_ref.row_index}"
                            ),
                            alignment=alignment,
                            spec=spec,
                            channel="uncertainty",
                            context_member_ids=uncertainty_context,
                            value_type="number",
                            numeric_value=score,
                            text_value=level,
                            json_value=json.dumps(
                                {
                                    "reason": reason,
                                    "related_channel": "observed",
                                },
                                sort_keys=True,
                            ),
                            provenance_bundle_id=row_ref.provenance_bundle_id,
                            source_row_index=row_ref.row_index,
                            source_entity_id=row_ref.row.get("entity_id", "").strip(),
                            source_entity_label=row_ref.row.get("entity_label", "").strip(),
                        )
                    )

        if alignment.alignment_status == "id_conflict":
            conflict_context = _ordered_context_member_ids(
                atlas_conflict_spec,
                channel="conflict",
                alignment_status=alignment.alignment_status,
                extra_members=[taxonomy_member_id("conflict_kind", "id_conflict")],
            )
            conflict_payload = {
                "reason": "id_conflict",
                "source_entity_ids": {
                    source_name: sorted(
                        {
                            row_ref.row.get("entity_id", "").strip()
                            for row_ref in row_refs
                            if row_ref.row.get("entity_id", "").strip()
                        }
                    )
                    for source_name, row_refs in sorted(alignment.row_refs_by_source.items())
                },
                "row_refs": [
                    {
                        "provenance_bundle_id": row_ref.provenance_bundle_id,
                        "source_name": row_ref.source_name,
                        "row_index": row_ref.row_index,
                    }
                    for row_ref in alignment.row_refs
                ],
            }
            tensor_rows.append(
                _row_payload(
                    slice_id=f"{alignment.alignment_id}|atlas|alignment_entity_id_conflict|conflict|0",
                    alignment=alignment,
                    spec=atlas_conflict_spec,
                    channel="conflict",
                    context_member_ids=conflict_context,
                    value_type="json",
                    text_value="id_conflict",
                    json_value=json.dumps(conflict_payload, sort_keys=True),
                )
            )
            uncertainty_context = _ordered_context_member_ids(
                atlas_conflict_spec,
                channel="uncertainty",
                alignment_status=alignment.alignment_status,
                extra_members=[taxonomy_member_id("uncertainty_level", "high")],
            )
            tensor_rows.append(
                _row_payload(
                    slice_id=(
                        f"{alignment.alignment_id}|atlas|alignment_entity_id_conflict|uncertainty|0"
                    ),
                    alignment=alignment,
                    spec=atlas_conflict_spec,
                    channel="uncertainty",
                    context_member_ids=uncertainty_context,
                    value_type="number",
                    numeric_value=0.9,
                    text_value="high",
                    json_value=json.dumps(
                        {
                            "reason": "cross_source_entity_id_conflict",
                            "related_channel": "conflict",
                        },
                        sort_keys=True,
                    ),
                )
            )

    tensor_rows.sort(
        key=lambda row: (
            str(row["alignment_id"]),
            str(row["source_name"]),
            str(row["feature_id"]),
            str(row["channel"]),
            str(row["source_row_index"]),
            str(row["text_value"]),
        )
    )
    for row in tensor_rows:
        channel_counts[str(row["channel"])] += 1

    provenance_file = resolved_output_dir / "provenance_bundles.csv"
    alignments_file = resolved_output_dir / "entity_alignments.csv"
    tensor_file = resolved_output_dir / "evidence_tensor.csv"
    manifest_file = resolved_output_dir / "tensor_manifest.json"

    write_csv(
        provenance_file,
        provenance_rows,
        [
            "provenance_bundle_id",
            "source_name",
            "dataset_name",
            "processed_output_file",
            "processed_metadata_file",
            "raw_manifest_file",
            "raw_status",
            "raw_artifact_count",
            "staged_artifacts_json",
            "source_contract_json",
            "row_count",
        ],
    )
    write_csv(
        alignments_file,
        alignment_rows,
        [
            "alignment_id",
            "alignment_label",
            "normalized_label",
            "alignment_status",
            "source_count",
            "row_count",
            "source_names_json",
            "entity_ids_json",
            "source_entity_ids_json",
            "source_row_refs_json",
        ],
    )
    write_csv(
        tensor_file,
        tensor_rows,
        [
            "slice_id",
            "alignment_id",
            "alignment_label",
            "alignment_status",
            "source_name",
            "dataset_name",
            "feature_id",
            "feature_group",
            "channel",
            "context_member_ids_json",
            "value_type",
            "numeric_value",
            "text_value",
            "json_value",
            "provenance_bundle_id",
            "source_row_index",
            "source_entity_id",
            "source_entity_label",
        ],
    )
    write_json(
        manifest_file,
        {
            "contract_version": ATLAS_TENSOR_CONTRACT_VERSION,
            "ingest_manifest_file": str(resolved_manifest_file),
            "output_dir": str(resolved_output_dir),
            "taxonomy_output_dir": str(resolved_taxonomy_dir),
            "alignment_count": len(alignment_rows),
            "provenance_bundle_count": len(provenance_rows),
            "tensor_row_count": len(tensor_rows),
            "channel_counts": dict(sorted(channel_counts.items())),
            "emitted_artifacts": {
                "provenance_bundles_file": str(provenance_file),
                "entity_alignments_file": str(alignments_file),
                "evidence_tensor_file": str(tensor_file),
                "taxonomy_manifest_file": str(taxonomy_result["manifest_file"]),
            },
        },
    )

    return {
        "contract_version": ATLAS_TENSOR_CONTRACT_VERSION,
        "ingest_manifest_file": str(resolved_manifest_file),
        "output_dir": str(resolved_output_dir),
        "taxonomy_output_dir": str(resolved_taxonomy_dir),
        "provenance_bundles_file": str(provenance_file),
        "entity_alignments_file": str(alignments_file),
        "evidence_tensor_file": str(tensor_file),
        "manifest_file": str(manifest_file),
        "alignment_count": len(alignment_rows),
        "provenance_bundle_count": len(provenance_rows),
        "tensor_row_count": len(tensor_rows),
        "channel_counts": dict(sorted(channel_counts.items())),
        "taxonomy": taxonomy_result,
    }
