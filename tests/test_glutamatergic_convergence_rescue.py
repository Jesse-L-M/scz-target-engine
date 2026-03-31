import json
from pathlib import Path

import pytest

from scz_target_engine.artifacts import load_artifact
from scz_target_engine.atlas.convergence import materialize_convergence_hubs
from scz_target_engine.atlas.mechanistic_axes import load_atlas_tensor_bundle
from scz_target_engine.atlas.tensor import materialize_atlas_tensor
from scz_target_engine.io import read_csv_rows, read_json
from scz_target_engine.rescue import (
    GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH,
    GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH,
    GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH,
    load_glutamatergic_convergence_rescue_bundle,
    validate_glutamatergic_convergence_raw_snapshot_bundle,
)
from scz_target_engine.rescue.registry import (
    load_rescue_task_registrations,
    resolve_rescue_task_contract,
)
from scz_target_engine.rescue.tasks import (
    build_glutamatergic_convergence_ranked_predictions,
    evaluate_glutamatergic_convergence_ranked_predictions,
    load_glutamatergic_convergence_rescue_task_bundle,
    materialize_glutamatergic_convergence_rescue_evaluation,
)


def test_glutamatergic_convergence_task_registration_resolves() -> None:
    registration = next(
        registration
        for registration in load_rescue_task_registrations()
        if registration.task_id == "glutamatergic_convergence_rescue_task"
    )
    contract = resolve_rescue_task_contract(
        rescue_task_id="glutamatergic_convergence_rescue_task"
    )

    assert registration.registry_status == "active"
    assert registration.task_card_file == GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH
    assert contract.task_label == "Glutamatergic convergence rescue task"
    assert contract.leakage_boundary.freeze_manifest_required is True
    assert contract.leakage_boundary.raw_to_frozen_lineage_required is True


def test_glutamatergic_convergence_task_card_loads_through_artifact_registry() -> None:
    artifact = load_artifact(
        GLUTAMATERGIC_CONVERGENCE_TASK_CARD_PATH.resolve(),
        artifact_name="rescue_task_card",
    )

    assert artifact.artifact_name == "rescue_task_card"
    assert artifact.payload.task_id == "glutamatergic_convergence_rescue_task"
    assert artifact.payload.governance_status == "active"


def test_load_glutamatergic_convergence_rescue_bundle_reads_frozen_artifacts() -> None:
    bundle = load_glutamatergic_convergence_rescue_bundle()

    assert bundle.governance_bundle.task_card.task_id == (
        "glutamatergic_convergence_rescue_task"
    )
    assert {row["gene_symbol"] for row in bundle.ranking_input_rows} == {
        "GRIA1",
        "GRIN2A",
        "GRM3",
        "GRM5",
    }
    assert {
        row["gene_symbol"]: row["evaluation_label"] for row in bundle.evaluation_label_rows
    } == {
        "GRIA1": "0",
        "GRIN2A": "1",
        "GRM3": "1",
        "GRM5": "0",
    }
    assert {
        row["gene_symbol"]: row["split_name"] for row in bundle.ranking_input_rows
    } == {
        "GRIA1": "validation",
        "GRIN2A": "train",
        "GRM3": "test",
        "GRM5": "train",
    }
    assert {
        row["gene_symbol"]: row["split_name"] for row in bundle.evaluation_label_rows
    } == {
        "GRIA1": "validation",
        "GRIN2A": "train",
        "GRM3": "test",
        "GRM5": "train",
    }


def test_glutamatergic_raw_snapshot_manifest_chain_is_self_contained() -> None:
    raw_snapshot_bundle = validate_glutamatergic_convergence_raw_snapshot_bundle()

    assert raw_snapshot_bundle.raw_snapshot_manifest_file.resolve() == (
        GLUTAMATERGIC_CONVERGENCE_RAW_SNAPSHOT_MANIFEST_PATH
    )
    assert raw_snapshot_bundle.atlas_ingest_manifest_file.resolve() == (
        GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH
    )
    assert tuple(sorted(raw_snapshot_bundle.tensor_artifact_files)) == (
        "entity_alignments_file",
        "evidence_tensor_file",
        "provenance_bundles_file",
        "taxonomy_manifest_file",
    )
    assert tuple(sorted(raw_snapshot_bundle.convergence_artifact_files)) == (
        "convergence_hubs_file",
        "hub_axis_members_file",
        "hub_evidence_links_file",
    )
    assert len(raw_snapshot_bundle.provenance_bundle_rows) == 2


def test_glutamatergic_raw_snapshot_manifest_rebuilds_checked_in_convergence(
    tmp_path: Path,
) -> None:
    raw_snapshot_bundle = validate_glutamatergic_convergence_raw_snapshot_bundle()

    tensor_bundle = load_atlas_tensor_bundle(raw_snapshot_bundle.tensor_manifest_file)
    assert len(tensor_bundle.tensor_rows) == 88

    regenerated_convergence = materialize_convergence_hubs(
        tensor_manifest_file=raw_snapshot_bundle.tensor_manifest_file,
        output_dir=tmp_path / "convergence",
    )

    assert read_csv_rows(Path(regenerated_convergence["convergence_hubs_file"])) == (
        read_csv_rows(
            raw_snapshot_bundle.convergence_artifact_files["convergence_hubs_file"]
        )
    )
    assert read_csv_rows(Path(regenerated_convergence["hub_axis_members_file"])) == (
        read_csv_rows(
            raw_snapshot_bundle.convergence_artifact_files["hub_axis_members_file"]
        )
    )
    assert read_csv_rows(Path(regenerated_convergence["hub_evidence_links_file"])) == (
        read_csv_rows(
            raw_snapshot_bundle.convergence_artifact_files["hub_evidence_links_file"]
        )
    )


def test_glutamatergic_fixture_regeneration_emits_portable_provenance_paths(
    tmp_path: Path,
) -> None:
    tensor_result = materialize_atlas_tensor(
        ingest_manifest_file=GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH,
        output_dir=tmp_path / "tensor",
    )
    convergence_result = materialize_convergence_hubs(
        tensor_manifest_file=Path(tensor_result["manifest_file"]),
        output_dir=tmp_path / "convergence",
    )

    tensor_manifest = read_json(Path(tensor_result["manifest_file"]))
    taxonomy_manifest = read_json(tmp_path / "tensor" / "taxonomy" / "taxonomy_manifest.json")
    convergence_manifest = read_json(Path(convergence_result["manifest_file"]))
    provenance_rows = read_csv_rows(Path(tensor_result["provenance_bundles_file"]))

    assert tensor_manifest["ingest_manifest_file"] == (
        "data/curated/atlas/glutamatergic_convergence_fixture/example_ingest_manifest.json"
    )
    assert tensor_manifest["output_dir"] == "."
    assert tensor_manifest["taxonomy_output_dir"] == "taxonomy"
    assert tensor_manifest["emitted_artifacts"] == {
        "entity_alignments_file": "entity_alignments.csv",
        "evidence_tensor_file": "evidence_tensor.csv",
        "provenance_bundles_file": "provenance_bundles.csv",
        "taxonomy_manifest_file": "taxonomy/taxonomy_manifest.json",
    }
    assert taxonomy_manifest["ingest_manifest_file"] == (
        "data/curated/atlas/glutamatergic_convergence_fixture/example_ingest_manifest.json"
    )
    assert taxonomy_manifest["output_dir"] == "."
    assert taxonomy_manifest["emitted_artifacts"] == {
        "context_dimensions_file": "context_dimensions.csv",
        "context_members_file": "context_members.csv",
        "feature_taxonomy_file": "feature_taxonomy.csv",
    }
    assert convergence_manifest["tensor_manifest_file"] == "../tensor/tensor_manifest.json"
    assert convergence_manifest["evidence_tensor_file"] == "../tensor/evidence_tensor.csv"
    assert convergence_manifest["output_dir"] == "."
    assert convergence_manifest["emitted_artifacts"] == {
        "convergence_hubs_file": "convergence_hubs.csv",
        "hub_axis_members_file": "hub_axis_members.csv",
        "hub_evidence_links_file": "hub_evidence_links.csv",
    }
    assert {
        row["processed_output_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/glutamatergic_convergence_fixture/example_sources/opentargets/glutamatergic_baseline.csv",
        "data/curated/atlas/glutamatergic_convergence_fixture/example_sources/pgc/glutamatergic_prioritized_genes.csv",
    }
    assert {
        row["processed_metadata_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/glutamatergic_convergence_fixture/example_sources/opentargets/glutamatergic_baseline.metadata.json",
        "data/curated/atlas/glutamatergic_convergence_fixture/example_sources/pgc/glutamatergic_prioritized_genes.metadata.json",
    }
    assert {
        row["raw_manifest_file"]
        for row in provenance_rows
    } == {
        "data/curated/atlas/glutamatergic_convergence_fixture/example_raw/opentargets/manifest.json",
        "data/curated/atlas/glutamatergic_convergence_fixture/example_raw/pgc/manifest.json",
    }


def test_glutamatergic_ranking_inputs_match_materialized_convergence_fixture(
    tmp_path: Path,
) -> None:
    frozen_bundle = load_glutamatergic_convergence_rescue_bundle()

    tensor_result = materialize_atlas_tensor(
        ingest_manifest_file=GLUTAMATERGIC_CONVERGENCE_ATLAS_FIXTURE_MANIFEST_PATH,
        output_dir=tmp_path / "tensor",
    )
    convergence_result = materialize_convergence_hubs(
        tensor_manifest_file=Path(tensor_result["manifest_file"]),
        output_dir=tmp_path / "convergence",
    )

    hub_rows = {
        row["alignment_label"]: row
        for row in read_csv_rows(Path(convergence_result["convergence_hubs_file"]))
    }
    axis_rows = {
        (row["alignment_label"], row["axis_id"]): row
        for row in read_csv_rows(Path(convergence_result["hub_axis_members_file"]))
    }

    for ranking_row in frozen_bundle.ranking_input_rows:
        hub_row = hub_rows[ranking_row["gene_symbol"]]
        assert ranking_row["hub_id"] == hub_row["hub_id"]
        assert ranking_row["alignment_id"] == hub_row["alignment_id"]
        assert ranking_row["convergence_contract_version"] == "atlas-convergence-hubs/v1"
        assert ranking_row["source_coverage_state"] == hub_row["source_coverage_state"]
        assert ranking_row["axis_coverage_state"] == hub_row["axis_coverage_state"]
        assert ranking_row["missingness_state"] == hub_row["missingness_state"]
        assert ranking_row["conflict_state"] == hub_row["conflict_state"]
        assert ranking_row["uncertainty_max_level"] == hub_row["uncertainty_max_level"]
        assert json.loads(ranking_row["supported_axis_ids_json"]) == json.loads(
            hub_row["supported_axis_ids_json"]
        )
        assert json.loads(ranking_row["partial_axis_ids_json"]) == json.loads(
            hub_row["partial_axis_ids_json"]
        )
        assert json.loads(ranking_row["unobserved_axis_ids_json"]) == json.loads(
            hub_row["unobserved_axis_ids_json"]
        )

        clinical_axis = axis_rows[
            (ranking_row["gene_symbol"], "mechanistic_axis:clinical-translation")
        ]
        disease_axis = axis_rows[
            (ranking_row["gene_symbol"], "mechanistic_axis:disease-association")
        ]
        variant_axis = axis_rows[
            (ranking_row["gene_symbol"], "mechanistic_axis:variant-to-gene")
        ]
        assert ranking_row["clinical_translation_state"] == clinical_axis["support_state"]
        assert (
            ranking_row["clinical_translation_uncertainty_max_level"]
            == clinical_axis["uncertainty_max_level"]
        )
        assert ranking_row["disease_association_state"] == disease_axis["support_state"]
        assert (
            ranking_row["disease_association_missingness_state"]
            == disease_axis["missingness_state"]
        )
        assert (
            ranking_row["disease_association_uncertainty_max_level"]
            == disease_axis["uncertainty_max_level"]
        )
        assert ranking_row["variant_to_gene_state"] == variant_axis["support_state"]
        assert (
            ranking_row["variant_to_gene_missingness_state"]
            == variant_axis["missingness_state"]
        )
        assert (
            ranking_row["variant_to_gene_uncertainty_max_level"]
            == variant_axis["uncertainty_max_level"]
        )


def test_glutamatergic_convergence_task_ranks_full_convergence_genes_first() -> None:
    bundle = load_glutamatergic_convergence_rescue_task_bundle()
    predictions = build_glutamatergic_convergence_ranked_predictions(bundle)

    assert len(predictions) == 4
    assert [row["rank"] for row in predictions] == [1, 2, 3, 4]
    assert {
        predictions[0]["gene_symbol"],
        predictions[1]["gene_symbol"],
    } == {"GRIN2A", "GRM3"}
    assert predictions[0]["priority_tier"] == "full_convergence"
    assert predictions[0]["rescue_score"] >= predictions[-1]["rescue_score"]


def test_glutamatergic_convergence_task_evaluation_uses_held_out_labels_only() -> None:
    bundle = load_glutamatergic_convergence_rescue_task_bundle()
    predictions = build_glutamatergic_convergence_ranked_predictions(bundle)
    evaluation = evaluate_glutamatergic_convergence_ranked_predictions(
        predictions=predictions,
        bundle=bundle,
    )

    assert evaluation["summary"]["candidate_count"] == 4
    assert evaluation["summary"]["positive_label_count"] == 2
    assert evaluation["summary"]["metric_values"] == {
        "average_precision_any_positive_outcome": 1.0,
        "mean_reciprocal_rank_any_positive_outcome": 1.0,
        "precision_at_1_any_positive_outcome": 1.0,
        "precision_at_3_any_positive_outcome": 0.666667,
        "precision_at_5_any_positive_outcome": 0.5,
        "recall_at_1_any_positive_outcome": 0.5,
        "recall_at_3_any_positive_outcome": 1.0,
        "recall_at_5_any_positive_outcome": 1.0,
    }
    assert {
        row["gene_symbol"]: row["evaluation_label"]
        for row in evaluation["evaluation_rows"]
    } == {
        "GRIN2A": "1",
        "GRM3": "1",
        "GRIA1": "0",
        "GRM5": "0",
    }
    assert evaluation["summary"]["split_counts"] == {
        "train": 2,
        "validation": 1,
        "test": 1,
    }
    assert {
        split_name: split_summary["candidate_count"]
        for split_name, split_summary in evaluation["summary"]["split_summaries"].items()
    } == {
        "all": 4,
        "train": 2,
        "validation": 1,
        "test": 1,
    }


def test_glutamatergic_convergence_evaluation_rejects_prediction_split_conflicts() -> None:
    bundle = load_glutamatergic_convergence_rescue_task_bundle()
    predictions = build_glutamatergic_convergence_ranked_predictions(bundle)
    conflicted_predictions = [dict(row) for row in predictions]
    conflicted_predictions[0]["split_name"] = "validation"

    with pytest.raises(ValueError, match="prediction split_name conflicts"):
        evaluate_glutamatergic_convergence_ranked_predictions(
            predictions=conflicted_predictions,
            bundle=bundle,
        )


def test_glutamatergic_convergence_evaluation_rejects_unknown_prediction_ids() -> None:
    bundle = load_glutamatergic_convergence_rescue_task_bundle()
    predictions = build_glutamatergic_convergence_ranked_predictions(bundle)
    invalid_predictions = [dict(row) for row in predictions]
    invalid_predictions[0]["gene_id"] = "ENSG99999999999"

    with pytest.raises(ValueError, match="unknown gene_id"):
        evaluate_glutamatergic_convergence_ranked_predictions(
            predictions=invalid_predictions,
            bundle=bundle,
        )


def test_glutamatergic_convergence_materializer_writes_end_to_end_outputs(
    tmp_path: Path,
) -> None:
    result = materialize_glutamatergic_convergence_rescue_evaluation(
        output_dir=tmp_path / "glutamatergic-run"
    )

    predictions = read_csv_rows(Path(result["predictions_file"]))
    evaluation_rows = read_csv_rows(Path(result["evaluation_rows_file"]))
    evaluation_summary = read_json(Path(result["evaluation_summary_file"]))
    run_manifest = read_json(Path(result["run_manifest_file"]))

    assert [row["gene_symbol"] for row in predictions[:2]] == ["GRIN2A", "GRM3"]
    assert [row["rank"] for row in predictions] == ["1", "2", "3", "4"]
    assert {row["split_name"] for row in evaluation_rows} == {
        "train",
        "validation",
        "test",
    }
    assert evaluation_summary["metric_values"]["average_precision_any_positive_outcome"] == 1.0
    assert run_manifest["task_id"] == "glutamatergic_convergence_rescue_task"
