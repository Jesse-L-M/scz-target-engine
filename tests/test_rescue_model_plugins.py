import pytest

from scz_target_engine.rescue.baselines.reporting import RescueComparisonRow
from scz_target_engine.rescue.models import (
    RescueModelDefinition,
    build_rescue_model_admission_summary,
    list_rescue_model_plugins,
)
from scz_target_engine.rescue.tasks import (
    NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID,
    NPC_SIGNATURE_REVERSAL_TASK_ID,
)


def test_rescue_model_registry_lists_shipped_npc_plugin() -> None:
    plugins = list_rescue_model_plugins(NPC_SIGNATURE_REVERSAL_TASK_ID)

    assert [plugin.definition.model_id for plugin in plugins] == [
        NPC_SIGNATURE_REVERSAL_PRIMARY_SCORER_ID
    ]
    assert plugins[0].definition.principal_split == "test"
    assert plugins[0].definition.admission_metric_names == (
        "average_precision",
        "mean_reciprocal_rank",
        "first_positive_rank",
    )


def test_model_admission_requires_clean_win_on_all_declared_metrics() -> None:
    model_definition = RescueModelDefinition(
        model_id="candidate_model_v1",
        task_id="example_rescue_task",
        label="Candidate model v1",
        description="Example model candidate.",
        leakage_rule="Ranking-only frozen inputs.",
        input_fields=("score",),
        admission_metric_names=(
            "average_precision",
            "mean_reciprocal_rank",
            "first_positive_rank",
        ),
        principal_split="test",
        stage="candidate",
    )
    comparison_rows = (
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="baseline_a",
            scorer_label="Baseline A",
            scorer_role="baseline",
            candidate_count=12,
            positive_count=2,
            metrics={
                "average_precision": 0.4,
                "mean_reciprocal_rank": 0.2,
                "first_positive_rank": 5,
            },
        ),
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="baseline_b",
            scorer_label="Baseline B",
            scorer_role="baseline",
            candidate_count=12,
            positive_count=2,
            metrics={
                "average_precision": 0.45,
                "mean_reciprocal_rank": 0.3,
                "first_positive_rank": 4,
            },
        ),
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="candidate_model_v1",
            scorer_label="Candidate model v1",
            scorer_role="model",
            candidate_count=12,
            positive_count=2,
            metrics={
                "average_precision": 0.46,
                "mean_reciprocal_rank": 0.31,
                "first_positive_rank": 6,
            },
        ),
    )

    summary = build_rescue_model_admission_summary(
        comparison_rows=comparison_rows,
        model_definitions=(model_definition,),
        principal_split="test",
        baseline_scorer_ids=("baseline_a", "baseline_b"),
    )

    assert summary["admitted_model_ids"] == []
    assert summary["decisions"][0]["blocking_metric_names"] == [
        "first_positive_rank"
    ]
    assert summary["decisions"][0]["best_baseline_by_metric"][
        "average_precision"
    ] == {
        "baseline_id": "baseline_b",
        "baseline_label": "Baseline B",
        "metric_value": 0.45,
        "model_metric_value": 0.46,
        "model_beats_baseline": True,
    }


def test_model_admission_rejects_missing_declared_baseline_row() -> None:
    model_definition = RescueModelDefinition(
        model_id="candidate_model_v1",
        task_id="example_rescue_task",
        label="Candidate model v1",
        description="Example model candidate.",
        leakage_rule="Ranking-only frozen inputs.",
        input_fields=("score",),
        admission_metric_names=("average_precision",),
        principal_split="test",
        stage="candidate",
    )
    comparison_rows = (
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="baseline_a",
            scorer_label="Baseline A",
            scorer_role="baseline",
            candidate_count=12,
            positive_count=2,
            metrics={"average_precision": 0.4},
        ),
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="candidate_model_v1",
            scorer_label="Candidate model v1",
            scorer_role="model",
            candidate_count=12,
            positive_count=2,
            metrics={"average_precision": 0.46},
        ),
    )

    with pytest.raises(
        ValueError,
        match="missing baseline scorer rows: baseline_b",
    ):
        build_rescue_model_admission_summary(
            comparison_rows=comparison_rows,
            model_definitions=(model_definition,),
            principal_split="test",
            baseline_scorer_ids=("baseline_a", "baseline_b"),
        )


def test_model_admission_rejects_principal_split_mismatch() -> None:
    model_definition = RescueModelDefinition(
        model_id="candidate_model_v1",
        task_id="example_rescue_task",
        label="Candidate model v1",
        description="Example model candidate.",
        leakage_rule="Ranking-only frozen inputs.",
        input_fields=("score",),
        admission_metric_names=("average_precision",),
        principal_split="validation",
        stage="candidate",
    )
    comparison_rows = (
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="baseline_a",
            scorer_label="Baseline A",
            scorer_role="baseline",
            candidate_count=12,
            positive_count=2,
            metrics={"average_precision": 0.4},
        ),
        RescueComparisonRow(
            task_id="example_rescue_task",
            task_label="Example rescue task",
            evaluation_split="test",
            scorer_id="candidate_model_v1",
            scorer_label="Candidate model v1",
            scorer_role="model",
            candidate_count=12,
            positive_count=2,
            metrics={"average_precision": 0.46},
        ),
    )

    with pytest.raises(
        ValueError,
        match="principal_split must match the admission split",
    ):
        build_rescue_model_admission_summary(
            comparison_rows=comparison_rows,
            model_definitions=(model_definition,),
            principal_split="test",
            baseline_scorer_ids=("baseline_a",),
        )
