import random
from pathlib import Path

from scz_target_engine.benchmark_metrics import (
    BenchmarkConfidenceIntervalPayload,
    BenchmarkMetricOutputPayload,
    RankedEvaluationRow,
    _resample_rows_preserving_rank_order,
    build_ranked_evaluation_rows,
    calculate_metric_values,
    estimate_bootstrap_intervals,
    read_benchmark_confidence_interval_payload,
    read_benchmark_metric_output_payload,
    write_benchmark_confidence_interval_payload,
    write_benchmark_metric_output_payload,
)


def test_calculate_metric_values_returns_rank_and_top_k_metrics() -> None:
    rows = build_ranked_evaluation_rows(
        ("gene_a", "gene_b", "gene_c"),
        ("gene_a", "gene_b", "gene_c"),
        {
            "gene_a": True,
            "gene_b": False,
            "gene_c": True,
        },
    )

    metrics = calculate_metric_values(rows)

    assert metrics == {
        "average_precision_any_positive_outcome": 0.833333,
        "mean_reciprocal_rank_any_positive_outcome": 1.0,
        "precision_at_1_any_positive_outcome": 1.0,
        "precision_at_3_any_positive_outcome": 0.666667,
        "precision_at_5_any_positive_outcome": 0.666667,
        "recall_at_1_any_positive_outcome": 0.5,
        "recall_at_3_any_positive_outcome": 1.0,
        "recall_at_5_any_positive_outcome": 1.0,
    }


def test_estimate_bootstrap_intervals_is_deterministic() -> None:
    rows = build_ranked_evaluation_rows(
        ("gene_a", "gene_b", "gene_c", "gene_d"),
        ("gene_a", "gene_b", "gene_c", "gene_d"),
        {
            "gene_a": True,
            "gene_b": False,
            "gene_c": True,
            "gene_d": False,
        },
    )

    first = estimate_bootstrap_intervals(
        rows,
        iterations=25,
        random_seed=123,
    )
    second = estimate_bootstrap_intervals(
        rows,
        iterations=25,
        random_seed=123,
    )

    assert first == second
    point_estimate, interval_low, interval_high = first[
        "average_precision_any_positive_outcome"
    ]
    assert point_estimate == 0.833333
    assert interval_low <= point_estimate <= interval_high


def test_estimate_bootstrap_intervals_preserve_original_rank_order_in_replicates() -> None:
    rows = (
        RankedEvaluationRow(entity_id="gene_a", relevant=False),
        RankedEvaluationRow(entity_id="gene_b", relevant=False),
        RankedEvaluationRow(entity_id="gene_c", relevant=False),
        RankedEvaluationRow(entity_id="gene_d", relevant=True),
    )

    resampled_rows = _resample_rows_preserving_rank_order(rows, random.Random(0))
    assert [row.entity_id for row in resampled_rows] == [
        "gene_a",
        "gene_c",
        "gene_d",
        "gene_d",
    ]

    intervals = estimate_bootstrap_intervals(
        rows,
        iterations=1,
        random_seed=0,
    )

    assert intervals["mean_reciprocal_rank_any_positive_outcome"] == (
        0.25,
        0.333333,
        0.333333,
    )
    assert intervals["average_precision_any_positive_outcome"] == (
        0.25,
        0.416667,
        0.416667,
    )


def test_build_ranked_evaluation_rows_keeps_uncovered_admissible_entities() -> None:
    rows = build_ranked_evaluation_rows(
        ("gene_a", "gene_b", "gene_c", "gene_d"),
        ("gene_a", "gene_c"),
        {
            "gene_a": True,
            "gene_b": True,
            "gene_c": False,
            "gene_d": False,
        },
    )

    assert [row.entity_id for row in rows] == [
        "gene_a",
        "gene_c",
        "gene_b",
        "gene_d",
    ]
    assert calculate_metric_values(rows) == {
        "average_precision_any_positive_outcome": 0.833333,
        "mean_reciprocal_rank_any_positive_outcome": 1.0,
        "precision_at_1_any_positive_outcome": 1.0,
        "precision_at_3_any_positive_outcome": 0.666667,
        "precision_at_5_any_positive_outcome": 0.5,
        "recall_at_1_any_positive_outcome": 0.5,
        "recall_at_3_any_positive_outcome": 1.0,
        "recall_at_5_any_positive_outcome": 1.0,
    }


def test_metric_payload_helpers_round_trip(tmp_path: Path) -> None:
    metric_payload = BenchmarkMetricOutputPayload(
        run_id="fixture_run",
        snapshot_id="fixture_snapshot",
        baseline_id="v0_current",
        entity_type="gene",
        horizon="3y",
        metric_name="average_precision_any_positive_outcome",
        metric_value=0.75,
        cohort_size=4,
        notes="fixture metric payload",
    )
    interval_payload = BenchmarkConfidenceIntervalPayload(
        run_id="fixture_run",
        snapshot_id="fixture_snapshot",
        baseline_id="v0_current",
        entity_type="gene",
        horizon="3y",
        metric_name="average_precision_any_positive_outcome",
        point_estimate=0.75,
        interval_low=0.5,
        interval_high=1.0,
        confidence_level=0.95,
        bootstrap_iterations=50,
        resample_unit="entity",
        random_seed=7,
        notes="fixture interval payload",
    )

    metric_path = tmp_path / "metric.json"
    interval_path = tmp_path / "interval.json"
    write_benchmark_metric_output_payload(metric_path, metric_payload)
    write_benchmark_confidence_interval_payload(interval_path, interval_payload)

    assert read_benchmark_metric_output_payload(metric_path) == metric_payload
    assert read_benchmark_confidence_interval_payload(interval_path) == interval_payload
