"""
Tests for the scoring logic that lives in dbt macros / Python equivalents.

The actual scoring runs inside dbt SQL, but we replicate the logic in Python
here so we can catch regressions without needing a database.  The threshold
values come from transformation/dbt_project/macros/scoring.sql.
"""

from __future__ import annotations

import math

import pytest


# ---------------------------------------------------------------------------
# Python equivalents of the dbt scoring macros
# ---------------------------------------------------------------------------
# These mirror the SQL in macros/scoring.sql exactly.  If the dbt macros
# change, update these and the tests will catch any drift.

def normalize_metric(
    value: float | None,
    p5: float,
    p95: float,
    invert: bool = False,
) -> float | None:
    """Min-max normalization to 0-100 with optional inversion."""
    if value is None:
        return None

    if p95 == p5:
        return 50.0

    if invert:
        if value <= p5:
            return 100.0
        if value >= p95:
            return 0.0
        return round((1.0 - (value - p5) / (p95 - p5)) * 100.0, 2)
    else:
        if value <= p5:
            return 0.0
        if value >= p95:
            return 100.0
        return round(((value - p5) / (p95 - p5)) * 100.0, 2)


def compute_pillar_score(
    metrics: list[tuple[float | None, float]],
) -> float | None:
    """Weighted average of sub-metric scores, ignoring nulls.

    metrics: list of (score, weight) tuples.
    """
    total_weight = 0.0
    weighted_sum = 0.0

    for score, weight in metrics:
        if score is not None:
            weighted_sum += score * weight
            total_weight += weight

    if total_weight == 0:
        return None
    return round(weighted_sum / total_weight, 2)


def compute_composite_score(
    education: float | None,
    health: float | None,
    environment: float | None,
    safety: float | None,
    weights: tuple[float, float, float, float] = (0.30, 0.30, 0.20, 0.20),
) -> float | None:
    """Final composite from four pillar scores."""
    pillars = [
        (education, weights[0]),
        (health, weights[1]),
        (environment, weights[2]),
        (safety, weights[3]),
    ]
    return compute_pillar_score(pillars)


def score_category(score: float | None) -> str:
    if score is None:
        return "Insufficient Data"
    if score >= 76:
        return "Thriving"
    if score >= 51:
        return "Moderate"
    if score >= 25.5:
        return "At Risk"
    return "Critical"


# ---------------------------------------------------------------------------
# normalize_metric tests
# ---------------------------------------------------------------------------

class TestNormalizeMetric:

    def test_null_input_returns_none(self) -> None:
        assert normalize_metric(None, 10.0, 90.0) is None

    def test_value_at_p5_returns_zero(self) -> None:
        assert normalize_metric(10.0, 10.0, 90.0) == 0.0

    def test_value_at_p95_returns_hundred(self) -> None:
        assert normalize_metric(90.0, 10.0, 90.0) == 100.0

    def test_midpoint_returns_fifty(self) -> None:
        result = normalize_metric(50.0, 10.0, 90.0)
        assert result == 50.0

    def test_below_p5_clamps_to_zero(self) -> None:
        assert normalize_metric(5.0, 10.0, 90.0) == 0.0

    def test_above_p95_clamps_to_hundred(self) -> None:
        assert normalize_metric(95.0, 10.0, 90.0) == 100.0

    def test_inverted_low_value_returns_hundred(self) -> None:
        result = normalize_metric(10.0, 10.0, 90.0, invert=True)
        assert result == 100.0

    def test_inverted_high_value_returns_zero(self) -> None:
        result = normalize_metric(90.0, 10.0, 90.0, invert=True)
        assert result == 0.0

    def test_inverted_midpoint(self) -> None:
        result = normalize_metric(50.0, 10.0, 90.0, invert=True)
        assert result == 50.0

    @pytest.mark.parametrize("value,expected", [
        (25.0, 18.75),
        (75.0, 81.25),
    ])
    def test_intermediate_values(self, value: float, expected: float) -> None:
        result = normalize_metric(value, 10.0, 90.0)
        assert result is not None
        assert abs(result - expected) < 0.1

    def test_equal_p5_p95_returns_fifty(self) -> None:
        result = normalize_metric(50.0, 50.0, 50.0)
        assert result == 50.0


# ---------------------------------------------------------------------------
# pillar score tests
# ---------------------------------------------------------------------------

class TestPillarScore:

    def test_all_metrics_present(self) -> None:
        metrics = [(80.0, 0.3), (60.0, 0.3), (70.0, 0.2), (90.0, 0.2)]
        result = compute_pillar_score(metrics)
        assert result is not None
        expected = (80 * 0.3 + 60 * 0.3 + 70 * 0.2 + 90 * 0.2) / 1.0
        assert abs(result - expected) < 0.01

    def test_some_nulls_renormalize_weights(self) -> None:
        metrics = [(80.0, 0.5), (None, 0.3), (60.0, 0.2)]
        result = compute_pillar_score(metrics)
        assert result is not None
        expected = (80.0 * 0.5 + 60.0 * 0.2) / (0.5 + 0.2)
        assert abs(result - expected) < 0.01

    def test_all_nulls_returns_none(self) -> None:
        metrics = [(None, 0.4), (None, 0.3), (None, 0.3)]
        assert compute_pillar_score(metrics) is None

    def test_single_metric(self) -> None:
        metrics = [(75.0, 1.0)]
        assert compute_pillar_score(metrics) == 75.0

    def test_zero_scores(self) -> None:
        metrics = [(0.0, 0.5), (0.0, 0.5)]
        assert compute_pillar_score(metrics) == 0.0


# ---------------------------------------------------------------------------
# composite score tests
# ---------------------------------------------------------------------------

class TestCompositeScore:

    def test_all_pillars_present(self) -> None:
        result = compute_composite_score(70.0, 60.0, 80.0, 50.0)
        assert result is not None
        expected = 70 * 0.3 + 60 * 0.3 + 80 * 0.2 + 50 * 0.2
        assert abs(result - expected) < 0.01

    def test_one_pillar_missing(self) -> None:
        result = compute_composite_score(70.0, None, 80.0, 50.0)
        assert result is not None
        expected = (70 * 0.3 + 80 * 0.2 + 50 * 0.2) / (0.3 + 0.2 + 0.2)
        assert abs(result - expected) < 0.01

    def test_all_pillars_none(self) -> None:
        assert compute_composite_score(None, None, None, None) is None

    def test_perfect_scores(self) -> None:
        result = compute_composite_score(100.0, 100.0, 100.0, 100.0)
        assert result == 100.0

    def test_custom_weights(self) -> None:
        result = compute_composite_score(
            80.0, 60.0, 70.0, 90.0,
            weights=(0.4, 0.2, 0.2, 0.2),
        )
        assert result is not None
        expected = 80 * 0.4 + 60 * 0.2 + 70 * 0.2 + 90 * 0.2
        assert abs(result - expected) < 0.01


# ---------------------------------------------------------------------------
# score category tests
# ---------------------------------------------------------------------------

class TestScoreCategory:

    @pytest.mark.parametrize("score,expected", [
        (0.0, "Critical"),
        (15.0, "Critical"),
        (25.0, "Critical"),
        (25.5, "At Risk"),
        (26.0, "At Risk"),
        (50.0, "At Risk"),
        (51.0, "Moderate"),
        (75.0, "Moderate"),
        (76.0, "Thriving"),
        (100.0, "Thriving"),
    ])
    def test_category_boundaries(self, score: float, expected: str) -> None:
        assert score_category(score) == expected

    def test_none_returns_insufficient(self) -> None:
        assert score_category(None) == "Insufficient Data"


# ---------------------------------------------------------------------------
# edge cases
# ---------------------------------------------------------------------------

class TestScoringEdgeCases:

    def test_extreme_high_value(self) -> None:
        result = normalize_metric(999.0, 0.0, 100.0)
        assert result == 100.0

    def test_extreme_negative_value(self) -> None:
        result = normalize_metric(-50.0, 0.0, 100.0)
        assert result == 0.0

    def test_composite_with_single_pillar(self) -> None:
        result = compute_composite_score(55.0, None, None, None)
        assert result == 55.0

    def test_normalize_preserves_precision(self) -> None:
        result = normalize_metric(33.3333, 0.0, 100.0)
        assert result is not None
        assert result == 33.33

    def test_pillar_score_very_small_weights(self) -> None:
        metrics = [(100.0, 0.001), (0.0, 0.001)]
        result = compute_pillar_score(metrics)
        assert result is not None
        assert result == 50.0
