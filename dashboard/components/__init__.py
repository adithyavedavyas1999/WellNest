"""Reusable dashboard components — cards, gauges, maps."""

from dashboard.components.maps import (
    add_school_markers,
    create_choropleth,
)
from dashboard.components.school_card import render_school_card, render_score_badge
from dashboard.components.score_gauge import (
    render_gauge,
    render_mini_gauge,
    score_to_category,
)

__all__ = [
    "add_school_markers",
    "create_choropleth",
    "render_gauge",
    "render_mini_gauge",
    "render_school_card",
    "render_score_badge",
    "score_to_category",
]
