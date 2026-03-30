"""
Circular score gauge built with Plotly.

Renders a donut-style gauge (0-100) with color mapped to the WellNest
score categories. Used on school detail cards, county summaries, and the
comparison page.
"""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from dashboard.ui_theme import theme_colors

COLORS = {
    "critical": "#C73E1D",
    "at_risk": "#F18F01",
    "moderate": "#2E86AB",
    "thriving": "#3BB273",
}

CATEGORY_THRESHOLDS = [
    (25, "critical", "Critical"),
    (50, "at_risk", "At Risk"),
    (75, "moderate", "Moderate"),
    (100, "thriving", "Thriving"),
]


def score_to_category(score: float) -> tuple[str, str, str]:
    """Returns (key, label, hex_color) for a given score."""
    for threshold, key, label in CATEGORY_THRESHOLDS:
        if score <= threshold:
            return key, label, COLORS[key]
    return "thriving", "Thriving", COLORS["thriving"]


def render_gauge(
    score: float,
    label: str = "Wellbeing Score",
    size: int = 200,
    show_category: bool = True,
) -> None:
    """
    Render an inline circular gauge. Plotly's indicator trace does the heavy
    lifting; we just massage the colors and layout to match our palette.
    """
    _cat_key, cat_label, color = score_to_category(score)
    tc = theme_colors()

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number={
                "font": {"size": 36, "color": tc["plot_font"], "family": "Inter, sans-serif"},
                "suffix": "",
            },
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickwidth": 0,
                    "tickcolor": "rgba(0,0,0,0)",
                    "tickfont": {"size": 1, "color": "rgba(0,0,0,0)"},
                },
                "bar": {"color": color, "thickness": 0.8},
                "bgcolor": tc["grid"],
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 25], "color": "rgba(199,62,29,0.08)"},
                    {"range": [25, 50], "color": "rgba(241,143,1,0.08)"},
                    {"range": [50, 75], "color": "rgba(46,134,171,0.08)"},
                    {"range": [75, 100], "color": "rgba(59,178,115,0.08)"},
                ],
                "threshold": {
                    "line": {"color": color, "width": 3},
                    "thickness": 0.85,
                    "value": score,
                },
            },
            title={
                "text": label,
                "font": {"size": 13, "color": tc["text_muted"], "family": "Inter, sans-serif"},
            },
        )
    )

    annotation_text = f"<b>{cat_label}</b>" if show_category else ""

    fig.update_layout(
        height=size,
        margin={"t": 40, "b": 10, "l": 20, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif"},
        annotations=[
            {
                "text": annotation_text,
                "x": 0.5,
                "y": -0.05,
                "showarrow": False,
                "font": {"size": 12, "color": color},
            }
        ]
        if show_category
        else [],
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_mini_gauge(score: float, size: int = 120) -> None:
    """Compact version for cards and tables — no labels, just the ring and number."""
    _, _, color = score_to_category(score)
    tc = theme_colors()

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number={"font": {"size": 22, "color": tc["plot_font"]}},
            gauge={
                "axis": {"range": [0, 100], "visible": False},
                "bar": {"color": color, "thickness": 0.75},
                "bgcolor": tc["grid"],
                "borderwidth": 0,
            },
        )
    )

    fig.update_layout(
        height=size,
        margin={"t": 10, "b": 5, "l": 10, "r": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
