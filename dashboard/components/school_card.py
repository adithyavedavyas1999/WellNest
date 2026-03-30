"""
Reusable school card component.

Renders a styled card with score badge, key metrics, and an optional
sparkline showing score trend. Built for the School Explorer results list
and the Compare page.
"""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from dashboard.components.score_gauge import score_to_category
from dashboard.ui_theme import theme_colors


def render_score_badge(score: float) -> str:
    """Return HTML for a small colored score pill."""
    _, label, color = score_to_category(score)
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;'
        f"background:{color};color:#fff;font-size:13px;font-weight:600;"
        f'letter-spacing:0.3px">{score:.0f} &middot; {label}</span>'
    )


def render_school_card(
    name: str,
    city: str,
    state: str,
    composite_score: float,
    enrollment: int | None = None,
    title_i: bool | None = None,
    education_score: float | None = None,
    health_score: float | None = None,
    environment_score: float | None = None,
    safety_score: float | None = None,
    trend_values: list[float] | None = None,
    nces_id: str | None = None,
) -> None:
    """
    Full school card. Designed to sit in a column or expander.

    Pillar scores are optional — if missing, we skip the breakdown row.
    Trend values should be a list of historical composite scores (oldest first).
    """
    _, _cat_label, color = score_to_category(composite_score)
    border_color = color
    tc = theme_colors()

    enrollment_text = f"{enrollment:,}" if enrollment else "N/A"
    title_i_text = "Yes" if title_i else ("No" if title_i is False else "--")

    pillar_html = ""
    if any(s is not None for s in [education_score, health_score, environment_score, safety_score]):
        pills = []
        for pillar_name, val, pcolor in [
            ("Edu", education_score, "#2E86AB"),
            ("Health", health_score, "#A23B72"),
            ("Env", environment_score, "#F18F01"),
            ("Safety", safety_score, "#3BB273"),
        ]:
            if val is not None:
                pills.append(
                    f'<span style="font-size:12px;color:{pcolor};font-weight:500">'
                    f"{pillar_name} {val:.0f}</span>"
                )
        pillar_html = (
            '<div style="margin-top:8px;display:flex;gap:12px;flex-wrap:wrap">'
            + " ".join(pills)
            + "</div>"
        )

    card_html = f"""
    <div style="
        border:1px solid {tc["border"]};
        border-left:4px solid {border_color};
        border-radius:8px;
        padding:16px 20px;
        margin-bottom:12px;
        background:{tc["surface"]};
        transition:box-shadow 0.2s ease;
    ">
        <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div>
                <div style="font-size:16px;font-weight:600;color:{tc["text_primary"]};margin-bottom:2px">
                    {name}
                </div>
                <div style="font-size:13px;color:{tc["text_muted"]}">
                    {city}, {state}
                </div>
            </div>
            <div style="text-align:right">
                {render_score_badge(composite_score)}
            </div>
        </div>
        <div style="
            display:flex;gap:20px;margin-top:10px;font-size:12px;color:{tc["text_muted"]}
        ">
            <span>Enrollment: <b style="color:{tc["text_primary"]}">{enrollment_text}</b></span>
            <span>Title I: <b style="color:{tc["text_primary"]}">{title_i_text}</b></span>
        </div>
        {pillar_html}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

    if trend_values and len(trend_values) > 1:
        _render_sparkline(trend_values, color)


def _render_sparkline(values: list[float], color: str) -> None:
    """Tiny line chart showing score trajectory."""
    fig = go.Figure(
        go.Scatter(
            y=values,
            mode="lines",
            line={"color": color, "width": 2, "shape": "spline"},
            fill="tozeroy",
            fillcolor=f"rgba({_hex_to_rgb(color)},0.08)",
        )
    )
    fig.update_layout(
        height=50,
        margin={"t": 0, "b": 0, "l": 0, "r": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"visible": False},
        yaxis={"visible": False, "range": [0, 100]},
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _hex_to_rgb(hex_color: str) -> str:
    """'#2E86AB' -> '46,134,171'"""
    h = hex_color.lstrip("#")
    return ",".join(str(int(h[i : i + 2], 16)) for i in (0, 2, 4))
