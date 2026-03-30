"""
Resource Gaps — schools and communities with critical resource deficits.

Pulls from gold.resource_gaps, which flags schools in food deserts, HPSA
designations, and high-poverty tracts. The priority ranking is based on
severity score (composite of gap depth and school size).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from dashboard.ui_theme import setup_page_theme, theme_colors
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="Resource Gaps | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

tc = setup_page_theme()

st.title("Resource Gap Analysis")
st.markdown(
    f'<p style="font-size:15px;color:{tc["text_muted"]};margin-top:-10px;margin-bottom:20px">'
    "Identifying communities where children face the widest gaps between needs and "
    "available resources</p>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Filters")

    states = get_states()
    filter_state = st.selectbox("State", ["All States", *states], index=0)

    gap_types = ["All Types", "healthcare", "food_access", "mental_health", "dental"]
    filter_gap = st.selectbox(
        "Gap type",
        gap_types,
        format_func=lambda x: x.replace("_", " ").title(),
    )

    min_severity = st.slider("Minimum severity", 0, 100, 25, step=5)

    limit = st.selectbox("Show top N", [50, 100, 200, 500], index=1)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

where_parts = ["rg.severity >= :min_sev"]
params: dict = {"min_sev": min_severity, "limit": limit}

if filter_state != "All States":
    where_parts.append("rg.state = :state")
    params["state"] = filter_state

if filter_gap != "All Types":
    where_parts.append("rg.gap_type = :gap_type")
    params["gap_type"] = filter_gap

where_sql = " AND ".join(where_parts)

gaps_df = run_query(
    f"""
    SELECT
        rg.nces_id, rg.school_name, rg.state, rg.county_fips,
        rg.gap_type, rg.severity, rg.composite_score, rg.description
    FROM gold.resource_gaps rg
    WHERE {where_sql}
    ORDER BY rg.severity DESC
    LIMIT :limit
    """,
    params,
)


# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

if not gaps_df.empty:
    with col1:
        st.metric("Schools Flagged", f"{len(gaps_df):,}")
    with col2:
        avg_severity = gaps_df["severity"].mean()
        st.metric("Avg Severity", f"{avg_severity:.1f}")
    with col3:
        healthcare_count = len(gaps_df[gaps_df["gap_type"] == "healthcare"])
        st.metric("Healthcare Gaps", f"{healthcare_count:,}")
    with col4:
        food_count = len(gaps_df[gaps_df["gap_type"] == "food_access"])
        st.metric("Food Access Gaps", f"{food_count:,}")
else:
    st.info("No resource gaps found matching your filters.")
    st.stop()


# ---------------------------------------------------------------------------
# Gap type distribution
# ---------------------------------------------------------------------------

st.subheader("Gap Distribution")

tab_type, tab_state, tab_table = st.tabs(["By Gap Type", "By State", "Priority Table"])

with tab_type:
    type_counts = gaps_df["gap_type"].value_counts().reset_index()
    type_counts.columns = ["Gap Type", "Count"]
    type_counts["Gap Type"] = type_counts["Gap Type"].str.replace("_", " ").str.title()

    color_map = {
        "Healthcare": "#C73E1D",
        "Food Access": "#F18F01",
        "Mental Health": "#A23B72",
        "Dental": "#2E86AB",
    }

    fig_types = go.Figure(
        go.Bar(
            x=type_counts["Gap Type"],
            y=type_counts["Count"],
            marker_color=[color_map.get(t, tc["text_muted"]) for t in type_counts["Gap Type"]],
            text=type_counts["Count"],
            textposition="outside",
            textfont={"size": 13, "family": "Inter, sans-serif"},
        )
    )
    fig_types.update_layout(
        height=340,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        xaxis={"tickfont": {"size": 12}},
        yaxis={"gridcolor": tc["grid"], "title": "Schools"},
        showlegend=False,
    )
    st.plotly_chart(fig_types, use_container_width=True, config={"displayModeBar": False})


with tab_state:
    state_gaps = (
        gaps_df.groupby("state")
        .agg(count=("nces_id", "count"), avg_severity=("severity", "mean"))
        .reset_index()
        .sort_values("count", ascending=False)
        .head(20)
    )

    fig_states = go.Figure()
    fig_states.add_trace(
        go.Bar(
            x=state_gaps["state"],
            y=state_gaps["count"],
            name="Schools with Gaps",
            marker_color="#C73E1D",
            text=state_gaps["count"],
            textposition="outside",
            textfont={"size": 11},
        )
    )
    fig_states.update_layout(
        height=380,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        xaxis={"tickfont": {"size": 11}, "title": "State"},
        yaxis={"gridcolor": tc["grid"], "title": "Schools Flagged"},
        showlegend=False,
    )
    st.plotly_chart(fig_states, use_container_width=True, config={"displayModeBar": False})


with tab_table:
    st.markdown(
        f'<div style="font-size:13px;color:{tc["text_muted"]};margin-bottom:8px">'
        "Schools ranked by gap severity (higher = more urgent)</div>",
        unsafe_allow_html=True,
    )

    display = gaps_df[["school_name", "state", "gap_type", "severity", "composite_score"]].copy()
    display.columns = ["School", "State", "Gap Type", "Severity", "Wellbeing Score"]
    display["Gap Type"] = display["Gap Type"].str.replace("_", " ").str.title()
    display = display.reset_index(drop=True)
    display.index = display.index + 1

    def _severity_color(val):
        if val >= 75:
            return "color: #C73E1D; font-weight: 600"
        elif val >= 50:
            return "color: #F18F01; font-weight: 600"
        return f"color: {theme_colors()['text_primary']}"

    styled = display.style.format({"Severity": "{:.1f}", "Wellbeing Score": "{:.1f}"}).map(
        _severity_color, subset=["Severity"]
    )

    st.dataframe(styled, use_container_width=True, height=500)


# ---------------------------------------------------------------------------
# Severity distribution histogram
# ---------------------------------------------------------------------------

st.subheader("Severity Distribution")

fig_hist = go.Figure(
    go.Histogram(
        x=gaps_df["severity"],
        nbinsx=20,
        marker_color="#C73E1D",
        marker_line_color="#A03018",
        marker_line_width=1,
        opacity=0.85,
    )
)
fig_hist.update_layout(
    height=280,
    margin={"t": 10, "b": 40, "l": 50, "r": 20},
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
    xaxis={"title": "Severity Score", "gridcolor": tc["grid"]},
    yaxis={"title": "Schools", "gridcolor": tc["grid"]},
)
st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})
