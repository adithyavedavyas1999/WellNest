"""
Trends — year-over-year score changes across states and counties.

Shows line charts of historical composite scores, identifies biggest
improvers and decliners, and highlights statistical anomalies. All charts
use the WellNest color palette.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from dashboard.ui_theme import setup_page_theme
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="Trends | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

tc = setup_page_theme()

st.title("Trend Analysis")
st.markdown(
    f'<p style="font-size:15px;color:{tc["text_muted"]};margin-top:-10px;margin-bottom:20px">'
    "Year-over-year changes in child wellbeing scores</p>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Trend Filters")

    states = get_states()
    selected_states = st.multiselect(
        "States to compare",
        states,
        default=states[:5] if len(states) >= 5 else states,
        max_selections=10,
    )

    view_level = st.radio("Aggregation level", ["State", "County"], index=0)

    pillar_options = {
        "Composite": "composite_score",
        "Education": "education_score",
        "Health": "health_score",
        "Environment": "environment_score",
        "Safety": "safety_score",
    }
    selected_pillar = st.selectbox("Metric", list(pillar_options.keys()), index=0)
    metric_col = pillar_options[selected_pillar]


# ---------------------------------------------------------------------------
# State-level trends
# ---------------------------------------------------------------------------

if view_level == "State":
    if not selected_states:
        st.info("Select at least one state from the sidebar.")
        st.stop()

    placeholders = ", ".join(f"'{s}'" for s in selected_states)
    trend_df = run_query(
        f"""
        SELECT
            year, state,
            round(avg({metric_col})::numeric, 2) AS avg_score
        FROM gold.trend_metrics
        WHERE state IN ({placeholders})
        GROUP BY year, state
        ORDER BY year, state
        """,
    )

    if trend_df.empty:
        st.warning("No trend data available for the selected states.")
        st.stop()

    st.subheader(f"{selected_pillar} Score Over Time")

    palette = [
        "#2E86AB",
        "#A23B72",
        "#F18F01",
        "#3BB273",
        "#C73E1D",
        tc["palette_line_1"],
        tc["palette_line_2"],
        "#6C5CE7",
        "#00B894",
        "#E17055",
    ]

    fig = go.Figure()
    for i, state in enumerate(selected_states):
        state_data = trend_df[trend_df["state"] == state]
        if state_data.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=state_data["year"],
                y=state_data["avg_score"],
                mode="lines+markers",
                name=state,
                line={"color": palette[i % len(palette)], "width": 2.5},
                marker={"size": 6},
            )
        )

    fig.update_layout(
        height=420,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        xaxis={"title": "Year", "gridcolor": tc["grid"], "dtick": 1},
        yaxis={"title": "Average Score", "range": [0, 100], "gridcolor": tc["grid"]},
        legend={"orientation": "h", "y": -0.15, "x": 0.5, "xanchor": "center"},
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


else:
    # county-level view
    if not selected_states:
        st.info("Select at least one state to see county-level trends.")
        st.stop()

    # just pick the first state for county view to keep it manageable
    focus_state = selected_states[0] if selected_states else "IL"

    county_trends = run_query(
        """
        SELECT
            tm.year, cs.name AS county_name, cs.fips,
            round(avg(tm.composite_score)::numeric, 2) AS avg_score
        FROM gold.trend_metrics tm
        JOIN gold.county_summary cs ON tm.county_fips = cs.fips
        WHERE tm.state = :state
        GROUP BY tm.year, cs.name, cs.fips
        ORDER BY tm.year
        """,
        {"state": focus_state},
    )

    if county_trends.empty:
        st.warning(f"No county-level trend data for {focus_state}.")
        st.stop()

    top_counties = (
        county_trends.groupby("county_name")["avg_score"].mean().nlargest(8).index.tolist()
    )
    filtered = county_trends[county_trends["county_name"].isin(top_counties)]

    st.subheader(f"County Trends in {focus_state}")

    fig_county = go.Figure()
    palette = [
        "#2E86AB",
        "#A23B72",
        "#F18F01",
        "#3BB273",
        "#C73E1D",
        tc["palette_line_1"],
        "#6C5CE7",
        "#E17055",
    ]

    for i, county in enumerate(top_counties):
        cdata = filtered[filtered["county_name"] == county]
        fig_county.add_trace(
            go.Scatter(
                x=cdata["year"],
                y=cdata["avg_score"],
                mode="lines+markers",
                name=county,
                line={"color": palette[i % len(palette)], "width": 2},
                marker={"size": 5},
            )
        )

    fig_county.update_layout(
        height=420,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        xaxis={"title": "Year", "gridcolor": tc["grid"], "dtick": 1},
        yaxis={"title": "Average Score", "range": [0, 100], "gridcolor": tc["grid"]},
        legend={"orientation": "h", "y": -0.15, "x": 0.5, "xanchor": "center"},
        hovermode="x unified",
    )
    st.plotly_chart(fig_county, use_container_width=True, config={"displayModeBar": False})


# ---------------------------------------------------------------------------
# Biggest movers
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Biggest Movers (Year-over-Year)")

movers_df = run_query("""
    SELECT
        nces_id, school_name, state, composite_score, score_change_1y
    FROM gold.child_wellbeing_score
    WHERE score_change_1y IS NOT NULL
    ORDER BY abs(score_change_1y) DESC
    LIMIT 40
""")

if not movers_df.empty:
    improvers = movers_df[movers_df["score_change_1y"] > 0].head(10)
    decliners = movers_df[movers_df["score_change_1y"] < 0].head(10)

    col_up, col_down = st.columns(2)

    with col_up:
        st.markdown(
            '<div style="font-size:15px;font-weight:600;color:#3BB273;'
            'margin-bottom:8px">Top Improvers</div>',
            unsafe_allow_html=True,
        )
        if not improvers.empty:
            for _, row in improvers.iterrows():
                change = row["score_change_1y"]
                st.markdown(
                    f'<div style="padding:6px 0;border-bottom:1px solid {tc["border"]};'
                    f'font-size:13px">'
                    f'<span style="font-weight:500">{row["school_name"]}</span> '
                    f'<span style="color:{tc["text_muted"]}">({row["state"]})</span> '
                    f'<span style="color:#3BB273;font-weight:600;float:right">'
                    f"+{change:.1f}</span></div>",
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No improvers found.")

    with col_down:
        st.markdown(
            '<div style="font-size:15px;font-weight:600;color:#C73E1D;'
            'margin-bottom:8px">Biggest Declines</div>',
            unsafe_allow_html=True,
        )
        if not decliners.empty:
            for _, row in decliners.iterrows():
                change = row["score_change_1y"]
                st.markdown(
                    f'<div style="padding:6px 0;border-bottom:1px solid {tc["border"]};'
                    f'font-size:13px">'
                    f'<span style="font-weight:500">{row["school_name"]}</span> '
                    f'<span style="color:{tc["text_muted"]}">({row["state"]})</span> '
                    f'<span style="color:#C73E1D;font-weight:600;float:right">'
                    f"{change:.1f}</span></div>",
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No decliners found.")


# ---------------------------------------------------------------------------
# Anomaly highlights
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Anomaly Highlights")

anomalies = run_query("""
    SELECT
        school_name, state, composite_score,
        score_change_1y, z_score, anomaly_type, narrative
    FROM gold.anomalies
    ORDER BY abs(z_score) DESC
    LIMIT 10
""")

if not anomalies.empty:
    for _, anom in anomalies.iterrows():
        icon_color = "#3BB273" if anom["anomaly_type"] == "improvement" else "#C73E1D"
        direction = "Improvement" if anom["anomaly_type"] == "improvement" else "Decline"
        z = abs(anom["z_score"])

        st.markdown(
            f'<div style="padding:12px 16px;margin-bottom:8px;background:{tc["surface"]};'
            f"border-radius:8px;border-left:4px solid {icon_color};"
            f'border:1px solid {tc["border"]}">'
            f'<div style="font-weight:600;color:{tc["text_primary"]}">'
            f"{anom['school_name']} "
            f'<span style="color:{tc["text_muted"]};font-weight:400">({anom["state"]})</span>'
            f'<span style="float:right;color:{icon_color};font-size:13px">'
            f"{direction} | z={z:.1f}</span></div>"
            f'<div style="font-size:13px;color:{tc["text_muted"]};margin-top:4px">'
            f"{anom.get('narrative', '')}</div></div>",
            unsafe_allow_html=True,
        )
else:
    st.caption("No anomalies detected in the current dataset.")
