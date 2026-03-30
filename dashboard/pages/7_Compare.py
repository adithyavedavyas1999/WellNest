"""
Compare — side-by-side comparison of schools or counties.

Multi-select up to 5 entities, see their scores overlaid on a radar chart
and broken out in a detail table. Useful for funders comparing grant
applicants or NGOs benchmarking peer communities.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from dashboard.components.score_gauge import COLORS, score_to_category
from dashboard.ui_theme import setup_page_theme
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="Compare | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

tc = setup_page_theme()

st.title("Compare")
st.markdown(
    f'<p style="font-size:15px;color:{tc["text_muted"]};margin-top:-10px;margin-bottom:20px">'
    "Side-by-side comparison of schools or counties</p>",
    unsafe_allow_html=True,
)

PALETTE = ["#2E86AB", "#A23B72", "#F18F01", "#3BB273", "#C73E1D"]


def _hex_rgb(hex_color: str) -> str:
    """Convert '#2E86AB' to '46,134,171' for rgba() usage."""
    h = hex_color.lstrip("#")
    return ",".join(str(int(h[i : i + 2], 16)) for i in (0, 2, 4))

compare_mode = st.radio(
    "Compare",
    ["Schools", "Counties"],
    horizontal=True,
    label_visibility="collapsed",
)

# ---------------------------------------------------------------------------
# School comparison
# ---------------------------------------------------------------------------

if compare_mode == "Schools":
    with st.sidebar:
        st.subheader("Select Schools")

        states = get_states()
        cmp_state = st.selectbox("Filter by state", ["All States"] + states, index=0, key="cmp_st")

        where = ""
        params: dict = {}
        if cmp_state != "All States":
            where = "WHERE s.state = :state"
            params["state"] = cmp_state

        school_list = run_query(
            f"""
            SELECT s.nces_id, s.name, s.city, s.state
            FROM silver.school_profiles s
            JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
            {where}
            ORDER BY s.name
            LIMIT 2000
            """,
            params,
        )

        if school_list.empty:
            st.warning("No schools found.")
            st.stop()

        options = {
            f"{r['name']} ({r['city']}, {r['state']})": r["nces_id"]
            for _, r in school_list.iterrows()
        }

        selected_labels = st.multiselect(
            "Schools (up to 5)",
            list(options.keys()),
            max_selections=5,
        )

    if not selected_labels:
        st.info("Select 2-5 schools from the sidebar to compare them.")
        st.stop()

    selected_ids = [options[lbl] for lbl in selected_labels]
    placeholders = ", ".join(f"'{nid}'" for nid in selected_ids)

    compare_df = run_query(f"""
        SELECT
            s.nces_id, s.name, s.city, s.state, s.enrollment, s.title_i,
            cws.composite_score, cws.education_score, cws.health_score,
            cws.environment_score, cws.safety_score, cws.category,
            cws.national_rank, cws.score_change_1y
        FROM silver.school_profiles s
        JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
        WHERE s.nces_id IN ({placeholders})
    """)

    if compare_df.empty:
        st.warning("Could not load data for the selected schools.")
        st.stop()

    # radar chart overlay
    st.subheader("Pillar Comparison")

    pillar_labels = ["Education", "Health", "Environment", "Safety"]
    pillar_keys = ["education_score", "health_score", "environment_score", "safety_score"]

    fig_radar = go.Figure()
    for i, (_, row) in enumerate(compare_df.iterrows()):
        vals = [float(row[k]) if row[k] is not None else 0 for k in pillar_keys]
        vals_closed = vals + [vals[0]]
        labels_closed = pillar_labels + [pillar_labels[0]]
        color = PALETTE[i % len(PALETTE)]

        fig_radar.add_trace(
            go.Scatterpolar(
                r=vals_closed,
                theta=labels_closed,
                fill="toself",
                fillcolor=f"rgba({_hex_rgb(color)},0.1)",
                line={"color": color, "width": 2},
                marker={"size": 5, "color": color},
                name=f'{row["name"]} ({row["state"]})',
            )
        )

    fig_radar.update_layout(
        polar={
            "radialaxis": {
                "visible": True,
                "range": [0, 100],
                "gridcolor": tc["grid"],
                "tickfont": {"size": 10, "color": tc["text_muted"]},
            },
            "angularaxis": {
                "tickfont": {"size": 13, "color": tc["plot_font"], "family": "Inter, sans-serif"},
            },
            "bgcolor": "rgba(0,0,0,0)",
        },
        height=420,
        margin={"t": 40, "b": 40, "l": 80, "r": 80},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif"},
        legend={
            "orientation": "h",
            "y": -0.1,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 12},
        },
    )
    st.plotly_chart(fig_radar, use_container_width=True, config={"displayModeBar": False})

    # composite scores as bar chart
    st.subheader("Composite Score Comparison")

    fig_bar = go.Figure()
    for i, (_, row) in enumerate(compare_df.iterrows()):
        color = PALETTE[i % len(PALETTE)]
        _, cat_label, _ = score_to_category(float(row["composite_score"]))
        fig_bar.add_trace(
            go.Bar(
                x=[f'{row["name"]}\n({row["state"]})'],
                y=[row["composite_score"]],
                marker_color=color,
                text=[f'{row["composite_score"]:.1f}'],
                textposition="outside",
                textfont={"size": 13},
                name=row["name"],
                showlegend=False,
            )
        )

    fig_bar.update_layout(
        height=340,
        margin={"t": 20, "b": 60, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        yaxis={"range": [0, 110], "gridcolor": tc["grid"], "title": "Score"},
        xaxis={"tickfont": {"size": 11}},
        showlegend=False,
    )
    st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

    # detail table
    st.subheader("Detailed Metrics")

    detail_cols = [
        "name", "state", "enrollment", "composite_score",
        "education_score", "health_score", "environment_score",
        "safety_score", "national_rank", "score_change_1y",
    ]
    display = compare_df[detail_cols].copy()
    display.columns = [
        "School", "State", "Enrollment", "Composite",
        "Education", "Health", "Environment",
        "Safety", "National Rank", "YoY Change",
    ]
    display = display.reset_index(drop=True)

    st.dataframe(
        display.style.format({
            "Composite": "{:.1f}",
            "Education": "{:.1f}",
            "Health": "{:.1f}",
            "Environment": "{:.1f}",
            "Safety": "{:.1f}",
            "Enrollment": "{:,.0f}",
            "YoY Change": "{:+.1f}",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# County comparison
# ---------------------------------------------------------------------------

else:
    with st.sidebar:
        st.subheader("Select Counties")

        county_list = run_query("""
            SELECT fips, name, state
            FROM gold.county_summary
            ORDER BY name
            LIMIT 5000
        """)

        if county_list.empty:
            st.warning("No county data available.")
            st.stop()

        county_options = {
            f'{r["name"]}, {r["state"]} ({r["fips"]})': r["fips"]
            for _, r in county_list.iterrows()
        }

        selected_county_labels = st.multiselect(
            "Counties (up to 5)",
            list(county_options.keys()),
            max_selections=5,
        )

    if not selected_county_labels:
        st.info("Select 2-5 counties from the sidebar to compare them.")
        st.stop()

    selected_fips = [county_options[lbl] for lbl in selected_county_labels]
    fips_str = ", ".join(f"'{f}'" for f in selected_fips)

    county_compare = run_query(f"""
        SELECT
            fips, name, state, composite_score,
            education_score, health_score, environment_score, safety_score,
            school_count, population, score_change_1y
        FROM gold.county_summary
        WHERE fips IN ({fips_str})
    """)

    if county_compare.empty:
        st.warning("Could not load data for the selected counties.")
        st.stop()

    # radar
    st.subheader("Pillar Comparison")

    pillar_labels = ["Education", "Health", "Environment", "Safety"]
    pillar_keys = ["education_score", "health_score", "environment_score", "safety_score"]

    fig_county_radar = go.Figure()
    for i, (_, row) in enumerate(county_compare.iterrows()):
        vals = [float(row[k]) if row[k] is not None else 0 for k in pillar_keys]
        vals_closed = vals + [vals[0]]
        labels_closed = pillar_labels + [pillar_labels[0]]
        color = PALETTE[i % len(PALETTE)]

        fig_county_radar.add_trace(
            go.Scatterpolar(
                r=vals_closed,
                theta=labels_closed,
                fill="toself",
                fillcolor=f"rgba({_hex_rgb(color)},0.1)",
                line={"color": color, "width": 2},
                marker={"size": 5, "color": color},
                name=f'{row["name"]}, {row["state"]}',
            )
        )

    fig_county_radar.update_layout(
        polar={
            "radialaxis": {
                "visible": True,
                "range": [0, 100],
                "gridcolor": tc["grid"],
                "tickfont": {"size": 10, "color": tc["text_muted"]},
            },
            "angularaxis": {
                "tickfont": {"size": 13, "color": tc["plot_font"], "family": "Inter, sans-serif"},
            },
            "bgcolor": "rgba(0,0,0,0)",
        },
        height=420,
        margin={"t": 40, "b": 40, "l": 80, "r": 80},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif"},
        legend={
            "orientation": "h",
            "y": -0.1,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 12},
        },
    )
    st.plotly_chart(fig_county_radar, use_container_width=True, config={"displayModeBar": False})

    # bar chart
    st.subheader("Composite Score Comparison")

    fig_cbar = go.Figure()
    for i, (_, row) in enumerate(county_compare.iterrows()):
        color = PALETTE[i % len(PALETTE)]
        fig_cbar.add_trace(
            go.Bar(
                x=[f'{row["name"]}\n({row["state"]})'],
                y=[row["composite_score"]],
                marker_color=color,
                text=[f'{row["composite_score"]:.1f}'],
                textposition="outside",
                textfont={"size": 13},
                showlegend=False,
            )
        )

    fig_cbar.update_layout(
        height=340,
        margin={"t": 20, "b": 60, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        yaxis={"range": [0, 110], "gridcolor": tc["grid"], "title": "Score"},
        xaxis={"tickfont": {"size": 11}},
    )
    st.plotly_chart(fig_cbar, use_container_width=True, config={"displayModeBar": False})

    # detail table
    st.subheader("Detailed Metrics")

    display_counties = county_compare[
        ["name", "state", "population", "school_count", "composite_score",
         "education_score", "health_score", "environment_score",
         "safety_score", "score_change_1y"]
    ].copy()
    display_counties.columns = [
        "County", "State", "Population", "Schools", "Composite",
        "Education", "Health", "Environment", "Safety", "YoY Change",
    ]

    st.dataframe(
        display_counties.style.format({
            "Composite": "{:.1f}",
            "Education": "{:.1f}",
            "Health": "{:.1f}",
            "Environment": "{:.1f}",
            "Safety": "{:.1f}",
            "Population": "{:,.0f}",
            "YoY Change": "{:+.1f}",
        }),
        use_container_width=True,
        hide_index=True,
    )
