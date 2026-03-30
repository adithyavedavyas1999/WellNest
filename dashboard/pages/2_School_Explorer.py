"""
School Explorer — search, browse, and inspect individual schools.

The main workflow: search by name/city/state, see results in a sortable table,
click through to a detail card with radar chart and historical trend.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import plotly.graph_objects as go
import streamlit as st

from dashboard.components.school_card import render_school_card
from dashboard.components.score_gauge import render_gauge, score_to_category
from dashboard.ui_theme import setup_page_theme
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="School Explorer | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

tc = setup_page_theme()

st.title("School Explorer")
st.markdown(
    f'<p style="font-size:15px;color:{tc["text_muted"]};margin-top:-10px;margin-bottom:20px">'
    "Search and explore child wellbeing scores for individual schools</p>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Search panel
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Search")

    search_name = st.text_input("School name", placeholder="e.g. Lincoln Elementary")
    search_city = st.text_input("City", placeholder="e.g. Chicago")

    states = get_states()
    search_state = st.selectbox("State", ["All States"] + states, index=0)

    score_filter = st.slider("Minimum score", 0, 100, 0, step=5)

    category_filter = st.multiselect(
        "Category",
        ["Critical", "At Risk", "Moderate", "Thriving"],
        default=[],
    )

    sort_by = st.selectbox(
        "Sort by",
        ["composite_score DESC", "composite_score ASC", "name ASC", "enrollment DESC"],
        format_func=lambda x: {
            "composite_score DESC": "Score (high to low)",
            "composite_score ASC": "Score (low to high)",
            "name ASC": "Name (A-Z)",
            "enrollment DESC": "Enrollment (largest)",
        }[x],
    )

    per_page = st.selectbox("Results per page", [25, 50, 100], index=0)


# ---------------------------------------------------------------------------
# Build query
# ---------------------------------------------------------------------------

where_parts = ["cws.composite_score >= :min_score"]
params: dict = {"min_score": score_filter}

if search_name:
    where_parts.append("LOWER(s.name) LIKE :name_pat")
    params["name_pat"] = f"%{search_name.lower()}%"

if search_city:
    where_parts.append("LOWER(s.city) LIKE :city_pat")
    params["city_pat"] = f"%{search_city.lower()}%"

if search_state != "All States":
    where_parts.append("s.state = :state")
    params["state"] = search_state

category_map = {
    "Critical": "critical",
    "At Risk": "at_risk",
    "Moderate": "moderate",
    "Thriving": "thriving",
}
if category_filter:
    cats = [category_map[c] for c in category_filter]
    placeholders = ", ".join(f"'{c}'" for c in cats)
    where_parts.append(f"cws.category IN ({placeholders})")

where_sql = " AND ".join(where_parts) if where_parts else "1=1"

# count
count_df = run_query(
    f"""
    SELECT count(*) AS n
    FROM silver.school_profiles s
    JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
    WHERE {where_sql}
    """,
    params,
)
total = int(count_df.iloc[0]["n"]) if not count_df.empty else 0

# paginate
if "explorer_page" not in st.session_state:
    st.session_state.explorer_page = 0

max_pages = max(1, (total + per_page - 1) // per_page)
st.session_state.explorer_page = min(st.session_state.explorer_page, max_pages - 1)
offset = st.session_state.explorer_page * per_page

results = run_query(
    f"""
    SELECT
        s.nces_id, s.name, s.city, s.state, s.enrollment, s.title_i,
        s.county_name,
        cws.composite_score, cws.education_score, cws.health_score,
        cws.environment_score, cws.safety_score, cws.category,
        cws.national_rank, cws.state_rank, cws.score_change_1y
    FROM silver.school_profiles s
    JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
    WHERE {where_sql}
    ORDER BY {sort_by}
    LIMIT :limit OFFSET :offset
    """,
    {**params, "limit": per_page, "offset": offset},
)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

st.markdown(
    f'<div style="font-size:14px;color:{tc["text_muted"]};margin-bottom:12px">'
    f"Showing {offset + 1}-{min(offset + per_page, total)} of "
    f"<b>{total:,}</b> schools</div>",
    unsafe_allow_html=True,
)

if results.empty:
    st.info("No schools match your search criteria. Try broadening your filters.")
    st.stop()


# sortable results table
display_df = results[
    ["name", "city", "state", "composite_score", "enrollment", "category"]
].copy()
display_df.columns = ["School", "City", "State", "Score", "Enrollment", "Category"]
display_df["Category"] = display_df["Category"].str.replace("_", " ").str.title()

st.dataframe(
    display_df.style.format({"Score": "{:.1f}", "Enrollment": "{:,.0f}"}),
    use_container_width=True,
    hide_index=True,
    height=400,
)

# pagination
nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
with nav_col1:
    if st.button("Previous", disabled=st.session_state.explorer_page == 0):
        st.session_state.explorer_page -= 1
        st.rerun()
with nav_col3:
    if st.button("Next", disabled=st.session_state.explorer_page >= max_pages - 1):
        st.session_state.explorer_page += 1
        st.rerun()
with nav_col2:
    st.markdown(
        f'<div style="text-align:center;font-size:13px;color:{tc["text_muted"]};padding-top:8px">'
        f"Page {st.session_state.explorer_page + 1} of {max_pages}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# School detail panel
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("School Detail")

school_names = results[["nces_id", "name"]].values.tolist()
school_options = {f"{name} ({nid})": nid for nid, name in school_names}

if school_options:
    selected_label = st.selectbox(
        "Select a school from results",
        options=list(school_options.keys()),
    )
    selected_nces = school_options[selected_label]

    row = results[results["nces_id"] == selected_nces].iloc[0]

    detail_col1, detail_col2 = st.columns([2, 1])

    with detail_col1:
        render_school_card(
            name=row["name"],
            city=row["city"],
            state=row["state"],
            composite_score=float(row["composite_score"]),
            enrollment=int(row["enrollment"]) if row["enrollment"] else None,
            title_i=bool(row["title_i"]) if row["title_i"] is not None else None,
            education_score=float(row["education_score"]) if row["education_score"] else None,
            health_score=float(row["health_score"]) if row["health_score"] else None,
            environment_score=float(row["environment_score"]) if row["environment_score"] else None,
            safety_score=float(row["safety_score"]) if row["safety_score"] else None,
            nces_id=selected_nces,
        )

        if row.get("score_change_1y") is not None:
            change = float(row["score_change_1y"])
            change_color = "#3BB273" if change >= 0 else "#C73E1D"
            arrow = "+" if change >= 0 else ""
            st.markdown(
                f'<div style="font-size:14px;margin-top:8px">'
                f'Year-over-Year Change: '
                f'<span style="color:{change_color};font-weight:600">'
                f"{arrow}{change:.1f}</span></div>",
                unsafe_allow_html=True,
            )

    with detail_col2:
        render_gauge(float(row["composite_score"]), label="Composite Score", size=220)

    # radar chart for pillar breakdown
    pillar_vals = []
    pillar_labels = ["Education", "Health", "Environment", "Safety"]
    pillar_keys = ["education_score", "health_score", "environment_score", "safety_score"]
    pillar_colors = ["#2E86AB", "#A23B72", "#F18F01", "#3BB273"]

    for k in pillar_keys:
        val = row.get(k)
        pillar_vals.append(float(val) if val is not None else 0)

    if any(v > 0 for v in pillar_vals):
        fig_radar = go.Figure()
        fig_radar.add_trace(
            go.Scatterpolar(
                r=pillar_vals + [pillar_vals[0]],
                theta=pillar_labels + [pillar_labels[0]],
                fill="toself",
                fillcolor="rgba(46,134,171,0.15)",
                line={"color": "#2E86AB", "width": 2},
                marker={"size": 6, "color": "#2E86AB"},
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
                    "tickfont": {"size": 12, "color": tc["plot_font"], "family": "Inter, sans-serif"},
                },
                "bgcolor": "rgba(0,0,0,0)",
            },
            height=320,
            margin={"t": 30, "b": 30, "l": 60, "r": 60},
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            font={"family": "Inter, sans-serif"},
        )
        st.plotly_chart(fig_radar, use_container_width=True, config={"displayModeBar": False})

    # historical trend
    trend_data = run_query(
        """
        SELECT year, composite_score
        FROM gold.trend_metrics
        WHERE nces_id = :nces_id
        ORDER BY year
        """,
        {"nces_id": selected_nces},
    )

    if not trend_data.empty and len(trend_data) > 1:
        st.subheader("Score Trend")
        fig_trend = go.Figure()
        fig_trend.add_trace(
            go.Scatter(
                x=trend_data["year"],
                y=trend_data["composite_score"],
                mode="lines+markers",
                line={"color": "#2E86AB", "width": 2.5, "shape": "spline"},
                marker={"size": 7, "color": "#2E86AB"},
                fill="tozeroy",
                fillcolor="rgba(46,134,171,0.06)",
            )
        )
        fig_trend.update_layout(
            height=260,
            margin={"t": 10, "b": 40, "l": 50, "r": 20},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis={"title": "Year", "gridcolor": tc["grid"], "dtick": 1},
            yaxis={"title": "Score", "range": [0, 100], "gridcolor": tc["grid"]},
            font={"family": "Inter, sans-serif", "color": tc["plot_font"]},
        )
        st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})
