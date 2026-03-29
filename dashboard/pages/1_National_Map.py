"""
National Map — county-level choropleth of child wellbeing scores.

Uses Folium + streamlit-folium for the interactive map. County boundaries
come from the Census Bureau cartographic boundary files (loaded via CDN).
School markers are clustered to avoid melting browsers.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
from streamlit_folium import st_folium

from dashboard.components.maps import add_school_markers, create_choropleth
from dashboard.components.score_gauge import score_to_category
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="National Map | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

st.title("National Map")
st.markdown(
    '<p style="font-size:15px;color:#636E72;margin-top:-10px;margin-bottom:20px">'
    "County-level child wellbeing scores across the United States</p>",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Map Filters")

    states = get_states()
    selected_state = st.selectbox(
        "State",
        options=["All States"] + states,
        index=0,
    )

    pillar_options = ["Composite", "Education", "Health", "Environment", "Safety"]
    selected_pillar = st.selectbox("Color by", options=pillar_options, index=0)

    score_range = st.slider(
        "Score range",
        min_value=0,
        max_value=100,
        value=(0, 100),
        step=5,
    )

    show_schools = st.checkbox("Show school markers", value=False)

    st.markdown("---")
    st.markdown(
        '<div style="font-size:11px;color:#636E72">'
        "Tip: Click a county on the map for details. "
        "Toggle school markers to see individual schools.</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Build query based on filters
# ---------------------------------------------------------------------------

pillar_column_map = {
    "Composite": "composite_score",
    "Education": "education_score",
    "Health": "health_score",
    "Environment": "environment_score",
    "Safety": "safety_score",
}
value_col = pillar_column_map[selected_pillar]

where_clauses = [f"{value_col} BETWEEN :lo AND :hi"]
params: dict = {"lo": score_range[0], "hi": score_range[1]}

if selected_state != "All States":
    where_clauses.append("state = :state")
    params["state"] = selected_state

where_sql = " AND ".join(where_clauses)

county_df = run_query(
    f"""
    SELECT
        fips, name, state,
        composite_score, education_score, health_score,
        environment_score, safety_score,
        school_count, population
    FROM gold.county_summary
    WHERE {where_sql}
    ORDER BY {value_col} DESC
    """,
    params,
)


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------

col_stats = st.columns(4)

if not county_df.empty:
    with col_stats[0]:
        st.metric("Counties", f"{len(county_df):,}")
    with col_stats[1]:
        avg = county_df[value_col].mean()
        st.metric("Avg Score", f"{avg:.1f}")
    with col_stats[2]:
        critical = len(county_df[county_df["composite_score"] <= 25])
        st.metric("Critical Counties", f"{critical:,}")
    with col_stats[3]:
        total_schools = int(county_df["school_count"].sum())
        st.metric("Total Schools", f"{total_schools:,}")
else:
    st.info("No counties match your filters. Try widening the score range.")
    st.stop()


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

center = (39.8, -98.5)
zoom = 4

if selected_state != "All States":
    state_center = run_query(
        """
        SELECT
            avg(latitude) AS lat, avg(longitude) AS lon
        FROM silver.school_profiles
        WHERE state = :state AND latitude IS NOT NULL
        """,
        {"state": selected_state},
    )
    if not state_center.empty and state_center.iloc[0]["lat"]:
        center = (float(state_center.iloc[0]["lat"]), float(state_center.iloc[0]["lon"]))
        zoom = 6

m = create_choropleth(county_df, value_column=value_col, center=center, zoom=zoom)

if show_schools:
    school_filter_sql = "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    school_params: dict = {}
    if selected_state != "All States":
        school_filter_sql += " AND s.state = :state"
        school_params["state"] = selected_state

    schools_for_map = run_query(
        f"""
        SELECT
            s.nces_id, s.name, s.city, s.state, s.latitude, s.longitude,
            s.enrollment, cws.composite_score
        FROM silver.school_profiles s
        JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
        {school_filter_sql}
        LIMIT 5000
        """,
        school_params,
    )
    m = add_school_markers(m, schools_for_map, cluster=True)

map_data = st_folium(m, width=None, height=550, returned_objects=["last_object_clicked"])


# ---------------------------------------------------------------------------
# County detail panel (on click)
# ---------------------------------------------------------------------------

if map_data and map_data.get("last_object_clicked"):
    clicked = map_data["last_object_clicked"]
    click_lat = clicked.get("lat")
    click_lng = clicked.get("lng")

    if click_lat and click_lng:
        st.markdown("---")
        st.subheader("County Detail")

        nearby = run_query(
            """
            SELECT
                cs.fips, cs.name, cs.state, cs.composite_score,
                cs.education_score, cs.health_score,
                cs.environment_score, cs.safety_score,
                cs.school_count, cs.population
            FROM gold.county_summary cs
            ORDER BY abs(cs.latitude - :lat) + abs(cs.longitude - :lng)
            LIMIT 1
            """,
            {"lat": click_lat, "lng": click_lng},
        )

        if not nearby.empty:
            c = nearby.iloc[0]
            _, cat_label, cat_color = score_to_category(float(c["composite_score"]))

            col_name, col_score = st.columns([2, 1])
            with col_name:
                st.markdown(
                    f'<div style="font-size:20px;font-weight:600;color:#2D3436">'
                    f'{c["name"]}, {c["state"]}</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div style="font-size:13px;color:#636E72">'
                    f'FIPS: {c["fips"]} | '
                    f'Schools: {int(c["school_count"]):,} | '
                    f'Population: {int(c["population"]):,}</div>',
                    unsafe_allow_html=True,
                )
            with col_score:
                st.markdown(
                    f'<div style="text-align:right;font-size:32px;font-weight:700;'
                    f'color:{cat_color}">{c["composite_score"]:.1f}</div>'
                    f'<div style="text-align:right;font-size:13px;color:{cat_color}">'
                    f"{cat_label}</div>",
                    unsafe_allow_html=True,
                )

            pcol1, pcol2, pcol3, pcol4 = st.columns(4)
            pillars_display = [
                ("Education", c.get("education_score"), "#2E86AB", pcol1),
                ("Health", c.get("health_score"), "#A23B72", pcol2),
                ("Environment", c.get("environment_score"), "#F18F01", pcol3),
                ("Safety", c.get("safety_score"), "#3BB273", pcol4),
            ]
            for pname, pval, pcolor, pcol in pillars_display:
                with pcol:
                    val_str = f"{pval:.1f}" if pval is not None else "--"
                    st.markdown(
                        f'<div style="text-align:center;padding:8px;background:#fff;'
                        f'border-radius:8px;border:1px solid #E0E4EA">'
                        f'<div style="font-size:11px;color:#636E72;text-transform:uppercase;'
                        f'letter-spacing:0.5px">{pname}</div>'
                        f'<div style="font-size:22px;font-weight:700;color:{pcolor}">'
                        f"{val_str}</div></div>",
                        unsafe_allow_html=True,
                    )
