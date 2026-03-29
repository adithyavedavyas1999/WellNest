"""
WellNest Dashboard — main entry point.

Run with:
    streamlit run dashboard/app.py

This is the home page. Streamlit picks up the pages/ directory automatically
for multi-page navigation.
"""

from __future__ import annotations

import streamlit as st

from dashboard.components.score_gauge import render_gauge, score_to_category
from dashboard.utils.cache import check_staleness, format_freshness
from dashboard.utils.db import check_db_health, get_data_freshness, run_query

# ---------------------------------------------------------------------------
# Page setup — must be the first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="WellNest | Child Wellbeing Intelligence",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS — WellNest color palette
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* global resets */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        color: #2D3436;
    }

    /* background */
    .stApp {
        background-color: #F5F7FA;
    }

    /* main content area */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    /* sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E0E4EA;
    }
    section[data-testid="stSidebar"] .block-container {
        padding-top: 1.5rem;
    }

    /* headers */
    h1 {
        color: #2E86AB !important;
        font-weight: 700 !important;
        letter-spacing: -0.5px !important;
    }
    h2, h3 {
        color: #2D3436 !important;
        font-weight: 600 !important;
    }

    /* metric cards */
    [data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E0E4EA;
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    [data-testid="stMetricLabel"] {
        font-size: 13px !important;
        color: #636E72 !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    [data-testid="stMetricValue"] {
        font-size: 28px !important;
        font-weight: 700 !important;
        color: #2D3436 !important;
    }

    /* buttons */
    .stButton > button {
        background-color: #2E86AB;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
        font-size: 14px;
        transition: background-color 0.2s;
    }
    .stButton > button:hover {
        background-color: #246D8C;
        color: white;
        border: none;
    }

    /* selectbox and multiselect */
    .stSelectbox [data-baseweb="select"],
    .stMultiSelect [data-baseweb="select"] {
        border-radius: 8px;
    }

    /* tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: #2E86AB;
        color: white;
    }

    /* dataframe styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }

    /* custom divider */
    hr {
        border: none;
        border-top: 1px solid #E0E4EA;
        margin: 1.5rem 0;
    }

    /* expander */
    .streamlit-expanderHeader {
        font-weight: 600;
        color: #2D3436;
    }

    /* info/warning/error boxes */
    .stAlert {
        border-radius: 8px;
    }

    /* score badge helper classes */
    .score-critical { color: #C73E1D; }
    .score-at-risk { color: #F18F01; }
    .score-moderate { color: #2E86AB; }
    .score-thriving { color: #3BB273; }

    /* footer */
    footer {visibility: hidden;}

    /* sidebar nav links */
    section[data-testid="stSidebar"] a {
        color: #2D3436 !important;
        text-decoration: none;
    }
    section[data-testid="stSidebar"] a:hover {
        color: #2E86AB !important;
    }

    /* hide the "made with streamlit" */
    .viewerBadge_container__r5tak {display: none;}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        '<div style="font-size:24px;font-weight:700;color:#2E86AB;'
        'margin-bottom:4px;letter-spacing:-0.5px">WellNest</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="font-size:13px;color:#636E72;margin-bottom:20px">'
        "Child Wellbeing Intelligence Platform</div>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # data freshness
    freshness = get_data_freshness()
    db_ok, db_msg = check_db_health()

    if db_ok:
        freshness_label = format_freshness(freshness)
        is_stale = check_staleness(freshness)
        dot_color = "#F18F01" if is_stale else "#3BB273"
        st.markdown(
            f'<div style="font-size:12px;color:#636E72;display:flex;'
            f'align-items:center;gap:6px">'
            f'<span style="width:8px;height:8px;border-radius:50%;'
            f'background:{dot_color};display:inline-block"></span>'
            f"Data updated: {freshness_label}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="font-size:12px;color:#C73E1D;display:flex;'
            'align-items:center;gap:6px">'
            '<span style="width:8px;height:8px;border-radius:50%;'
            'background:#C73E1D;display:inline-block"></span>'
            "Database offline</div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    with st.expander("About WellNest"):
        st.markdown(
            """
            WellNest maps child wellbeing for every public school in the
            United States by fusing 12+ federal data sources into a composite
            score covering education, health, environment, and safety.

            Built for NGOs, funders, and policymakers.

            **Data Sources**: NCES, CDC PLACES, Census ACS, EPA, HRSA,
            USDA, FEMA, FBI UCR, NOAA

            **Methodology**: See the Methodology page in our docs.
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Home page content
# ---------------------------------------------------------------------------

st.title("Child Wellbeing Dashboard")

st.markdown(
    '<p style="font-size:16px;color:#636E72;margin-top:-10px;margin-bottom:24px">'
    "National overview of child wellbeing across 130,000+ public schools</p>",
    unsafe_allow_html=True,
)

# key stats row
stats = run_query("""
    SELECT
        count(*) AS total_schools,
        count(DISTINCT county_fips) AS total_counties,
        round(avg(composite_score)::numeric, 1) AS avg_score,
        round(min(composite_score)::numeric, 1) AS min_score,
        round(max(composite_score)::numeric, 1) AS max_score,
        count(*) FILTER (WHERE composite_score <= 25) AS critical_count,
        count(*) FILTER (WHERE composite_score > 25 AND composite_score <= 50) AS at_risk_count,
        count(*) FILTER (WHERE composite_score > 50 AND composite_score <= 75) AS moderate_count,
        count(*) FILTER (WHERE composite_score > 75) AS thriving_count
    FROM gold.child_wellbeing_score
""")

if stats.empty or stats.iloc[0]["total_schools"] == 0:
    st.warning(
        "No data found in gold.child_wellbeing_score. "
        "Run the pipeline first: `make run-dagster`"
    )
    st.stop()

s = stats.iloc[0]

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Schools Scored", f"{int(s['total_schools']):,}")
with col2:
    st.metric("Counties Covered", f"{int(s['total_counties']):,}")
with col3:
    st.metric("National Avg Score", f"{s['avg_score']:.1f}")
with col4:
    delta_text = f"{s['min_score']:.0f} - {s['max_score']:.0f}"
    st.metric("Score Range", delta_text)

st.markdown("<br>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Score distribution
# ---------------------------------------------------------------------------

st.subheader("Score Distribution")

import plotly.graph_objects as go

category_data = {
    "Critical (0-25)": int(s["critical_count"]),
    "At Risk (26-50)": int(s["at_risk_count"]),
    "Moderate (51-75)": int(s["moderate_count"]),
    "Thriving (76-100)": int(s["thriving_count"]),
}
colors = ["#C73E1D", "#F18F01", "#2E86AB", "#3BB273"]

col_chart, col_gauge = st.columns([3, 1])

with col_chart:
    fig = go.Figure(
        go.Bar(
            x=list(category_data.keys()),
            y=list(category_data.values()),
            marker_color=colors,
            text=[f"{v:,}" for v in category_data.values()],
            textposition="outside",
            textfont={"size": 13, "family": "Inter, sans-serif"},
        )
    )
    fig.update_layout(
        height=320,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": "#2D3436"},
        xaxis={"tickfont": {"size": 12}},
        yaxis={"gridcolor": "#E8ECF1", "title": "Schools", "title_font": {"size": 12}},
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

with col_gauge:
    render_gauge(float(s["avg_score"]), label="National Average", size=250)


# ---------------------------------------------------------------------------
# Top / Bottom states table
# ---------------------------------------------------------------------------

st.subheader("State Rankings")

state_scores = run_query("""
    SELECT
        state,
        count(*) AS school_count,
        round(avg(composite_score)::numeric, 1) AS avg_score,
        round(min(composite_score)::numeric, 1) AS min_score,
        round(max(composite_score)::numeric, 1) AS max_score,
        count(*) FILTER (WHERE composite_score <= 25) AS critical_schools
    FROM gold.child_wellbeing_score
    GROUP BY state
    ORDER BY avg_score DESC
""")

if not state_scores.empty:
    tab_top, tab_bottom = st.tabs(["Top 10 States", "Bottom 10 States"])

    with tab_top:
        top_df = state_scores.head(10).reset_index(drop=True)
        top_df.index = top_df.index + 1
        top_df.columns = ["State", "Schools", "Avg Score", "Min", "Max", "Critical Schools"]
        st.dataframe(
            top_df.style.format({"Avg Score": "{:.1f}", "Min": "{:.1f}", "Max": "{:.1f}"}),
            use_container_width=True,
            hide_index=False,
        )

    with tab_bottom:
        bottom_df = state_scores.tail(10).sort_values("avg_score").reset_index(drop=True)
        bottom_df.index = bottom_df.index + 1
        bottom_df.columns = ["State", "Schools", "Avg Score", "Min", "Max", "Critical Schools"]
        st.dataframe(
            bottom_df.style.format({"Avg Score": "{:.1f}", "Min": "{:.1f}", "Max": "{:.1f}"}),
            use_container_width=True,
            hide_index=False,
        )


# ---------------------------------------------------------------------------
# Pillar averages
# ---------------------------------------------------------------------------

st.subheader("Pillar Scores (National Average)")

pillar_avgs = run_query("""
    SELECT
        round(avg(education_score)::numeric, 1) AS education,
        round(avg(health_score)::numeric, 1) AS health,
        round(avg(environment_score)::numeric, 1) AS environment,
        round(avg(safety_score)::numeric, 1) AS safety
    FROM gold.child_wellbeing_score
""")

if not pillar_avgs.empty:
    p = pillar_avgs.iloc[0]
    pillars = [
        ("Education", float(p["education"]) if p["education"] else 0, "#2E86AB"),
        ("Health & Resources", float(p["health"]) if p["health"] else 0, "#A23B72"),
        ("Environment", float(p["environment"]) if p["environment"] else 0, "#F18F01"),
        ("Safety", float(p["safety"]) if p["safety"] else 0, "#3BB273"),
    ]

    fig_pillars = go.Figure()
    for name, val, color in pillars:
        fig_pillars.add_trace(
            go.Bar(
                x=[name],
                y=[val],
                marker_color=color,
                text=[f"{val:.1f}"],
                textposition="outside",
                textfont={"size": 14, "family": "Inter, sans-serif"},
                name=name,
                showlegend=False,
            )
        )

    fig_pillars.update_layout(
        height=280,
        margin={"t": 20, "b": 40, "l": 50, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": "#2D3436"},
        yaxis={
            "range": [0, 100],
            "gridcolor": "#E8ECF1",
            "title": "Score",
            "title_font": {"size": 12},
        },
        xaxis={"tickfont": {"size": 12}},
        bargap=0.4,
    )
    st.plotly_chart(fig_pillars, use_container_width=True, config={"displayModeBar": False})


# ---------------------------------------------------------------------------
# Quick links
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(
    '<div style="text-align:center;color:#636E72;font-size:13px;padding:10px 0">'
    "WellNest v0.1 | Built for ChiEAC | "
    '<a href="https://github.com/chieac/wellnest" style="color:#2E86AB">'
    "GitHub</a></div>",
    unsafe_allow_html=True,
)
