"""
AI Insights — county-level AI briefs, anomaly narratives, and quality checks.

The briefs come from gold.county_ai_briefs, generated monthly by the
GPT-4o-mini pipeline. Quality validation results show which records the
LLM flagged as potentially suspicious (usually data artifacts, not real
changes).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st

from dashboard.components.score_gauge import score_to_category
from dashboard.ui_theme import setup_page_theme
from dashboard.utils.cache import TTLCache
from dashboard.utils.db import get_states, run_query

st.set_page_config(
    page_title="AI Insights | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

tc = setup_page_theme()

st.title("AI Insights")
st.markdown(
    f'<p style="font-size:15px;color:{tc["text_muted"]};margin-top:-10px;margin-bottom:20px">'
    "GPT-generated community briefs, anomaly narratives, and data quality flags</p>",
    unsafe_allow_html=True,
)

brief_cache = TTLCache(namespace="ai_briefs", ttl_seconds=600)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_briefs, tab_anomalies, tab_quality = st.tabs(
    ["County Briefs", "Anomaly Narratives", "Quality Validation"]
)


# ---------------------------------------------------------------------------
# County briefs
# ---------------------------------------------------------------------------

with tab_briefs:
    with st.sidebar:
        st.subheader("Brief Filters")
        states = get_states()
        brief_state = st.selectbox("State", ["All States", *states], index=0, key="brief_state")
        brief_category = st.selectbox(
            "Score Category",
            ["All", "Critical", "At Risk", "Moderate", "Thriving"],
            index=0,
            key="brief_cat",
        )

    where_parts = []
    params: dict = {}

    if brief_state != "All States":
        where_parts.append("cab.state = :state")
        params["state"] = brief_state

    if brief_category != "All":
        cat_map = {
            "Critical": "critical",
            "At Risk": "at_risk",
            "Moderate": "moderate",
            "Thriving": "thriving",
        }
        where_parts.append("cs.category = :cat")
        params["cat"] = cat_map[brief_category]

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    briefs = run_query(
        f"""
        SELECT
            cab.fips, cab.county_name, cab.state, cab.brief,
            cab.generated_at,
            cs.composite_score, cs.category
        FROM gold.county_ai_briefs cab
        JOIN gold.county_summary cs ON cab.fips = cs.fips
        WHERE {where_sql}
        ORDER BY cs.composite_score ASC
        LIMIT 50
        """,
        params,
    )

    if briefs.empty:
        st.info("No AI briefs available. Run the brief generation pipeline first.")
    else:
        st.markdown(
            f'<div style="font-size:13px;color:{tc["text_muted"]};margin-bottom:16px">'
            f"Showing {len(briefs)} county briefs (sorted by score, lowest first)</div>",
            unsafe_allow_html=True,
        )

        for _, row in briefs.iterrows():
            _, cat_label, color = score_to_category(float(row["composite_score"]))
            generated = str(row["generated_at"])[:10] if row["generated_at"] else "Unknown"

            with st.expander(
                f"{row['county_name']}, {row['state']} -- "
                f"Score: {row['composite_score']:.1f} ({cat_label})"
            ):
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;'
                    f'align-items:center;margin-bottom:12px">'
                    f"<div>"
                    f'<span style="font-size:13px;color:{tc["text_muted"]}">'
                    f"FIPS: {row['fips']}</span>"
                    f"</div>"
                    f"<div>"
                    f'<span style="display:inline-block;padding:2px 10px;'
                    f"border-radius:10px;background:{color};color:#fff;"
                    f'font-size:12px;font-weight:600">{cat_label}</span>'
                    f"</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                st.markdown(row["brief"])

                st.markdown(
                    f'<div style="font-size:11px;color:{tc["text_muted"]};margin-top:12px;'
                    f'text-align:right">Generated: {generated}</div>',
                    unsafe_allow_html=True,
                )

    if st.button("Refresh Briefs", key="refresh_briefs"):
        brief_cache.invalidate()
        st.cache_data.clear()
        st.rerun()


# ---------------------------------------------------------------------------
# Anomaly narratives
# ---------------------------------------------------------------------------

with tab_anomalies:
    anomalies = run_query("""
        SELECT
            school_name, state, composite_score,
            score_change_1y, z_score, anomaly_type, narrative, detected_at
        FROM gold.anomalies
        WHERE narrative IS NOT NULL
        ORDER BY abs(z_score) DESC
        LIMIT 30
    """)

    if anomalies.empty:
        st.info("No anomaly narratives generated yet.")
    else:
        st.markdown(
            f'<div style="font-size:13px;color:{tc["text_muted"]};margin-bottom:16px">'
            f"{len(anomalies)} anomalies with AI-generated explanations</div>",
            unsafe_allow_html=True,
        )

        for _, anom in anomalies.iterrows():
            is_improvement = anom["anomaly_type"] == "improvement"
            border_color = "#3BB273" if is_improvement else "#C73E1D"
            direction_label = "Improvement" if is_improvement else "Decline"
            change_str = (
                f"+{anom['score_change_1y']:.1f}"
                if is_improvement
                else f"{anom['score_change_1y']:.1f}"
            )

            st.markdown(
                f'<div style="background:{tc["surface"]};border:1px solid {tc["border"]};'
                f"border-left:4px solid {border_color};border-radius:8px;"
                f'padding:14px 18px;margin-bottom:10px">'
                f'<div style="display:flex;justify-content:space-between;'
                f'align-items:flex-start;margin-bottom:8px">'
                f"<div>"
                f'<span style="font-weight:600;color:{tc["text_primary"]};font-size:15px">'
                f"{anom['school_name']}</span> "
                f'<span style="color:{tc["text_muted"]};font-size:13px">({anom["state"]})</span>'
                f"</div>"
                f'<div style="text-align:right">'
                f'<span style="font-size:12px;color:{border_color};'
                f'font-weight:600">{direction_label}</span><br>'
                f'<span style="font-size:11px;color:{tc["text_muted"]}">'
                f"Change: {change_str} | z: {abs(anom['z_score']):.1f}</span>"
                f"</div>"
                f"</div>"
                f'<div style="font-size:13px;color:{tc["text_primary"]};line-height:1.6">'
                f"{anom['narrative']}"
                f"</div>"
                f"</div>",
                unsafe_allow_html=True,
            )


# ---------------------------------------------------------------------------
# Quality validation
# ---------------------------------------------------------------------------

with tab_quality:
    st.markdown(
        f'<div style="font-size:13px;color:{tc["text_muted"]};margin-bottom:16px">'
        "Records flagged by the LLM-based data quality validator. Most of these "
        "are legitimate data artifacts (school mergers, enrollment spikes after "
        "redistricting) rather than actual data errors.</div>",
        unsafe_allow_html=True,
    )

    quality_flags = run_query("""
        SELECT
            nces_id, school_name, state, flag_type, flag_reason,
            confidence, flagged_at
        FROM gold.quality_flags
        WHERE confidence >= 0.6
        ORDER BY confidence DESC
        LIMIT 50
    """)

    if quality_flags.empty:
        st.info("No quality flags in the current dataset.")
    else:
        display = quality_flags[
            ["school_name", "state", "flag_type", "flag_reason", "confidence"]
        ].copy()
        display.columns = ["School", "State", "Flag Type", "Reason", "Confidence"]
        display["Confidence"] = display["Confidence"].apply(lambda x: f"{x:.0%}")
        display["Flag Type"] = display["Flag Type"].str.replace("_", " ").str.title()

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=500,
        )
