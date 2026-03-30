"""
Dark-mode CSS and color tokens for every dashboard page.

Call ``setup_page_theme()`` once per page, right after ``set_page_config``.
It injects the global stylesheet and returns the color dict so inline
HTML / Plotly can reference ``tc["grid"]``, ``tc["text_muted"]``, etc.
"""

from __future__ import annotations

import streamlit as st

from dashboard.theme import DARK


def theme_colors() -> dict[str, str]:
    """Return the dark-mode token dict."""
    return DARK


_CSS = f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        color: {DARK["text_primary"]};
    }}

    .stApp {{
        background-color: {DARK["app_bg"]};
    }}

    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}

    section[data-testid="stSidebar"] {{
        background-color: {DARK["sidebar_bg"]};
        border-right: 1px solid {DARK["sidebar_border"]};
    }}
    section[data-testid="stSidebar"] .block-container {{
        padding-top: 1.5rem;
    }}

    h1 {{
        color: #2E86AB !important;
        font-weight: 700 !important;
        letter-spacing: -0.5px !important;
    }}
    h2, h3 {{
        color: {DARK["text_primary"]} !important;
        font-weight: 600 !important;
    }}

    [data-testid="stMetric"] {{
        background: {DARK["metric_bg"]};
        border: 1px solid {DARK["metric_border"]};
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px {DARK["metric_shadow"]};
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 13px !important;
        color: {DARK["text_muted"]} !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    [data-testid="stMetricValue"] {{
        font-size: 28px !important;
        font-weight: 700 !important;
        color: {DARK["text_primary"]} !important;
    }}

    .stButton > button {{
        background-color: #2E86AB;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
        font-size: 14px;
        transition: background-color 0.2s;
    }}
    .stButton > button:hover {{
        background-color: #246D8C;
        color: white;
        border: none;
    }}

    .stSelectbox [data-baseweb="select"],
    .stMultiSelect [data-baseweb="select"] {{
        border-radius: 8px;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
        font-weight: 500;
    }}
    .stTabs [aria-selected="true"] {{
        background-color: #2E86AB;
        color: white;
    }}

    .stDataFrame {{
        border-radius: 8px;
        overflow: hidden;
    }}

    hr {{
        border: none;
        border-top: 1px solid {DARK["hr"]};
        margin: 1.5rem 0;
    }}

    .streamlit-expanderHeader {{
        font-weight: 600;
        color: {DARK["text_primary"]};
    }}

    .stAlert {{
        border-radius: 8px;
    }}

    .score-critical {{ color: #C73E1D; }}
    .score-at-risk {{ color: #F18F01; }}
    .score-moderate {{ color: #2E86AB; }}
    .score-thriving {{ color: #3BB273; }}

    footer {{visibility: hidden;}}

    section[data-testid="stSidebar"] a {{
        color: {DARK["text_primary"]} !important;
        text-decoration: none;
    }}
    section[data-testid="stSidebar"] a:hover {{
        color: #2E86AB !important;
    }}

    .viewerBadge_container__r5tak {{display: none;}}
</style>
"""


def inject_global_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def setup_page_theme() -> dict[str, str]:
    """Inject dark-mode CSS and return the color dict. Call after set_page_config."""
    inject_global_css()
    return theme_colors()
