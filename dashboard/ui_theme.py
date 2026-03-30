"""
Theme selector + dynamic CSS for light/dark appearance.

Session state key: wn_theme — "dark" | "light"
"""

from __future__ import annotations

import streamlit as st

from dashboard.theme import THEMES, ThemeMode

SESSION_KEY = "wn_theme"


def ensure_theme_state() -> None:
    if SESSION_KEY not in st.session_state:
        st.session_state[SESSION_KEY] = "dark"


def current_mode() -> ThemeMode:
    ensure_theme_state()
    m = st.session_state[SESSION_KEY]
    return m if m in ("dark", "light") else "dark"


def theme_colors() -> dict[str, str]:
    return THEMES[current_mode()]


def render_theme_selector() -> None:
    """Visible Dark/Light toggle at the top-right of the page."""
    ensure_theme_state()

    left, right = st.columns([10, 2])
    with right:
        current = "Dark" if st.session_state[SESSION_KEY] == "dark" else "Light"
        choice = st.radio(
            "Theme",
            ["Dark", "Light"],
            index=0 if current == "Dark" else 1,
            horizontal=True,
            key="wn_global_theme_radio",
        )
    st.session_state[SESSION_KEY] = "dark" if choice == "Dark" else "light"


def build_css(colors: dict[str, str]) -> str:
    """Global Streamlit chrome overrides for the active theme."""
    t = colors
    is_light = t["app_bg"] == "#F5F7FA"

    light_widget_overrides = ""
    if is_light:
        light_widget_overrides = f"""
    /* --- Light-mode overrides for Streamlit native widgets --- */
    header[data-testid="stHeader"] {{
        background-color: {t["app_bg"]} !important;
    }}
    [data-testid="stToolbar"] {{
        background-color: transparent !important;
    }}
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {{
        background-color: {t["surface"]} !important;
        color: {t["text_primary"]} !important;
        border-color: {t["border"]} !important;
    }}

    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background-color: {t["surface"]} !important;
        color: {t["text_primary"]} !important;
    }}
    .stSelectbox [data-baseweb="select"] > div,
    .stMultiSelect [data-baseweb="select"] > div {{
        background-color: {t["surface"]} !important;
        color: {t["text_primary"]} !important;
    }}
    [data-baseweb="select"] [data-baseweb="tag"] {{
        background-color: {t["metric_bg"]} !important;
    }}
    [data-baseweb="popover"] > div,
    [data-baseweb="menu"] {{
        background-color: {t["surface"]} !important;
        color: {t["text_primary"]} !important;
    }}
    [data-baseweb="menu"] li {{
        color: {t["text_primary"]} !important;
    }}
    [data-baseweb="menu"] li:hover {{
        background-color: {t["metric_bg"]} !important;
    }}

    .stSlider > div > div > div {{
        color: {t["text_primary"]} !important;
    }}

    .stCheckbox label span,
    .stRadio label span {{
        color: {t["text_primary"]} !important;
    }}

    .stDataFrame, .stDataFrame > div {{
        background-color: {t["surface"]} !important;
    }}

    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] p {{
        color: {t["text_primary"]} !important;
    }}

    section[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {{
        background-color: {t["metric_bg"]} !important;
    }}

    section[data-testid="stSidebar"] .stTextInput > div > div > input {{
        background-color: {t["metric_bg"]} !important;
        color: {t["text_primary"]} !important;
        border-color: {t["border"]} !important;
    }}

    .stExpander {{
        background-color: {t["surface"]} !important;
        border-color: {t["border"]} !important;
    }}

    .stTabs [data-baseweb="tab"] {{
        color: {t["text_primary"]} !important;
    }}

    .stMarkdown, .stMarkdown p {{
        color: {t["text_primary"]};
    }}

    div[data-testid="stMetricDelta"] {{
        color: {t["text_muted"]} !important;
    }}

    .stCaption, .stCaption p {{
        color: {t["text_muted"]} !important;
    }}
"""

    return f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        color: {t["text_primary"]};
    }}

    .stApp {{
        background-color: {t["app_bg"]};
    }}

    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}

    section[data-testid="stSidebar"] {{
        background-color: {t["sidebar_bg"]};
        border-right: 1px solid {t["sidebar_border"]};
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
        color: {t["text_primary"]} !important;
        font-weight: 600 !important;
    }}

    [data-testid="stMetric"] {{
        background: {t["metric_bg"]};
        border: 1px solid {t["metric_border"]};
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px {t["metric_shadow"]};
    }}
    [data-testid="stMetricLabel"] {{
        font-size: 13px !important;
        color: {t["text_muted"]} !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    [data-testid="stMetricValue"] {{
        font-size: 28px !important;
        font-weight: 700 !important;
        color: {t["text_primary"]} !important;
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
        border-top: 1px solid {t["hr"]};
        margin: 1.5rem 0;
    }}

    .streamlit-expanderHeader {{
        font-weight: 600;
        color: {t["text_primary"]};
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
        color: {t["text_primary"]} !important;
        text-decoration: none;
    }}
    section[data-testid="stSidebar"] a:hover {{
        color: #2E86AB !important;
    }}

    .viewerBadge_container__r5tak {{display: none;}}

    {light_widget_overrides}
</style>
"""


def inject_global_css() -> None:
    st.markdown(build_css(theme_colors()), unsafe_allow_html=True)


def setup_page_theme() -> dict[str, str]:
    """Selector + CSS + current color dict. Call once per page after set_page_config."""
    ensure_theme_state()
    render_theme_selector()
    inject_global_css()
    return theme_colors()
