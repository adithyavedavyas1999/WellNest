"""
Dark color tokens for the Streamlit dashboard.

Used by ui_theme.py for CSS injection and inline HTML / Plotly.
"""

from __future__ import annotations

DARK: dict[str, str] = {
    "text_primary": "#E6EDF3",
    "text_muted": "#9AA4B2",
    "app_bg": "#0E1117",
    "sidebar_bg": "#161B22",
    "sidebar_border": "#30363D",
    "metric_bg": "#161B22",
    "metric_border": "#30363D",
    "metric_shadow": "rgba(0,0,0,0.3)",
    "hr": "#30363D",
    "grid": "#30363D",
    "plot_font": "#E6EDF3",
    "surface": "#161B22",
    "border": "#30363D",
    "palette_line_1": "#9AA4B2",
    "palette_line_2": "#E6EDF3",
    "map_tile": "CartoDB dark_matter",
    "map_nan_fill": "#21262D",
    "legend_bg": "#161B22",
    "legend_border": "#30363D",
    "legend_text": "#E6EDF3",
    "map_highlight": "#9AA4B2",
}
