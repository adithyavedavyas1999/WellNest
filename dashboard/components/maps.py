"""
Map helpers for Folium-based visualizations.

Handles choropleth creation, school markers, popup formatting, and the
legend. All maps use the WellNest color scheme.

County boundaries come from the US Census Bureau's cartographic boundary
files. We load them from a CDN GeoJSON to avoid shipping a 30MB file.
"""

from __future__ import annotations

import folium
import pandas as pd
from folium.plugins import MarkerCluster

from dashboard.ui_theme import theme_colors

COUNTY_GEOJSON_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
)

SCORE_COLORS = {
    "critical": "#C73E1D",
    "at_risk": "#F18F01",
    "moderate": "#2E86AB",
    "thriving": "#3BB273",
}

CATEGORY_RANGES = [
    (0, 25, "Critical", "#C73E1D"),
    (26, 50, "At Risk", "#F18F01"),
    (51, 75, "Moderate", "#2E86AB"),
    (76, 100, "Thriving", "#3BB273"),
]


def _score_color(score: float) -> str:
    if score <= 25:
        return SCORE_COLORS["critical"]
    elif score <= 50:
        return SCORE_COLORS["at_risk"]
    elif score <= 75:
        return SCORE_COLORS["moderate"]
    return SCORE_COLORS["thriving"]


def create_choropleth(
    county_data: pd.DataFrame,
    value_column: str = "composite_score",
    center: tuple[float, float] = (39.8, -98.5),
    zoom: int = 4,
) -> folium.Map:
    """
    Build a county-level choropleth from a DataFrame with a 'fips' column.

    The DataFrame needs at minimum: fips, name, state, and whatever value_column
    you're coloring by. We add popups for each county automatically.
    """
    tc = theme_colors()
    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles=tc["map_tile"],
        attr="CartoDB",
        control_scale=True,
    )

    if county_data.empty:
        return m

    df = county_data.copy()
    # zero-pad FIPS in case it came in as int
    df["fips"] = df["fips"].astype(str).str.zfill(5)

    choropleth = folium.Choropleth(
        geo_data=COUNTY_GEOJSON_URL,
        data=df,
        columns=["fips", value_column],
        key_on="feature.id",
        fill_color="YlGnBu",
        fill_opacity=0.7,
        line_opacity=0.2,
        line_weight=0.5,
        legend_name="Child Wellbeing Score",
        nan_fill_color=tc["map_nan_fill"],
        threshold_scale=[0, 25, 50, 75, 100],
    )
    choropleth.add_to(m)

    # hide the default Branca color scale, we'll add our own legend
    for key in choropleth._children:
        if key.startswith("color_map"):
            choropleth._children[key].caption = ""

    _add_county_tooltips(m, df, value_column)
    _add_legend(m)

    return m


def _add_county_tooltips(
    m: folium.Map,
    df: pd.DataFrame,
    value_column: str,
) -> None:
    """
    Attach hover tooltips showing county name and score.

    This is a workaround — Folium's Choropleth doesn't have native tooltip
    support, so we overlay a transparent GeoJson with tooltip on top of it.
    """
    lookup = {}
    for _, row in df.iterrows():
        lookup[row["fips"]] = {
            "name": row.get("name", ""),
            "state": row.get("state", ""),
            "score": row.get(value_column, 0),
            "school_count": row.get("school_count", 0),
        }

    def style_fn(feature):
        return {
            "fillOpacity": 0,
            "weight": 0,
            "color": "transparent",
        }

    def highlight_fn(feature):
        return {
            "fillOpacity": 0.1,
            "weight": 2,
            "color": theme_colors()["map_highlight"],
        }

    import json
    import urllib.request

    try:
        with urllib.request.urlopen(COUNTY_GEOJSON_URL) as resp:
            geojson = json.loads(resp.read())
    except Exception:
        return

    for feat in geojson["features"]:
        fips = feat.get("id", "")
        info = lookup.get(fips)
        if info:
            feat["properties"]["tooltip_text"] = (
                f"{info['name']}, {info['state']}: {info['score']:.0f}"
            )
        else:
            feat["properties"]["tooltip_text"] = ""

    tooltip_layer = folium.GeoJson(
        geojson,
        style_function=style_fn,
        highlight_function=highlight_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["tooltip_text"],
            aliases=[""],
            style="font-size:13px;font-family:Inter,sans-serif;padding:4px 8px;",
        ),
    )
    tooltip_layer.add_to(m)


def _add_legend(m: folium.Map) -> None:
    """Custom HTML legend that matches our color palette."""
    tc = theme_colors()
    legend_html = f"""
    <div style="
        position:fixed;bottom:30px;right:30px;z-index:1000;
        background:{tc["legend_bg"]};padding:12px 16px;border-radius:8px;
        border:1px solid {tc["legend_border"]};
        box-shadow:0 2px 8px rgba(0,0,0,0.4);font-family:Inter,sans-serif;
        font-size:12px;line-height:1.6;color:{tc["legend_text"]};
    ">
        <div style="font-weight:600;margin-bottom:6px;color:{tc["legend_text"]}">
            Wellbeing Score
        </div>
        <div><span style="background:#3BB273;width:12px;height:12px;
            display:inline-block;border-radius:2px;margin-right:6px"></span>
            76-100 Thriving</div>
        <div><span style="background:#2E86AB;width:12px;height:12px;
            display:inline-block;border-radius:2px;margin-right:6px"></span>
            51-75 Moderate</div>
        <div><span style="background:#F18F01;width:12px;height:12px;
            display:inline-block;border-radius:2px;margin-right:6px"></span>
            26-50 At Risk</div>
        <div><span style="background:#C73E1D;width:12px;height:12px;
            display:inline-block;border-radius:2px;margin-right:6px"></span>
            0-25 Critical</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))


def add_school_markers(
    m: folium.Map,
    schools: pd.DataFrame,
    cluster: bool = True,
) -> folium.Map:
    """
    Drop pins on the map for individual schools.

    Expects columns: latitude, longitude, name, composite_score, and ideally
    city, state, enrollment. Uses MarkerCluster by default because 130K
    markers would melt the browser otherwise.
    """
    if schools.empty:
        return m

    target = MarkerCluster(name="Schools") if cluster else m

    for _, row in schools.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue

        score = row.get("composite_score", 0)
        color = _score_color(score)
        popup_html = _format_school_popup(row)

        folium.CircleMarker(
            location=[lat, lon],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            weight=1,
            popup=folium.Popup(popup_html, max_width=280),
        ).add_to(target)

    if cluster:
        target.add_to(m)

    return m


def _format_school_popup(row: pd.Series) -> str:
    """HTML for the map popup when you click a school marker."""
    tc = theme_colors()
    name = row.get("name", "Unknown")
    city = row.get("city", "")
    state = row.get("state", "")
    score = row.get("composite_score", 0)
    enrollment = row.get("enrollment")
    _, label, color = _score_category_info(score)

    tm = tc["text_muted"]
    tp = tc["text_primary"]
    enrollment_line = (
        f"<div style='font-size:12px;color:{tm}'>Enrollment: {enrollment:,.0f}</div>"
        if enrollment and not pd.isna(enrollment)
        else ""
    )

    return f"""
    <div style="font-family:Inter,sans-serif;min-width:200px">
        <div style="font-size:14px;font-weight:600;color:{tp};margin-bottom:4px">
            {name}
        </div>
        <div style="font-size:12px;color:{tm};margin-bottom:6px">
            {city}, {state}
        </div>
        <div style="
            display:inline-block;padding:2px 8px;border-radius:10px;
            background:{color};color:#fff;font-size:12px;font-weight:600;
            margin-bottom:4px
        ">{score:.0f} - {label}</div>
        {enrollment_line}
    </div>
    """


def _score_category_info(score: float) -> tuple[str, str, str]:
    """Lighter version of score_to_category that doesn't import from gauge module."""
    if score <= 25:
        return "critical", "Critical", SCORE_COLORS["critical"]
    elif score <= 50:
        return "at_risk", "At Risk", SCORE_COLORS["at_risk"]
    elif score <= 75:
        return "moderate", "Moderate", SCORE_COLORS["moderate"]
    return "thriving", "Thriving", SCORE_COLORS["thriving"]
