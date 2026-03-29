"""
Geospatial utilities for WellNest.

Most of the spatial heavy lifting lives here -- FIPS wrangling, haversine
distances, and H3 hex helpers.  We settled on H3 resolution 8 for mapping
schools to neighborhoods after testing res 7 (too coarse -- merged distinct
communities on Chicago's south side) and res 9 (too fine -- left gaps in
rural counties).
"""

from __future__ import annotations

import math
import re
from typing import Any

import h3
import polars as pl
import structlog

logger = structlog.get_logger(__name__)

EARTH_RADIUS_KM = 6371.0


# ------------------------------------------------------------------
# Haversine
# ------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers between two points.

    Good enough for anything we're doing -- we're not landing spacecraft,
    just estimating how far a kid has to travel to reach a clinic.
    """
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def haversine_series(
    df: pl.DataFrame,
    lat1_col: str,
    lon1_col: str,
    lat2_col: str,
    lon2_col: str,
) -> pl.Series:
    """Vectorized haversine over a Polars DataFrame.  Returns km."""
    rlat1 = df[lat1_col].cast(pl.Float64) * (math.pi / 180.0)
    rlat2 = df[lat2_col].cast(pl.Float64) * (math.pi / 180.0)
    dlat = (df[lat2_col] - df[lat1_col]).cast(pl.Float64) * (math.pi / 180.0)
    dlon = (df[lon2_col] - df[lon1_col]).cast(pl.Float64) * (math.pi / 180.0)

    a = (dlat / 2).map_elements(math.sin, return_dtype=pl.Float64).pow(2) + rlat1.map_elements(
        math.cos, return_dtype=pl.Float64
    ) * rlat2.map_elements(math.cos, return_dtype=pl.Float64) * (dlon / 2).map_elements(
        math.sin, return_dtype=pl.Float64
    ).pow(2)

    return 2 * EARTH_RADIUS_KM * a.sqrt().map_elements(math.asin, return_dtype=pl.Float64)


# ------------------------------------------------------------------
# FIPS code helpers
# ------------------------------------------------------------------

# FIPS anatomy:
#   state  = 2 digits  (e.g. "17" = Illinois)
#   county = 3 digits  (e.g. "031" = Cook County)
#   tract  = 6 digits  (e.g. "842100")
#   full   = 11 digits (e.g. "17031842100")

def format_fips(state: str | int, county: str | int, tract: str | int | None = None) -> str:
    """Build a zero-padded FIPS code from its parts.

    Accepts integers or strings -- the Census API returns FIPS as strings
    but NCES gives them as ints, so we deal with both.
    """
    s = str(state).zfill(2)
    c = str(county).zfill(3)
    if tract is not None:
        t = str(tract).zfill(6)
        return f"{s}{c}{t}"
    return f"{s}{c}"


def parse_fips(fips_code: str) -> dict[str, str]:
    """Split a FIPS code into its constituent parts.

    Handles 5-digit (county) and 11-digit (tract) codes.  Anything else
    gets a warning -- we've seen 4-digit codes in older NCES files where
    the leading zero was dropped.
    """
    fips = fips_code.strip()
    if len(fips) == 11:
        return {"state": fips[:2], "county": fips[2:5], "tract": fips[5:]}
    elif len(fips) == 5:
        return {"state": fips[:2], "county": fips[2:5]}
    elif len(fips) == 2:
        return {"state": fips}
    else:
        logger.warning("unusual_fips_length", fips=fips, length=len(fips))
        # best-effort: assume state is first 2
        return {"state": fips[:2], "remainder": fips[2:]}


def county_fips(state_fips: str | int, county_fips_code: str | int) -> str:
    """Convenience: 5-digit county FIPS from state + county codes."""
    return format_fips(state_fips, county_fips_code)


def normalize_fips_column(df: pl.DataFrame, col: str, width: int = 5) -> pl.DataFrame:
    """Zero-pad a FIPS column to the specified width.

    NCES files love storing FIPS as integers, which strips leading zeros.
    Connecticut's state FIPS is "09" but shows up as 9 in half our sources.
    """
    return df.with_columns(
        pl.col(col).cast(pl.Utf8).str.zfill(width).alias(col)
    )


# ------------------------------------------------------------------
# H3 hex helpers
# ------------------------------------------------------------------

# Resolution guide (approximate):
#   7 -> ~5.16 km2  (good for metro-level grouping)
#   8 -> ~0.74 km2  (our default -- neighborhood scale)
#   9 -> ~0.10 km2  (block-level -- overkill for most analyses)

DEFAULT_H3_RES = 8


def latlng_to_h3(lat: float, lng: float, resolution: int = DEFAULT_H3_RES) -> str:
    """Convert a lat/lng to an H3 cell index."""
    return h3.latlng_to_cell(lat, lng, resolution)


def h3_to_parent(h3_index: str, parent_res: int) -> str:
    """Get the parent cell at a coarser resolution."""
    return h3.cell_to_parent(h3_index, parent_res)


def h3_to_latlng(h3_index: str) -> tuple[float, float]:
    """Center lat/lng of an H3 cell."""
    return h3.cell_to_latlng(h3_index)


def h3_ring(h3_index: str, k: int = 1) -> list[str]:
    """Get the k-ring (neighbors) around a cell.  Useful for "nearby" queries."""
    return list(h3.grid_disk(h3_index, k))


def add_h3_column(
    df: pl.DataFrame,
    lat_col: str = "latitude",
    lng_col: str = "longitude",
    resolution: int = DEFAULT_H3_RES,
    output_col: str = "h3_index",
) -> pl.DataFrame:
    """Add an H3 index column to a Polars DataFrame with lat/lng.

    We do this row-by-row because h3-py doesn't have a vectorized API.
    It's fast enough for our ~130K school rows (~0.4s on an M2 Mac).
    """
    indices = [
        latlng_to_h3(lat, lng, resolution) if lat is not None and lng is not None else None
        for lat, lng in zip(df[lat_col].to_list(), df[lng_col].to_list())
    ]
    return df.with_columns(pl.Series(name=output_col, values=indices))


def get_h3_resolution_for_area(target_area_km2: float) -> int:
    """Pick the H3 resolution whose cell area is closest to the target.

    Handy when adapting analysis from one geography to another -- rural
    counties need coarser cells than dense urban areas.
    """
    res_areas = {
        0: 4250546.848, 1: 607220.978, 2: 86745.854, 3: 12392.264,
        4: 1770.324, 5: 252.903, 6: 36.129, 7: 5.161,
        8: 0.737, 9: 0.105, 10: 0.015, 11: 0.002, 12: 0.0003,
    }
    best_res = 0
    best_diff = abs(res_areas[0] - target_area_km2)
    for res, area in res_areas.items():
        diff = abs(area - target_area_km2)
        if diff < best_diff:
            best_diff = diff
            best_res = res
    return best_res


# ------------------------------------------------------------------
# Nearest-point lookup
# ------------------------------------------------------------------

def find_nearest(
    lat: float,
    lon: float,
    points: pl.DataFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    k: int = 1,
) -> pl.DataFrame:
    """Brute-force k-nearest lookup.  Fine for small point sets (<50K).

    For larger sets we'd want a proper spatial index, but the HRSA facility
    list is only ~7K rows so this is plenty fast.
    """
    distances = [
        haversine(lat, lon, plat, plon)
        for plat, plon in zip(points[lat_col].to_list(), points[lon_col].to_list())
    ]
    result = points.with_columns(pl.Series(name="distance_km", values=distances))
    return result.sort("distance_km").head(k)


# ------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------

_FIPS_RE = re.compile(r"^\d{2,11}$")


def is_valid_fips(code: str) -> bool:
    """Quick check that a string looks like a FIPS code."""
    return bool(_FIPS_RE.match(code.strip()))


def is_valid_latlon(lat: Any, lon: Any) -> bool:
    """Sanity check for lat/lon values.

    We've seen a few NCES records with lat=0, lon=0 (somewhere in the Gulf
    of Guinea) and some CDC data with lon > 0 for US locations.
    """
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return False

    if lat_f == 0.0 and lon_f == 0.0:
        return False
    # rough bounding box for US + territories
    return 17.0 <= lat_f <= 72.0 and -180.0 <= lon_f <= -65.0
