"""
Database helpers for the Streamlit dashboard.

Uses st.cache_resource for the engine (one per process) and st.cache_data for
query results. Default TTL is 10 minutes — long enough that refreshing a page
doesn't hammer the DB, short enough that data stays reasonably fresh.

NOTE: The dashboard reads from gold.* and silver.* schemas. It never writes.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger("wellnest.dashboard.db")

_DEFAULT_TTL = 600  # 10 min


@st.cache_resource
def get_engine() -> Engine:
    """
    Single shared engine for the whole dashboard process.

    Reads DATABASE_URL from env; falls back to local dev defaults.
    We keep the pool small — Streamlit isn't handling 50 concurrent requests.
    """
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://wellnest:changeme@localhost:5432/wellnest",
    )
    return create_engine(
        db_url,
        pool_size=3,
        max_overflow=5,
        pool_recycle=1800,
        pool_pre_ping=True,
    )


def run_query(
    sql: str,
    params: Optional[dict[str, Any]] = None,
    ttl: int = _DEFAULT_TTL,
) -> pd.DataFrame:
    """
    Execute a read-only query and return a DataFrame. Results are cached by
    Streamlit based on the SQL text + params hash.

    We return pandas here instead of polars because Streamlit's native
    dataframe widget works best with pandas. Annoying, but less friction.
    """
    return _cached_query(sql, _freeze_params(params), ttl)


@st.cache_data(ttl=_DEFAULT_TTL, show_spinner=False)
def _cached_query(sql: str, params_tuple: tuple, ttl: int) -> pd.DataFrame:
    engine = get_engine()
    param_dict = dict(params_tuple) if params_tuple else None
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=param_dict)
    except Exception:
        logger.exception("Query failed: %s", sql[:200])
        return pd.DataFrame()


def _freeze_params(params: Optional[dict[str, Any]]) -> tuple:
    """Convert dict to sorted tuple of pairs so it's hashable for st.cache."""
    if not params:
        return ()
    return tuple(sorted(params.items()))


def get_states() -> list[str]:
    """All states that have at least one scored school."""
    df = run_query("""
        SELECT DISTINCT state
        FROM gold.child_wellbeing_score
        ORDER BY state
    """)
    return df["state"].tolist() if not df.empty else []


def get_school_detail(nces_id: str) -> Optional[dict]:
    """Pull everything we know about one school."""
    df = run_query(
        """
        SELECT
            s.nces_id, s.name, s.city, s.state, s.county_fips, s.county_name,
            s.school_type, s.grade_range, s.enrollment, s.title_i,
            s.latitude, s.longitude,
            cws.composite_score, cws.education_score, cws.health_score,
            cws.environment_score, cws.safety_score, cws.category,
            cws.national_rank, cws.state_rank, cws.score_change_1y
        FROM silver.school_profiles s
        JOIN gold.child_wellbeing_score cws ON s.nces_id = cws.nces_id
        WHERE s.nces_id = :nces_id
        """,
        {"nces_id": nces_id},
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_county_summary(fips: str) -> Optional[dict]:
    """County-level aggregate stats."""
    df = run_query(
        """
        SELECT *
        FROM gold.county_summary
        WHERE fips = :fips
        """,
        {"fips": fips},
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def check_db_health() -> tuple[bool, str]:
    """Quick connectivity check. Returns (ok, message)."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                return True, "Connected"
    except Exception as exc:
        return False, str(exc)[:120]
    return False, "Unexpected state"


def get_data_freshness() -> Optional[str]:
    """Last time gold.child_wellbeing_score was updated."""
    df = run_query("""
        SELECT max(updated_at) AS last_update
        FROM gold.child_wellbeing_score
    """)
    if df.empty or pd.isna(df.iloc[0]["last_update"]):
        return None
    return str(df.iloc[0]["last_update"])
