"""
Database helpers for the Streamlit dashboard.

Supports two backends:
  1. PostgreSQL (when DATABASE_URL is set) — production / local Docker dev
  2. DuckDB in-memory loaded from sample CSVs — Streamlit Cloud / demo mode

The rest of the dashboard calls run_query(sql) and never knows the difference.
DuckDB supports the same SQL syntax (::numeric casts, FILTER(WHERE ...), etc.).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

logger = logging.getLogger("wellnest.dashboard.db")

_DEFAULT_TTL = 600  # 10 min

SAMPLE_DATA_DIR = Path(__file__).resolve().parent.parent / "sample_data"

_USING_DUCKDB: bool = False


def _has_postgres() -> bool:
    """Check if a real PostgreSQL connection is available."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return False
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_url, pool_size=1, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


@st.cache_resource
def _get_duckdb_conn():
    """
    Create an in-memory DuckDB database seeded from the sample CSVs.
    Creates gold.* and silver.* schemas/views so all dashboard SQL works.
    """
    import duckdb

    conn = duckdb.connect(":memory:")

    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    csv_to_table = {
        "child_wellbeing_scores.csv": "gold.child_wellbeing_score",
        "county_summary.csv": "gold.county_summary",
        "trend_metrics.csv": "gold.trend_metrics",
        "resource_gaps.csv": "gold.resource_gaps",
        "anomalies.csv": "gold.anomalies",
        "county_ai_briefs.csv": "gold.county_ai_briefs",
        "quality_flags.csv": "gold.quality_flags",
        "school_profiles.csv": "silver.school_profiles",
    }

    for csv_file, table_name in csv_to_table.items():
        csv_path = SAMPLE_DATA_DIR / csv_file
        if csv_path.exists():
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
            count = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            logger.info("Loaded %s -> %s (%d rows)", csv_file, table_name, count)
        else:
            logger.warning("Sample CSV not found: %s", csv_path)

    return conn


@st.cache_resource
def get_engine():
    """
    Returns a SQLAlchemy engine for PostgreSQL, or None if using DuckDB.
    """
    global _USING_DUCKDB

    if _has_postgres():
        from sqlalchemy import create_engine

        db_url = os.getenv("DATABASE_URL")
        _USING_DUCKDB = False
        return create_engine(
            db_url,
            pool_size=3,
            max_overflow=5,
            pool_recycle=1800,
            pool_pre_ping=True,
        )

    _USING_DUCKDB = True
    return None


def run_query(
    sql: str,
    params: dict[str, Any] | None = None,
    ttl: int = _DEFAULT_TTL,
) -> pd.DataFrame:
    """
    Execute a read-only query and return a DataFrame.
    Works with both PostgreSQL and DuckDB backends.
    """
    return _cached_query(sql, _freeze_params(params), ttl)


@st.cache_data(ttl=_DEFAULT_TTL, show_spinner=False)
def _cached_query(sql: str, params_tuple: tuple, ttl: int) -> pd.DataFrame:
    engine = get_engine()

    if engine is not None:
        from sqlalchemy import text

        param_dict = dict(params_tuple) if params_tuple else None
        try:
            with engine.connect() as conn:
                return pd.read_sql(text(sql), conn, params=param_dict)
        except Exception:
            logger.exception("PostgreSQL query failed: %s", sql[:200])
            return pd.DataFrame()
    else:
        conn = _get_duckdb_conn()
        adapted_sql = _adapt_sql_for_duckdb(sql, dict(params_tuple) if params_tuple else None)
        try:
            result = conn.execute(adapted_sql).fetchdf()
            return result
        except Exception:
            logger.exception("DuckDB query failed: %s", adapted_sql[:200])
            return pd.DataFrame()


def _adapt_sql_for_duckdb(sql: str, params: dict | None) -> str:
    """
    Substitute :param_name placeholders with literal values for DuckDB.
    DuckDB's Python API doesn't support SQLAlchemy-style :name params.
    """
    if not params:
        return sql
    result = sql
    for key, value in params.items():
        placeholder = f":{key}"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            result = result.replace(placeholder, f"'{escaped}'")
        elif value is None:
            result = result.replace(placeholder, "NULL")
        else:
            result = result.replace(placeholder, str(value))
    return result


def _freeze_params(params: dict[str, Any] | None) -> tuple:
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


def get_school_detail(nces_id: str) -> dict | None:
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


def get_county_summary(fips: str) -> dict | None:
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
    """Quick connectivity check."""
    engine = get_engine()
    if engine is not None:
        try:
            from sqlalchemy import text

            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                if result == 1:
                    return True, "Connected"
        except Exception as exc:
            return False, str(exc)[:120]
        return False, "Unexpected state"
    else:
        try:
            conn = _get_duckdb_conn()
            result = conn.execute("SELECT 1").fetchone()[0]
            if result == 1:
                return True, "Connected (demo mode)"
        except Exception as exc:
            return False, str(exc)[:120]
        return False, "DuckDB unavailable"


def get_data_freshness() -> str | None:
    """Last time gold.child_wellbeing_score was updated."""
    df = run_query("""
        SELECT max(updated_at) AS last_update
        FROM gold.child_wellbeing_score
    """)
    if df.empty or pd.isna(df.iloc[0]["last_update"]):
        return None
    return str(df.iloc[0]["last_update"])
