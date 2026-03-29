"""
Shared utilities for the ingestion layer.

Convention: connectors import what they need from here rather than reaching
into sub-modules directly.  Keeps the import lines short.
"""

from __future__ import annotations

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ingestion.utils.geo_utils import (
    add_h3_column,
    county_fips,
    find_nearest,
    format_fips,
    haversine,
    is_valid_fips,
    is_valid_latlon,
    latlng_to_h3,
    normalize_fips_column,
    parse_fips,
)
from ingestion.utils.http_client import RateLimiter, WellNestHTTPClient, retry_on_http_error

__all__ = [
    "WellNestHTTPClient",
    "RateLimiter",
    "retry_on_http_error",
    "haversine",
    "format_fips",
    "parse_fips",
    "county_fips",
    "normalize_fips_column",
    "latlng_to_h3",
    "add_h3_column",
    "find_nearest",
    "is_valid_fips",
    "is_valid_latlon",
    "get_pg_engine",
    "get_pg_url",
    "ensure_schema",
]


def get_pg_url() -> str:
    """Read the Postgres connection string from the environment."""
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    # fall back to building it from individual vars (docker-compose sets these)
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "wellnest")
    user = os.environ.get("POSTGRES_USER", "wellnest")
    pw = os.environ.get("POSTGRES_PASSWORD", "changeme")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


_engine_cache: Engine | None = None


def get_pg_engine(fresh: bool = False) -> Engine:
    """Return a cached SQLAlchemy engine.

    We cache a single engine because creating one per connector per run
    leaks connections under dagster -- learned that the hard way during
    a weekend data backfill that killed the free-tier Supabase instance.
    """
    global _engine_cache
    if _engine_cache is None or fresh:
        _engine_cache = create_engine(
            get_pg_url(),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
    return _engine_cache


def ensure_schema(schema_name: str = "raw") -> None:
    """Create a Postgres schema if it doesn't already exist."""
    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
