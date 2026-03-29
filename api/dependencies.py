"""
FastAPI dependency injection — database sessions, auth, pagination, query filters.

We use sync SQLAlchemy here because the actual queries are all simple selects
against Postgres views/tables built by dbt. Async would add complexity for
no real throughput gain at our scale (~100 req/s max).
"""

from __future__ import annotations

import logging
from typing import Annotated, Generator, Optional

from fastapi import Depends, Header, HTTPException, Query, status
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from api.config import Settings, get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_engine = None
_SessionLocal: sessionmaker | None = None  # type: ignore[type-arg]


def _get_engine(settings: Settings) -> None:
    global _engine, _SessionLocal
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=True,
        )
        _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
        logger.info("Database engine created for %s", settings.database_url.split("@")[-1])


def get_db(settings: Annotated[Settings, Depends(get_settings)]) -> Generator[Session, None, None]:
    """Yield a database session, auto-close on request teardown."""
    _get_engine(settings)
    assert _SessionLocal is not None
    session = _SessionLocal()
    try:
        yield session
    finally:
        session.close()


def close_db_pool() -> None:
    """Called on shutdown to clean up the connection pool."""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
        _engine = None
        _SessionLocal = None
        logger.info("Database connection pool disposed")


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def verify_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[Optional[str], Header(alias="X-API-Key")] = None,
) -> Optional[str]:
    """
    If API_KEY is configured in settings, require it on every request.
    If it's not set (dev mode), skip auth entirely.
    """
    if settings.api_key is None:
        return None

    if x_api_key is None or x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    return x_api_key


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class PaginationParams:
    """Parsed pagination params with sane defaults and upper bounds."""

    def __init__(
        self,
        page: Annotated[int, Query(ge=1, description="Page number (1-indexed)")] = 1,
        per_page: Annotated[int, Query(ge=1, le=200, description="Items per page")] = 50,
    ):
        self.page = page
        self.per_page = per_page

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page


# ---------------------------------------------------------------------------
# Common query filters
# ---------------------------------------------------------------------------

class SchoolFilters:
    """Query params shared across several school-related endpoints."""

    def __init__(
        self,
        state: Annotated[Optional[str], Query(max_length=2, description="2-letter state code")] = None,
        score_below: Annotated[Optional[float], Query(ge=0, le=100, description="Max composite score")] = None,
        score_above: Annotated[Optional[float], Query(ge=0, le=100, description="Min composite score")] = None,
        pillar: Annotated[Optional[str], Query(description="Filter by pillar: education, health, environment, safety")] = None,
        title_i: Annotated[Optional[bool], Query(description="Title I eligible schools only")] = None,
    ):
        self.state = state.upper() if state else None
        self.score_below = score_below
        self.score_above = score_above
        self.pillar = pillar.lower() if pillar else None
        self.title_i = title_i

        # catch bad pillar values early
        valid_pillars = {"education", "health", "environment", "safety"}
        if self.pillar and self.pillar not in valid_pillars:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid pillar '{self.pillar}'. Must be one of: {', '.join(sorted(valid_pillars))}",
            )


class CountyFilters:
    """Query params for county endpoints."""

    def __init__(
        self,
        state: Annotated[Optional[str], Query(max_length=2, description="2-letter state code")] = None,
        score_below: Annotated[Optional[float], Query(ge=0, le=100)] = None,
        score_above: Annotated[Optional[float], Query(ge=0, le=100)] = None,
        min_schools: Annotated[Optional[int], Query(ge=1, description="Minimum school count in county")] = None,
    ):
        self.state = state.upper() if state else None
        self.score_below = score_below
        self.score_above = score_above
        self.min_schools = min_schools
