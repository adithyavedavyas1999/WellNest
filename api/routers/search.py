"""
Search endpoint — text matching on school name, city, and state.

Uses postgres ILIKE for now. If we ever need fuzzy matching or
autocomplete-style behavior, we should swap to pg_trgm and a GIN
index. The current approach handles the ~130K school dataset fine
since the dbt model already has a btree on school_name.
"""

from __future__ import annotations

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import PaginationParams, get_db
from api.models.common import GeoPoint, PaginatedResponse, ScoreCategory
from api.models.school import SchoolSummary

logger = logging.getLogger(__name__)
router = APIRouter(tags=["search"])


def _score_to_category(score: float) -> ScoreCategory:
    if score <= 25:
        return ScoreCategory.critical
    elif score <= 50:
        return ScoreCategory.at_risk
    elif score <= 75:
        return ScoreCategory.moderate
    return ScoreCategory.thriving


@router.get("/search", response_model=PaginatedResponse[SchoolSummary])
def search_schools(
    q: Annotated[str, Query(min_length=2, max_length=200, description="Search query")],
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    state: Annotated[Optional[str], Query(max_length=2, description="Narrow by state")] = None,
) -> PaginatedResponse[SchoolSummary]:
    """
    Search schools by name, city, or state. We match against all three
    columns with OR so that searching 'Springfield IL' works intuitively.
    """
    like_pattern = f"%{q}%"

    where_clauses = [
        "(s.school_name ILIKE :q OR s.city ILIKE :q OR s.state ILIKE :q)"
    ]
    params: dict = {"q": like_pattern}

    if state:
        where_clauses.append("s.state = :state")
        params["state"] = state.upper()

    where = "WHERE " + " AND ".join(where_clauses)

    total = db.execute(
        text(f"SELECT count(*) FROM gold.child_wellbeing_score s {where}"),
        params,
    ).scalar() or 0

    query = f"""
        SELECT
            s.nces_id,
            s.school_name AS name,
            s.city,
            s.state,
            s.composite_score,
            s.enrollment,
            s.title_i,
            s.latitude,
            s.longitude
        FROM gold.child_wellbeing_score s
        {where}
        ORDER BY s.composite_score DESC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = pagination.per_page
    params["offset"] = pagination.offset

    rows = db.execute(text(query), params).mappings().all()

    items = [
        SchoolSummary(
            nces_id=r["nces_id"],
            name=r["name"],
            city=r["city"],
            state=r["state"],
            composite_score=r["composite_score"],
            category=_score_to_category(r["composite_score"]),
            enrollment=r.get("enrollment"),
            title_i=r.get("title_i"),
            location=GeoPoint(lat=r["latitude"], lon=r["longitude"])
            if r.get("latitude") and r.get("longitude")
            else None,
        )
        for r in rows
    ]

    return PaginatedResponse.build(items, total, pagination.page, pagination.per_page)
