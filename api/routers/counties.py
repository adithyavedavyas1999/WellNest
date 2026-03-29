"""
County endpoints.

Counties are aggregated from school-level data in gold.county_summary.
The AI brief is a separate join to gold.county_ai_briefs — not every
county has one yet (we generate them in batches of ~500 to avoid
burning through the OpenAI budget).
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import CountyFilters, PaginationParams, get_db
from api.models.common import GeoPoint, PaginatedResponse, ScoreCategory
from api.models.county import CountyDetail, CountySummary
from api.models.school import SchoolSummary

logger = logging.getLogger(__name__)
router = APIRouter(tags=["counties"])


def _score_to_category(score: float) -> ScoreCategory:
    if score <= 25:
        return ScoreCategory.critical
    elif score <= 50:
        return ScoreCategory.at_risk
    elif score <= 75:
        return ScoreCategory.moderate
    return ScoreCategory.thriving


def _build_county_where(filters: CountyFilters) -> tuple[str, dict]:
    clauses: list[str] = []
    params: dict = {}

    if filters.state:
        clauses.append("c.state = :state")
        params["state"] = filters.state
    if filters.score_below is not None:
        clauses.append("c.composite_score <= :score_below")
        params["score_below"] = filters.score_below
    if filters.score_above is not None:
        clauses.append("c.composite_score >= :score_above")
        params["score_above"] = filters.score_above
    if filters.min_schools is not None:
        clauses.append("c.school_count >= :min_schools")
        params["min_schools"] = filters.min_schools

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ---------------------------------------------------------------------------
# GET /counties
# ---------------------------------------------------------------------------

@router.get("/counties", response_model=PaginatedResponse[CountySummary])
def list_counties(
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    filters: CountyFilters = Depends(),
) -> PaginatedResponse[CountySummary]:
    where, params = _build_county_where(filters)

    total = db.execute(
        text(f"SELECT count(*) FROM gold.county_summary c {where}"), params
    ).scalar() or 0

    query = f"""
        SELECT
            c.fips,
            c.county_name AS name,
            c.state,
            c.composite_score,
            c.school_count,
            c.population
        FROM gold.county_summary c
        {where}
        ORDER BY c.composite_score ASC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = pagination.per_page
    params["offset"] = pagination.offset

    rows = db.execute(text(query), params).mappings().all()

    items = [
        CountySummary(
            fips=r["fips"],
            name=r["name"],
            state=r["state"],
            composite_score=r["composite_score"],
            category=_score_to_category(r["composite_score"]),
            school_count=r["school_count"],
            population=r.get("population"),
        )
        for r in rows
    ]

    return PaginatedResponse.build(items, total, pagination.page, pagination.per_page)


# ---------------------------------------------------------------------------
# GET /counties/{fips}
# ---------------------------------------------------------------------------

@router.get("/counties/{fips}", response_model=CountyDetail)
def get_county(fips: str, db: Session = Depends(get_db)) -> CountyDetail:
    query = text("""
        SELECT
            c.fips,
            c.county_name,
            c.state,
            c.composite_score,
            c.school_count,
            c.population,
            c.centroid_lat,
            c.centroid_lon,
            c.education_score,
            c.health_score,
            c.environment_score,
            c.safety_score,
            c.avg_poverty_rate,
            c.avg_chronic_absenteeism,
            c.pct_title_i,
            c.score_change_1y,
            c.updated_at,
            b.brief AS ai_brief
        FROM gold.county_summary c
        LEFT JOIN gold.county_ai_briefs b ON b.fips = c.fips
        WHERE c.fips = :fips
    """)

    row = db.execute(query, {"fips": fips}).mappings().first()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"County {fips} not found",
        )

    centroid = None
    if row.get("centroid_lat") and row.get("centroid_lon"):
        centroid = GeoPoint(lat=row["centroid_lat"], lon=row["centroid_lon"])

    return CountyDetail(
        fips=row["fips"],
        name=row["county_name"],
        state=row["state"],
        composite_score=row["composite_score"],
        category=_score_to_category(row["composite_score"]),
        school_count=row["school_count"],
        population=row.get("population"),
        centroid=centroid,
        education_score=row.get("education_score"),
        health_score=row.get("health_score"),
        environment_score=row.get("environment_score"),
        safety_score=row.get("safety_score"),
        avg_poverty_rate=row.get("avg_poverty_rate"),
        avg_chronic_absenteeism=row.get("avg_chronic_absenteeism"),
        pct_title_i=row.get("pct_title_i"),
        ai_brief=row.get("ai_brief"),
        score_change_1y=row.get("score_change_1y"),
        updated_at=row.get("updated_at"),
    )


# ---------------------------------------------------------------------------
# GET /counties/{fips}/schools
# ---------------------------------------------------------------------------

@router.get("/counties/{fips}/schools", response_model=PaginatedResponse[SchoolSummary])
def list_county_schools(
    fips: str,
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
) -> PaginatedResponse[SchoolSummary]:
    """All schools within a given county, ordered by composite score."""
    total = db.execute(
        text("SELECT count(*) FROM gold.child_wellbeing_score WHERE county_fips = :fips"),
        {"fips": fips},
    ).scalar() or 0

    if total == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No schools found in county {fips}",
        )

    query = text("""
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
        WHERE s.county_fips = :fips
        ORDER BY s.composite_score ASC
        LIMIT :limit OFFSET :offset
    """)

    rows = db.execute(query, {
        "fips": fips,
        "limit": pagination.per_page,
        "offset": pagination.offset,
    }).mappings().all()

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
