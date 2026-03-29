"""
School endpoints — the meat of the API.

All data comes from dbt gold tables:
  - gold.child_wellbeing_score  (per-school composite + pillar scores)
  - gold.school_rankings        (national + state rank)
  - gold.school_predictions     (XGBoost next-year predictions)
  - gold.resource_gaps          (HPSA/food desert gap analysis)

We use raw SQL via text() because it maps cleanly to the dbt output
and keeps us from needing an ORM model layer for read-only views.
"""

from __future__ import annotations

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import PaginationParams, SchoolFilters, get_db
from api.models.common import GeoPoint, PaginatedResponse, ScoreCategory
from api.models.school import SchoolDetail, SchoolMetrics, SchoolPrediction, SchoolSummary
from api.models.score import Anomaly, RankingEntry, ResourceGap

logger = logging.getLogger(__name__)
router = APIRouter(tags=["schools"])


def _score_to_category(score: float) -> ScoreCategory:
    if score <= 25:
        return ScoreCategory.critical
    elif score <= 50:
        return ScoreCategory.at_risk
    elif score <= 75:
        return ScoreCategory.moderate
    return ScoreCategory.thriving


def _build_school_where(filters: SchoolFilters) -> tuple[str, dict]:
    """Build WHERE clause fragments from filter params."""
    clauses: list[str] = []
    params: dict = {}

    if filters.state:
        clauses.append("s.state = :state")
        params["state"] = filters.state
    if filters.score_below is not None:
        clauses.append("s.composite_score <= :score_below")
        params["score_below"] = filters.score_below
    if filters.score_above is not None:
        clauses.append("s.composite_score >= :score_above")
        params["score_above"] = filters.score_above
    if filters.title_i is not None:
        clauses.append("s.title_i = :title_i")
        params["title_i"] = filters.title_i

    where = " AND ".join(clauses)
    if where:
        where = "WHERE " + where
    return where, params


# ---------------------------------------------------------------------------
# GET /schools
# ---------------------------------------------------------------------------

@router.get("/schools", response_model=PaginatedResponse[SchoolSummary])
def list_schools(
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    filters: SchoolFilters = Depends(),
) -> PaginatedResponse[SchoolSummary]:
    where, params = _build_school_where(filters)

    count_sql = f"SELECT count(*) FROM gold.child_wellbeing_score s {where}"
    total = db.execute(text(count_sql), params).scalar() or 0

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
        ORDER BY s.composite_score ASC
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


# ---------------------------------------------------------------------------
# GET /schools/{nces_id}
# ---------------------------------------------------------------------------

@router.get("/schools/{nces_id}", response_model=SchoolDetail)
def get_school(nces_id: str, db: Session = Depends(get_db)) -> SchoolDetail:
    query = text("""
        SELECT
            s.nces_id,
            s.school_name,
            s.address,
            s.city,
            s.state,
            s.zip_code,
            s.county_fips,
            s.county_name,
            s.school_type,
            s.grade_range,
            s.enrollment,
            s.title_i,
            s.latitude,
            s.longitude,
            s.composite_score,
            s.education_score,
            s.health_score,
            s.environment_score,
            s.safety_score,
            s.score_change_1y,
            s.math_proficiency,
            s.reading_proficiency,
            s.chronic_absenteeism_rate,
            s.student_teacher_ratio,
            s.poverty_rate,
            s.uninsured_children_rate,
            s.food_desert,
            s.hpsa_score,
            s.aqi_avg,
            s.violent_crime_rate,
            s.social_vulnerability,
            s.updated_at,
            r.national_rank,
            r.state_rank
        FROM gold.child_wellbeing_score s
        LEFT JOIN gold.school_rankings r ON r.nces_id = s.nces_id
        WHERE s.nces_id = :nces_id
    """)

    row = db.execute(query, {"nces_id": nces_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"School {nces_id} not found")

    from api.models.school import PillarScore as SchoolPillarScore
    from api.models.common import Pillar

    pillar_scores = []
    for pillar_name, col in [
        (Pillar.education, "education_score"),
        (Pillar.health, "health_score"),
        (Pillar.environment, "environment_score"),
        (Pillar.safety, "safety_score"),
    ]:
        val = row.get(col)
        if val is not None:
            pillar_scores.append(SchoolPillarScore(
                pillar=pillar_name,
                score=val,
                category=_score_to_category(val),
            ))

    metrics = SchoolMetrics(
        math_proficiency=row.get("math_proficiency"),
        reading_proficiency=row.get("reading_proficiency"),
        chronic_absenteeism_rate=row.get("chronic_absenteeism_rate"),
        student_teacher_ratio=row.get("student_teacher_ratio"),
        poverty_rate=row.get("poverty_rate"),
        uninsured_children_rate=row.get("uninsured_children_rate"),
        food_desert=row.get("food_desert"),
        hpsa_score=row.get("hpsa_score"),
        aqi_avg=row.get("aqi_avg"),
        violent_crime_rate=row.get("violent_crime_rate"),
        social_vulnerability=row.get("social_vulnerability"),
    )

    location = None
    if row.get("latitude") and row.get("longitude"):
        location = GeoPoint(lat=row["latitude"], lon=row["longitude"])

    return SchoolDetail(
        nces_id=row["nces_id"],
        name=row["school_name"],
        address=row.get("address"),
        city=row["city"],
        state=row["state"],
        zip_code=row.get("zip_code"),
        county_fips=row.get("county_fips"),
        county_name=row.get("county_name"),
        school_type=row.get("school_type"),
        grade_range=row.get("grade_range"),
        enrollment=row.get("enrollment"),
        title_i=row.get("title_i"),
        location=location,
        composite_score=row["composite_score"],
        category=_score_to_category(row["composite_score"]),
        national_rank=row.get("national_rank"),
        state_rank=row.get("state_rank"),
        pillar_scores=pillar_scores,
        metrics=metrics,
        score_change_1y=row.get("score_change_1y"),
        updated_at=row.get("updated_at"),
    )


# ---------------------------------------------------------------------------
# GET /schools/{nces_id}/predictions
# ---------------------------------------------------------------------------

@router.get("/schools/{nces_id}/predictions", response_model=SchoolPrediction)
def get_school_predictions(nces_id: str, db: Session = Depends(get_db)) -> SchoolPrediction:
    query = text("""
        SELECT
            p.nces_id,
            p.predicted_score_change,
            p.confidence_interval_low,
            p.confidence_interval_high,
            p.risk_flag,
            p.top_contributing_factors,
            p.model_version,
            p.predicted_at
        FROM gold.school_predictions p
        WHERE p.nces_id = :nces_id
        ORDER BY p.predicted_at DESC
        LIMIT 1
    """)

    row = db.execute(query, {"nces_id": nces_id}).mappings().first()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No predictions found for school {nces_id}",
        )

    # top_contributing_factors is stored as a postgres array
    factors = row.get("top_contributing_factors") or []
    if isinstance(factors, str):
        factors = [f.strip() for f in factors.split(",")]

    return SchoolPrediction(
        nces_id=row["nces_id"],
        predicted_score_change=row["predicted_score_change"],
        confidence_interval_low=row["confidence_interval_low"],
        confidence_interval_high=row["confidence_interval_high"],
        risk_flag=row["risk_flag"],
        top_contributing_factors=factors,
        model_version=row.get("model_version"),
        predicted_at=row.get("predicted_at"),
    )


# ---------------------------------------------------------------------------
# GET /rankings
# ---------------------------------------------------------------------------

@router.get("/rankings", response_model=PaginatedResponse[RankingEntry])
def get_rankings(
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    state: Annotated[Optional[str], Query(max_length=2, description="Filter by state")] = None,
) -> PaginatedResponse[RankingEntry]:
    """National or state-level school rankings by composite score."""
    rank_col = "r.state_rank" if state else "r.national_rank"
    where = ""
    params: dict = {}

    if state:
        where = "WHERE s.state = :state"
        params["state"] = state.upper()

    count_sql = f"""
        SELECT count(*) FROM gold.school_rankings r
        JOIN gold.child_wellbeing_score s ON s.nces_id = r.nces_id
        {where}
    """
    total = db.execute(text(count_sql), params).scalar() or 0

    query = f"""
        SELECT
            {rank_col} AS rank,
            r.nces_id,
            s.school_name,
            s.city,
            s.state,
            s.composite_score,
            s.score_change_1y
        FROM gold.school_rankings r
        JOIN gold.child_wellbeing_score s ON s.nces_id = r.nces_id
        {where}
        ORDER BY {rank_col} ASC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = pagination.per_page
    params["offset"] = pagination.offset

    rows = db.execute(text(query), params).mappings().all()

    items = [
        RankingEntry(
            rank=r["rank"],
            nces_id=r["nces_id"],
            school_name=r["school_name"],
            city=r["city"],
            state=r["state"],
            composite_score=r["composite_score"],
            category=_score_to_category(r["composite_score"]),
            score_change_1y=r.get("score_change_1y"),
        )
        for r in rows
    ]

    return PaginatedResponse.build(items, total, pagination.page, pagination.per_page)


# ---------------------------------------------------------------------------
# GET /anomalies
# ---------------------------------------------------------------------------

@router.get("/anomalies", response_model=PaginatedResponse[Anomaly])
def list_anomalies(
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    state: Annotated[Optional[str], Query(max_length=2)] = None,
    anomaly_type: Annotated[Optional[str], Query(description="'improvement' or 'decline'")] = None,
) -> PaginatedResponse[Anomaly]:
    """Schools flagged by isolation forest / z-score anomaly detection."""
    clauses: list[str] = []
    params: dict = {}

    if state:
        clauses.append("a.state = :state")
        params["state"] = state.upper()
    if anomaly_type:
        clauses.append("a.anomaly_type = :anomaly_type")
        params["anomaly_type"] = anomaly_type

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    total = db.execute(
        text(f"SELECT count(*) FROM gold.anomalies a {where}"), params
    ).scalar() or 0

    query = f"""
        SELECT
            a.nces_id,
            a.school_name,
            a.state,
            a.composite_score,
            a.score_change_1y,
            a.z_score,
            a.anomaly_type,
            a.narrative,
            a.detected_at
        FROM gold.anomalies a
        {where}
        ORDER BY abs(a.z_score) DESC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = pagination.per_page
    params["offset"] = pagination.offset

    rows = db.execute(text(query), params).mappings().all()

    items = [
        Anomaly(
            nces_id=r["nces_id"],
            school_name=r["school_name"],
            state=r["state"],
            composite_score=r["composite_score"],
            score_change_1y=r["score_change_1y"],
            z_score=r["z_score"],
            anomaly_type=r["anomaly_type"],
            narrative=r.get("narrative"),
            detected_at=r.get("detected_at"),
        )
        for r in rows
    ]

    return PaginatedResponse.build(items, total, pagination.page, pagination.per_page)
