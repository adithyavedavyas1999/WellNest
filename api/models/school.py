"""
School-related Pydantic models.

SchoolSummary is the lightweight version for list endpoints — we pull
just enough data to render a card. SchoolDetail carries all the pillar
breakdowns, metrics, and prediction info for the detail page.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from api.models.common import GeoPoint, Pillar, ScoreCategory


class PillarScore(BaseModel):
    pillar: Pillar
    score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    national_percentile: int | None = Field(None, ge=0, le=100)


class SchoolSummary(BaseModel):
    """List/search result representation. Intentionally lean."""

    nces_id: str = Field(..., description="12-digit NCES school ID")
    name: str
    city: str
    state: str = Field(..., max_length=2)
    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    enrollment: int | None = None
    title_i: bool | None = None
    location: GeoPoint | None = None

    model_config = {"from_attributes": True}


class SchoolMetrics(BaseModel):
    """Raw metrics that feed into the scoring model."""

    math_proficiency: float | None = None
    reading_proficiency: float | None = None
    chronic_absenteeism_rate: float | None = None
    student_teacher_ratio: float | None = None
    poverty_rate: float | None = None
    uninsured_children_rate: float | None = None
    food_desert: bool | None = None
    hpsa_score: float | None = None
    aqi_avg: float | None = None
    violent_crime_rate: float | None = None
    social_vulnerability: float | None = None


class SchoolDetail(BaseModel):
    """Full school profile — used on the detail page and school explorer."""

    nces_id: str
    name: str
    address: str | None = None
    city: str
    state: str
    zip_code: str | None = None
    county_fips: str | None = None
    county_name: str | None = None
    school_type: str | None = None
    grade_range: str | None = None
    enrollment: int | None = None
    title_i: bool | None = None
    location: GeoPoint | None = None

    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    national_rank: int | None = None
    state_rank: int | None = None

    pillar_scores: list[PillarScore] = []
    metrics: SchoolMetrics | None = None

    # year-over-year delta, if we have historical data
    score_change_1y: float | None = None

    prediction: SchoolPrediction | None = None

    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class SchoolPrediction(BaseModel):
    """Next-year proficiency change prediction from the XGBoost model."""

    nces_id: str
    predicted_score_change: float
    confidence_interval_low: float
    confidence_interval_high: float
    risk_flag: bool = Field(
        ...,
        description="True if model predicts significant decline",
    )
    top_contributing_factors: list[str] = []
    model_version: str | None = None
    predicted_at: date | None = None


# needed for forward reference in SchoolDetail
SchoolDetail.model_rebuild()
