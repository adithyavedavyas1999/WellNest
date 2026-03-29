"""
School-related Pydantic models.

SchoolSummary is the lightweight version for list endpoints — we pull
just enough data to render a card. SchoolDetail carries all the pillar
breakdowns, metrics, and prediction info for the detail page.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field

from api.models.common import GeoPoint, Pillar, ScoreCategory


class PillarScore(BaseModel):
    pillar: Pillar
    score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    national_percentile: Optional[int] = Field(None, ge=0, le=100)


class SchoolSummary(BaseModel):
    """List/search result representation. Intentionally lean."""
    nces_id: str = Field(..., description="12-digit NCES school ID")
    name: str
    city: str
    state: str = Field(..., max_length=2)
    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    enrollment: Optional[int] = None
    title_i: Optional[bool] = None
    location: Optional[GeoPoint] = None

    model_config = {"from_attributes": True}


class SchoolMetrics(BaseModel):
    """Raw metrics that feed into the scoring model."""
    math_proficiency: Optional[float] = None
    reading_proficiency: Optional[float] = None
    chronic_absenteeism_rate: Optional[float] = None
    student_teacher_ratio: Optional[float] = None
    poverty_rate: Optional[float] = None
    uninsured_children_rate: Optional[float] = None
    food_desert: Optional[bool] = None
    hpsa_score: Optional[float] = None
    aqi_avg: Optional[float] = None
    violent_crime_rate: Optional[float] = None
    social_vulnerability: Optional[float] = None


class SchoolDetail(BaseModel):
    """Full school profile — used on the detail page and school explorer."""
    nces_id: str
    name: str
    address: Optional[str] = None
    city: str
    state: str
    zip_code: Optional[str] = None
    county_fips: Optional[str] = None
    county_name: Optional[str] = None
    school_type: Optional[str] = None
    grade_range: Optional[str] = None
    enrollment: Optional[int] = None
    title_i: Optional[bool] = None
    location: Optional[GeoPoint] = None

    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    national_rank: Optional[int] = None
    state_rank: Optional[int] = None

    pillar_scores: list[PillarScore] = []
    metrics: Optional[SchoolMetrics] = None

    # year-over-year delta, if we have historical data
    score_change_1y: Optional[float] = None

    prediction: Optional[SchoolPrediction] = None

    updated_at: Optional[datetime] = None

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
    model_version: Optional[str] = None
    predicted_at: Optional[date] = None


# needed for forward reference in SchoolDetail
SchoolDetail.model_rebuild()
