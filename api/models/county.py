"""
County-level response models.

County data comes from gold.county_summary (dbt) and gold.county_ai_briefs (LLM).
We serve the AI brief as a plain string — the frontend renders it as markdown.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from api.models.common import GeoPoint, ScoreCategory


class CountySummary(BaseModel):
    """Compact representation for list/map views."""

    fips: str = Field(..., description="5-digit county FIPS code")
    name: str
    state: str = Field(..., max_length=2)
    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    school_count: int
    population: int | None = None

    model_config = {"from_attributes": True}


class CountyDetail(BaseModel):
    """
    Full county profile with AI-generated brief.

    The pillar scores here are averages across all schools in the county,
    weighted by enrollment. Not perfect, but good enough for the county view.
    """

    fips: str
    name: str
    state: str
    composite_score: float = Field(..., ge=0, le=100)
    category: ScoreCategory
    school_count: int
    population: int | None = None
    centroid: GeoPoint | None = None

    education_score: float | None = None
    health_score: float | None = None
    environment_score: float | None = None
    safety_score: float | None = None

    # enrollment-weighted mean across county schools
    avg_poverty_rate: float | None = None
    avg_chronic_absenteeism: float | None = None
    pct_title_i: float | None = None

    # the GPT-generated brief (stored in gold.county_ai_briefs)
    ai_brief: str | None = None

    score_change_1y: float | None = None
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class CountyBrief(BaseModel):
    """Just the AI brief — for the /counties/{fips} detail endpoint sidebar."""

    fips: str
    county_name: str
    state: str
    brief: str
    generated_at: datetime | None = None
