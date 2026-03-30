"""
Shared response models used across multiple endpoints.

The generic PaginatedResponse is the workhorse — every list endpoint wraps
its results in this so the frontend always gets the same pagination envelope.
"""

from __future__ import annotations

import math
from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ScoreCategory(StrEnum):
    critical = "critical"  # 0-25
    at_risk = "at_risk"  # 26-50
    moderate = "moderate"  # 51-75
    thriving = "thriving"  # 76-100


class Pillar(StrEnum):
    education = "education"
    health = "health"
    environment = "environment"
    safety = "safety"


# ---------------------------------------------------------------------------
# Common building blocks
# ---------------------------------------------------------------------------


class GeoPoint(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)


class StateFilter(BaseModel):
    """Used in dropdown-style responses where we list available states."""

    code: str = Field(..., max_length=2, description="2-letter state code")
    name: str
    school_count: int


# ---------------------------------------------------------------------------
# Pagination wrapper
# ---------------------------------------------------------------------------


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int
    per_page: int
    pages: int

    @classmethod
    def build(cls, items: list[T], total: int, page: int, per_page: int) -> PaginatedResponse[T]:
        return cls(
            items=items,
            total=total,
            page=page,
            per_page=per_page,
            pages=max(1, math.ceil(total / per_page)),
        )


# ---------------------------------------------------------------------------
# Error / health responses
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    detail: str
    status_code: int = 400
    timestamp: datetime | None = None


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    environment: str
    database: str = "unknown"  # "connected" or "unreachable"
    timestamp: datetime
