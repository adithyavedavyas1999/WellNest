"""
API response models — re-exported here for convenience.

Usage:
    from api.models import SchoolSummary, PaginatedResponse
"""

from api.models.common import (
    ErrorResponse,
    GeoPoint,
    HealthResponse,
    PaginatedResponse,
    Pillar,
    ScoreCategory,
    StateFilter,
)
from api.models.county import CountyBrief, CountyDetail, CountySummary
from api.models.school import (
    SchoolDetail,
    SchoolMetrics,
    SchoolPrediction,
    SchoolSummary,
)
from api.models.score import (
    Anomaly,
    RankingEntry,
    ResourceGap,
    ScoreBreakdown,
    WellbeingScore,
)

__all__ = [
    "Anomaly",
    "CountyBrief",
    "CountyDetail",
    "CountySummary",
    "ErrorResponse",
    "GeoPoint",
    "HealthResponse",
    "PaginatedResponse",
    "Pillar",
    "RankingEntry",
    "ResourceGap",
    "SchoolDetail",
    "SchoolMetrics",
    "SchoolPrediction",
    "SchoolSummary",
    "ScoreBreakdown",
    "ScoreCategory",
    "StateFilter",
    "WellbeingScore",
]
