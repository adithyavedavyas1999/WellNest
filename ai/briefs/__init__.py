"""Community brief generation for US counties."""

from ai.briefs.generator import BriefGenerator
from ai.briefs.prompts import (
    ANOMALY_NARRATIVE_SYSTEM,
    ANOMALY_NARRATIVE_USER,
    COUNTY_BRIEF_SYSTEM,
    COUNTY_BRIEF_USER,
    DATA_QUALITY_SYSTEM,
    DATA_QUALITY_USER,
)

__all__: list[str] = [
    "BriefGenerator",
    "COUNTY_BRIEF_SYSTEM",
    "COUNTY_BRIEF_USER",
    "ANOMALY_NARRATIVE_SYSTEM",
    "ANOMALY_NARRATIVE_USER",
    "DATA_QUALITY_SYSTEM",
    "DATA_QUALITY_USER",
]
