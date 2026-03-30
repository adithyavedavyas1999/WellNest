"""
Shared pytest fixtures for the WellNest test suite.

Most tests mock the database — we don't require a running PostgreSQL instance
for the unit test suite.  Integration tests that do need a real DB are marked
with @pytest.mark.integration and skipped by default (see pyproject.toml).

The fixtures here try to mirror real-ish data shapes so tests catch schema
drift early.  If you change a gold model column, update the sample data here
and any broken tests will tell you exactly what downstream code needs fixing.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_db_session() -> MagicMock:
    """A mock SQLAlchemy Session that returns empty results by default.

    Tests can override `.execute().mappings().first()` etc. on the returned
    mock to inject whatever result set they need.
    """
    session = MagicMock()
    session.execute.return_value.mappings.return_value.first.return_value = None
    session.execute.return_value.mappings.return_value.all.return_value = []
    session.execute.return_value.scalar.return_value = 0
    return session


@pytest.fixture()
def db_url() -> str:
    """Database URL for tests — uses env var or a default that won't
    accidentally connect to production."""
    return os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://wellnest_test:test@localhost:5432/wellnest_test",
    )


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_school_row() -> dict[str, Any]:
    """A single row matching the shape of gold.child_wellbeing_score."""
    return {
        "nces_id": "170993000943",
        "school_name": "Lincoln Elementary School",
        "address": "123 Main St",
        "city": "Springfield",
        "state": "IL",
        "zip_code": "62701",
        "county_fips": "17167",
        "county_name": "Sangamon",
        "school_type": "Regular",
        "grade_range": "PK-5",
        "enrollment": 412,
        "title_i": True,
        "latitude": 39.7817,
        "longitude": -89.6501,
        "composite_score": 58.4,
        "education_score": 62.1,
        "health_score": 51.3,
        "environment_score": 67.8,
        "safety_score": 52.4,
        "score_change_1y": -2.3,
        "math_proficiency": 34.5,
        "reading_proficiency": 41.2,
        "chronic_absenteeism_rate": 22.1,
        "student_teacher_ratio": 18.3,
        "poverty_rate": 28.4,
        "uninsured_children_rate": 5.7,
        "food_desert": False,
        "hpsa_score": 14.0,
        "aqi_avg": 42.0,
        "violent_crime_rate": 312.5,
        "social_vulnerability": 0.65,
        "updated_at": "2025-03-15T12:00:00Z",
        "national_rank": 67432,
        "state_rank": 1205,
    }


@pytest.fixture()
def sample_county_data() -> dict[str, Any]:
    """A row matching the shape of gold.county_summary."""
    return {
        "fips": "17031",
        "county_name": "Cook County",
        "state": "IL",
        "composite_score": 52.7,
        "school_count": 1847,
        "population": 5275541,
        "education_score": 48.3,
        "health_score": 55.1,
        "environment_score": 58.9,
        "safety_score": 47.2,
        "avg_poverty_rate": 14.2,
        "avg_chronic_absenteeism": 19.8,
        "pct_title_i": 72.3,
        "thriving_count": 312,
        "moderate_count": 645,
        "at_risk_count": 589,
        "critical_count": 301,
        "schools_with_gaps": 423,
        "national_rank": 1542,
        "total_counties": 3143,
        "ai_brief": (
            "Cook County serves over 5.2 million residents across 1,847 assessed schools. "
            "The county's average Child Wellbeing Index score of 52.7/100 places it in the "
            "Moderate category, ranking 1,542nd nationally. Health and environment pillars "
            "show relative strength, while safety and education lag behind."
        ),
    }


@pytest.fixture()
def sample_school_list(sample_school_row: dict[str, Any]) -> list[dict[str, Any]]:
    """A small list of school rows for pagination/list tests."""
    schools = []
    for i in range(5):
        row = dict(sample_school_row)
        row["nces_id"] = f"17099300094{i}"
        row["school_name"] = f"Test School {i}"
        row["composite_score"] = 30.0 + i * 15.0
        row["city"] = ["Springfield", "Chicago", "Peoria", "Rockford", "Naperville"][i]
        schools.append(row)
    return schools


# ---------------------------------------------------------------------------
# API test client
# ---------------------------------------------------------------------------


@pytest.fixture()
def api_client(mock_db_session: MagicMock) -> Generator[TestClient, None, None]:
    """FastAPI TestClient with the DB dependency overridden to use our mock."""
    from api.dependencies import get_db
    from api.main import app

    def _override_db():
        yield mock_db_session

    app.dependency_overrides[get_db] = _override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Mock OpenAI
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_openai() -> Generator[MagicMock, None, None]:
    """Patch the OpenAI client so no real API calls are made."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = (
        "Cook County faces significant challenges in child wellbeing, with a composite "
        "score of 52.7/100. The environment pillar (58.9) shows the most promise, while "
        "safety (47.2) remains the greatest concern."
    )
    mock_response.usage = MagicMock()
    mock_response.usage.prompt_tokens = 820
    mock_response.usage.completion_tokens = 485

    with patch("openai.OpenAI") as mock_cls:
        instance = MagicMock()
        instance.chat.completions.create.return_value = mock_response
        mock_cls.return_value = instance
        yield instance


# ---------------------------------------------------------------------------
# Temp directory for file outputs
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_output_dir() -> Generator[Path, None, None]:
    """Temporary directory for test file outputs — cleaned up after the test."""
    with tempfile.TemporaryDirectory(prefix="wellnest_test_") as tmpdir:
        yield Path(tmpdir)
