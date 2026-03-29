"""
Tests for the /api/schools endpoints.

Uses FastAPI's TestClient with the DB dependency mocked out.  These test the
HTTP layer — status codes, response shapes, pagination, filters — without
touching a real database.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def _make_school_row(
    nces_id: str = "170993000943",
    name: str = "Lincoln Elementary",
    score: float = 58.4,
    **overrides: Any,
) -> dict[str, Any]:
    base = {
        "nces_id": nces_id,
        "name": name,
        "city": "Springfield",
        "state": "IL",
        "composite_score": score,
        "enrollment": 412,
        "title_i": True,
        "latitude": 39.7817,
        "longitude": -89.6501,
    }
    base.update(overrides)
    return base


def _make_detail_row(nces_id: str = "170993000943", **overrides: Any) -> dict[str, Any]:
    base = {
        "nces_id": nces_id,
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
        "updated_at": "2025-03-15T12:00:00",
        "national_rank": 67432,
        "state_rank": 1205,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# GET /api/schools — list endpoint
# ---------------------------------------------------------------------------

class TestListSchools:

    def test_returns_paginated_response(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        rows = [_make_school_row(nces_id=f"1709930009{i}", score=30.0 + i * 10)
                for i in range(3)]
        mock_db_session.execute.return_value.scalar.return_value = 3
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = rows

        resp = api_client.get("/api/schools")
        assert resp.status_code == 200

        body = resp.json()
        assert "items" in body
        assert "total" in body
        assert "page" in body
        assert "per_page" in body
        assert body["total"] == 3

    def test_pagination_params(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 100
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/schools?page=2&per_page=10")
        assert resp.status_code == 200
        body = resp.json()
        assert body["page"] == 2
        assert body["per_page"] == 10

    def test_empty_result_set(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 0
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/schools")
        assert resp.status_code == 200
        assert resp.json()["items"] == []

    def test_filter_by_state(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 0
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/schools?state=IL")
        assert resp.status_code == 200

    def test_filter_by_score_range(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 0
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/schools?score_above=50&score_below=80")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /api/schools/{nces_id} — detail endpoint
# ---------------------------------------------------------------------------

class TestGetSchool:

    def test_returns_school_detail(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        row = _make_detail_row()
        mock_db_session.execute.return_value.mappings.return_value.first.return_value = row

        resp = api_client.get("/api/schools/170993000943")
        assert resp.status_code == 200

        body = resp.json()
        assert body["nces_id"] == "170993000943"
        assert body["name"] == "Lincoln Elementary School"
        assert body["composite_score"] == 58.4
        assert "pillar_scores" in body

    def test_404_for_nonexistent_school(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.mappings.return_value.first.return_value = None

        resp = api_client.get("/api/schools/999999999999")
        assert resp.status_code == 404

    def test_detail_includes_category(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        row = _make_detail_row(composite_score=85.0)
        mock_db_session.execute.return_value.mappings.return_value.first.return_value = row

        resp = api_client.get("/api/schools/170993000943")
        assert resp.status_code == 200
        assert resp.json()["category"] == "thriving"

    def test_detail_includes_location(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        row = _make_detail_row()
        mock_db_session.execute.return_value.mappings.return_value.first.return_value = row

        resp = api_client.get("/api/schools/170993000943")
        body = resp.json()
        assert body["location"]["lat"] == pytest.approx(39.7817)
        assert body["location"]["lon"] == pytest.approx(-89.6501)

    def test_detail_handles_null_location(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        row = _make_detail_row(latitude=None, longitude=None)
        mock_db_session.execute.return_value.mappings.return_value.first.return_value = row

        resp = api_client.get("/api/schools/170993000943")
        assert resp.status_code == 200
        assert resp.json()["location"] is None
