"""
Tests for the /api/search endpoint.

Search uses ILIKE matching against school_name, city, and state.
Not fuzzy — exact substring match.  If that ever changes to pg_trgm,
these tests should still pass since we're testing the HTTP interface, not
the query internals.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient


def _make_search_result(
    name: str = "Lincoln Elementary",
    city: str = "Springfield",
    state: str = "IL",
    score: float = 58.4,
    nces_id: str = "170993000943",
) -> dict[str, Any]:
    return {
        "nces_id": nces_id,
        "name": name,
        "city": city,
        "state": state,
        "composite_score": score,
        "enrollment": 412,
        "title_i": True,
        "latitude": 39.7817,
        "longitude": -89.6501,
    }


class TestSearchByName:
    def test_search_returns_results(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        rows = [
            _make_search_result(name="Lincoln Elementary", nces_id="170993000943"),
            _make_search_result(name="Lincoln Middle School", nces_id="170993000944"),
        ]
        mock_db_session.execute.return_value.scalar.return_value = 2
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = rows

        resp = api_client.get("/api/search?q=Lincoln")
        assert resp.status_code == 200

        body = resp.json()
        assert body["total"] == 2
        assert len(body["items"]) == 2

    def test_search_requires_min_length(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        resp = api_client.get("/api/search?q=L")
        assert resp.status_code == 422


class TestSearchByCity:
    def test_city_search(self, api_client: TestClient, mock_db_session: MagicMock) -> None:
        rows = [_make_search_result(city="Chicago")]
        mock_db_session.execute.return_value.scalar.return_value = 1
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = rows

        resp = api_client.get("/api/search?q=Chicago")
        assert resp.status_code == 200
        assert resp.json()["total"] == 1


class TestSearchEdgeCases:
    def test_empty_query_rejected(self, api_client: TestClient, mock_db_session: MagicMock) -> None:
        resp = api_client.get("/api/search?q=")
        assert resp.status_code == 422

    def test_missing_query_param(self, api_client: TestClient, mock_db_session: MagicMock) -> None:
        resp = api_client.get("/api/search")
        assert resp.status_code == 422

    def test_no_results_returns_empty_list(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 0
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/search?q=xyznonexistent")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 0
        assert body["items"] == []

    def test_search_with_state_filter(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 0
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/search?q=Lincoln&state=IL")
        assert resp.status_code == 200


class TestSearchResultLimit:
    def test_per_page_limits_results(
        self, api_client: TestClient, mock_db_session: MagicMock
    ) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 100
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/search?q=school&per_page=5")
        assert resp.status_code == 200
        assert resp.json()["per_page"] == 5

    def test_pagination_works(self, api_client: TestClient, mock_db_session: MagicMock) -> None:
        mock_db_session.execute.return_value.scalar.return_value = 50
        mock_db_session.execute.return_value.mappings.return_value.all.return_value = []

        resp = api_client.get("/api/search?q=school&page=3&per_page=10")
        assert resp.status_code == 200
        body = resp.json()
        assert body["page"] == 3
        assert body["pages"] == 5
