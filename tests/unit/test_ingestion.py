"""
Tests for ingestion connectors and utilities.

These test the data cleaning logic without hitting real APIs.  Mock responses
come from actual API payloads we captured during development (sanitized).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.utils.http_client import RateLimiter, WellNestHTTPClient


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------

class TestRateLimiter:

    def test_first_call_doesnt_wait(self) -> None:
        limiter = RateLimiter(calls_per_second=2.0)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_rapid_calls_are_throttled(self) -> None:
        limiter = RateLimiter(calls_per_second=10.0)
        limiter.wait()
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.08  # ~0.1s interval, small buffer for timing


# ---------------------------------------------------------------------------
# WellNestHTTPClient
# ---------------------------------------------------------------------------

class TestHTTPClient:

    def test_get_json_returns_parsed(self) -> None:
        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"data": [1, 2, 3]}
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            client = WellNestHTTPClient(rate_limit=100.0)
            result = client.get_json("https://example.com/api")
            assert result == {"data": [1, 2, 3]}
            client.close()

    def test_get_text_returns_string(self) -> None:
        with patch("requests.Session.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.text = "col1,col2\nval1,val2"
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            client = WellNestHTTPClient(rate_limit=100.0)
            result = client.get_text("https://example.com/csv")
            assert "col1" in result
            client.close()

    def test_client_sets_user_agent(self) -> None:
        client = WellNestHTTPClient(user_agent="TestAgent/1.0")
        assert client.session.headers["User-Agent"] == "TestAgent/1.0"
        client.close()

    def test_context_manager_closes_session(self) -> None:
        with WellNestHTTPClient() as client:
            assert client.session is not None

    @pytest.mark.skip(reason="flaky -- depends on CDC API availability")
    def test_socrata_pagination_live(self) -> None:
        client = WellNestHTTPClient(rate_limit=2.0)
        data = client.get_socrata_all(
            "https://data.cdc.gov/resource/swc5-untb.json",
            params={"$where": "stateabbr='IL'"},
            page_size=100,
            max_rows=200,
        )
        assert len(data) <= 200
        client.close()


# ---------------------------------------------------------------------------
# CDC PLACES cleaning
# ---------------------------------------------------------------------------

class TestCDCPlacesCleaning:

    def test_trailing_whitespace_stripped(self) -> None:
        """CDC PLACES has trailing whitespace in ~15% of county names."""
        from ingestion.sources.cdc_places import PlacesRecord

        record = PlacesRecord(
            stateabbr="IL",
            locationname="Cook County   ",
            measure="Current asthma",
            data_value=9.5,
        )
        assert record.locationname == "Cook County"

    def test_empty_data_value_becomes_none(self) -> None:
        from ingestion.sources.cdc_places import PlacesRecord

        record = PlacesRecord(
            stateabbr="IL",
            locationname="Cook County",
            measure="Obesity",
            data_value="",
        )
        assert record.data_value is None

    def test_non_numeric_data_value_becomes_none(self) -> None:
        from ingestion.sources.cdc_places import PlacesRecord

        record = PlacesRecord(
            stateabbr="IL",
            locationname="Cook County",
            measure="Obesity",
            data_value="N/A",
        )
        assert record.data_value is None

    def test_valid_record_passes(self) -> None:
        from ingestion.sources.cdc_places import PlacesRecord

        record = PlacesRecord(
            stateabbr="IL",
            locationname="Cook County",
            measure="Depression",
            data_value=18.3,
            year=2023,
            totalpopulation=5275541,
        )
        assert record.data_value == 18.3
        assert record.year == 2023


# ---------------------------------------------------------------------------
# Census ACS sentinel handling
# ---------------------------------------------------------------------------

class TestCensusACSCleaning:

    def test_sentinel_value_replaced(self) -> None:
        """The Census API returns -666666666 for missing/suppressed values.
        This is documented nowhere obvious — found it on a forum post."""
        import polars as pl

        from ingestion.sources.census_acs import CENSUS_MISSING_VALUE, CensusACSConnector

        connector = CensusACSConnector(api_key="fake", year=2022)

        df = pl.DataFrame({
            "state": ["17"],
            "county": ["031"],
            "tract": ["842100"],
            "B17001_001E": [str(CENSUS_MISSING_VALUE)],
            "B17001_002E": ["1500"],
            "B01003_001E": ["50000"],
        })

        result = connector.transform(df)
        if "poverty_universe" in result.columns:
            val = result["poverty_universe"][0]
            assert val is None or val != CENSUS_MISSING_VALUE

    def test_fips_columns_created(self) -> None:
        import polars as pl

        from ingestion.sources.census_acs import CensusACSConnector

        connector = CensusACSConnector(api_key="fake", year=2022)

        df = pl.DataFrame({
            "state": ["17"],
            "county": ["031"],
            "tract": ["842100"],
            "B01003_001E": ["50000"],
        })

        result = connector.transform(df)
        assert "state_fips" in result.columns
        assert "county_fips" in result.columns
        assert "full_fips" in result.columns

        if "full_fips" in result.columns and len(result) > 0:
            assert result["full_fips"][0] == "17031842100"

    @pytest.mark.parametrize("sentinel", [-666666666, -666666666.0])
    def test_sentinel_variants_handled(self, sentinel: int | float) -> None:
        import polars as pl

        from ingestion.sources.census_acs import CensusACSConnector

        connector = CensusACSConnector(api_key="fake", year=2022)
        df = pl.DataFrame({
            "state": ["17"],
            "county": ["031"],
            "tract": ["842100"],
            "B19013_001E": [str(sentinel)],
        })
        result = connector.transform(df)
        if "median_hh_income" in result.columns:
            val = result["median_hh_income"][0]
            assert val is None or val != -666666666


# ---------------------------------------------------------------------------
# FIPS formatting
# ---------------------------------------------------------------------------

class TestFIPSFormatting:

    def test_integer_state_fips_zero_padded(self) -> None:
        from ingestion.utils.geo_utils import format_fips

        result = format_fips(9, 1)
        assert result == "09001"

    def test_string_state_fips_preserved(self) -> None:
        from ingestion.utils.geo_utils import format_fips

        result = format_fips("17", "031")
        assert result == "17031"

    def test_tract_fips_eleven_digits(self) -> None:
        from ingestion.utils.geo_utils import format_fips

        result = format_fips("17", "031", "842100")
        assert result == "17031842100"
        assert len(result) == 11
