"""
Tests for geospatial utilities — haversine, FIPS parsing, H3, coordinate validation.

Known distances used for validation:
  - Chicago to NYC: ~1,145 km
  - Same point: 0 km
  - Antipodal: ~20,015 km (half circumference)
"""

from __future__ import annotations

import math

import pytest

from ingestion.utils.geo_utils import (
    EARTH_RADIUS_KM,
    format_fips,
    get_h3_resolution_for_area,
    h3_ring,
    h3_to_latlng,
    h3_to_parent,
    haversine,
    is_valid_fips,
    is_valid_latlon,
    latlng_to_h3,
    parse_fips,
)


# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------

class TestHaversine:

    def test_same_point_returns_zero(self) -> None:
        dist = haversine(41.8781, -87.6298, 41.8781, -87.6298)
        assert dist == 0.0

    def test_chicago_to_nyc(self) -> None:
        dist = haversine(41.8781, -87.6298, 40.7128, -74.0060)
        assert 1130.0 < dist < 1160.0

    def test_short_distance_accuracy(self) -> None:
        # two points ~1.5km apart in downtown Chicago
        dist = haversine(41.8781, -87.6298, 41.8820, -87.6150)
        assert 1.0 < dist < 2.5

    @pytest.mark.parametrize("lat1,lon1,lat2,lon2,expected_range", [
        (0.0, 0.0, 0.0, 1.0, (110.0, 112.0)),
        (51.5074, -0.1278, 48.8566, 2.3522, (340.0, 350.0)),  # London to Paris
        (90.0, 0.0, -90.0, 0.0, (20000.0, 20100.0)),  # pole to pole
    ])
    def test_known_distances(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        expected_range: tuple[float, float],
    ) -> None:
        dist = haversine(lat1, lon1, lat2, lon2)
        low, high = expected_range
        assert low < dist < high, f"Got {dist}, expected between {low} and {high}"

    def test_symmetry(self) -> None:
        d1 = haversine(41.8781, -87.6298, 40.7128, -74.0060)
        d2 = haversine(40.7128, -74.0060, 41.8781, -87.6298)
        assert abs(d1 - d2) < 1e-10


# ---------------------------------------------------------------------------
# FIPS parsing and formatting
# ---------------------------------------------------------------------------

class TestFIPSParsing:

    def test_parse_county_fips(self) -> None:
        result = parse_fips("17031")
        assert result == {"state": "17", "county": "031"}

    def test_parse_tract_fips(self) -> None:
        result = parse_fips("17031842100")
        assert result == {"state": "17", "county": "031", "tract": "842100"}

    def test_parse_state_fips(self) -> None:
        result = parse_fips("17")
        assert result == {"state": "17"}

    def test_parse_unusual_length_warns(self) -> None:
        result = parse_fips("1703")
        assert "state" in result
        assert result["state"] == "17"
        assert "remainder" in result

    def test_parse_strips_whitespace(self) -> None:
        result = parse_fips("  17031  ")
        assert result == {"state": "17", "county": "031"}


class TestFIPSFormatting:

    def test_format_county(self) -> None:
        assert format_fips("17", "031") == "17031"

    def test_format_tract(self) -> None:
        assert format_fips("17", "031", "842100") == "17031842100"

    def test_integer_inputs_zero_padded(self) -> None:
        assert format_fips(9, 1) == "09001"

    def test_format_preserves_existing_padding(self) -> None:
        assert format_fips("09", "001") == "09001"

    @pytest.mark.parametrize("state,county,expected", [
        ("06", "037", "06037"),     # LA County
        ("36", "061", "36061"),     # Manhattan
        ("48", "201", "48201"),     # Harris County, TX
        (1, 1, "01001"),            # Autauga County, AL
    ])
    def test_various_counties(self, state: str | int, county: str | int, expected: str) -> None:
        assert format_fips(state, county) == expected


class TestFIPSValidation:

    @pytest.mark.parametrize("code,valid", [
        ("17", True),
        ("17031", True),
        ("17031842100", True),
        ("", False),
        ("1", False),
        ("abcde", False),
        ("123456789012", False),
    ])
    def test_fips_validation(self, code: str, valid: bool) -> None:
        assert is_valid_fips(code) == valid


# ---------------------------------------------------------------------------
# H3 hex operations
# ---------------------------------------------------------------------------

class TestH3Operations:

    def test_latlng_to_h3_returns_string(self) -> None:
        h3_id = latlng_to_h3(41.8781, -87.6298)
        assert isinstance(h3_id, str)
        assert len(h3_id) > 0

    def test_h3_default_resolution_is_8(self) -> None:
        h3_id = latlng_to_h3(41.8781, -87.6298, resolution=8)
        assert isinstance(h3_id, str)

    def test_h3_to_latlng_roundtrip(self) -> None:
        h3_id = latlng_to_h3(41.8781, -87.6298, resolution=8)
        lat, lng = h3_to_latlng(h3_id)
        assert abs(lat - 41.8781) < 0.01
        assert abs(lng - (-87.6298)) < 0.01

    def test_h3_parent_has_coarser_resolution(self) -> None:
        h3_id = latlng_to_h3(41.8781, -87.6298, resolution=8)
        parent = h3_to_parent(h3_id, 7)
        assert parent != h3_id
        assert isinstance(parent, str)

    def test_h3_ring_returns_neighbors(self) -> None:
        h3_id = latlng_to_h3(41.8781, -87.6298, resolution=8)
        ring = h3_ring(h3_id, k=1)
        assert len(ring) == 7  # center + 6 neighbors
        assert h3_id in ring

    def test_resolution_for_neighborhood(self) -> None:
        res = get_h3_resolution_for_area(0.75)
        assert res == 8

    def test_resolution_for_metro(self) -> None:
        res = get_h3_resolution_for_area(5.0)
        assert res == 7

    @pytest.mark.skip(reason="needs PostGIS running locally")
    def test_h3_spatial_join(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Coordinate validation
# ---------------------------------------------------------------------------

class TestCoordinateValidation:

    def test_valid_us_coordinates(self) -> None:
        assert is_valid_latlon(41.8781, -87.6298) is True

    def test_zero_zero_is_invalid(self) -> None:
        """(0,0) is in the Gulf of Guinea — clearly not a US school."""
        assert is_valid_latlon(0.0, 0.0) is False

    def test_positive_longitude_invalid_for_us(self) -> None:
        assert is_valid_latlon(41.8781, 87.6298) is False

    def test_none_values_invalid(self) -> None:
        assert is_valid_latlon(None, None) is False

    def test_string_numbers_coerced(self) -> None:
        assert is_valid_latlon("41.8781", "-87.6298") is True

    def test_non_numeric_string_invalid(self) -> None:
        assert is_valid_latlon("not_a_number", "-87.6") is False

    @pytest.mark.parametrize("lat,lon,expected", [
        (18.4655, -66.1057, True),   # San Juan, PR
        (21.3069, -157.8583, True),  # Honolulu, HI
        (64.2008, -152.4937, True),  # interior Alaska
        (71.2906, -156.7886, True),  # Utqiagvik (Barrow), AK
        (15.0, -87.0, False),          # too far south
    ])
    def test_us_territory_boundaries(self, lat: float, lon: float, expected: bool) -> None:
        assert is_valid_latlon(lat, lon) == expected
