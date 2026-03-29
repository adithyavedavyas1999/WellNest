"""
CDC Environmental Health Tracking Network connector.

Provides indicators like childhood blood lead levels, asthma ED visits,
and environmental exposures by county or state.

The API is at ephtracking.cdc.gov and has a somewhat unusual structure:
you first need to look up content areas, then indicators within each area,
then measures within indicators, and finally fetch the actual data using
measure IDs + geographic type + temporal parameters.

Quirks:
  - The API uses numeric IDs for everything.  Indicator 296 is "Asthma
    emergency department visits" but there's no way to know that without
    hitting the metadata endpoint first.
  - Rate limiting is aggressive -- we've been throttled at ~2 req/s.
  - Geographic stratification uses their own scheme, not FIPS directly.
    Type 2 = state, type 3 = county.  County codes mostly match FIPS
    but there are exceptions for independent cities in Virginia.
  - Some measures return data as rates, others as counts, and the API
    doesn't always tell you which.
  - Temporal parameters are weird -- some measures use single years,
    others use year ranges like "2018-2020".
"""

from __future__ import annotations

import os

import polars as pl
import structlog
from pydantic import BaseModel, Field

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url, retry_on_http_error

logger = structlog.get_logger(__name__)

EPH_BASE = "https://ephtracking.cdc.gov/apigateway/api/v1"

# content areas and measures we care about
# these IDs come from the /getCoreHolder endpoint
MEASURES_CONFIG = {
    "lead": {
        "content_area_id": 24,
        "indicator_id": 296,
        "measure_ids": [888, 889],  # % elevated blood lead, count
    },
    "asthma_ed": {
        "content_area_id": 11,
        "indicator_id": 283,
        "measure_ids": [435, 436],  # ED visit rate, crude rate
    },
    "heat_illness": {
        "content_area_id": 36,
        "indicator_id": 311,
        "measure_ids": [547, 548],
    },
}

# geo type 2 = state, 3 = county
GEO_TYPE_COUNTY = 3
GEO_TYPE_STATE = 2


class EnvHealthRecord(BaseModel):
    """A single data point from the environmental health tracking API."""
    geo_id: str
    geo_type: int
    geo_name: str | None = None
    measure_id: int
    measure_name: str | None = None
    content_area: str | None = None
    year: str | None = None
    data_value: float | None = None
    data_value_unit: str | None = None
    confidence_interval: str | None = None


class CDCEnvHealthConnector:
    """Pulls environmental health indicators from the CDC tracking network."""

    def __init__(
        self,
        geo_type: int = GEO_TYPE_COUNTY,
        state_fips: str | None = None,
        year: str = "2020",
    ):
        self.geo_type = geo_type
        self.state_fips = state_fips
        self.year = year
        self.http = WellNestHTTPClient(rate_limit=1.5, timeout=60)

    def _get_data_url(self, measure_id: int) -> str:
        return f"{EPH_BASE}/getData"

    @retry_on_http_error(max_attempts=3)
    def _fetch_measure(self, measure_id: int) -> list[dict]:
        """Fetch data for a single measure.

        The query parameters are positional-ish and poorly documented.
        This combo was figured out by reverse-engineering the tracking
        network's own dashboard requests via browser devtools.
        """
        params = {
            "measureId": measure_id,
            "geo": f"geo_type={self.geo_type}",
            "temporal": self.year,
        }
        if self.state_fips:
            params["geo"] = f"geo_type={self.geo_type}&geo_filter={self.state_fips}"

        url = self._get_data_url(measure_id)
        logger.debug("env_health_fetch", measure_id=measure_id, geo_type=self.geo_type)

        try:
            result = self.http.get_json(url, params=params)
        except Exception:
            logger.warning("env_health_fetch_failed", measure_id=measure_id)
            return []

        if isinstance(result, dict):
            # sometimes the API wraps data in a "tableResult" key
            return result.get("tableResult", result.get("data", [result]))
        if isinstance(result, list):
            return result
        return []

    def extract(self) -> pl.DataFrame:
        """Pull data for all configured measures."""
        all_records: list[dict] = []

        for area_name, cfg in MEASURES_CONFIG.items():
            for mid in cfg["measure_ids"]:
                rows = self._fetch_measure(mid)
                for row in rows:
                    all_records.append({
                        "geo_id": str(row.get("geoId", row.get("geo_id", ""))),
                        "geo_type": self.geo_type,
                        "geo_name": row.get("geo", row.get("geoName", "")),
                        "measure_id": mid,
                        "measure_name": row.get("displayName", ""),
                        "content_area": area_name,
                        "year": str(row.get("year", row.get("temporal", self.year))),
                        "data_value": row.get("dataValue", row.get("value")),
                        "data_value_unit": row.get("unitName", ""),
                        "confidence_interval": row.get("confidenceInterval", ""),
                    })

        if not all_records:
            logger.warning("env_health_no_data")
            return pl.DataFrame()

        df = pl.DataFrame(all_records)
        logger.info("env_health_extracted", rows=len(df))
        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean up types and handle the API's inconsistent value formats."""
        if df.is_empty():
            return df

        df = df.with_columns([
            pl.col("data_value").cast(pl.Float64, strict=False),
            pl.col("measure_id").cast(pl.Int32),
            pl.col("geo_type").cast(pl.Int32),
        ])

        # the API sometimes returns "*" or "N/A" for suppressed values
        df = df.with_columns(
            pl.when(pl.col("data_value").is_null())
            .then(None)
            .otherwise(pl.col("data_value"))
            .alias("data_value")
        )

        # strip whitespace from geo names (same issue as CDC PLACES)
        if "geo_name" in df.columns:
            df = df.with_columns(pl.col("geo_name").str.strip_chars())

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate against pydantic model."""
        if df.is_empty():
            return df

        valid = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                EnvHealthRecord(**row)
                valid.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("env_health_validation_dropped", count=bad)

        return pl.DataFrame(valid, schema=df.schema) if valid else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            logger.warning("env_health_nothing_to_load")
            return 0

        ensure_schema("raw")
        table = "raw.cdc_env_health"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("env_health_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("env_health_pipeline_done", rows=count)
        return count
