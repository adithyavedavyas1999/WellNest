"""
EPA AirNow / AQS air quality connector.

Two data paths:
  1. Real-time observations via the AirNow API (lat/lon lookups, hourly)
  2. Annual summary files from AQS (bulk CSV downloads, one per pollutant/year)

For the scoring pipeline we mostly use the annual summaries since we need
historical averages.  The real-time API is used for the dashboard's "current
conditions" widget.

Quirks:
  - The AirNow API rate limit documentation says 500/hr but in practice
    they start returning 429 around 300 requests.  We throttle to 2/s.
  - The annual summary CSVs from AQS are large (~200MB each for PM2.5).
    They come gzipped which helps.
  - County-level annual AQI summary file has a weird header: the first
    column is called "State" but contains the state name, not the FIPS.
    You have to join on state+county name to get the FIPS, which is fragile
    because of spelling differences ("St. Louis" vs "Saint Louis").
  - AQI values of -999 indicate insufficient data for that county/year.
  - The "days_with_aqi" column tells you data completeness -- counties with
    fewer than 200 days should probably be flagged.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url, retry_on_http_error

logger = structlog.get_logger(__name__)

# AirNow real-time API
AIRNOW_API_BASE = "https://www.airnowapi.org/aq"
AIRNOW_OBS_URL = f"{AIRNOW_API_BASE}/observation/latLong/current/"
AIRNOW_FORECAST_URL = f"{AIRNOW_API_BASE}/forecast/latLong/"

# AQS annual data downloads
AQS_DOWNLOAD_BASE = "https://aqs.epa.gov/aqsweb/airdata"
AQS_ANNUAL_AQI_URL = f"{AQS_DOWNLOAD_BASE}/annual_aqi_by_county_{{year}}.zip"


class AirQualityObservation(BaseModel):
    """A single AirNow observation."""
    latitude: float
    longitude: float
    aqi: int | None = None
    parameter_name: str | None = None
    category_number: int | None = None
    category_name: str | None = None
    reporting_area: str | None = None
    state_code: str | None = None
    date_observed: str | None = None
    hour_observed: int | None = None

    @field_validator("aqi")
    @classmethod
    def aqi_range(cls, v):
        if v is not None and v < 0:
            return None
        return v


class AnnualAQIRecord(BaseModel):
    """One row from the AQS annual county AQI summary."""
    state_name: str
    county_name: str
    state_code: str = Field(..., min_length=2, max_length=2)
    county_code: str = Field(..., min_length=3, max_length=3)
    year: int
    days_with_aqi: int | None = None
    good_days: int | None = None
    moderate_days: int | None = None
    unhealthy_sensitive_days: int | None = None
    unhealthy_days: int | None = None
    very_unhealthy_days: int | None = None
    hazardous_days: int | None = None
    max_aqi: int | None = None
    percentile_90_aqi: int | None = None
    median_aqi: int | None = None

    @field_validator("state_code")
    @classmethod
    def pad_state(cls, v: str) -> str:
        return v.zfill(2)

    @field_validator("county_code")
    @classmethod
    def pad_county(cls, v: str) -> str:
        return v.zfill(3)


class EPAAirNowConnector:
    """Ingests air quality data from both AirNow API and AQS annual files."""

    def __init__(
        self,
        api_key: str | None = None,
        years: list[int] | None = None,
        data_dir: str | None = None,
    ):
        self.api_key = api_key or os.environ.get("AIRNOW_API_KEY", "")
        self.years = years or [2020, 2021, 2022]
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_aqi_"))
        self.http = WellNestHTTPClient(rate_limit=2.0, timeout=90)

    # ------------------------------------------------------------------
    # Real-time observation (used by dashboard, not bulk pipeline)
    # ------------------------------------------------------------------

    @retry_on_http_error(max_attempts=3)
    def fetch_current_aqi(self, lat: float, lon: float) -> list[dict]:
        """Get current AQI for a lat/lon.  Returns one entry per pollutant."""
        if not self.api_key:
            raise ValueError("AIRNOW_API_KEY required for real-time API")

        params = {
            "format": "application/json",
            "latitude": lat,
            "longitude": lon,
            "distance": 25,  # miles
            "API_KEY": self.api_key,
        }
        return self.http.get_json(AIRNOW_OBS_URL, params=params)

    # ------------------------------------------------------------------
    # Annual AQI summary (bulk pipeline)
    # ------------------------------------------------------------------

    def extract_annual(self) -> pl.DataFrame:
        """Download and concatenate annual AQI county summaries."""
        frames = []

        for yr in self.years:
            url = AQS_ANNUAL_AQI_URL.format(year=yr)
            zip_path = self.data_dir / f"annual_aqi_{yr}.zip"

            logger.info("aqi_downloading_year", year=yr, url=url)
            try:
                self.http.download_file(url, zip_path)
            except Exception:
                logger.warning("aqi_download_failed", year=yr)
                continue

            # the zip contains a single CSV
            import zipfile
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_name = zf.namelist()[0]
                zf.extract(csv_name, self.data_dir)
                csv_path = self.data_dir / csv_name

            df = pl.read_csv(csv_path, infer_schema_length=5000, ignore_errors=True)
            df = df.with_columns(pl.lit(yr).alias("year"))
            frames.append(df)
            logger.info("aqi_year_loaded", year=yr, rows=len(df))

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames, how="diagonal")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean up the AQS annual summary data."""
        if df.is_empty():
            return df

        # standardize column names -- AQS headers have spaces and mixed case
        rename_map = {}
        for col in df.columns:
            clean = (
                col.strip()
                .lower()
                .replace(" ", "_")
                .replace(".", "")
            )
            rename_map[col] = clean
        df = df.rename(rename_map)

        # column name normalization for known variations
        col_aliases = {
            "state": "state_name",
            "county": "county_name",
            "state_code": "state_code",
            "county_code": "county_code",
            "days_with_aqi": "days_with_aqi",
            "good_days": "good_days",
            "moderate_days": "moderate_days",
            "unhealthy_for_sensitive_groups_days": "unhealthy_sensitive_days",
            "unhealthy_days": "unhealthy_days",
            "very_unhealthy_days": "very_unhealthy_days",
            "hazardous_days": "hazardous_days",
            "max_aqi": "max_aqi",
            "90th_percentile_aqi": "percentile_90_aqi",
            "median_aqi": "median_aqi",
        }
        existing_renames = {k: v for k, v in col_aliases.items() if k in df.columns and k != v}
        if existing_renames:
            df = df.rename(existing_renames)

        # zero-pad FIPS codes
        if "state_code" in df.columns:
            df = df.with_columns(pl.col("state_code").cast(pl.Utf8).str.zfill(2))
        if "county_code" in df.columns:
            df = df.with_columns(pl.col("county_code").cast(pl.Utf8).str.zfill(3))

        # build a full county FIPS
        if "state_code" in df.columns and "county_code" in df.columns:
            df = df.with_columns(
                (pl.col("state_code") + pl.col("county_code")).alias("county_fips")
            )

        # cast numeric columns
        int_cols = [
            "days_with_aqi", "good_days", "moderate_days",
            "unhealthy_sensitive_days", "unhealthy_days",
            "very_unhealthy_days", "hazardous_days",
            "max_aqi", "percentile_90_aqi", "median_aqi", "year",
        ]
        for col in int_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int32, strict=False))

        # AQI of -999 means insufficient data
        for col in ["max_aqi", "percentile_90_aqi", "median_aqi"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) < 0).then(None).otherwise(pl.col(col)).alias(col)
                )

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        dropped = 0
        for row in df.iter_rows(named=True):
            try:
                AnnualAQIRecord(**row)
                good.append(row)
            except Exception:
                dropped += 1

        if dropped:
            logger.warning("aqi_validation_dropped", count=dropped)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.epa_aqi_annual"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("aqi_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        """Run the annual bulk pipeline (not real-time)."""
        raw = self.extract_annual()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("aqi_pipeline_done", years=self.years, rows=count)
        return count
