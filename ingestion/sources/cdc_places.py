"""
CDC PLACES connector — county and census-tract health indicators.

PLACES (Population Level Analysis and Community Estimates) provides model-based
estimates for 36 health measures at county and tract level.  We pull both
resolutions and join tracts to schools downstream in the dbt silver layer.

API is Socrata-based (data.cdc.gov) with SoQL query support.  No auth required
but a Socrata app token bumps the rate limit from ~60/min to ~1000/min.

Known data quirks:
  - County names have trailing whitespace in about 15% of records.
  - The "short_question_text" column is the human-readable measure name
    (e.g. "Current asthma") — use that instead of "measureid" for display.
  - Tract-level dataset is ~800K rows; county is ~90K.
  - Confidence intervals are stored as strings like "12.3 (10.1, 14.5)".
    We pull the point estimate from "data_value" and ignore the CI for now.
  - Some measures have "age-adjusted" and "crude" variants; we default to
    age-adjusted where available.
  - 2024 release updated the resource IDs — verify annually.
"""

from __future__ import annotations

import os

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

# Socrata resource IDs (2023 release as of our last check)
COUNTY_RESOURCE = "swc5-untb"
TRACT_RESOURCE = "cwsq-ngmh"

CDC_BASE = "https://data.cdc.gov/resource"

# measures we actually use in scoring
MEASURES_OF_INTEREST = [
    "Current asthma",
    "Obesity",
    "Mental health not good",
    "No health insurance",
    "Core preventive services",
    "Disability",
    "Frequent mental distress",
    "Depression",
    "No leisure-time physical activity",
    "Binge drinking",
]


class PlacesRecord(BaseModel):
    """One CDC PLACES row (either county or tract level)."""
    year: int | None = None
    stateabbr: str = Field(..., min_length=2, max_length=2)
    statedesc: str | None = None
    locationname: str
    data_value: float | None = None
    data_value_unit: str | None = None
    measure: str
    measureid: str | None = None
    category: str | None = None
    categoryid: str | None = None
    data_value_type: str | None = None
    # CDC sometimes includes these
    low_confidence_limit: float | None = None
    high_confidence_limit: float | None = None
    totalpopulation: int | None = None
    locationid: str | None = None
    geolocation: str | None = None

    @field_validator("locationname")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        # CDC PLACES has trailing whitespace in county names
        return v.strip()

    @field_validator("data_value", mode="before")
    @classmethod
    def coerce_data_value(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class CDCPlacesConnector:
    """Pulls county- and tract-level health data from CDC PLACES via Socrata."""

    def __init__(
        self,
        app_token: str | None = None,
        measures: list[str] | None = None,
    ):
        self.app_token = app_token or os.environ.get("SOCRATA_APP_TOKEN")
        self.measures = measures or MEASURES_OF_INTEREST
        self.http = WellNestHTTPClient(rate_limit=5.0, timeout=60)

        # Socrata bumps rate limits if you pass the token as a header
        if self.app_token:
            self.http.session.headers["X-App-Token"] = self.app_token

    def _socrata_url(self, resource_id: str) -> str:
        return f"{CDC_BASE}/{resource_id}.json"

    def _build_where_clause(self) -> str:
        """SoQL WHERE clause to filter to our measures of interest.

        We filter server-side to avoid downloading 800K rows when we only
        need ~10 measures.  Socrata's SoQL is a bit janky with quoting so
        we single-quote each measure name.
        """
        quoted = ", ".join(f"'{m}'" for m in self.measures)
        return f"short_question_text in({quoted})"

    def extract_county(self) -> list[dict]:
        """Pull county-level PLACES data."""
        url = self._socrata_url(COUNTY_RESOURCE)
        params = {
            "$where": self._build_where_clause(),
            "$select": (
                "year,stateabbr,statedesc,locationname,locationid,"
                "data_value,data_value_unit,data_value_type,"
                "low_confidence_limit,high_confidence_limit,"
                "short_question_text as measure,measureid,"
                "category,categoryid,totalpopulation,geolocation"
            ),
        }
        logger.info("places_county_fetch", url=url)
        return self.http.get_socrata_all(url, params=params, page_size=10000)

    def extract_tract(self) -> list[dict]:
        """Pull tract-level PLACES data.

        This is the big one -- can be 300K+ rows even after filtering.
        Socrata has a hard limit of 50K per request, so we paginate.
        """
        url = self._socrata_url(TRACT_RESOURCE)
        params = {
            "$where": self._build_where_clause(),
            "$select": (
                "year,stateabbr,statedesc,locationname,locationid,"
                "data_value,data_value_unit,data_value_type,"
                "low_confidence_limit,high_confidence_limit,"
                "short_question_text as measure,measureid,"
                "category,categoryid,totalpopulation,geolocation"
            ),
        }
        logger.info("places_tract_fetch", url=url)
        return self.http.get_socrata_all(url, params=params, page_size=50000)

    def _records_to_df(self, records: list[dict]) -> pl.DataFrame:
        """Convert Socrata JSON records to a Polars DataFrame."""
        if not records:
            return pl.DataFrame()

        df = pl.DataFrame(records)

        # strip trailing whitespace from location names -- this bit us in early
        # development when Cook County didn't join because of a trailing space
        if "locationname" in df.columns:
            df = df.with_columns(pl.col("locationname").str.strip_chars())

        if "data_value" in df.columns:
            df = df.with_columns(pl.col("data_value").cast(pl.Float64, strict=False))
        if "low_confidence_limit" in df.columns:
            df = df.with_columns(pl.col("low_confidence_limit").cast(pl.Float64, strict=False))
        if "high_confidence_limit" in df.columns:
            df = df.with_columns(pl.col("high_confidence_limit").cast(pl.Float64, strict=False))
        if "year" in df.columns:
            df = df.with_columns(pl.col("year").cast(pl.Int32, strict=False))
        if "totalpopulation" in df.columns:
            df = df.with_columns(pl.col("totalpopulation").cast(pl.Int64, strict=False))

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate records, dropping rows that fail pydantic checks."""
        good_rows = []
        dropped = 0
        for row in df.iter_rows(named=True):
            try:
                PlacesRecord(**row)
                good_rows.append(row)
            except Exception:
                dropped += 1

        if dropped > 0:
            logger.warning("places_validation_dropped", count=dropped, pct=round(dropped / max(len(df), 1) * 100, 1))

        return pl.DataFrame(good_rows, schema=df.schema) if good_rows else df.clear()

    def load(self, df: pl.DataFrame, table_name: str) -> int:
        ensure_schema("raw")
        logger.info("places_load", table=table_name, rows=len(df))

        df.write_database(
            table_name=table_name,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        return len(df)

    def run(self) -> dict[str, int]:
        """Full pipeline for both county and tract datasets."""
        results = {}

        # county level
        county_raw = self.extract_county()
        county_df = self._records_to_df(county_raw)
        if len(county_df) > 0:
            county_df = self.validate(county_df)
            results["county"] = self.load(county_df, "raw.cdc_places_county")
        else:
            logger.warning("places_county_empty")
            results["county"] = 0

        # tract level
        tract_raw = self.extract_tract()
        tract_df = self._records_to_df(tract_raw)
        if len(tract_df) > 0:
            tract_df = self.validate(tract_df)
            results["tract"] = self.load(tract_df, "raw.cdc_places_tract")
        else:
            logger.warning("places_tract_empty")
            results["tract"] = 0

        self.http.close()
        logger.info("places_pipeline_done", results=results)
        return results
