"""
HRSA Health Professional Shortage Area (HPSA) connector.

Downloads HPSA designation data from data.hrsa.gov.  HPSAs identify areas
with shortages in primary care, dental, or mental health providers.

The HPSA score (0-25 for primary care, 0-26 for mental health) is a key
input to our health & resources pillar -- higher score = worse shortage.

Data comes as a CSV download that changes daily (they update designations
continuously as applications are processed).

Quirks:
  - The CSV has 50+ columns but we only need about 15.
  - HPSA "Component State FIPS Code" and "Component County FIPS Code" are
    separate columns that need to be concatenated and zero-padded.
  - Some HPSAs are "single county", others are "partial county" or "multi-
    county."  The geographic scope field matters for joining to tracts.
  - Withdrawn designations still appear in the file with Status = "Withdrawn."
    Filter those out or you'll double-count.
  - The "HPSA Score" column occasionally has decimal values even though
    the official scoring is integer-based.  We round.
  - There's a separate file for each discipline (primary care, dental,
    mental health).  We pull all three and tag them.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

# HRSA download URLs -- these redirect to the actual CSV
# The "BCD_HPSA" dataset includes all disciplines in one file
HPSA_DOWNLOAD_URL = (
    "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv"
)

# fallback: sometimes the direct link changes and we need to hit the catalog
HPSA_CATALOG_URL = "https://data.hrsa.gov/data/download"

HPSA_DISCIPLINES = {
    "PC": "Primary Care",
    "DH": "Dental Health",
    "MH": "Mental Health",
}


class HPSARecord(BaseModel):
    """Validated HPSA designation record."""
    hpsa_id: str
    hpsa_name: str
    state_abbr: str = Field(..., min_length=2, max_length=2)
    state_fips: str | None = None
    county_fips: str | None = None
    hpsa_score: int | None = None
    hpsa_status: str
    designation_type: str | None = None
    discipline: str | None = None
    hpsa_component_type: str | None = None
    designation_date: str | None = None
    last_update: str | None = None
    # geography
    geo_type: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    @field_validator("hpsa_score", mode="before")
    @classmethod
    def round_score(cls, v):
        if v is None:
            return None
        try:
            return round(float(v))
        except (ValueError, TypeError):
            return None


class HRSAHPSAConnector:
    """Ingests HPSA shortage area designations from HRSA."""

    def __init__(
        self,
        disciplines: list[str] | None = None,
        data_dir: str | None = None,
    ):
        self.disciplines = disciplines or ["PC", "DH", "MH"]
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_hpsa_"))
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=120)

    def _download_url(self, discipline: str) -> str:
        """Build download URL for a specific discipline.

        HRSA's download links follow a pattern, but they've changed the
        naming scheme twice since we started this project.
        """
        return f"https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_{discipline}.csv"

    def extract(self) -> pl.DataFrame:
        """Download and concatenate HPSA files for all disciplines."""
        frames = []

        for disc in self.disciplines:
            url = self._download_url(disc)
            dest = self.data_dir / f"hpsa_{disc.lower()}.csv"
            logger.info("hpsa_downloading", discipline=disc, url=url)

            try:
                self.http.download_file(url, dest)
            except Exception:
                logger.warning("hpsa_download_failed", discipline=disc, url=url)
                continue

            df = pl.read_csv(
                dest,
                infer_schema_length=5000,
                ignore_errors=True,
                encoding="utf8-lossy",
                truncate_ragged_lines=True,
            )
            df = df.with_columns(pl.lit(disc).alias("discipline_code"))
            frames.append(df)
            logger.info("hpsa_loaded_file", discipline=disc, rows=len(df))

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames, how="diagonal")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Standardize HPSA data."""
        if df.is_empty():
            return df

        # HRSA column names are verbose and inconsistent
        col_map = {}
        for col in df.columns:
            lc = col.strip().lower().replace(" ", "_")
            col_map[col] = lc
        df = df.rename(col_map)

        # try to map known columns to our schema
        known = {
            "hpsa_source_id": "hpsa_id",
            "source_id": "hpsa_id",
            "hpsa_name": "hpsa_name",
            "common_state_abbreviation": "state_abbr",
            "state_abbreviation": "state_abbr",
            "common_state_fips_code": "state_fips",
            "state_fips_code": "state_fips",
            "component_state_fips_code": "state_fips",
            "common_county_fips_code": "county_fips_3",
            "component_county_fips_code": "county_fips_3",
            "hpsa_score": "hpsa_score",
            "hpsa_status": "hpsa_status",
            "status_code": "hpsa_status",
            "designation_type": "designation_type",
            "hpsa_component_type_description": "hpsa_component_type",
            "designation_date": "designation_date",
            "hpsa_date_last_updated": "last_update",
            "hpsa_geo_type": "geo_type",
            "latitude": "latitude",
            "longitude": "longitude",
        }
        renames = {k: v for k, v in known.items() if k in df.columns and k != v}
        df = df.rename(renames)

        # filter out withdrawn designations
        if "hpsa_status" in df.columns:
            before = len(df)
            df = df.filter(~pl.col("hpsa_status").str.to_lowercase().str.contains("withdrawn"))
            withdrawn = before - len(df)
            if withdrawn:
                logger.info("hpsa_filtered_withdrawn", count=withdrawn)

        # zero-pad FIPS
        if "state_fips" in df.columns:
            df = df.with_columns(pl.col("state_fips").cast(pl.Utf8).str.zfill(2))
        if "county_fips_3" in df.columns and "state_fips" in df.columns:
            df = df.with_columns(
                pl.col("county_fips_3").cast(pl.Utf8).str.zfill(3)
            )
            df = df.with_columns(
                (pl.col("state_fips") + pl.col("county_fips_3")).alias("county_fips")
            )
        elif "county_fips" in df.columns:
            df = df.with_columns(pl.col("county_fips").cast(pl.Utf8).str.zfill(5))

        # map discipline code to name
        if "discipline_code" in df.columns:
            df = df.with_columns(
                pl.col("discipline_code")
                .map_elements(lambda x: HPSA_DISCIPLINES.get(x, x), return_dtype=pl.Utf8)
                .alias("discipline")
            )

        # round the HPSA score
        if "hpsa_score" in df.columns:
            df = df.with_columns(
                pl.col("hpsa_score").cast(pl.Float64, strict=False).round(0).cast(pl.Int32, strict=False)
            )

        if "latitude" in df.columns:
            df = df.with_columns(pl.col("latitude").cast(pl.Float64, strict=False))
        if "longitude" in df.columns:
            df = df.with_columns(pl.col("longitude").cast(pl.Float64, strict=False))

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                HPSARecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("hpsa_validation_dropped", count=bad, total=len(df))

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.hrsa_hpsa"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("hpsa_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("hpsa_pipeline_done", rows=count)
        return count
