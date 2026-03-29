"""
HRSA Medically Underserved Areas / Populations (MUA/MUP) connector.

Similar structure to HPSA but for a different designation.  MUAs are
geographic areas (usually counties or census tracts) where residents have
a shortage of personal health services.  MUPs are specific population
groups within a geographic area that face barriers to care.

The Index of Medical Underservice (IMU) score is 0-100 where lower = worse.
Areas with IMU <= 62 qualify as medically underserved.

Quirks:
  - MUA/MUP data comes from the same HRSA download portal as HPSA but
    in a completely different CSV format.  Columns don't match at all.
  - Some MUA designations cover partial counties using census tract IDs.
    The tract FIPS are stored in a separate "Component Census Tract" column
    that can contain multiple comma-separated values.
  - Governor-designated MUPs bypass the normal IMU threshold and can have
    IMU scores above 62.  We keep these but tag them.
  - The "Rural Status" column is useful for our rural/urban analysis but
    it only covers ~70% of records.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

MUA_DOWNLOAD_URL = (
    "https://data.hrsa.gov/DataDownload/DD_Files/MUA_DET.csv"
)

IMU_THRESHOLD = 62.0  # scores at or below this qualify as MUA


class MUARecord(BaseModel):
    """Validated MUA/MUP designation record."""
    mua_source_id: str
    mua_name: str | None = None
    state_abbr: str = Field(..., min_length=2, max_length=2)
    state_fips: str | None = None
    county_fips: str | None = None
    designation_type: str  # "MUA" or "MUP"
    imu_score: float | None = None
    status: str | None = None
    designation_date: str | None = None
    rural_status: str | None = None
    medically_underserved: bool = True

    @field_validator("imu_score", mode="before")
    @classmethod
    def coerce_imu(cls, v):
        if v is None or v == "":
            return None
        try:
            return round(float(v), 1)
        except (ValueError, TypeError):
            return None


class HRSAMUAConnector:
    """Ingests MUA/MUP designations from HRSA."""

    def __init__(self, data_dir: str | None = None):
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_mua_"))
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=120)

    def extract(self) -> pl.DataFrame:
        dest = self.data_dir / "mua_det.csv"
        logger.info("mua_downloading", url=MUA_DOWNLOAD_URL)

        self.http.download_file(MUA_DOWNLOAD_URL, dest)

        df = pl.read_csv(
            dest,
            infer_schema_length=5000,
            ignore_errors=True,
            encoding="utf8-lossy",
            truncate_ragged_lines=True,
        )
        logger.info("mua_raw_loaded", rows=len(df), cols=len(df.columns))
        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        # normalize column names
        col_map = {}
        for col in df.columns:
            col_map[col] = col.strip().lower().replace(" ", "_")
        df = df.rename(col_map)

        known_cols = {
            "medically_underserved_area/population_(mua/p)_source_id": "mua_source_id",
            "mua/p_source_id": "mua_source_id",
            "source_id": "mua_source_id",
            "mua/p_name": "mua_name",
            "area_name": "mua_name",
            "common_state_abbreviation": "state_abbr",
            "state_abbreviation": "state_abbr",
            "common_state_fips_code": "state_fips",
            "state_fips_code": "state_fips",
            "common_county_fips_code": "county_fips_3",
            "county_fips_code": "county_fips_3",
            "designation_type": "designation_type",
            "imu_score": "imu_score",
            "index_of_medical_underservice_score": "imu_score",
            "mua/p_status": "status",
            "mua/p_status_code": "status",
            "designation_date": "designation_date",
            "rural_status_description": "rural_status",
            "rural_status": "rural_status",
        }

        renames = {k: v for k, v in known_cols.items() if k in df.columns and k != v}
        df = df.rename(renames)

        # filter out withdrawn
        if "status" in df.columns:
            df = df.filter(~pl.col("status").str.to_lowercase().str.contains("withdrawn"))

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

        # parse IMU score and flag underserved
        if "imu_score" in df.columns:
            df = df.with_columns(
                pl.col("imu_score").cast(pl.Float64, strict=False)
            )
            df = df.with_columns(
                (pl.col("imu_score").le(IMU_THRESHOLD) | pl.col("imu_score").is_null())
                .alias("medically_underserved")
            )

        # tag MUA vs MUP
        if "designation_type" not in df.columns:
            # sometimes it's encoded in the name
            if "mua_name" in df.columns:
                df = df.with_columns(
                    pl.when(pl.col("mua_name").str.contains("MUP"))
                    .then(pl.lit("MUP"))
                    .otherwise(pl.lit("MUA"))
                    .alias("designation_type")
                )

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                MUARecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("mua_validation_dropped", count=bad)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.hrsa_mua"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("mua_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("mua_pipeline_done", rows=count)
        return count
