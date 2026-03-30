"""
NCES EDGE geocoded school locations connector.

The Education Demographic and Geographic Estimates (EDGE) program publishes
precise lat/lon coordinates for every public school.  This is separate from
the CCD directory -- CCD has *some* coordinates but EDGE is the authoritative
geocoded source.

The EDGE files are CSV with columns like NCESSCH, LAT, LON, NMADDR, CITY, etc.
We join these to CCD records by NCESSCH to get accurate coordinates.

Quirks:
  - A handful of schools in the EDGE file have coordinates that map to the
    county centroid rather than the actual school -- usually new schools
    that haven't been field-verified yet.
  - About 200 schools have lat=0 / lon=0 (geocoding failures).  We null
    these out and pick up coordinates from CCD as a fallback.
  - The CSV download URL changes every year and there's no stable API for it.
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import (
    WellNestHTTPClient,
    ensure_schema,
    get_pg_url,
)

logger = structlog.get_logger(__name__)

# URL for the EDGE public school geocode file (2022-23 vintage)
EDGE_DOWNLOAD_URL = "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2223.zip"


class EdgeSchoolLocation(BaseModel):
    """Validated EDGE school location record."""

    ncessch: str = Field(..., min_length=12, max_length=12)
    latitude: float | None = None
    longitude: float | None = None
    street_address: str | None = None
    city: str | None = None
    state: str | None = Field(None, min_length=2, max_length=2)
    zip_code: str | None = None
    county_name: str | None = None
    county_fips: str | None = None
    locale_code: str | None = None

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def zero_to_none(cls, v):
        """EDGE uses 0.0 as a sentinel for missing coords."""
        if v is not None and float(v) == 0.0:
            return None
        return v


class NCESEdgeConnector:
    """Ingests school geographic data from the NCES EDGE program."""

    def __init__(
        self,
        download_url: str | None = None,
        data_dir: str | None = None,
    ):
        self.url = download_url or EDGE_DOWNLOAD_URL
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_edge_"))
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=180)

    def extract(self) -> pl.DataFrame:
        """Download and parse the EDGE geocode CSV."""
        zip_dest = self.data_dir / "edge_geocode.zip"
        logger.info("edge_download", url=self.url)

        self.http.download_file(self.url, zip_dest)
        csv_path = self._extract_csv(zip_dest)

        df = pl.read_csv(
            csv_path,
            infer_schema_length=10000,
            ignore_errors=True,
            encoding="utf8-lossy",
        )
        logger.info("edge_raw_loaded", rows=len(df), cols=len(df.columns))
        return df

    def _extract_csv(self, zip_path: Path) -> Path:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError(f"No CSV in {zip_path}")
            # they usually name it something like EDGE_GEOCODE_PUBLICSCH_2223.csv
            target = csv_files[0]
            zf.extract(target, self.data_dir)
            return self.data_dir / target

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Standardize columns and clean up coordinates."""
        col_map = {
            "NCESSCH": "ncessch",
            "LAT": "latitude",
            "LON": "longitude",
            "NMADDR": "street_address",
            "MADDR": "street_address",
            "CITY": "city",
            "STATE": "state",
            "STABR": "state",
            "ZIP": "zip_code",
            "CNTY": "county_name",
            "CONUM": "county_fips",
            "CNTY_FIPS": "county_fips",
            "LOCALE": "locale_code",
            "ULOCALE": "locale_code",
        }

        renames = {}
        for col in df.columns:
            uc = col.upper().strip()
            if uc in col_map:
                renames[col] = col_map[uc]
        df = df.rename(renames)

        keep = [
            c
            for c in [
                "ncessch",
                "latitude",
                "longitude",
                "street_address",
                "city",
                "state",
                "zip_code",
                "county_name",
                "county_fips",
                "locale_code",
            ]
            if c in df.columns
        ]
        df = df.select(keep)

        # cast and clean
        df = df.with_columns(
            [
                pl.col("ncessch").cast(pl.Utf8).str.zfill(12),
                pl.col("latitude").cast(pl.Float64, strict=False),
                pl.col("longitude").cast(pl.Float64, strict=False),
            ]
        )

        # null out obviously bad coordinates (0,0 or outside US bounds)
        df = df.with_columns(
            [
                pl.when(
                    (pl.col("latitude") == 0.0)
                    | (pl.col("latitude").is_null())
                    | (pl.col("latitude") < 17.0)
                    | (pl.col("latitude") > 72.0)
                )
                .then(None)
                .otherwise(pl.col("latitude"))
                .alias("latitude"),
                pl.when(
                    (pl.col("longitude") == 0.0)
                    | (pl.col("longitude").is_null())
                    | (pl.col("longitude") > -65.0)
                )
                .then(None)
                .otherwise(pl.col("longitude"))
                .alias("longitude"),
            ]
        )

        null_coords = df.filter(pl.col("latitude").is_null()).height
        if null_coords > 0:
            logger.warning("edge_null_coords", count=null_coords)

        if "county_fips" in df.columns:
            df = df.with_columns(pl.col("county_fips").cast(pl.Utf8).str.zfill(5))

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Run pydantic checks.  We're lenient here -- the main thing we
        care about is having a valid NCESSCH to join on later."""
        good = []
        dropped = 0
        for row in df.iter_rows(named=True):
            try:
                EdgeSchoolLocation(**row)
                good.append(row)
            except Exception:
                dropped += 1

        if dropped:
            logger.warning("edge_validation_dropped", count=dropped)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        """Write to raw.nces_edge_geocode."""
        ensure_schema("raw")
        table = "raw.nces_edge_geocode"

        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("edge_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("edge_pipeline_done", rows=count)
        return count
