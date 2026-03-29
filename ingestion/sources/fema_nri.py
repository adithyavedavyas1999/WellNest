"""
FEMA National Risk Index (NRI) connector.

The NRI provides county-level composite risk scores for 18 natural hazards.
We use the expected annual loss, social vulnerability, and community
resilience scores in our Environment and Safety pillars.

The data is a single large CSV (~3200 rows, one per county + some tribal
areas and census tracts).

Quirks:
  - The CSV download has a UTF-8 BOM at the start of the file.  Polars
    handles this fine but it tripped us up with an older duckdb version.
  - Column names are in ALL_CAPS and use abbreviations that are documented
    in a separate PDF codebook.  RISK_RATNG = overall risk rating,
    EAL_VALT = expected annual loss (total), SOVI_RATNG = social
    vulnerability rating.
  - Some columns have footnote markers (asterisks) in numeric fields.
    Casting to float with strict=False handles this.
  - Census tract-level NRI data exists but is a separate download (~73K rows).
    We pull county-level for now and might add tract later.
  - The "RISK_SCORE" is 0-100, but "EAL_VALT" (expected annual loss in
    dollars) can be any positive number.  We use the score, not the dollar
    amount, because it's already normalized.
  - About 50 counties are missing risk scores due to insufficient data.
    These are mostly small island territories.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

NRI_DOWNLOAD_URL = (
    "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload/"
    "NRI_Table_Counties/NRI_Table_Counties.zip"
)


class NRIRecord(BaseModel):
    """One county from the FEMA National Risk Index."""
    state_fips: str = Field(..., min_length=2, max_length=2)
    county_fips: str = Field(..., min_length=5, max_length=5)
    county_name: str
    state_name: str
    risk_score: float | None = None
    risk_rating: str | None = None
    eal_score: float | None = None   # expected annual loss (score, 0-100)
    eal_value: float | None = None   # expected annual loss (dollars)
    sovi_score: float | None = None  # social vulnerability (0-100)
    sovi_rating: str | None = None
    resl_score: float | None = None  # community resilience (0-100)
    resl_rating: str | None = None
    population: int | None = None
    building_value: float | None = None

    @field_validator("risk_score", "eal_score", "sovi_score", "resl_score", mode="before")
    @classmethod
    def coerce_score(cls, v):
        if v is None or v == "" or v == "*":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("county_fips")
    @classmethod
    def pad_fips(cls, v: str) -> str:
        return v.zfill(5)


class FEMANRIConnector:
    """Ingests county-level risk data from the FEMA National Risk Index."""

    def __init__(self, data_dir: str | None = None):
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_nri_"))
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=120)

    def extract(self) -> pl.DataFrame:
        zip_path = self.data_dir / "nri_counties.zip"
        logger.info("nri_downloading", url=NRI_DOWNLOAD_URL)

        self.http.download_file(NRI_DOWNLOAD_URL, zip_path)

        import zipfile
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise FileNotFoundError("No CSV in NRI zip")
            target = csv_names[0]
            zf.extract(target, self.data_dir)
            csv_path = self.data_dir / target

        df = pl.read_csv(
            csv_path,
            infer_schema_length=5000,
            ignore_errors=True,
            encoding="utf8-lossy",
        )
        logger.info("nri_raw_loaded", rows=len(df), cols=len(df.columns))
        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        # NRI uses ALL_CAPS column names
        col_map = {
            "STATEFIPS": "state_fips",
            "STATE": "state_name",
            "STATEABBRV": "state_abbr",
            "COUNTY": "county_name",
            "COUNTYTYPE": "county_type",
            "COUNTYFIPS": "county_fips",
            "STCOFIPS": "county_fips",  # some versions use this instead
            "POPULATION": "population",
            "BUILDVALUE": "building_value",
            "RISK_SCORE": "risk_score",
            "RISK_RATNG": "risk_rating",
            "EAL_SCORE": "eal_score",
            "EAL_VALT": "eal_value",
            "SOVI_SCORE": "sovi_score",
            "SOVI_RATNG": "sovi_rating",
            "RESL_SCORE": "resl_score",
            "RESL_RATNG": "resl_rating",
        }

        # also grab individual hazard scores if present
        hazard_cols = {
            "AVLN_AFREQ": "avalanche_freq",
            "ERQK_AFREQ": "earthquake_freq",
            "HRCN_AFREQ": "hurricane_freq",
            "RFLD_AFREQ": "riverine_flood_freq",
            "TRND_AFREQ": "tornado_freq",
            "WFIR_AFREQ": "wildfire_freq",
            "DRGT_AFREQ": "drought_freq",
            "HWAV_AFREQ": "heat_wave_freq",
        }
        col_map.update(hazard_cols)

        renames = {}
        for col in df.columns:
            uc = col.strip().upper()
            if uc in col_map:
                renames[col] = col_map[uc]
        df = df.rename(renames)

        # select known columns
        keep = [c for c in col_map.values() if c in df.columns]
        df = df.select(keep)

        # zero-pad FIPS
        if "state_fips" in df.columns:
            df = df.with_columns(pl.col("state_fips").cast(pl.Utf8).str.zfill(2))
        if "county_fips" in df.columns:
            df = df.with_columns(pl.col("county_fips").cast(pl.Utf8).str.zfill(5))

        # cast scores to float, handling asterisks and blanks
        score_cols = ["risk_score", "eal_score", "eal_value", "sovi_score", "resl_score"]
        for col in score_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8)
                    .str.replace_all(r"\*", "")
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )

        # cast integer columns
        if "population" in df.columns:
            df = df.with_columns(pl.col("population").cast(pl.Int64, strict=False))
        if "building_value" in df.columns:
            df = df.with_columns(pl.col("building_value").cast(pl.Float64, strict=False))

        # strip whitespace from names
        for col in ["county_name", "state_name"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).str.strip_chars())

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                NRIRecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("nri_validation_dropped", count=bad)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.fema_nri"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("nri_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("nri_pipeline_done", rows=count)
        return count
