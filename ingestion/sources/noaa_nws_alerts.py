"""
NOAA / National Weather Service active alerts connector.

The NWS API provides real-time weather alerts as GeoJSON.  We use it for
the dashboard's situational awareness layer -- schools in areas with active
severe weather alerts get a visual indicator.

No API key needed, just a User-Agent header (they actually require one and
will 403 you without it).

The data is ephemeral by nature -- alerts expire.  We snapshot active alerts
at ingestion time and store them for historical tracking.

Quirks:
  - The API occasionally returns 503 during heavy weather events (exactly
    when you most need it).  The NWS status page says "by design" because
    their operational systems take priority.
  - Alert areas are identified by state FIPS + a "zone" code, not county
    FIPS.  The zone-to-county mapping is available from the API but it's
    a many-to-many relationship (one zone can span multiple counties).
  - GeoJSON features include optional polygon geometry, but many alerts
    use the "UGC" area description instead of actual coordinates.
  - The "severity" field has values like "Extreme", "Severe", "Moderate",
    "Minor", "Unknown" -- not a numeric scale.
  - Pagination uses a cursor-based "next" URL in the response.
  - Rate limit appears to be ~5 req/s based on testing, but officially
    they just say "don't abuse it."
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import structlog
from pydantic import BaseModel, Field

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

SEVERITY_ORDER = {
    "Extreme": 5,
    "Severe": 4,
    "Moderate": 3,
    "Minor": 2,
    "Unknown": 1,
}


class WeatherAlert(BaseModel):
    """A single NWS weather alert."""

    alert_id: str
    event: str
    severity: str
    certainty: str | None = None
    urgency: str | None = None
    headline: str | None = None
    description: str | None = None
    area_desc: str | None = None
    affected_zones: list[str] = Field(default_factory=list)
    state: str | None = None
    onset: str | None = None
    expires: str | None = None
    sender: str | None = None
    message_type: str | None = None
    severity_rank: int = 1
    ingested_at: str | None = None


class NOAANWSAlertsConnector:
    """Pulls active weather alerts from the NWS API."""

    def __init__(self, state_filter: str | None = None):
        """
        Args:
            state_filter: Two-letter state code to filter alerts (e.g., "IL").
                          If None, fetches all active US alerts.
        """
        self.state_filter = state_filter
        # NWS requires a descriptive User-Agent or they 403 you
        self.http = WellNestHTTPClient(
            rate_limit=3.0,
            timeout=30,
            user_agent="WellNest/0.1 (wellnest-health-equity; contact@chieac.org)",
        )

    def extract(self) -> list[dict]:
        """Fetch all active alerts, handling cursor-based pagination."""
        params = {}
        if self.state_filter:
            params["area"] = self.state_filter

        all_features: list[dict] = []
        url: str | None = NWS_ALERTS_URL

        while url:
            logger.debug("nws_fetching_page", url=url)
            try:
                response = self.http.get_json(url, params=params if url == NWS_ALERTS_URL else None)
            except Exception:
                logger.warning("nws_fetch_failed", url=url)
                break

            features = response.get("features", [])
            all_features.extend(features)

            # cursor-based pagination
            pagination = response.get("pagination", {})
            url = pagination.get("next")

        logger.info("nws_alerts_fetched", count=len(all_features))
        return all_features

    def _parse_feature(self, feature: dict) -> dict:
        """Extract fields from a GeoJSON feature."""
        props = feature.get("properties", {})
        now_iso = datetime.now(UTC).isoformat()

        zones = props.get("affectedZones", [])
        # zones come as full URLs like "https://api.weather.gov/zones/county/ILC031"
        # we just want the zone ID part
        zone_ids = [z.split("/")[-1] if isinstance(z, str) else str(z) for z in zones]

        severity = props.get("severity", "Unknown")

        return {
            "alert_id": props.get("id", ""),
            "event": props.get("event", ""),
            "severity": severity,
            "certainty": props.get("certainty"),
            "urgency": props.get("urgency"),
            "headline": props.get("headline"),
            "description": (props.get("description", "") or "")[
                :2000
            ],  # truncate long descriptions
            "area_desc": props.get("areaDesc"),
            "affected_zones": zone_ids,
            "state": self._extract_state(props),
            "onset": props.get("onset"),
            "expires": props.get("expires"),
            "sender": props.get("senderName"),
            "message_type": props.get("messageType"),
            "severity_rank": SEVERITY_ORDER.get(severity, 1),
            "ingested_at": now_iso,
        }

    def _extract_state(self, props: dict) -> str | None:
        """Try to extract a state code from the alert properties.

        NWS doesn't put a clean state field in the response, but the
        senderName usually contains the state, and the geocode has
        UGC codes like "ILC031" where the first two chars are the state.
        """
        # try geocode UGC first
        geocode = props.get("geocode", {})
        ugc_list = geocode.get("UGC", [])
        if ugc_list:
            first_ugc = str(ugc_list[0])
            if len(first_ugc) >= 2 and first_ugc[:2].isalpha():
                return first_ugc[:2]

        # fallback to filtering state from areaDesc
        area = props.get("areaDesc", "")
        if self.state_filter and self.state_filter in area:
            return self.state_filter

        return None

    def transform(self, features: list[dict]) -> pl.DataFrame:
        """Parse GeoJSON features into a flat DataFrame."""
        if not features:
            return pl.DataFrame()

        records = [self._parse_feature(f) for f in features]

        # flatten affected_zones list to comma-separated string for storage
        for rec in records:
            rec["affected_zones"] = ",".join(rec["affected_zones"])

        df = pl.DataFrame(records)
        logger.info("nws_transformed", rows=len(df))
        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                row_copy = dict(row)
                # convert back to list for pydantic validation
                row_copy["affected_zones"] = row_copy.get("affected_zones", "").split(",")
                WeatherAlert(**row_copy)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("nws_validation_dropped", count=bad)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            logger.info("nws_no_active_alerts")
            return 0

        ensure_schema("raw")
        # we append to the alerts table instead of replacing, since
        # we want to track alert history over time
        table = "raw.noaa_nws_alerts"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="append",
            engine="sqlalchemy",
        )
        logger.info("nws_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        features = self.extract()
        df = self.transform(features)
        validated = self.validate(df)
        count = self.load(validated)
        self.http.close()
        logger.info("nws_pipeline_done", alerts=count)
        return count
