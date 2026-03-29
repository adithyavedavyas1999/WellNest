"""
HTTP client for WellNest data ingestion.

Wraps requests.Session with retry logic, rate limiting, and structured logging.
Most federal APIs are surprisingly flaky -- CDC endpoints in particular will
randomly 503 during peak hours (Monday mornings, apparently everyone runs
their ETLs at 6 AM EST).
"""

from __future__ import annotations

import io
import logging
import time
from pathlib import Path
from typing import Any

import requests
import structlog
from requests.adapters import HTTPAdapter
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util.retry import Retry

logger = structlog.get_logger(__name__)


class RateLimiter:
    """Dead-simple token-bucket rate limiter.

    Nothing fancy -- we just need to avoid getting IP-banned by data.cdc.gov
    and the Census Bureau.  Both start throttling around 10 req/s without
    an app token.
    """

    def __init__(self, calls_per_second: float = 2.0):
        self._min_interval = 1.0 / calls_per_second
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self._min_interval:
            gap = self._min_interval - elapsed
            time.sleep(gap)
        self._last_call = time.monotonic()


def retry_on_http_error(max_attempts: int = 4, min_wait: int = 2, max_wait: int = 60):
    """Reusable tenacity decorator for connectors that talk to flaky endpoints.

    The defaults here come from trial and error -- CDC's Socrata gateway
    needs at least a 2-second backoff before it stops returning 429s.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=min_wait, max=max_wait),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.Timeout)
        ),
        before_sleep=before_sleep_log(logging.getLogger("wellnest.http"), logging.WARNING),
        reraise=True,
    )


class WellNestHTTPClient:
    """Session-based HTTP client with sane defaults for federal data APIs.

    Usage::

        client = WellNestHTTPClient(rate_limit=5.0)
        data = client.get_json(
            "https://api.census.gov/data/2022/acs/acs5",
            params={"get": "B17001_001E", "for": "tract:*", "in": "state:17"},
        )
    """

    DEFAULT_TIMEOUT = 45  # seconds -- NCES can be really slow
    DEFAULT_USER_AGENT = "WellNest/0.1 (health-equity-research; contact@chieac.org)"

    def __init__(
        self,
        rate_limit: float = 2.0,
        timeout: int | None = None,
        max_retries: int = 3,
        user_agent: str | None = None,
    ):
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.limiter = RateLimiter(calls_per_second=rate_limit)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent or self.DEFAULT_USER_AGENT,
                "Accept": "application/json",
            }
        )

        # urllib3-level retries handle transient connection blips.
        # tenacity (via retry_on_http_error) handles app-level retries.
        transport_retry = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(
            max_retries=transport_retry,
            pool_connections=10,
            pool_maxsize=20,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ------------------------------------------------------------------
    # Core request methods
    # ------------------------------------------------------------------

    def get_json(self, url: str, params: dict[str, Any] | None = None, **kwargs) -> Any:
        """GET -> parsed JSON.  Raises on 4xx/5xx."""
        self.limiter.wait()
        logger.debug("http_get_json", url=url, params=params)
        resp = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_text(self, url: str, params: dict[str, Any] | None = None, **kwargs) -> str:
        """GET -> raw text.  Useful for CSV endpoints that don't set content-type."""
        self.limiter.wait()
        logger.debug("http_get_text", url=url)
        resp = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.text

    def get_csv_bytes(self, url: str, params: dict[str, Any] | None = None, **kwargs) -> io.BytesIO:
        """GET -> BytesIO buffer.  Feed this straight into polars.read_csv()."""
        self.limiter.wait()
        logger.debug("http_get_csv", url=url)
        resp = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return io.BytesIO(resp.content)

    def download_file(
        self, url: str, dest: Path, chunk_size: int = 8192
    ) -> Path:
        """Stream a large file to disk.  Used for NCES zips, FEMA CSVs, etc."""
        self.limiter.wait()
        logger.info("downloading_file", url=url, dest=str(dest))
        dest.parent.mkdir(parents=True, exist_ok=True)

        with self.session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    fh.write(chunk)
                    downloaded += len(chunk)

        size_mb = dest.stat().st_size / (1024 * 1024)
        logger.info("download_complete", path=str(dest), size_mb=round(size_mb, 2))
        return dest

    # ------------------------------------------------------------------
    # Paginated helpers -- Socrata is the main one that needs this
    # ------------------------------------------------------------------

    def get_socrata_all(
        self,
        base_url: str,
        params: dict[str, Any] | None = None,
        page_size: int = 5000,
        max_rows: int | None = None,
    ) -> list[dict]:
        """Drain a Socrata dataset using $limit / $offset pagination.

        Socrata's default limit is 1000 which almost nobody realizes until
        they get exactly 1000 rows back and wonder where the rest went.
        """
        params = dict(params or {})
        params.setdefault("$limit", page_size)

        all_rows: list[dict] = []
        offset = 0

        while True:
            params["$offset"] = offset
            page = self.get_json(base_url, params=params)

            if not page:
                break

            all_rows.extend(page)
            offset += len(page)

            if len(page) < page_size:
                break
            if max_rows and offset >= max_rows:
                break

            logger.debug("socrata_paging", offset=offset, fetched_so_far=len(all_rows))

        logger.info("socrata_fetch_done", total_rows=len(all_rows), url=base_url)
        return all_rows

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> WellNestHTTPClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
