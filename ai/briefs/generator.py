"""
Community brief generator — batch-produce county-level wellbeing summaries.

Processes all ~3,200 US counties through GPT-4o-mini to generate 200-word
briefs for NGO grant proposals.  Each brief summarizes the Child Wellbeing
Index data for a county in plain language.

Rate limiting strategy:
  OpenAI's tier-1 rate limits for gpt-4o-mini are 500 RPM / 200K TPM.
  Each brief is ~1,300 tokens, so we can do about 150 counties/minute before
  hitting the TPM limit.  We process in batches of 20 with a 10-second sleep
  between batches, which puts us at ~120 RPM — well under the limit with
  headroom for retries.

  If you're on tier-2+ you can bump BATCH_SIZE and shrink BATCH_SLEEP_SEC,
  but honestly the bottleneck is usually Postgres writes, not the API.

Cost tracking:
  Full run (3,200 counties) costs approximately $2.75 at 2025-01 pricing.
  We log token counts per batch so you can estimate costs from the logs.
  TODO: pipe token counts into a dagster asset metadata field so we can
  track spend over time without digging through logs.

Caching:
  Briefs are cached in the gold.county_ai_briefs table.  If a county
  already has a brief from the current month, we skip it.  Pass
  force_refresh=True to regenerate everything.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import polars as pl
import structlog
from openai import (
    APIConnectionError,
    APITimeoutError,
    OpenAI,
    RateLimitError,
)
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ai.briefs.prompts import COUNTY_BRIEF_SYSTEM, COUNTY_BRIEF_USER

logger = structlog.get_logger(__name__)

BRIEFS_TABLE: str = "gold.county_ai_briefs"
DEFAULT_MODEL: str = "gpt-4o-mini"

# batch tuning — see docstring for the math
BATCH_SIZE: int = int(os.environ.get("BRIEF_BATCH_SIZE", "20"))
BATCH_SLEEP_SEC: float = float(os.environ.get("BRIEF_BATCH_SLEEP", "10.0"))

# safety valve so a runaway loop doesn't drain the API budget
MAX_COUNTIES: int = int(os.environ.get("BRIEF_MAX_COUNTIES", "3500"))


class BriefGenerator:
    """Generates AI-written community briefs for US counties.

    Typical usage::

        gen = BriefGenerator(pg_url="postgresql://...", api_key="sk-...")
        gen.generate_all(force_refresh=False)

    Or for a single county during development::

        brief = gen.generate_for_county(county_row)
        print(brief)
    """

    def __init__(
        self,
        pg_url: str,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
        batch_size: int = BATCH_SIZE,
        batch_sleep: float = BATCH_SLEEP_SEC,
    ) -> None:
        self._pg_url: str = pg_url
        self._model: str = model
        self._batch_size: int = batch_size
        self._batch_sleep: float = batch_sleep

        resolved_key: str = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not resolved_key:
            raise ValueError("No OpenAI API key — set OPENAI_API_KEY or pass api_key")

        self._client: OpenAI = OpenAI(
            api_key=resolved_key,
            max_retries=2,
            timeout=45.0,
        )

        self._total_prompt_tokens: int = 0
        self._total_completion_tokens: int = 0

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def generate_all(self, *, force_refresh: bool = False) -> dict[str, int]:
        """Generate briefs for all counties in gold.county_summary.

        Returns a dict with counts: generated, skipped, failed.
        """
        county_df: pl.DataFrame = self._load_counties()
        if county_df.is_empty():
            logger.warning("no_counties_found", table="gold.county_summary")
            return {"generated": 0, "skipped": 0, "failed": 0}

        existing_fips: set[str] = set()
        if not force_refresh:
            existing_fips = self._get_existing_briefs_this_month()

        rows: list[dict[str, Any]] = list(county_df.head(MAX_COUNTIES).iter_rows(named=True))

        generated: int = 0
        skipped: int = 0
        failed: int = 0
        batch_buffer: list[dict[str, Any]] = []

        for row in rows:
            fips: str = str(row.get("county_fips", row.get("fips_code", "")))

            if fips in existing_fips:
                skipped += 1
                continue

            try:
                brief_text: str = self.generate_for_county(row)
                batch_buffer.append(self._make_record(row, brief_text))
                generated += 1
            except Exception:
                logger.exception("brief_failed", county=row.get("county_name", "?"))
                failed += 1

            # flush batch and sleep to respect rate limits
            if len(batch_buffer) >= self._batch_size:
                self._flush_batch(batch_buffer)
                batch_buffer.clear()
                logger.info(
                    "brief_batch_complete",
                    generated=generated,
                    skipped=skipped,
                    failed=failed,
                    prompt_tokens=self._total_prompt_tokens,
                    completion_tokens=self._total_completion_tokens,
                )
                time.sleep(self._batch_sleep)

        # flush any remaining
        if batch_buffer:
            self._flush_batch(batch_buffer)

        logger.info(
            "brief_generation_complete",
            generated=generated,
            skipped=skipped,
            failed=failed,
            total_prompt_tokens=self._total_prompt_tokens,
            total_completion_tokens=self._total_completion_tokens,
        )

        return {"generated": generated, "skipped": skipped, "failed": failed}

    def generate_for_county(self, county_row: dict[str, Any]) -> str:
        """Generate a single brief for one county.  Good for testing prompts."""
        prompt: str = self._build_prompt(county_row)
        return self._call_llm(prompt)

    # ------------------------------------------------------------------
    # prompt construction
    # ------------------------------------------------------------------

    def _build_prompt(self, row: dict[str, Any]) -> str:
        """Fill the prompt template with county data.

        We do a best-effort fill — if some fields are missing we substitute
        "N/A" rather than crashing.  The LLM handles missing data gracefully
        enough (tested with ~50 counties that had partial data).
        """
        pillar_scores: dict[str, float] = {}
        for pillar in ["education", "health", "environment", "safety", "economic"]:
            key: str = f"avg_{pillar}_score"
            alt_key: str = f"{pillar}_score"
            val = row.get(key, row.get(alt_key))
            pillar_scores[pillar] = float(val) if val is not None else 0.0

        best_pillar: str = max(pillar_scores, key=pillar_scores.get)  # type: ignore[arg-type]
        worst_pillar: str = min(pillar_scores, key=pillar_scores.get)  # type: ignore[arg-type]

        template_vars: dict[str, Any] = {
            "county_name": row.get("county_name", "Unknown County"),
            "state_name": row.get("state_name", row.get("state_abbr", "Unknown")),
            "fips_code": row.get("county_fips", row.get("fips_code", "00000")),
            "population": int(row.get("total_population", row.get("population", 0)) or 0),
            "school_count": int(row.get("school_count", 0) or 0),
            "avg_wellbeing_score": float(row.get("avg_wellbeing_score", 0) or 0),
            "education_score": pillar_scores["education"],
            "health_score": pillar_scores["health"],
            "environment_score": pillar_scores["environment"],
            "safety_score": pillar_scores["safety"],
            "economic_score": pillar_scores["economic"],
            "top_strength": best_pillar.replace("_", " ").title(),
            "top_concern": worst_pillar.replace("_", " ").title(),
            "yoy_change_pct": float(row.get("yoy_change_pct", row.get("score_change_pct", 0)) or 0),
        }

        return COUNTY_BRIEF_USER.format(**template_vars)

    # ------------------------------------------------------------------
    # LLM call with retry
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((RateLimitError, APITimeoutError, APIConnectionError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=5, min=5, max=120),
        reraise=True,
    )
    def _call_llm(self, user_prompt: str) -> str:
        """Send a single brief request to the OpenAI API.

        Retries on rate limit (429), timeout, and connection errors.
        The wait_exponential starts at 5s because OpenAI's rate limit
        reset window is usually 60s and we want the first retry to
        actually have a chance of succeeding.
        """
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": COUNTY_BRIEF_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=600,
        )

        usage = response.usage
        if usage:
            self._total_prompt_tokens += usage.prompt_tokens
            self._total_completion_tokens += usage.completion_tokens

        return response.choices[0].message.content or ""

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------

    def save_brief(self, record: dict[str, Any]) -> None:
        """Save a single brief record to the database."""
        self._flush_batch([record])

    def _flush_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of brief records to gold.county_ai_briefs."""
        if not records:
            return

        df: pl.DataFrame = pl.DataFrame(records)

        try:
            from sqlalchemy import create_engine, text

            engine = create_engine(self._pg_url)
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
            engine.dispose()

            df.write_database(
                table_name=BRIEFS_TABLE,
                connection=self._pg_url,
                if_table_exists="append",
                engine="sqlalchemy",
            )
        except Exception:
            # don't lose the whole batch — dump to disk as a fallback
            fallback_path: str = f"/tmp/wellnest_briefs_fallback_{int(time.time())}.json"
            with open(fallback_path, "w") as f:
                json.dump(records, f, default=str)
            logger.exception("brief_save_failed", fallback=fallback_path, count=len(records))

    def _make_record(self, county_row: dict[str, Any], brief_text: str) -> dict[str, Any]:
        """Build a database record from a county row and generated brief."""
        return {
            "county_fips": str(county_row.get("county_fips", county_row.get("fips_code", ""))),
            "county_name": county_row.get("county_name", "Unknown"),
            "state_name": county_row.get("state_name", county_row.get("state_abbr", "")),
            "brief_text": brief_text,
            "model": self._model,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "prompt_version": "v1",  # bump this when you change the prompt template
        }

    # ------------------------------------------------------------------
    # data loading
    # ------------------------------------------------------------------

    def _load_counties(self) -> pl.DataFrame:
        """Load county summary data from the gold layer."""
        try:
            return pl.read_database(
                "SELECT * FROM gold.county_summary ORDER BY county_fips",
                connection=self._pg_url,
            )
        except Exception:
            logger.exception("county_load_failed")
            return pl.DataFrame()

    def _get_existing_briefs_this_month(self) -> set[str]:
        """Check which counties already have briefs generated this month.

        This is our caching mechanism — regenerating all 3,200 briefs every
        run would cost ~$3 and take 45 minutes for no reason if the underlying
        data hasn't changed.
        """
        month_start: str = datetime.now(timezone.utc).replace(day=1).strftime("%Y-%m-%d")

        try:
            df: pl.DataFrame = pl.read_database(
                f"SELECT county_fips FROM {BRIEFS_TABLE} "
                f"WHERE generated_at >= '{month_start}'",
                connection=self._pg_url,
            )
            return set(df["county_fips"].to_list())
        except Exception:
            # table might not exist yet on first run
            return set()
