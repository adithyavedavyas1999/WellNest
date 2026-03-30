"""
LLM-based data quality validator — uses GPT-4o-mini as a sanity checker.

This is NOT a replacement for statistical QA (that's soda-core's job).
It's a complementary "vibes check" — we sample the most suspicious-looking
records and ask the LLM whether they make sense given what it knows about
US schools and health data.

The LLM catches things that statistical checks miss:
  - A school with enrollment=1 that's actually a virtual testing center
  - A rural county with higher air quality scores than Manhattan (legit but
    flags as anomalous in z-score checks)
  - Scores that are technically valid but contextually nonsensical

Structured output:
  We use response_format=json_object to get reliable JSON back.  The
  schema is simple: verdict (valid/suspicious/invalid), confidence (0-1),
  and a short reason string.

  Early on we tried asking for freeform text and parsing it — that was
  brittle and broke every time OpenAI tweaked the model.  Structured
  output is worth the slightly higher cost from the JSON mode overhead.

Costs:
  Each validation batch (~50 records) runs about 2,400 tokens.
  A full validation run (5 batches of 50 = 250 records) costs ~$0.02.
  We run this monthly, so annual cost is negligible.

TODO: add a "confidence calibration" step where we test the LLM against
records we've manually labeled.  Right now we trust the verdicts at face
value, which is fine for flagging but not for automated remediation.
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from typing import Any

import polars as pl
import structlog
from openai import OpenAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ai.briefs.prompts import DATA_QUALITY_SYSTEM, DATA_QUALITY_USER

logger = structlog.get_logger(__name__)

QUALITY_TABLE: str = "ai.quality_reviews"
DEFAULT_MODEL: str = "gpt-4o-mini"

SAMPLE_SIZE: int = 50
N_BATCHES: int = 5

# thresholds for "suspicious" record sampling
EXTREME_SCORE_LOW: float = 10.0
EXTREME_SCORE_HIGH: float = 98.0
LARGE_YOY_CHANGE: float = 25.0  # points


class LLMValidator:
    """Uses GPT-4o-mini to review suspicious data records.

    Usage::

        validator = LLMValidator(pg_url="postgresql://...", api_key="sk-...")
        results = validator.validate_batch()
        print(f"Found {results['total_suspicious']} suspicious records")
    """

    def __init__(
        self,
        pg_url: str,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
        sample_size: int = SAMPLE_SIZE,
        n_batches: int = N_BATCHES,
    ) -> None:
        self._pg_url: str = pg_url
        self._model: str = model
        self._sample_size: int = sample_size
        self._n_batches: int = n_batches

        resolved_key: str = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not resolved_key:
            raise ValueError("No OpenAI API key — set OPENAI_API_KEY or pass api_key")

        self._client: OpenAI = OpenAI(
            api_key=resolved_key,
            max_retries=2,
            timeout=60.0,
        )

        self._total_prompt_tokens: int = 0
        self._total_completion_tokens: int = 0

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def validate_batch(self) -> dict[str, Any]:
        """Run the full validation pipeline.

        1. Sample suspicious records from gold layer
        2. Send batches to GPT-4o-mini for review
        3. Aggregate results
        4. Persist to database

        Returns summary stats.
        """
        suspicious_df: pl.DataFrame = self._sample_suspicious()

        if suspicious_df.is_empty():
            logger.info("no_suspicious_records_found")
            return {
                "total_reviewed": 0,
                "total_valid": 0,
                "total_suspicious": 0,
                "total_invalid": 0,
            }

        all_reviews: list[dict[str, Any]] = []
        records: list[dict[str, Any]] = list(suspicious_df.iter_rows(named=True))

        for batch_idx in range(self._n_batches):
            start: int = batch_idx * self._sample_size
            end: int = start + self._sample_size
            batch: list[dict[str, Any]] = records[start:end]

            if not batch:
                break

            try:
                reviews: list[dict[str, Any]] = self._validate_record(batch)
                all_reviews.extend(reviews)
            except Exception:
                logger.exception("validation_batch_failed", batch=batch_idx)

            # sleep between batches to be respectful of rate limits
            if batch_idx < self._n_batches - 1:
                time.sleep(5.0)

        if all_reviews:
            self.save_results(all_reviews)

        verdicts: dict[str, int] = {"valid": 0, "suspicious": 0, "invalid": 0}
        for review in all_reviews:
            verdict: str = review.get("verdict", "unknown")
            if verdict in verdicts:
                verdicts[verdict] += 1

        logger.info(
            "validation_complete",
            total=len(all_reviews),
            verdicts=verdicts,
            prompt_tokens=self._total_prompt_tokens,
            completion_tokens=self._total_completion_tokens,
        )

        return {
            "total_reviewed": len(all_reviews),
            "total_valid": verdicts["valid"],
            "total_suspicious": verdicts["suspicious"],
            "total_invalid": verdicts["invalid"],
            "reviews": all_reviews,
        }

    # ------------------------------------------------------------------
    # suspicious record sampling
    # ------------------------------------------------------------------

    def _sample_suspicious(self) -> pl.DataFrame:
        """Pull records that look weird enough to warrant LLM review.

        Sampling strategy:
          - Records with extreme composite scores (<10 or >98)
          - Records with large year-over-year changes (>25 points on any pillar)
          - Records where the composite score doesn't match pillar scores
            (e.g., all pillars >80 but composite <50)
          - Random sample of remaining records for baseline comparison

        We want a mix of likely-bad and likely-good records so we can evaluate
        the LLM's false positive rate.
        """
        total_needed: int = self._sample_size * self._n_batches

        try:
            df: pl.DataFrame = pl.read_database(
                "SELECT * FROM gold.child_wellbeing_score",
                connection=self._pg_url,
            )
        except Exception:
            logger.exception("wellbeing_load_failed")
            return pl.DataFrame()

        if df.is_empty():
            return df

        samples: list[pl.DataFrame] = []

        # extreme scores
        if "wellbeing_score" in df.columns:
            extremes: pl.DataFrame = df.filter(
                (pl.col("wellbeing_score") < EXTREME_SCORE_LOW)
                | (pl.col("wellbeing_score") > EXTREME_SCORE_HIGH)
            )
            if not extremes.is_empty():
                n: int = min(total_needed // 3, len(extremes))
                samples.append(extremes.sample(n=n, seed=42))

        # large YoY changes — check whichever columns exist
        yoy_cols: list[str] = [c for c in df.columns if c.endswith("_yoy_change")]
        if yoy_cols:
            yoy_filter = pl.lit(False)
            for col in yoy_cols:
                yoy_filter = yoy_filter | (pl.col(col).abs() > LARGE_YOY_CHANGE)

            big_changes: pl.DataFrame = df.filter(yoy_filter)
            if not big_changes.is_empty():
                n = min(total_needed // 3, len(big_changes))
                samples.append(big_changes.sample(n=n, seed=42))

        # inconsistent composite vs pillar scores
        pillar_cols: list[str] = [
            c
            for c in [
                "education_score",
                "health_score",
                "environment_score",
                "safety_score",
                "economic_score",
            ]
            if c in df.columns
        ]
        if pillar_cols and "wellbeing_score" in df.columns:
            pillar_mean_expr = pl.mean_horizontal(*[pl.col(c) for c in pillar_cols])
            inconsistent: pl.DataFrame = (
                df.with_columns(pillar_mean_expr.alias("_pillar_avg"))
                .filter((pl.col("wellbeing_score") - pl.col("_pillar_avg")).abs() > 20)
                .drop("_pillar_avg")
            )

            if not inconsistent.is_empty():
                n = min(total_needed // 4, len(inconsistent))
                samples.append(inconsistent.sample(n=n, seed=42))

        # fill remainder with random baseline
        collected: int = sum(len(s) for s in samples)
        remaining: int = total_needed - collected
        if remaining > 0:
            n = min(remaining, len(df))
            samples.append(df.sample(n=n, seed=123))

        if not samples:
            return df.sample(n=min(total_needed, len(df)), seed=42)

        combined: pl.DataFrame = pl.concat(samples)

        # deduplicate (a record can be extreme AND have a large YoY change)
        id_col: str = (
            "nces_school_id" if "nces_school_id" in combined.columns else combined.columns[0]
        )
        combined = combined.unique(subset=[id_col])

        return combined.head(total_needed)

    # ------------------------------------------------------------------
    # LLM validation
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=5, min=5, max=60),
        reraise=True,
    )
    def _validate_record(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Send a batch of records to GPT-4o-mini for quality review.

        Returns a list of review dicts, one per record.
        """
        records_json: str = json.dumps(records, default=str, indent=2)

        # truncate if too long — gpt-4o-mini context is 128K but we don't
        # want to burn tokens on a huge prompt when 50 records is plenty
        if len(records_json) > 30000:
            records_json = records_json[:30000] + "\n... (truncated)"

        user_prompt: str = DATA_QUALITY_USER.format(
            record_count=len(records),
            records_json=records_json,
        )

        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": DATA_QUALITY_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=2000,
        )

        if response.usage:
            self._total_prompt_tokens += response.usage.prompt_tokens
            self._total_completion_tokens += response.usage.completion_tokens

        raw: str = response.choices[0].message.content or "{}"
        parsed: dict[str, Any] = json.loads(raw)

        reviews: list[dict[str, Any]] = parsed.get("reviews", [])

        # attach timestamps and model info
        now: str = datetime.now(UTC).isoformat()
        for review in reviews:
            review["model"] = self._model
            review["reviewed_at"] = now

        return reviews

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------

    def save_results(self, reviews: list[dict[str, Any]]) -> None:
        """Persist validation results to the ai.quality_reviews table."""
        if not reviews:
            return

        # flatten to a consistent schema for Polars
        rows: list[dict[str, Any]] = []
        for r in reviews:
            rows.append(
                {
                    "record_id": str(r.get("record_id", "")),
                    "verdict": r.get("verdict", "unknown"),
                    "confidence": float(r.get("confidence", 0.0)),
                    "reason": str(r.get("reason", "")),
                    "model": r.get("model", self._model),
                    "reviewed_at": r.get("reviewed_at", datetime.now(UTC).isoformat()),
                }
            )

        df: pl.DataFrame = pl.DataFrame(rows)

        try:
            from sqlalchemy import create_engine, text

            engine = create_engine(self._pg_url)
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS ai"))
            engine.dispose()

            df.write_database(
                table_name=QUALITY_TABLE,
                connection=self._pg_url,
                if_table_exists="append",
                engine="sqlalchemy",
            )

            logger.info("quality_reviews_saved", count=len(rows))

        except Exception:
            # dump to disk so we don't lose the LLM output
            fallback: str = f"/tmp/wellnest_quality_reviews_{int(time.time())}.json"
            with open(fallback, "w") as f:
                json.dump(rows, f, default=str)
            logger.exception("quality_save_failed", fallback=fallback)
