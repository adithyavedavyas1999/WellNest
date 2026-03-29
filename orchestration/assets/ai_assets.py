"""
AI pipeline assets — RAG index, community briefs, LLM-based quality checks.

These are the most expensive assets to run (OpenAI API costs) so they're
on a monthly schedule by default.  The RAG index is used by the community
Q&A chatbot; briefs are auto-generated PDF summaries for each county.

Token costs as of last measurement (2024-12):
  - RAG index build: ~2M tokens embedding, ~$0.40
  - County briefs (all ~3200 counties): ~5M tokens, ~$15
  - Quality checks (spot-check 100 records): ~200K tokens, ~$0.60

TODO: add cost tracking per run so we can alert if spend spikes.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import structlog
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from orchestration.resources import OpenAIResource, PostgresResource

logger = structlog.get_logger(__name__)

AI_GROUP = "ai"
AI_TAGS = {"layer": "ai", "pipeline": "ai"}

RAG_INDEX_DIR = Path(__file__).resolve().parent.parent.parent / "ai" / "rag" / "index"
BRIEFS_OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "reports" / "briefs"

# max counties to generate briefs for in a single run
# set lower for dev/testing to save API costs
BRIEF_BATCH_SIZE = int(os.environ.get("BRIEF_BATCH_SIZE", "50"))


# ---------------------------------------------------------------------------
# RAG index
# ---------------------------------------------------------------------------

@asset(
    group_name=AI_GROUP,
    tags=AI_TAGS,
    deps=[
        "gold_child_wellbeing_score",
        "gold_county_summary",
        "gold_tract_summary",
    ],
    description=(
        "Builds a FAISS vector index over the gold layer data for the "
        "community Q&A chatbot.  Chunks school/county/tract records into "
        "text, embeds them, and writes the index to disk."
    ),
)
def ai_rag_index(
    context,
    postgres: PostgresResource,
    openai: OpenAIResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    county_df = _load_or_empty(engine, "gold.county_summary")
    school_df = _load_or_empty(engine, "gold.child_wellbeing_score")

    if county_df.is_empty() and school_df.is_empty():
        context.log.warning("No gold data available for RAG index")
        return MaterializeResult(metadata={"status": "skipped", "reason": "no data"})

    documents: list[str] = []
    metadata_list: list[dict[str, Any]] = []

    for row in county_df.iter_rows(named=True):
        doc = _county_to_document(row)
        documents.append(doc)
        metadata_list.append({
            "type": "county",
            "state_fips": row.get("state_fips", ""),
            "county_fips": row.get("county_fips", ""),
        })

    # sample schools to keep index size reasonable
    # TODO: switch to hierarchical chunking (county -> school) for better retrieval
    school_sample = school_df.sample(
        n=min(5000, len(school_df)),
        seed=42,
    ) if len(school_df) > 5000 else school_df

    for row in school_sample.iter_rows(named=True):
        doc = _school_to_document(row)
        documents.append(doc)
        metadata_list.append({
            "type": "school",
            "ncessch": row.get("ncessch", ""),
        })

    context.log.info(f"Embedding {len(documents)} documents for RAG index")

    # embed in batches — OpenAI embedding API accepts up to 2048 inputs
    BATCH_SIZE = 512
    all_embeddings: list[list[float]] = []

    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i : i + BATCH_SIZE]
        embeddings = openai.get_embeddings(batch)
        all_embeddings.extend(embeddings)
        context.log.info(f"Embedded batch {i // BATCH_SIZE + 1}/{(len(documents) - 1) // BATCH_SIZE + 1}")

    # build FAISS index
    import numpy as np

    try:
        import faiss

        dim = len(all_embeddings[0])
        index = faiss.IndexFlatIP(dim)

        vectors = np.array(all_embeddings, dtype=np.float32)
        faiss.normalize_L2(vectors)
        index.add(vectors)

        RAG_INDEX_DIR.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(RAG_INDEX_DIR / "wellnest.index"))

        with open(RAG_INDEX_DIR / "documents.json", "w") as f:
            json.dump(documents, f)
        with open(RAG_INDEX_DIR / "metadata.json", "w") as f:
            json.dump(metadata_list, f)

        context.log.info(f"FAISS index written to {RAG_INDEX_DIR}")
        status = "success"

    except ImportError:
        context.log.warning("faiss-cpu not installed, saving raw embeddings only")
        RAG_INDEX_DIR.mkdir(parents=True, exist_ok=True)
        np.save(str(RAG_INDEX_DIR / "embeddings.npy"), np.array(all_embeddings))
        with open(RAG_INDEX_DIR / "documents.json", "w") as f:
            json.dump(documents, f)
        with open(RAG_INDEX_DIR / "metadata.json", "w") as f:
            json.dump(metadata_list, f)
        status = "partial (no faiss)"

    return MaterializeResult(
        metadata={
            "status": status,
            "document_count": len(documents),
            "embedding_dim": len(all_embeddings[0]) if all_embeddings else 0,
            "index_path": MetadataValue.path(str(RAG_INDEX_DIR)),
        },
    )


# ---------------------------------------------------------------------------
# Community briefs
# ---------------------------------------------------------------------------

@asset(
    group_name=AI_GROUP,
    tags=AI_TAGS,
    deps=["gold_county_summary", "gold_child_wellbeing_score"],
    description=(
        "Auto-generated community brief for each county.  Uses GPT-4o to "
        "synthesize wellbeing data into a readable 1-page summary.  Output "
        "is markdown that gets rendered to PDF by the reports module."
    ),
)
def ai_community_briefs(
    context,
    postgres: PostgresResource,
    openai: OpenAIResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    county_df = _load_or_empty(engine, "gold.county_summary")

    if county_df.is_empty():
        context.log.warning("No county data for brief generation")
        return MaterializeResult(metadata={"status": "skipped", "reason": "no data"})

    BRIEFS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    counties = county_df.head(BRIEF_BATCH_SIZE)
    generated = 0
    errors = 0

    for row in counties.iter_rows(named=True):
        county_name = row.get("county_name", "Unknown")
        state = row.get("state_name", row.get("state_fips", ""))

        try:
            brief = _generate_county_brief(openai, row)
            slug = f"{state}_{county_name}".lower().replace(" ", "_").replace(".", "")
            out_path = BRIEFS_OUTPUT_DIR / f"{slug}.md"
            out_path.write_text(brief, encoding="utf-8")
            generated += 1
        except Exception as e:
            logger.warning("brief_generation_failed", county=county_name, error=str(e))
            errors += 1

    context.log.info(f"Generated {generated} briefs, {errors} failures")

    return MaterializeResult(
        metadata={
            "briefs_generated": generated,
            "briefs_failed": errors,
            "batch_size": BRIEF_BATCH_SIZE,
            "output_dir": MetadataValue.path(str(BRIEFS_OUTPUT_DIR)),
        },
    )


# ---------------------------------------------------------------------------
# LLM data quality validation
# ---------------------------------------------------------------------------

@asset(
    group_name=AI_GROUP,
    tags={**AI_TAGS, "quality": "true"},
    deps=["gold_child_wellbeing_score"],
    description=(
        "Uses GPT-4o to spot-check wellbeing scores for reasonableness.  "
        "Picks a random sample of schools and asks the LLM to flag any "
        "scores that seem implausible given the underlying indicators.  "
        "Not a replacement for statistical QA — more of a vibes check."
    ),
)
def ai_quality_validation(
    context,
    postgres: PostgresResource,
    openai: OpenAIResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    df = _load_or_empty(engine, "gold.child_wellbeing_score")

    if df.is_empty():
        return MaterializeResult(metadata={"status": "skipped", "reason": "no data"})

    sample_size = min(100, len(df))
    sample = df.sample(n=sample_size, seed=42)

    records_text = ""
    for row in sample.iter_rows(named=True):
        records_text += json.dumps(row, default=str) + "\n"

    prompt = f"""You are a data quality analyst reviewing child wellbeing scores for US schools.

Each record has a composite wellbeing_score (0-100) and individual pillar scores
(education, health, environment, safety, economic — each 0-100).

Review the following {sample_size} records and identify:
1. Scores that seem implausible (e.g., a school with excellent education but
   rock-bottom overall score without explanation)
2. Suspicious patterns (e.g., many schools with identical scores, scores at
   exact round numbers suggesting default values)
3. Missing data that could bias the scores

Respond in JSON with keys: "issues" (list of {{school, issue, severity}}),
"overall_quality" (good/acceptable/poor), "summary" (1-2 sentences).

Records:
{records_text[:15000]}"""

    try:
        response = openai.chat(
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2,
        )

        result = json.loads(response)
        n_issues = len(result.get("issues", []))
        quality = result.get("overall_quality", "unknown")
        summary = result.get("summary", "")

        postgres.ensure_schema("ml")
        _store_quality_result(postgres, result)

    except Exception as e:
        context.log.error(f"LLM quality check failed: {e}")
        return MaterializeResult(
            metadata={"status": "error", "error": str(e)},
        )

    context.log.info(f"LLM quality check: {quality}, {n_issues} issues found")

    return MaterializeResult(
        metadata={
            "overall_quality": quality,
            "issues_found": n_issues,
            "sample_size": sample_size,
            "summary": MetadataValue.text(summary),
            "full_result": MetadataValue.json(result),
        },
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _load_or_empty(engine: Any, table: str) -> pl.DataFrame:
    try:
        return pl.read_database(f"SELECT * FROM {table}", connection=engine)
    except Exception:
        return pl.DataFrame()


def _county_to_document(row: dict[str, Any]) -> str:
    """Convert a county summary row to a text chunk for embedding."""
    parts = [
        f"County: {row.get('county_name', 'Unknown')}, {row.get('state_name', '')}",
        f"Average wellbeing score: {row.get('avg_wellbeing_score', 'N/A')}",
        f"Number of schools: {row.get('school_count', 'N/A')}",
        f"Population: {row.get('total_population', 'N/A')}",
    ]

    for pillar in ["education", "health", "environment", "safety", "economic"]:
        key = f"avg_{pillar}_score"
        if key in row and row[key] is not None:
            parts.append(f"Average {pillar} score: {row[key]}")

    return " | ".join(parts)


def _school_to_document(row: dict[str, Any]) -> str:
    """Convert a school wellbeing row to a text chunk for embedding."""
    parts = [
        f"School: {row.get('school_name', 'Unknown')}",
        f"Wellbeing score: {row.get('wellbeing_score', 'N/A')}",
    ]

    for pillar in ["education", "health", "environment", "safety", "economic"]:
        key = f"{pillar}_score"
        if key in row and row[key] is not None:
            parts.append(f"{pillar.title()}: {row[key]}")

    return " | ".join(parts)


def _generate_county_brief(openai_resource: OpenAIResource, county: dict[str, Any]) -> str:
    """Generate a markdown community brief for a single county."""
    county_json = json.dumps(county, default=str)

    prompt = f"""Write a concise community wellbeing brief for this county.  Use a professional
but accessible tone — the audience is school administrators and community leaders.

Include:
- Overall wellbeing summary (2-3 sentences)
- Key strengths (based on highest pillar scores)
- Areas of concern (based on lowest pillar scores)
- One specific, actionable recommendation

Format as markdown with headers.  Keep it under 500 words.

County data:
{county_json}"""

    return openai_resource.chat(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=1000,
    )


def _store_quality_result(pg: PostgresResource, result: dict[str, Any]) -> None:
    """Persist the LLM quality check result to Postgres for tracking."""
    try:
        record = pl.DataFrame([{
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "overall_quality": result.get("overall_quality", "unknown"),
            "issues_count": len(result.get("issues", [])),
            "summary": result.get("summary", ""),
            "full_result": json.dumps(result),
        }])
        record.write_database(
            table_name="ml.llm_quality_checks",
            connection=pg.connection_url,
            if_table_exists="append",
            engine="sqlalchemy",
        )
    except Exception as e:
        logger.warning("quality_result_store_failed", error=str(e))


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_AI_ASSETS: list = [
    ai_rag_index,
    ai_community_briefs,
    ai_quality_validation,
]
