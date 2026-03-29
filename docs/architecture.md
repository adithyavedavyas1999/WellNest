# Architecture Overview

This document describes the system architecture of WellNest. If you're onboarding onto the project, start here and then skim `data_dictionary.md` and `methodology.md` for the details.

## System Diagram

There's a Mermaid diagram in the top-level README that shows the full data flow visually. Rather than duplicating it here, I'll describe each component in prose and call out the connections between them.

The short version: federal data lands in Postgres via Python ingestion scripts, gets transformed through a dbt medallion architecture (staging -> silver -> gold), feeds into ML/AI pipelines, and ultimately serves five consumer surfaces through a FastAPI backend.

## Component Descriptions

### Ingestion Layer (`ingestion/`)

Twelve Python connectors, each in `ingestion/sources/`, pull data from federal APIs and bulk CSV downloads. They all follow the same pattern: extract, transform with Polars, validate with Pydantic, load into `raw.*` tables in Postgres.

I chose to make each connector independent -- if the CDC API is down, the Census connector still runs fine. This was a lesson learned early on when a flaky HRSA endpoint was crashing the entire pipeline.

Each connector has its own Pydantic schema for validation and a JSON Schema contract in `ingestion/schemas/` for documentation purposes. The HTTP client in `ingestion/utils/http_client.py` wraps requests with retry logic (via tenacity), rate limiting, and structured logging. We hit the Census API at ~3 req/s and CDC at ~2 req/s -- any faster and they start returning 429s without warning.

**Data sources:**

| # | Source | API/Download | Update Frequency |
|---|--------|--------------|-----------------|
| 1 | NCES Common Core of Data | CSV download | Annual |
| 2 | NCES EDGE Locations | Shapefiles/CSV | Annual |
| 3 | CDC PLACES | Socrata API | Annual |
| 4 | CDC Environmental Health | REST API | Annual |
| 5 | Census ACS 5-Year | Census API | Annual (Dec) |
| 6 | EPA AirNow / AQS | REST API + bulk CSV | Real-time / Annual |
| 7 | HRSA HPSA | CSV download | Weekly |
| 8 | HRSA MUA/P | CSV download | Weekly |
| 9 | USDA Food Access Atlas | CSV download | ~5 years |
| 10 | FEMA National Risk Index | CSV download | Annual |
| 11 | NOAA/NWS Alerts | GeoJSON API | Real-time |
| 12 | FBI UCR Crime Data | CSV download | Annual |

### Transformation Layer (`transformation/`)

This is a dbt project with a standard medallion architecture, running against PostgreSQL.

**Staging (`models/staging/`):** Eight staging models, one per raw source (we combine NCES CCD and EDGE at this stage). These do the boring but important stuff -- type casting, column renaming, null handling. The Census connector returns `-666666666` for missing values (documented nowhere in the official Census API docs, by the way), and staging is where we replace that with actual NULLs.

**Silver (`models/silver/`):** Six enriched models that join schools with contextual data. The key design decision here was to use county-level joins for most metrics rather than census tract. Tract-level would be more precise, but the FIPS matching between NCES school data and Census tracts is unreliable for about 2,000 schools that don't have clean tract assignments. County-level joins get us to 99.6% coverage.

- `school_profiles` -- the spine table. Every other silver and gold model joins to this.
- `school_health_context` -- CDC PLACES + Census ACS demographics
- `school_environment` -- EPA AQI + FEMA hazard risk
- `school_safety` -- FBI crime rates + FEMA social vulnerability
- `school_resources` -- HRSA shortage designations + USDA food access
- `community_areas` -- county-level aggregation of everything above

**Gold (`models/gold/`):** Five analytical models that power the API and dashboard.

- `child_wellbeing_score` -- the composite score. This is the centerpiece of the whole project. See `methodology.md` for the full scoring breakdown.
- `school_rankings` -- national, state, and county rankings with percentiles
- `resource_gaps` -- schools with critical gaps suitable for intervention targeting
- `trend_metrics` -- year-over-year changes and anomaly flags
- `county_summary` -- county-level aggregation for the map view

The scoring macros (`macros/scoring.sql`) are where the normalization and pillar computation live. I pulled weights into a dbt seed file (`pillar_weights.csv`) so they're version-controlled and configurable without touching SQL.

### Data Quality (`transformation/quality/`)

Soda Core runs YAML-based checks on silver and gold tables. The checks cover:

- Freshness (is this table stale?)
- Row counts (did we lose data?)
- Value ranges (are scores between 0-100?)
- Completeness (how many nulls?)
- Referential integrity (does every school_health_context row have a matching school_profiles row?)

dbt also runs its own tests defined in the `_schema.yml` files -- uniqueness, not-null, accepted values, and range checks via `dbt_expectations`.

### ML Layer (`ml/`)

Two models, both relatively lightweight:

**Proficiency Predictor** (`training/train_proficiency_predictor.py`): XGBoost regressor that predicts year-over-year change in education score. We predict *change* rather than absolute level because predicting absolute proficiency is basically just predicting poverty, which isn't useful. The model gets an R-squared of 0.25-0.35, which is actually decent for education data (most published models get 0.15-0.25). Anything above 0.5 would make me suspicious of leakage.

**Anomaly Detector** (`training/train_anomaly_detector.py`): Two complementary approaches -- Isolation Forest on the pillar score vectors (catches schools with bizarre profiles) and z-score on year-over-year changes (catches sudden swings). The z-score threshold of 2.5 was tuned empirically -- 2.0 produced too many false positives from normal score volatility, 3.0 missed genuine anomalies in rural data.

Both models are tracked with MLflow (local file store) and serialized as pickle files to `ml/artifacts/`.

### AI Layer (`ai/`)

**Community Briefs** (`briefs/`): GPT-4o-mini generates a ~200-word brief per county for NGO grant proposals. About 3,200 counties total, generated in monthly batches. Cost is roughly $16 per full run. The prompts are in `briefs/prompts.py` and include structured output constraints so the briefs follow a consistent format.

**RAG Pipeline** (`rag/`): LangChain + FAISS over federal education and health policy documents (ESSA guidelines, Title I guidance, CDC school health guidelines). Documents are chunked with `RecursiveCharacterTextSplitter` at 512 tokens with 50-token overlap. Embeddings use `text-embedding-3-small`. The FAISS index is persisted to disk and loaded lazily by the API on the first `/ask` request.

**LLM Quality Validation** (`quality/`): An experimental feature where GPT-4o-mini reviews flagged data anomalies and suggests whether they're data quality issues or genuine events. Still in prototype -- not yet part of the production pipeline.

### API Layer (`api/`)

FastAPI serving data from the gold tables over REST. Key design decisions:

- Raw SQL via SQLAlchemy `text()` rather than an ORM model layer. The gold tables are read-only views built by dbt, so there's no benefit to an ORM. The SQL is simple enough that it stays readable.
- Header-based API key auth (`X-API-Key`). Simple but effective for our use case. If the key isn't configured (dev mode), auth is bypassed entirely.
- In-memory token bucket rate limiter. No Redis dependency. Fine for single-instance, which is all we need.
- Generic `PaginatedResponse[T]` wrapper so every list endpoint has the same envelope shape.

Endpoints:

```
GET  /api/health                    -- health check (no auth)
GET  /api/schools                   -- paginated school list with filters
GET  /api/schools/{nces_id}         -- full school detail with pillar breakdown
GET  /api/schools/{nces_id}/predictions  -- XGBoost prediction for one school
GET  /api/counties                  -- paginated county list
GET  /api/counties/{fips}           -- county detail with AI brief
GET  /api/counties/{fips}/schools   -- schools in a county
GET  /api/search?q=...              -- text search on school name/city/state
GET  /api/rankings                  -- national or state rankings
GET  /api/anomalies                 -- anomaly-flagged schools
POST /api/ask                       -- RAG question answering
GET  /api/reports/{fips}/pdf        -- download county PDF report
GET  /api/stats                     -- quick aggregate stats
```

OpenAPI docs at `/docs`, ReDoc at `/redoc`.

### Dashboard (`dashboard/`)

Streamlit multi-page app with 7 pages:

1. **National Map** -- choropleth of county scores on a Folium map
2. **School Explorer** -- searchable school detail view with score gauge
3. **Resource Gaps** -- schools prioritized for intervention
4. **Trends** -- year-over-year score changes with anomaly highlights
5. **AI Insights** -- community briefs and anomaly narratives
6. **Ask WellNest** -- RAG-powered Q&A interface
7. **Compare** -- side-by-side school or county comparison

Custom Plotly charts with a consistent color palette. The map uses `streamlit-folium` for interactive geospatial display.

### PWA (`pwa/`)

A static HTML/CSS/JS progressive web app for public school lookup. Installable on mobile via the manifest.json. Uses Leaflet.js for maps and talks to the FastAPI backend for data. Service worker caches static assets for offline use (the data itself obviously needs connectivity).

This is intentionally separate from the Streamlit dashboard because Streamlit's widget-heavy interface doesn't work well on mobile. The PWA is a lightweight read-only view optimized for "tell me about this school near me."

### Orchestration (`orchestration/`)

Dagster manages the full pipeline. Assets are organized by layer (bronze, silver, gold, ml, ai, quality). Schedules:

- **Weekly** (Sunday 2 AM UTC): full pipeline -- all 12 sources through gold, ML retraining, quality checks
- **Daily** (6 AM UTC): weather alerts only (NOAA data expires quickly)
- **Monthly** (1st, 4 AM UTC): AI brief regeneration (expensive, data doesn't change fast enough for more frequent runs)

The Dagster webserver runs on port 3000 and provides the asset lineage graph, run history, and schedule management.

### Infrastructure (`infrastructure/`)

**Docker Compose** -- local dev stack with PostgreSQL 16 + PostGIS 3.4, Dagster webserver + daemon, and the FastAPI server. One `docker compose up -d` gets you everything.

**Terraform** -- IaC for AWS (S3 + RDS + Lambda + CloudFront) and Azure (Azure SQL + App Service + Blob Storage). Neither is deployed yet -- the project runs on Supabase free tier for the database and Streamlit Community Cloud for the dashboard.

## Data Flow

```
Federal APIs / CSV Downloads
        |
        v
  [Ingestion - Python/Polars]
        |
        v
  raw.* tables (PostgreSQL)
        |
        v
  [dbt - staging models]
        |
        v
  staging.stg_* (type casts, renames, null handling)
        |
        v
  [dbt - silver models]
        |
        v
  silver.school_profiles (spine)
  silver.school_health_context
  silver.school_environment
  silver.school_safety
  silver.school_resources
  silver.community_areas
        |
        v
  [dbt - gold models]
        |
        v
  gold.child_wellbeing_score (composite scoring)
  gold.school_rankings
  gold.resource_gaps
  gold.trend_metrics
  gold.county_summary
        |
        v
  [ML Pipeline - XGBoost, IsolationForest]
  [AI Pipeline - GPT-4o-mini, FAISS, LangChain]
        |
        v
  gold.school_predictions
  gold.county_ai_briefs
  ml.anomalies
  ai/rag/faiss_index/
        |
        v
  FastAPI REST API
        |
        +------> Streamlit Dashboard
        +------> PWA (public school lookup)
        +------> PDF Reports (per-county)
        +------> Power BI (via direct PostgreSQL connection)
```

## Technology Choices Rationale

**Why Python 3.11+:** Type hints everywhere. The walrus operator and structural pattern matching are occasionally useful. Most importantly, 3.11 has significant performance improvements for the kind of data processing we do.

**Why Polars over pandas:** See `docs/adr/002-polars-over-pandas.md`. Short version: 3-5x faster on our workloads, better memory usage, and the lazy evaluation API catches bugs at compile time rather than runtime.

**Why Dagster over Airflow:** See `docs/adr/001-dagster-over-airflow.md`. Software-defined assets map perfectly to our dbt models and data sources. The type system catches issues before runtime, and the local dev experience is vastly better.

**Why PostgreSQL over a lakehouse:** See `docs/adr/003-postgresql-over-delta-lake.md`. Our data fits in a single Postgres instance (~2GB), we need PostGIS for spatial queries, and Supabase gives us a free tier that keeps costs under $3/month.

**Why FAISS over ChromaDB:** See `docs/adr/004-faiss-over-chromadb.md`. Our vector index is small (~5,000 chunks), FAISS is battle-tested, and persisting to a single file on disk is simpler than running a separate vector database service.

**Why dbt for transformations:** SQL is the right tool for the transformation logic in this project. The silver and gold models are all joins, aggregations, and window functions. Writing that in Python would be more code and harder to test. dbt gives us testing, documentation, and lineage tracking for free.

**Why FastAPI:** Automatic OpenAPI docs, Pydantic integration for request/response validation, and async support if we ever need it. The type safety is also nice -- response models catch serialization bugs before they hit production.

## Infrastructure Overview

### Local Development

```
Docker Compose:
  - PostgreSQL 16 + PostGIS 3.4 (port 5432)
  - Dagster webserver (port 3000)
  - Dagster daemon
  - FastAPI server (port 8000)

Streamlit runs separately:
  - streamlit run dashboard/app.py (port 8501)
```

### Production (Current)

- **Database:** Supabase free tier (PostgreSQL 15 with PostGIS)
- **Dashboard:** Streamlit Community Cloud
- **PWA:** GitHub Pages (static files)
- **API:** Not deployed yet -- runs locally or in Docker

### Production (Planned)

Terraform configs exist for both AWS and Azure, but neither is deployed. The target is to keep monthly costs under $3 by leveraging free tiers aggressively.

## Lessons Learned

**The Census API is a minefield.** The `-666666666` sentinel for missing values is the famous one, but there are others: the first row of the JSON response is a header (unlike every other Census endpoint), you can only request 50 variables per call, and requesting all states at once causes timeouts about 40% of the time. We iterate state by state now.

**County-level joins are good enough.** I initially tried to match every school to a census tract for maximum precision. About 2,000 schools (~1.5%) don't have clean tract FIPS codes, and the join logic was getting increasingly hacky. Switching to county-level joins for most metrics gave us 99.6% coverage with much simpler code. The precision loss is minimal for the metrics we're tracking.

**Score normalization is harder than it sounds.** Min-max scaling is sensitive to outliers. NYC schools and rural Alaska schools produce extreme values that compress everything else into a narrow band. We switched to p5/p95 percentile boundaries, which dampens outlier sensitivity while preserving meaningful variation in the middle of the distribution.

**The z-score threshold for anomaly detection took multiple iterations.** 2.0 standard deviations flagged ~8% of schools, most of which were just normal year-to-year volatility. 3.0 missed genuine anomalies in rural districts where small enrollment changes cause large score swings. 2.5 hits the sweet spot at ~3-5% flagging rate, with about 70% true positive rate based on manual review.

**LLM costs are surprisingly manageable.** A full community brief generation run (3,200 counties) costs about $16 with GPT-4o-mini. The RAG queries are even cheaper. But it adds up if you regenerate frequently, which is why we schedule monthly rather than weekly.

**Dagster's asset model maps beautifully to dbt.** Each dbt model becomes a Dagster asset with automatic upstream dependencies. When the bronze assets finish, silver runs; when silver finishes, gold runs. It's exactly the mental model you want, and it was one of the main reasons I chose Dagster. Airflow's DAG-centric approach would have required much more glue code.
