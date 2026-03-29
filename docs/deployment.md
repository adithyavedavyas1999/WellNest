# Deployment Guide

WellNest is designed to run cheaply -- the target is $0-3/month using free-tier services. This guide covers local development with Docker, production deployment options, and the environment configuration needed for each.

## Local Development with Docker

The local dev stack uses Docker Compose to run PostgreSQL with PostGIS, Dagster (webserver + daemon), and the FastAPI backend.

### Quick Start

```bash
# 1. copy env template
cp .env.example .env
# edit .env -- at minimum set POSTGRES_PASSWORD and CENSUS_API_KEY

# 2. bring up the stack
make docker-up
# or: docker compose -f infrastructure/docker/docker-compose.yml up -d

# 3. verify postgres is ready
docker compose -f infrastructure/docker/docker-compose.yml exec postgres pg_isready

# 4. check the services
open http://localhost:3000  # Dagster UI
open http://localhost:8000/docs  # API docs
```

### What Docker Compose Starts

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| postgres | wellnest-postgres | 5432 | PostgreSQL 16 + PostGIS 3.4 |
| dagster-webserver | wellnest-dagster-webserver | 3000 | Dagster UI |
| dagster-daemon | wellnest-dagster-daemon | -- | Dagster scheduler + sensor daemon |
| api | wellnest-api | 8000 | FastAPI backend |

The `init-db.sql` script runs on first boot and creates the schemas (raw, staging, silver, gold, ml, ai), extensions (PostGIS, pg_trgm, uuid-ossp), and utility functions.

### Services NOT in Docker

The Streamlit dashboard and the PWA run outside Docker:

```bash
# dashboard (separate terminal)
make run-dashboard  # -> http://localhost:8501

# PWA (just open the HTML file)
open pwa/index.html
```

I kept the dashboard out of Docker because Streamlit's hot-reload works better natively, and having the Python source mounted into a container was causing import path headaches during development.

### Tearing Down

```bash
# stop containers but keep data
docker compose -f infrastructure/docker/docker-compose.yml down

# nuke everything including the postgres volume (clean slate)
docker compose -f infrastructure/docker/docker-compose.yml down -v
```

### Troubleshooting Docker

**Port conflict on 5432:** If you have a local Postgres running, either stop it or change `POSTGRES_PORT` in `.env`.

**Dagster can't connect to Postgres:** The Dagster containers use the hostname `postgres` (the Docker service name). If you see connection errors, make sure `DAGSTER_PG_HOST=postgres` in the Docker environment (the compose file handles this, but double-check if you've customized anything).

**Slow first start:** The PostGIS image is ~500MB. The first `docker compose up` will take a few minutes to download. Subsequent starts are fast.

**init-db.sql fails:** The script is idempotent (uses `IF NOT EXISTS` everywhere), but if the Dagster database creation fails, you might need to `docker compose down -v` and start fresh.

## Production Deployment

### Option 1: Supabase + Streamlit Cloud (Current / Recommended)

This is what we're using now. Total cost: $0/month.

**Database: Supabase Free Tier**

Supabase provides a managed PostgreSQL instance with PostGIS support.

1. Create a project at https://supabase.com
2. Go to Settings > Database to get the connection string
3. Run the `init-db.sql` script against the Supabase database:
   ```bash
   psql "postgresql://postgres:[password]@[host]:5432/postgres" -f infrastructure/docker/init-db.sql
   ```
4. Update your `.env` with the Supabase connection string

Free tier limits (as of early 2025):
- 500MB database storage
- 2GB bandwidth per month
- 50,000 monthly active users
- Pauses after 1 week of inactivity (reactivates on next request)

Our data fits in ~200MB, so the 500MB limit is fine. The inactivity pause is annoying but acceptable for a demo/development deployment. For a production NGO deployment, the $25/month Pro tier removes the pause and gives 8GB storage.

**Dashboard: Streamlit Community Cloud**

1. Push the repo to GitHub
2. Go to https://share.streamlit.io
3. Deploy from the repo, pointing at `dashboard/app.py`
4. Set environment variables (DATABASE_URL, etc.) in the Streamlit Cloud secrets UI

Free, no limits for public apps.

**PWA: GitHub Pages**

1. Enable GitHub Pages in repo settings
2. Set the source to the `pwa/` directory (or use a GitHub Action to deploy it)
3. The PWA serves from `https://[user].github.io/wellnest/`

**API: Not yet deployed separately.** The Streamlit dashboard can connect directly to Supabase. For the API, options include:
- Render free tier (spins down after inactivity, 750 hours/month free)
- Railway ($5/month for hobby plan)
- Fly.io (3 shared VMs free)

### Option 2: AWS

Terraform configs are in `infrastructure/terraform/aws/`. Not yet deployed but the configs are ready.

**Architecture:**

```
Route 53 (DNS)
    |
CloudFront (CDN -- serves PWA static files)
    |
Application Load Balancer
    |
    +---> ECS Fargate (FastAPI container)
    +---> ECS Fargate (Dagster webserver)
    |
RDS PostgreSQL (with PostGIS)
    |
S3 (raw data files, model artifacts, PDF reports)
```

**Estimated cost:** $15-25/month with a `db.t3.micro` RDS instance. The Fargate tasks are the main expense -- Dagster only needs to run during pipeline execution, so you can schedule the tasks rather than running 24/7.

**Free tier potential:**
- RDS: 12 months free for `db.t3.micro` (750 hours/month)
- S3: 5GB free forever
- CloudFront: 1TB transfer free per month (first 12 months)
- Lambda: 1M requests/month free (alternative to Fargate for the API)

To deploy:

```bash
cd infrastructure/terraform/aws
terraform init
terraform plan -var-file=../variables.tf
terraform apply
```

### Option 3: Azure

Terraform configs are in `infrastructure/terraform/azure/`.

**Architecture:**

```
Azure Front Door (CDN + WAF)
    |
App Service (FastAPI + Dagster)
    |
Azure SQL (PostgreSQL Flexible Server with PostGIS)
    |
Blob Storage (raw data, artifacts)
```

**Estimated cost:** $10-20/month with a Burstable B1ms database.

Azure Database for PostgreSQL Flexible Server has a free tier:
- B1MS instance (1 vCore, 2GB RAM)
- 32GB storage
- Free for 12 months

This is actually more generous than RDS for our use case.

## Environment Variables Reference

All configuration is via environment variables, loaded through `pydantic-settings`. Create a `.env` file from `.env.example`.

### Database

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTGRES_HOST` | Yes | `localhost` | Database hostname |
| `POSTGRES_PORT` | No | `5432` | Database port |
| `POSTGRES_DB` | No | `wellnest` | Database name |
| `POSTGRES_USER` | No | `wellnest` | Database user |
| `POSTGRES_PASSWORD` | Yes | `changeme` | Database password |
| `DATABASE_URL` | No | (assembled) | Full connection string (overrides individual params) |

### Dagster

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DAGSTER_HOME` | Yes | `/path/to/orchestration` | Dagster working directory |
| `DAGSTER_PG_HOST` | No | (same as POSTGRES_HOST) | Dagster metadata DB host |
| `DAGSTER_PG_PORT` | No | `5432` | Dagster metadata DB port |
| `DAGSTER_PG_DB` | No | `dagster` | Dagster metadata DB name |
| `DAGSTER_PG_USER` | No | (same as POSTGRES_USER) | Dagster metadata DB user |
| `DAGSTER_PG_PASSWORD` | No | (same as POSTGRES_PASSWORD) | Dagster metadata DB password |

### dbt

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DBT_PROFILES_DIR` | No | `./transformation` | Path to profiles.yml |
| `DBT_TARGET` | No | `dev` | dbt target profile |

### API Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_HOST` | No | `0.0.0.0` | Bind address |
| `API_PORT` | No | `8000` | Port number |
| `API_RELOAD` | No | `false` | Auto-reload on file changes |
| `API_LOG_LEVEL` | No | `info` | Logging level |
| `API_KEY` | No | (none) | API key for auth. If unset, auth is disabled. |
| `API_CORS_ORIGINS` | No | `http://localhost:8501,http://localhost:3000` | Comma-separated CORS origins |
| `RATE_LIMIT_REQUESTS` | No | `100` | Max requests per window per IP |
| `RATE_LIMIT_WINDOW` | No | `60` | Rate limit window in seconds |

### OpenAI (for AI features)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | No | (none) | OpenAI API key. AI features disabled if unset. |
| `OPENAI_MODEL` | No | `gpt-4o-mini` | Model for briefs and RAG |
| `OPENAI_EMBEDDING_MODEL` | No | `text-embedding-3-small` | Model for embeddings |

### Data Source API Keys

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CENSUS_API_KEY` | Yes | (none) | Census Bureau API key |
| `SOCRATA_APP_TOKEN` | No | (none) | Socrata app token (increases rate limit) |

### MLflow

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MLFLOW_TRACKING_URI` | No | `http://localhost:5000` | MLflow tracking server URL |
| `MLFLOW_EXPERIMENT_NAME` | No | `wellnest-default` | Default experiment name |

### Misc

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ENVIRONMENT` | No | `development` | Environment name |
| `LOG_LEVEL` | No | `INFO` | Global log level |
| `REPORTS_OUTPUT_DIR` | No | `./reports/output` | PDF report output directory |

## Database Migrations

We don't use a migration tool like Alembic. Here's why:

The schema is managed by dbt. Staging, silver, and gold tables are created/modified by `dbt run`. Raw tables are created by the ingestion scripts (Polars `write_database` with `if_table_exists="replace"`). The `init-db.sql` creates the schemas and extensions.

This works because:
1. All tables are either created by dbt (transformation layer) or by ingestion scripts (raw layer)
2. Schema changes to gold tables are just dbt model changes -- `dbt run` handles the DDL
3. We don't have user-facing write operations that need careful migration

If we ever add user accounts, saved queries, or other state that isn't rebuilt from source data, we'd need to introduce Alembic. But for a read-heavy analytics platform, the current approach keeps things simple.

### Adding a New Table

For raw tables: the ingestion connector creates it on first load. Polars `write_database` with `if_table_exists="replace"` handles schema evolution.

For dbt tables: create the model SQL, run `dbt run --select model_name`, and the table is created with the correct schema.

### Changing a Column Type

For dbt models: modify the SQL, then `dbt run --select model_name --full-refresh`. The `--full-refresh` flag drops and recreates the table.

For raw tables: update the ingestion connector's transform/load logic. On next pipeline run, the table is replaced.

## Monitoring and Alerting

### What to Monitor

1. **Pipeline execution:** Dagster provides run history, success/failure status, and run duration in the UI at http://localhost:3000. Set up Dagster failure sensors to notify on Slack/email.

2. **Data freshness:** Soda Core checks include freshness assertions (`warn_after: {count: 90, period: day}`). dbt source freshness checks run as part of the pipeline.

3. **API health:** Hit `/api/health` from your monitoring tool. It checks database connectivity and returns the API version.

4. **API response times:** The `X-Response-Time` header is on every response. Log aggregation (CloudWatch, Datadog, etc.) can alert on p99 latency.

5. **Data quality:** Soda checks and dbt tests run as part of the weekly pipeline. Review the results in Dagster after each run.

### Current Monitoring

Honestly, monitoring is minimal right now. We have:
- Dagster failure sensors (configured in `orchestration/sensors.py`)
- dbt test results visible in the Dagster UI
- API request logging (method, path, status, duration) via the middleware
- Soda check results stored in the Dagster run logs

What we should add:
- Slack notifications for pipeline failures
- PagerDuty or similar for API downtime
- Score distribution monitoring (alert if the distribution shifts dramatically between pipeline runs)
- Cost monitoring for OpenAI API usage

### GitHub Actions CI/CD

Three workflows in `.github/workflows/`:

**ci.yml** -- runs on every PR:
- Lint (ruff + mypy)
- Unit tests (pytest, skips slow tests)
- dbt compile (syntax check, no database needed)
- Coverage report

**cd.yml** -- runs on merge to main:
- Full test suite with coverage
- Build Docker images
- Deploy to Streamlit Cloud (if configured)
- Deploy PWA to GitHub Pages (if configured)

**data-quality.yml** -- scheduled (after pipeline runs):
- dbt test
- Soda checks
- Report results to Slack (if configured)

## Cost Optimization

The $0-3/month target is achievable with these strategies:

### Database ($0)

Supabase free tier gives us 500MB and a managed PostgreSQL instance. Our data is ~200MB total, so there's headroom. The only catch is the inactivity pause after 7 days without queries -- the dashboard hitting the DB keeps it alive, but if traffic drops, the first request after a pause takes 10-15 seconds to spin up.

### Compute ($0)

- Streamlit Community Cloud: free for public apps
- GitHub Pages for PWA: free
- Dagster runs locally or in GitHub Actions (free tier: 2,000 minutes/month)

### AI ($1-3/month)

- Community briefs: ~$16 per full run (3,200 counties x GPT-4o-mini)
  - But we only run this monthly, so ~$16/month if we regenerate all counties
  - In practice, we regenerate a subset each month to spread costs: ~$3-4/month
- RAG queries: ~$0.001 per query (embedding + completion)
  - At ~100 queries/month: ~$0.10
- Total AI: $3-4/month at current usage

### Strategies to Stay Under $3/month

1. **Batch AI generation.** Don't regenerate all 3,200 county briefs every month. Regenerate only counties whose scores changed significantly since last generation.
2. **Cache aggressively.** PDF reports are cached on disk. RAG embeddings are pre-computed and stored in FAISS. API responses include `X-Response-Time` to track hot paths.
3. **Use free tiers.** Supabase, Streamlit Cloud, GitHub Pages, GitHub Actions free minutes.
4. **Right-size the database.** We don't need a powerful database. The queries are simple selects on pre-computed gold tables. A `db.t3.micro` (or equivalent) is plenty.
5. **Avoid real-time ingestion.** The weekly batch pipeline is much cheaper than real-time CDC or Socrata polling. Weather alerts are the only daily refresh, and NOAA's API is free with no key required.
