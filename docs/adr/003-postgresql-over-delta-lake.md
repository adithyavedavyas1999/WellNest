# ADR-003: PostgreSQL over Delta Lake for Storage

**Status:** Accepted
**Date:** 2024-08
**Author:** Engineering

## Context

WellNest needs a storage layer for ~2GB of data across raw, staging, silver, and gold tables. The data is primarily tabular (schools, counties, census tracts, scores, predictions). We also store geospatial data (school coordinates, county boundaries) and vector embeddings (for the RAG pipeline).

The project has a hard constraint: total infrastructure cost must stay under $3/month. This rules out most managed data warehouse services at production scale.

The team considered several storage architectures ranging from a traditional RDBMS to lakehouse approaches.

## Decision

We chose PostgreSQL 16 with PostGIS 3.4, hosted on Supabase's free tier for production and Docker for local development.

## What We Considered

### Delta Lake (on S3/local)

Delta Lake is the lakehouse format that sits on top of Parquet files. Combined with DuckDB or Spark for queries, it provides ACID transactions, schema evolution, and time travel on file-based storage.

This is the "modern data stack" approach. Store everything as Parquet in S3, use Delta Lake for ACID guarantees, query with DuckDB or Spark.

**What we liked:** Schema evolution is painless. Time travel (query historical versions) would be useful for debugging score changes. Parquet + S3 is extremely cheap. DuckDB can query it with zero infrastructure.

**Why we rejected it:**

1. **No PostGIS equivalent.** We need spatial queries: "find the nearest census tract to this school," "which schools fall within this county polygon." PostGIS handles these natively with spatial indexes. Delta Lake + DuckDB has the `spatial` extension, but it's much less mature -- no spatial indexes, limited function coverage, and I don't trust it for production geospatial workloads yet.

2. **Operational complexity.** A Delta Lake architecture means managing S3 (or MinIO locally), a metastore (Unity Catalog, Hive, or manual), and a query engine. That's three moving parts instead of one. For a two-person project targeting $0/month infra, this is too much.

3. **dbt support is indirect.** dbt doesn't natively support Delta Lake as a target. There's `dbt-duckdb` which can write to Parquet/Delta, but it's less mature than `dbt-postgres`. Our transformation layer is dbt-heavy (20+ models), and I don't want to be debugging dbt adapter issues on top of the actual data engineering work.

4. **Serving layer mismatch.** The API serves paginated, filtered queries over gold tables. PostgreSQL handles this natively with indexes, LIMIT/OFFSET, and WHERE clauses. Serving a FastAPI app from Delta Lake would require loading Parquet files into DuckDB for each request, which adds latency and memory overhead. Or we'd need a separate serving database anyway, defeating the purpose.

### Snowflake / BigQuery / Redshift

Cloud data warehouses are powerful but expensive. Snowflake's free trial is generous but expires. BigQuery has a free tier (1TB queries/month) but requires GCP infrastructure. Redshift Serverless has no free tier.

**Why we rejected these:** Cost. Even the cheapest option (BigQuery free tier) requires GCP accounts, IAM setup, and network configuration. The project's target audience (small NGOs) shouldn't need a cloud provider account to run WellNest. And once you exceed the free tier, costs scale quickly.

### SQLite

SQLite would be the simplest option: a single file, no server, zero configuration. DuckDB is similar but optimized for analytical queries.

**Why we rejected it:** No PostGIS. SQLite has SpatiaLite for spatial queries, but it's fiddly to install (requires C extension compilation) and much less capable than PostGIS. Also, SQLite doesn't support concurrent writers, which matters when the ingestion pipeline and API are both running.

### PostgreSQL

Boring technology. Battle-tested. First-class PostGIS support. Free tier available from multiple providers (Supabase, Neon, ElephantSQL). dbt-postgres is the most mature dbt adapter. FastAPI + SQLAlchemy + PostgreSQL is a well-trodden path.

## Why PostgreSQL Won

### 1. PostGIS for spatial queries

We do several types of spatial operations:
- Match schools to census tracts using lat/lon point-in-polygon
- Find the nearest HPSA designation area to a school
- Compute county centroids for the map view
- Aggregate tract-level data to county level with geographic context

PostGIS handles all of these efficiently. The `ST_Within`, `ST_Distance`, and `ST_Centroid` functions are mature and well-indexed. I've used PostGIS on three previous projects and trust it for production workloads.

The alternative (doing spatial joins in Python with GeoPandas) works for batch processing but doesn't help for API-time spatial queries.

### 2. Free tier availability

Supabase offers a free PostgreSQL instance with PostGIS enabled:
- 500MB storage (we use ~200MB)
- 2GB transfer/month
- PostGIS, pg_trgm, uuid-ossp extensions available
- Pauses after 7 days of inactivity (annoying but workable)

This keeps us at $0/month for production. For comparison:
- Neon: free tier has 512MB storage but no PostGIS
- ElephantSQL: free tier is only 20MB (too small)
- Railway: $5/month minimum

### 3. Simplicity

One database serves everything: raw data storage, dbt transformations, API queries, and Dagster metadata. There's no S3 to configure, no metastore to manage, no separate serving layer. The Docker Compose setup is one service (`postgis/postgis:16-3.4`), and `init-db.sql` creates the schemas.

For a small team, this simplicity is a feature. Every additional service is another thing to configure, monitor, and debug.

### 4. Query patterns fit perfectly

Our gold-layer queries are simple:
- `SELECT * FROM gold.child_wellbeing_score WHERE state = 'IL' ORDER BY composite_score LIMIT 50 OFFSET 100`
- `SELECT ... FROM gold.county_summary WHERE fips = '17031'`
- `SELECT ... FROM gold.school_rankings WHERE nces_id = '170993000943'`

These are bread-and-butter PostgreSQL queries. A btree index on `state`, `county_fips`, and `nces_id` keeps them fast. The entire gold layer is ~130K rows -- PostgreSQL handles this without breaking a sweat, even on a small instance.

We also use `ILIKE` for text search (school name lookup), `percentile_cont` for score normalization, and window functions (`RANK() OVER`) for rankings. All native PostgreSQL features.

### 5. dbt-postgres is the most mature adapter

dbt-postgres is the original dbt adapter and the most stable. Feature parity with dbt-core is essentially 100%. The macro ecosystem (dbt-utils, dbt-expectations) all work perfectly. We've had zero adapter-related issues.

## Consequences

### Positive

- Zero infrastructure cost on Supabase free tier.
- PostGIS handles all spatial query needs.
- Single database simplifies operations, backups, and debugging.
- dbt-postgres is rock-solid.
- Standard PostgreSQL skills are transferable -- any engineer can work on this.
- FastAPI + SQLAlchemy + PostgreSQL is a well-documented pattern.

### Negative

- **Vertical scaling limits.** PostgreSQL scales vertically, not horizontally. If we ever grow beyond what a single instance can handle (unlikely at 130K schools, but possible if we add historical time series or sub-county granularity), we'd need to rethink. Realistic ceiling is probably 50-100GB before we'd need to consider alternatives.

- **No time travel.** Delta Lake lets you query previous versions of a table ("what did the scores look like last month?"). PostgreSQL doesn't have this natively. We'd need to implement SCD (slowly changing dimensions) or snapshot tables manually if we want historical analysis. Right now we just keep the current version and rely on dbt's `scored_at` timestamp.

- **Supabase free tier pauses.** After 7 days without a query, the instance pauses. The next query takes 10-15 seconds to spin up. This is fine for development but annoying for a demo deployment. The $25/month Pro tier removes this limit.

- **No native columnar storage.** Analytical queries (aggregations across all 130K schools) would be faster on a columnar engine. In practice, our queries are fast enough on PostgreSQL that this doesn't matter at our scale. The `county_summary` model pre-computes the aggregations anyway.

### Revisiting This Decision

We should revisit if:
- Data volume exceeds 10GB (unlikely with current sources)
- We need historical time-series analysis at scale
- We add real-time streaming ingestion (current architecture is batch)
- The Supabase free tier becomes insufficient

The escape hatch is to keep PostgreSQL for serving (API queries) and add DuckDB or Delta Lake for analytical workloads. The two-database pattern (OLTP + OLAP) is well-established. But for now, one database is simpler.
