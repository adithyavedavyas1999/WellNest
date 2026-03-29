-- init-db.sql
-- Runs once on first postgres container boot (via docker-entrypoint-initdb.d).
-- Sets up extensions, schemas, and a couple utility functions we use everywhere.

-- ============================================================================
-- Extensions
-- ============================================================================

-- PostGIS — we store Chicago community area geometries, tract boundaries, etc.
CREATE EXTENSION IF NOT EXISTS postgis;

-- uuid-ossp — handy for generating PKs without leaking row counts
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- pg_trgm — fuzzy text search on community names, inspection descriptions
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- ============================================================================
-- Schemas
--
-- We follow a medallion-ish layout.  raw → staging → silver → gold.
-- ml and ai get their own schemas because the data science folks keep
-- creating tables with names like "experiment_42_final_v3" and it's
-- easier to just give them a sandbox.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Untouched data straight from Socrata, CDC, Census, etc.';

CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Light cleaning — type casts, dedup, column renames.';

CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Joined, enriched, business-logic-applied tables.';

CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Aggregated tables powering the dashboard and API.';

CREATE SCHEMA IF NOT EXISTS ml;
COMMENT ON SCHEMA ml IS 'Feature stores, predictions, model metadata.';

CREATE SCHEMA IF NOT EXISTS ai;
COMMENT ON SCHEMA ai IS 'RAG embeddings, Q&A logs, community feedback.';


-- ============================================================================
-- Utility: updated_at trigger
--
-- Every table that has an updated_at column should attach this trigger.
-- Saves us from remembering to SET updated_at = now() in every UPDATE.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.set_updated_at()
    IS 'Generic trigger function — keeps updated_at in sync without app-layer code.';


-- ============================================================================
-- Utility: generate short IDs
--
-- Sometimes we need a human-friendly short ID for reports or the dashboard
-- (like "WN-3F8A").  This gives us 8 random hex chars, prefixed.
-- Collisions are astronomically unlikely at our scale.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.generate_short_id(prefix TEXT DEFAULT 'WN')
RETURNS TEXT AS $$
BEGIN
    RETURN prefix || '-' || upper(substr(md5(random()::text), 1, 8));
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Utility: safe JSON extract
--
-- The CDC and Socrata APIs occasionally shove JSON blobs into text columns.
-- This wrapper avoids blowing up the whole query when one row has garbage JSON.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.safe_json_extract(raw_text TEXT, json_path TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN raw_text::jsonb #>> string_to_array(json_path, '.');
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION public.safe_json_extract(TEXT, TEXT)
    IS 'Extract a nested value from a JSON string without crashing on malformed input.';


-- ============================================================================
-- Dagster metadata database
--
-- dagster-postgres stores run history, event logs, and schedule state here.
-- We create it in the same cluster to keep the dev setup simple.
-- ============================================================================

-- This might already exist if POSTGRES_MULTIPLE_DATABASES handled it,
-- but belt-and-suspenders never hurt anybody.
SELECT 'CREATE DATABASE dagster'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')\gexec


-- done!  The dbt models and ingestion scripts handle table creation from here.
