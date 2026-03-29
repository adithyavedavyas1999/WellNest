# Changelog

All notable changes to WellNest are documented here.

Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.9.0] - 2024-12-15

### Added
- Power BI connection guide with DAX measures and row-level security setup
- PDF county reports now include pillar-level sparklines
- Email delivery for batch county reports (SMTP with TLS)

### Changed
- Bumped dbt-core to 1.9.x -- needed the new `--empty` flag for faster CI
- Moved Soda checks from inline Python to YAML definitions (way cleaner)
- County summary now includes median score alongside mean -- more robust to outliers

### Fixed
- FBI UCR connector was silently dropping ~400 counties that switched to NIBRS mid-year
- The anomaly detector was using the wrong year column for z-score computation, flagging
  schools that simply had data backfilled

---

## [0.8.0] - 2024-11-20

### Added
- Ask WellNest RAG pipeline -- policy document Q&A using FAISS + GPT-4o-mini
- Community brief generation for all 3,200 counties
- LLM-based data quality validator (samples suspicious records, asks the model to evaluate)
- AI Insights page in the Streamlit dashboard

### Changed
- Switched from text-embedding-ada-002 to text-embedding-3-small -- 5x cheaper, marginally
  better recall on our education policy corpus
- RAG chunking: went from 1024 tokens to 512 with 50 overlap after testing showed better
  retrieval precision for our short-answer use case

### Fixed
- OpenAI rate limiting was too aggressive -- we were sleeping 60s on every 429 when the
  retry-after header usually said 2-5s

---

## [0.7.0] - 2024-10-28

### Added
- XGBoost proficiency predictor with MLflow experiment tracking
- Isolation Forest anomaly detection on school score vectors
- Prediction serving endpoint (GET /api/schools/{id}/predictions)
- Feature engineering pipeline with lag features and interaction terms
- ML assets in Dagster for automated retraining

### Changed
- Refactored feature matrix to include 3-year rolling averages instead of just YoY deltas
- LightGBM now runs as a fallback when XGBoost fails to converge (rare, but happens with
  small state-level subsets)

### Fixed
- Feature leakage bug: was including current-year proficiency in the feature set for
  predicting current-year change. Classic mistake, caught it during code review.

---

## [0.6.0] - 2024-09-30

### Added
- Public PWA with Leaflet maps, school search, and installable on mobile
- Service worker with offline fallback
- Geolocation-based "schools near me" feature

### Changed
- Switched map tiles from OpenStreetMap to CartoDB Positron -- much cleaner look for
  data visualization
- MarkerCluster now uses custom cluster icons with score-colored backgrounds

### Fixed
- PWA was loading all 130K school markers on init. Now uses bounding box queries that
  only fetch visible schools. Should have done this from the start.

---

## [0.5.0] - 2024-09-05

### Added
- Streamlit multi-page dashboard (7 pages)
- National choropleth map with county-level scores
- School Explorer with search, detail cards, radar charts
- Resource Gaps analysis page
- Trend analysis with YoY comparison
- School/county comparison tool
- Custom WellNest color palette and CSS theming

### Changed
- Dashboard DB queries now use st.cache_data with TTL instead of raw SQLAlchemy
- Plotly charts use the WellNest color sequence everywhere

### Fixed
- Folium maps were breaking in Streamlit 1.38+ due to iframe height issue.
  Pinned streamlit-folium and added a CSS workaround.

---

## [0.4.0] - 2024-08-10

### Added
- FastAPI REST API with all planned endpoints
- API key authentication (optional, for rate limiting)
- Token bucket rate limiter (in-memory, per-IP)
- OpenAPI docs at /docs
- Pagination, filtering, sorting on list endpoints

### Changed
- Switched from async SQLAlchemy to sync -- our queries are all simple selects and the
  async overhead wasn't worth the complexity for this project size

### Fixed
- Rate limiter was leaking memory because stale buckets were never cleaned up.
  Added a cleanup sweep every 1000 requests.

---

## [0.3.0] - 2024-07-15

### Added
- dbt project with full medallion architecture (staging, silver, gold)
- 8 staging models, 6 silver models, 5 gold models
- Child Wellbeing Score computation with configurable pillar weights
- School rankings (national, state, county)
- Resource gap identification
- Trend metrics with YoY tracking
- Soda Core quality checks on silver and gold layers
- Custom dbt macros for scoring and geo operations

### Changed
- Moved from county-level joins to census tract-level for health and environment data.
  More accurate but made the spatial joins significantly more complex.
- Normalization changed from z-score to min-max with p5/p95 clipping -- z-scores were
  confusing for non-technical stakeholders

### Fixed
- Census tract join was dropping ~2,000 schools that had FIPS codes not matching the
  Census TIGER file vintage. Added a fuzzy match fallback using county FIPS.
- CDC PLACES trailing whitespace in county names was causing silent join failures.
  Spent two days debugging this.

---

## [0.2.0] - 2024-06-20

### Added
- Dagster orchestration with assets for all 12 data sources
- Weekly full pipeline schedule
- Daily weather alerts refresh
- File arrival sensor for manual data drops
- Quality failure alerting sensor
- Stale data detection sensor

### Changed
- Refactored connectors to return MaterializeResult with row count metadata
- Moved from monthly to weekly pipeline cadence after realizing EPA AQI and
  NOAA alerts change daily

### Fixed
- NOAA NWS connector was crashing during severe weather events when the API
  returned 503. Added exponential backoff.
- Dagster daemon was OOMing because the FEMA NRI asset was loading the entire
  CSV into memory. Now processes in chunks.

---

## [0.1.0] - 2024-05-15

### Added
- Initial project structure
- 12 data source connectors (NCES CCD, NCES EDGE, CDC PLACES, CDC Env Health,
  Census ACS, EPA AirNow, HRSA HPSA, HRSA MUA, USDA Food Access, FEMA NRI,
  NOAA NWS Alerts, FBI UCR)
- HTTP client with retry/rate limiting (tenacity-based)
- Geospatial utilities (haversine, FIPS parsing, H3 helpers)
- JSON Schema data contracts for all sources
- PostgreSQL + PostGIS setup with Docker Compose
- Basic project configuration (pyproject.toml, Makefile, pre-commit)

### Known Issues
- Census API has a hard 50-variable-per-request limit that makes the ACS connector
  painfully slow. Need to parallelize state-level requests.
- HRSA HPSA download URL changes without notice. Have to manually update when it breaks.
- FBI UCR coverage dropped significantly in 2021 due to the NIBRS transition.
  County-level crime rates for that year are unreliable.
