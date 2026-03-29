# Contributing Guide

Thanks for contributing to WellNest. This document covers how to set up your development environment, our code conventions, and the PR process.

## Development Setup

### Prerequisites

- Python 3.11 or 3.12
- Docker and Docker Compose (for PostgreSQL + PostGIS)
- Git
- A Census API key (free, from https://api.census.gov/data/key_signup.html)

Optional:
- An OpenAI API key (only needed for AI features: RAG, briefs, LLM validation)
- Node.js (only if modifying the PWA)

### Initial Setup

```bash
# clone the repo
git clone https://github.com/chieac/wellnest.git
cd wellnest

# create a virtualenv and install dependencies
make setup
# this does: python3 -m venv .venv && pip install -e ".[dev]" && pre-commit install

# activate the venv
source .venv/bin/activate

# copy the env template and fill in your keys
cp .env.example .env
# edit .env — at minimum set CENSUS_API_KEY

# start the database
make docker-up
# this brings up PostgreSQL 16 + PostGIS 3.4 on port 5432

# verify everything works
make lint
make test
```

### Running Services

```bash
# start the API server (auto-reloads on file changes)
make run-api
# -> http://localhost:8000/docs

# start the Streamlit dashboard
make run-dashboard
# -> http://localhost:8501

# start the Dagster dev server (webserver + daemon)
make run-dagster
# -> http://localhost:3000
```

### Running the Pipeline

The easiest way to run the full pipeline is through the Dagster UI at http://localhost:3000. Click "Materialize all" to kick off the full ingestion -> transformation -> scoring pipeline.

For targeted runs:

```bash
# run just dbt models
make dbt-run

# run dbt tests
make dbt-test

# seed reference data (FIPS codes, pillar weights)
cd transformation/dbt_project && dbt seed
```

## Code Style

### Python

We use **ruff** for linting and formatting. The configuration is in `pyproject.toml`:

- Line length: 100
- Quote style: double
- Import sorting: isort-compatible, with first-party packages declared
- Active rule sets: E, W, F, I, N, UP, B, SIM, T20, RUF

Notable ignores:
- `E501`: line length is handled by the formatter, not the linter
- `B008`: function call in default arg -- FastAPI's `Depends()` pattern triggers this
- `RUF012`: mutable class attrs -- Pydantic models need these

**mypy** runs in relaxed mode (`disallow_untyped_defs = false`) because the geo stack has no type stubs. Still, add type hints to every new function you write. The goal is to tighten mypy configuration as the codebase matures.

### Pre-commit Hooks

Pre-commit runs automatically on `git commit`:

1. `trailing-whitespace` and `end-of-file-fixer`
2. `check-yaml` (with `--unsafe` for dbt Jinja templates)
3. `check-toml`
4. `check-added-large-files` (500KB max -- catches accidental parquet commits)
5. `ruff-check` with auto-fix
6. `ruff-format`
7. `mypy`

If a hook modifies your files (ruff auto-fix, trailing whitespace), the commit is rejected. Just `git add` the changes and commit again.

### SQL (dbt)

- Use lowercase keywords (`select`, not `SELECT`)
- CTE names should be descriptive (`national_stats`, not `ns`)
- One column per line in SELECT clauses
- Comments for non-obvious logic, especially normalization and scoring formulas
- Model naming: `stg_*` for staging, descriptive names for silver and gold
- Tag your models: `tags=['gold', 'scoring']`

### General Conventions

- **Type hints** on every function signature. Even if mypy doesn't enforce it, it serves as documentation.
- **Pydantic models** for all data validation and API schemas. Don't use raw dicts for structured data crossing module boundaries.
- **Polars** for all data processing. Don't import pandas unless a library requires it (and if it does, convert to Polars at the boundary).
- **No hardcoded values.** Config goes in `.env` (runtime) or dbt seeds (data). If you find yourself typing a number into Python code, it probably belongs in a config file.
- **Structured logging** via `structlog`. Use `logger.info("event_name", key=value)` rather than f-string messages.
- **Docstrings** on every module and every public function. The module docstring should explain *why* the module exists and any important context.

## Branch Naming Conventions

```
feat/add-lead-poisoning-data
fix/census-tract-join-dropping-schools
refactor/split-scoring-macro
docs/update-methodology
test/add-search-api-coverage
chore/bump-dbt-version
```

Pattern: `{type}/{short-description-with-hyphens}`

Types match conventional commits: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`.

## PR Process

1. **Create a branch** from `main` using the naming convention above.

2. **Write your code.** Follow the style guidelines. Add/update tests for any behavior changes.

3. **Run local checks** before pushing:
   ```bash
   make lint       # ruff + mypy
   make test       # pytest (unit tests, skips slow tests)
   make dbt-test   # dbt tests (if you changed transformation code)
   ```

4. **Push and open a PR.** Use a descriptive title that matches the conventional commit format:
   ```
   feat: add CDC lead poisoning data to health pillar
   fix: census tract join was dropping ~2k schools without FIPS match
   ```

5. **PR description** should include:
   - What changed and why
   - How to test it
   - Any data implications (does this change scores? add a new column?)
   - Screenshot if it's a UI change

6. **CI runs automatically:**
   - Linting (ruff + mypy)
   - Tests (pytest)
   - dbt tests (on transformation changes)
   - Coverage check (60% minimum)

7. **Get a review.** At least one approval required. The reviewer should check:
   - Does the code follow conventions?
   - Are there tests?
   - Is the dbt schema.yml updated if columns changed?
   - Are there data quality implications?

8. **Merge via squash merge** to keep the commit history clean.

## Testing Requirements

### What to Test

- **Ingestion connectors:** Test the transform and validate methods with sample data. Use `@pytest.mark.skip` for tests that require live API access and note the reason.
- **dbt models:** Custom dbt tests for range checks, uniqueness, and referential integrity. The `_schema.yml` files define column-level tests.
- **API endpoints:** Use `httpx.AsyncClient` with the FastAPI `TestClient`. Test happy paths, 404s, and validation errors.
- **ML training:** Test that the trainer handles edge cases (too few rows, missing columns, all-null features).
- **Scoring logic:** Test normalization and pillar computation with known inputs/outputs.

### How to Run Tests

```bash
# fast tests only (skips anything marked @slow or @integration)
make test

# full suite with coverage
make test-ci

# specific test file
pytest tests/unit/test_scoring.py -v

# specific test
pytest tests/unit/test_scoring.py::test_normalize_metric_inverted -v

# dbt tests
make dbt-test
```

### Coverage

The coverage target is 60% (enforced in CI). The threshold is deliberately moderate because:
- Orchestration code (Dagster asset definitions) is hard to unit test in isolation
- Integration tests require a running database
- Some ingestion code is better tested manually against live APIs

That said, aim for 80%+ on modules you write. The test infrastructure is set up to make it easy.

### Test Markers

```python
@pytest.mark.slow
# Tests that hit real databases or external APIs. Skipped by default.
# Run with: pytest -m slow

@pytest.mark.integration
# End-to-end pipeline tests. Require Docker + PostgreSQL.
# Run with: pytest -m integration

@pytest.mark.skip(reason="flaky -- depends on CDC API availability")
# Tests that are known to be unreliable. Include the reason.
```

## dbt Development Workflow

### Adding a New Staging Model

1. Create the connector in `ingestion/sources/new_source.py`
2. Create the JSON schema in `ingestion/schemas/new_source_schema.json`
3. Add the raw table to `_staging_sources.yml`
4. Create `stg_new_source.sql` in `models/staging/`
5. Run: `cd transformation/dbt_project && dbt run --select stg_new_source`
6. Verify: `cd transformation/dbt_project && dbt test --select stg_new_source`

### Modifying a Silver or Gold Model

1. Edit the SQL file
2. Update the `_schema.yml` if columns changed
3. Run: `dbt run --select model_name+` (the `+` runs downstream models too)
4. Test: `dbt test --select model_name+`
5. Check Soda: review the relevant YAML in `transformation/quality/soda_checks/`

### Adding a New Metric to Scoring

1. Add the metric to the appropriate silver model
2. Update `_silver_schema.yml` with column description and tests
3. Add the normalization in `child_wellbeing_score.sql`
4. Add the sub-metric weight to `compute_pillar_score` call
5. Update `pillar_weights.csv` with the new weight entry
6. Run: `dbt seed && dbt run --select child_wellbeing_score+`
7. Update `methodology.md` to document the new metric

### dbt Seeds

Seeds are CSV files in `transformation/dbt_project/seeds/` that load as tables:

- `state_fips_codes.csv` -- state FIPS to name/abbreviation mapping
- `pillar_weights.csv` -- scoring weights for all metrics

After editing a seed: `dbt seed --full-refresh`

## Adding a New Data Source

This is a more involved process. Here's the checklist:

1. Create `ingestion/sources/new_source.py` following the pattern of existing connectors (extract, transform, validate, load)
2. Create Pydantic validation model
3. Create JSON schema in `ingestion/schemas/`
4. Add raw table to dbt `_staging_sources.yml`
5. Create staging model `stg_new_source.sql`
6. Wire into appropriate silver model(s)
7. If it affects scoring: update `child_wellbeing_score.sql`, the macros, and `pillar_weights.csv`
8. Create Dagster asset in `orchestration/assets/bronze.py`
9. Add to the weekly pipeline job selection
10. Write tests
11. Update data_dictionary.md and methodology.md
12. Open PR with data sample showing the pipeline works end-to-end

## Questions?

If something in this guide is unclear or out of date, open an issue or ping the team on Slack. This document should evolve with the project -- PRs to update it are always welcome.
