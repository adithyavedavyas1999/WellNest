.PHONY: setup lint format test test-ci run-api run-dashboard run-dagster \
       docker-up docker-down dbt-run dbt-test seed reports clean help

PYTHON   ?= python3
PIP      ?= pip
VENV     ?= .venv
ACTIVATE  = . $(VENV)/bin/activate

# Where things live
API_MOD       = api.main:app
DASHBOARD_DIR = dashboard
DBT_DIR       = transformation
DAGSTER_DIR   = orchestration
REPORTS_MOD   = reports.generate

# --------------------------------------------------------------------------- #
help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------------------- #
# Environment
# --------------------------------------------------------------------------- #
setup: ## create venv and install everything
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && $(PIP) install --upgrade pip setuptools wheel
	$(ACTIVATE) && $(PIP) install -e ".[dev]"
	$(ACTIVATE) && pre-commit install
	@echo "\n✓ Ready. Run 'source $(VENV)/bin/activate' to get started.\n"

# --------------------------------------------------------------------------- #
# Code quality
# --------------------------------------------------------------------------- #
lint: ## run ruff check + mypy
	ruff check .
	mypy .

format: ## autoformat with ruff
	ruff check --fix .
	ruff format .

# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
test: ## run pytest (skip slow tests)
	pytest -m "not slow" --tb=short

test-ci: ## full test suite with coverage
	pytest --cov --cov-report=term-missing --cov-report=xml

# --------------------------------------------------------------------------- #
# Services
# --------------------------------------------------------------------------- #
run-api: ## start the FastAPI server
	uvicorn $(API_MOD) --host 0.0.0.0 --port 8000 --reload

run-dashboard: ## launch streamlit dashboard
	streamlit run $(DASHBOARD_DIR)/app.py --server.port 8501

run-dagster: ## dagster dev server (webserver + daemon)
	cd $(DAGSTER_DIR) && dagster dev

# --------------------------------------------------------------------------- #
# Docker
# --------------------------------------------------------------------------- #
docker-up: ## bring up postgres + supporting services
	docker compose -f infrastructure/docker/docker-compose.yml up -d
	@echo "Waiting for postgres to accept connections..."
	@sleep 3
	@docker compose -f infrastructure/docker/docker-compose.yml exec postgres pg_isready -q && \
		echo "✓ postgres is ready" || echo "✗ postgres not ready yet — check logs"

docker-down: ## tear down docker services
	docker compose -f infrastructure/docker/docker-compose.yml down -v

# --------------------------------------------------------------------------- #
# dbt
# --------------------------------------------------------------------------- #
dbt-run: ## execute dbt models
	cd $(DBT_DIR) && dbt run --target $${DBT_TARGET:-dev}

dbt-test: ## run dbt tests
	cd $(DBT_DIR) && dbt test --target $${DBT_TARGET:-dev}

# --------------------------------------------------------------------------- #
# Seeding & Reports
# --------------------------------------------------------------------------- #
seed: ## load seed/reference data into postgres
	$(PYTHON) -m scripts.seed

reports: ## generate PDF equity reports
	$(PYTHON) -m $(REPORTS_MOD)

# --------------------------------------------------------------------------- #
# Housekeeping
# --------------------------------------------------------------------------- #
clean: ## nuke caches, build artifacts, and generated reports
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache htmlcov .coverage coverage.xml
	rm -rf dist build *.egg-info
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs
	rm -rf reports/output/*.pdf
	@echo "✓ clean"
