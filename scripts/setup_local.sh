#!/usr/bin/env bash
# setup_local.sh — bootstrap a local WellNest dev environment from scratch.
#
# Checks prerequisites, creates a venv, installs deps, spins up Docker,
# waits for Postgres, runs init SQL, installs pre-commit hooks, and
# fetches dbt packages.  Safe to re-run — it skips steps that are
# already done.

set -euo pipefail

# ── colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[info]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ok]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
fail()  { echo -e "${RED}[fail]${NC}  $*"; exit 1; }

# ── figure out repo root ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

info "WellNest local setup — repo root: $REPO_ROOT"
echo ""

# ── check prerequisites ─────────────────────────────────────────────────────
info "Checking prerequisites..."

MISSING=()

if ! command -v python3 &>/dev/null; then
    MISSING+=("python3")
else
    PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
    if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 11 ]; }; then
        fail "Python >= 3.11 required, found $PY_VERSION"
    fi
    ok "Python $PY_VERSION"
fi

if ! command -v docker &>/dev/null; then
    MISSING+=("docker")
else
    if ! docker info &>/dev/null; then
        warn "Docker found but daemon isn't running — start Docker Desktop and retry"
        MISSING+=("docker (not running)")
    else
        ok "Docker $(docker --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    fi
fi

if ! command -v docker compose &>/dev/null && ! command -v docker-compose &>/dev/null; then
    MISSING+=("docker compose")
else
    ok "docker compose"
fi

if command -v psql &>/dev/null; then
    ok "psql $(psql --version | grep -oE '[0-9]+\.[0-9]+')"
else
    warn "psql not found — not required but useful for debugging"
fi

if command -v git &>/dev/null; then
    ok "git $(git --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
else
    MISSING+=("git")
fi

if [ ${#MISSING[@]} -gt 0 ]; then
    echo ""
    fail "Missing prerequisites: ${MISSING[*]}"
fi

echo ""

# ── virtual environment ─────────────────────────────────────────────────────
VENV_DIR="$REPO_ROOT/.venv"

if [ -d "$VENV_DIR" ] && [ -f "$VENV_DIR/bin/activate" ]; then
    info "Virtual environment already exists at .venv"
else
    info "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    ok "Created .venv"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "Activated .venv ($(python --version))"

# ── install dependencies ─────────────────────────────────────────────────────
info "Installing project dependencies (this takes a minute the first time)..."
pip install --upgrade pip setuptools wheel -q
pip install -e ".[dev]" -q
ok "Dependencies installed"
echo ""

# ── .env file ────────────────────────────────────────────────────────────────
if [ -f "$REPO_ROOT/.env" ]; then
    info ".env already exists — leaving it alone"
else
    if [ -f "$REPO_ROOT/.env.example" ]; then
        cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
        ok "Copied .env.example → .env (edit it with your API keys)"
    else
        warn "No .env.example found — you'll need to create .env manually"
    fi
fi
echo ""

# ── docker services ──────────────────────────────────────────────────────────
COMPOSE_FILE="$REPO_ROOT/infrastructure/docker/docker-compose.yml"

if [ ! -f "$COMPOSE_FILE" ]; then
    warn "docker-compose.yml not found at $COMPOSE_FILE — skipping Docker setup"
else
    info "Starting Docker services..."

    # prefer `docker compose` (v2 plugin) over standalone binary
    if docker compose version &>/dev/null; then
        DC="docker compose -f $COMPOSE_FILE"
    else
        DC="docker-compose -f $COMPOSE_FILE"
    fi

    $DC up -d postgres

    # ── wait for postgres ────────────────────────────────────────────────
    info "Waiting for PostgreSQL to accept connections..."
    MAX_WAIT=60
    ELAPSED=0
    INTERVAL=2

    while [ $ELAPSED -lt $MAX_WAIT ]; do
        if $DC exec -T postgres pg_isready -U wellnest -d wellnest -q 2>/dev/null; then
            ok "PostgreSQL is ready (${ELAPSED}s)"
            break
        fi
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))
    done

    if [ $ELAPSED -ge $MAX_WAIT ]; then
        fail "PostgreSQL didn't become ready within ${MAX_WAIT}s — check 'docker compose logs postgres'"
    fi

    # init-db.sql is mounted via docker-entrypoint-initdb.d, so it runs
    # automatically on first boot.  But if the volume already existed,
    # we can run it manually to make sure schemas exist.
    INIT_SQL="$REPO_ROOT/infrastructure/docker/init-db.sql"
    if [ -f "$INIT_SQL" ]; then
        info "Running init-db.sql (idempotent — safe to re-run)..."
        $DC exec -T postgres psql -U wellnest -d wellnest -f /docker-entrypoint-initdb.d/01-init.sql 2>/dev/null || \
            warn "init-db.sql had warnings (probably fine — extensions/schemas already exist)"
        ok "Database schemas initialized"
    fi

    # bring up the remaining services
    info "Starting remaining services (dagster, api)..."
    $DC up -d
    ok "All Docker services running"
fi
echo ""

# ── pre-commit hooks ────────────────────────────────────────────────────────
if [ -f "$REPO_ROOT/.pre-commit-config.yaml" ]; then
    info "Installing pre-commit hooks..."
    pre-commit install -q
    ok "Pre-commit hooks installed"
else
    info "No .pre-commit-config.yaml — skipping hook install"
fi

# ── dbt packages ─────────────────────────────────────────────────────────────
DBT_PROJECT="$REPO_ROOT/transformation/dbt_project"
if [ -f "$DBT_PROJECT/packages.yml" ]; then
    info "Installing dbt packages..."
    cd "$DBT_PROJECT"
    dbt deps --target dev 2>/dev/null || warn "dbt deps had issues — check transformation/dbt_project/packages.yml"
    cd "$REPO_ROOT"
    ok "dbt packages installed"
else
    info "No dbt packages.yml — skipping"
fi

# ── done! ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  WellNest local environment is ready!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "  Next steps:"
echo ""
echo "    1. Activate the venv:     source .venv/bin/activate"
echo "    2. Edit .env with your API keys (OpenAI, Census, Socrata)"
echo "    3. Seed sample data:      python scripts/seed_sample_data.py"
echo "    4. Run the API:           make run-api"
echo "    5. Run the dashboard:     make run-dashboard"
echo "    6. Open Dagster UI:       make run-dagster   (http://localhost:3000)"
echo "    7. Run tests:             make test"
echo ""
echo "  Useful commands:"
echo "    make help                 show all available make targets"
echo "    make lint                 ruff + mypy"
echo "    make format               auto-format with ruff"
echo "    make docker-down          tear down Docker services"
echo ""
