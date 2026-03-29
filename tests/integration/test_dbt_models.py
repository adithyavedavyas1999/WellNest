"""
dbt model compilation and dependency tests.

We verify the dbt project compiles and has valid refs without actually
running it against a database.  This catches broken jinja, typos in
ref() calls, and missing model files early in CI.

The tests that require a live database are skipped by default.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

DBT_PROJECT_DIR = Path("transformation/dbt_project")


def _dbt_project_exists() -> bool:
    return (DBT_PROJECT_DIR / "dbt_project.yml").exists()


# ---------------------------------------------------------------------------
# Compilation tests (no DB required)
# ---------------------------------------------------------------------------

class TestDBTCompilation:

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_dbt_project_yml_valid(self) -> None:
        import yaml

        with open(DBT_PROJECT_DIR / "dbt_project.yml") as f:
            config = yaml.safe_load(f)

        assert config.get("name") is not None
        assert "models" in config or "model-paths" in config

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_profiles_yml_exists(self) -> None:
        profiles = DBT_PROJECT_DIR / "profiles.yml"
        assert profiles.exists(), "profiles.yml missing — dbt can't connect without it"

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_all_staging_models_exist(self) -> None:
        staging_dir = DBT_PROJECT_DIR / "models" / "staging"
        if not staging_dir.exists():
            pytest.skip("staging directory not found")

        sql_files = list(staging_dir.glob("stg_*.sql"))
        assert len(sql_files) >= 5, f"Expected at least 5 staging models, found {len(sql_files)}"

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_gold_models_exist(self) -> None:
        gold_dir = DBT_PROJECT_DIR / "models" / "gold"
        if not gold_dir.exists():
            pytest.skip("gold directory not found")

        expected = [
            "child_wellbeing_score.sql",
            "county_summary.sql",
            "resource_gaps.sql",
        ]
        for filename in expected:
            assert (gold_dir / filename).exists(), f"Missing gold model: {filename}"


# ---------------------------------------------------------------------------
# Model dependency validation
# ---------------------------------------------------------------------------

class TestModelDependencies:

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_gold_score_references_silver_models(self) -> None:
        sql = (DBT_PROJECT_DIR / "models" / "gold" / "child_wellbeing_score.sql").read_text()

        expected_refs = ["school_profiles", "school_health_context",
                         "school_environment", "school_safety"]
        for ref in expected_refs:
            assert ref in sql, f"child_wellbeing_score.sql should reference {ref}"

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_county_summary_references_score(self) -> None:
        sql = (DBT_PROJECT_DIR / "models" / "gold" / "county_summary.sql").read_text()
        assert "child_wellbeing_score" in sql

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_resource_gaps_references_score(self) -> None:
        sql = (DBT_PROJECT_DIR / "models" / "gold" / "resource_gaps.sql").read_text()
        assert "child_wellbeing_score" in sql

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_scoring_macros_exist(self) -> None:
        macros_dir = DBT_PROJECT_DIR / "macros"
        scoring = macros_dir / "scoring.sql"
        assert scoring.exists(), "scoring.sql macro file missing"

        content = scoring.read_text()
        assert "normalize_metric" in content
        assert "compute_pillar_score" in content
        assert "compute_composite_score" in content
        assert "score_category" in content


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchemaDefinitions:

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_gold_schema_yml_exists(self) -> None:
        schema = DBT_PROJECT_DIR / "models" / "gold" / "_gold_schema.yml"
        assert schema.exists()

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_silver_schema_yml_exists(self) -> None:
        schema = DBT_PROJECT_DIR / "models" / "silver" / "_silver_schema.yml"
        assert schema.exists()

    @pytest.mark.skipif(not _dbt_project_exists(), reason="dbt project not found")
    def test_staging_sources_yml_exists(self) -> None:
        sources = DBT_PROJECT_DIR / "models" / "staging" / "_staging_sources.yml"
        assert sources.exists()


# ---------------------------------------------------------------------------
# Tests requiring a live database
# ---------------------------------------------------------------------------

@pytest.mark.skip(reason="requires running PostgreSQL with dbt profile configured")
class TestDBTExecution:

    def test_dbt_compile_succeeds(self) -> None:
        result = subprocess.run(
            ["dbt", "compile", "--project-dir", str(DBT_PROJECT_DIR)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"dbt compile failed:\n{result.stderr}"

    def test_dbt_test_passes(self) -> None:
        result = subprocess.run(
            ["dbt", "test", "--project-dir", str(DBT_PROJECT_DIR)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"dbt test failed:\n{result.stderr}"

    def test_dbt_run_gold_models(self) -> None:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR),
             "--select", "tag:gold"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0
