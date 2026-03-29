"""
Tests for AI brief generation — prompt templates and LLM interaction.

All tests mock the OpenAI client.  The one live test is skipped by default
because it costs ~$0.001 per run and is flaky if OpenAI has degraded
performance (which happens more than you'd think on Monday mornings).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Prompt template rendering
# ---------------------------------------------------------------------------

class TestPromptTemplates:

    def test_county_brief_template_renders(self) -> None:
        from ai.briefs.prompts import COUNTY_BRIEF_USER

        rendered = COUNTY_BRIEF_USER.format(
            county_name="Cook County",
            state_name="Illinois",
            fips_code="17031",
            population=5275541,
            school_count=1847,
            avg_wellbeing_score=52.7,
            education_score=48.3,
            health_score=55.1,
            environment_score=58.9,
            safety_score=47.2,
            economic_score=41.5,
            top_strength="Environment",
            top_concern="Economic",
            yoy_change_pct=-1.2,
        )

        assert "Cook County" in rendered
        assert "52.7" in rendered
        assert "1,847" in rendered or "1847" in rendered
        assert "+/-" not in rendered  # yoy should have sign
        assert "-1.2" in rendered

    def test_template_handles_zero_population(self) -> None:
        from ai.briefs.prompts import COUNTY_BRIEF_USER

        rendered = COUNTY_BRIEF_USER.format(
            county_name="Tiny County",
            state_name="Montana",
            fips_code="30001",
            population=0,
            school_count=2,
            avg_wellbeing_score=65.0,
            education_score=70.0,
            health_score=60.0,
            environment_score=80.0,
            safety_score=55.0,
            economic_score=45.0,
            top_strength="Environment",
            top_concern="Economic",
            yoy_change_pct=0.0,
        )
        assert "Tiny County" in rendered

    def test_system_prompt_exists_and_nonempty(self) -> None:
        from ai.briefs.prompts import COUNTY_BRIEF_SYSTEM

        assert len(COUNTY_BRIEF_SYSTEM) > 50
        assert "ChiEAC" in COUNTY_BRIEF_SYSTEM

    def test_anomaly_template_renders(self) -> None:
        from ai.briefs.prompts import ANOMALY_NARRATIVE_USER

        rendered = ANOMALY_NARRATIVE_USER.format(
            school_name="Lincoln Elementary",
            nces_id="170993000943",
            county_name="Sangamon",
            state_abbr="IL",
            wellbeing_score=23.4,
            education_score=15.2,
            health_score=30.1,
            environment_score=28.7,
            safety_score=19.6,
            detection_method="isolation_forest",
            anomaly_detail="z_score=-3.2, score dropped 18 points YoY",
        )
        assert "Lincoln Elementary" in rendered
        assert "23.4" in rendered


# ---------------------------------------------------------------------------
# BriefGenerator with mocked OpenAI
# ---------------------------------------------------------------------------

class TestBriefGenerator:

    def _make_county_row(self) -> dict[str, Any]:
        return {
            "county_fips": "17031",
            "county_name": "Cook County",
            "state_name": "Illinois",
            "state_abbr": "IL",
            "total_population": 5275541,
            "school_count": 1847,
            "avg_wellbeing_score": 52.7,
            "avg_education_score": 48.3,
            "avg_health_score": 55.1,
            "avg_environment_score": 58.9,
            "avg_safety_score": 47.2,
            "avg_economic_score": 41.5,
            "yoy_change_pct": -1.2,
        }

    def test_generate_for_county_returns_string(self, mock_openai: MagicMock) -> None:
        from ai.briefs.generator import BriefGenerator

        gen = BriefGenerator(pg_url="postgresql://fake:fake@localhost/fake", api_key="sk-fake")
        brief = gen.generate_for_county(self._make_county_row())

        assert isinstance(brief, str)
        assert len(brief) > 0

    def test_generate_for_county_calls_openai(self, mock_openai: MagicMock) -> None:
        from ai.briefs.generator import BriefGenerator

        gen = BriefGenerator(pg_url="postgresql://fake:fake@localhost/fake", api_key="sk-fake")
        gen.generate_for_county(self._make_county_row())

        mock_openai.chat.completions.create.assert_called_once()
        call_kwargs = mock_openai.chat.completions.create.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        assert len(messages) == 2
        assert messages[0]["role"] == "system"

    def test_token_tracking(self, mock_openai: MagicMock) -> None:
        from ai.briefs.generator import BriefGenerator

        gen = BriefGenerator(pg_url="postgresql://fake:fake@localhost/fake", api_key="sk-fake")
        gen.generate_for_county(self._make_county_row())

        assert gen._total_prompt_tokens == 820
        assert gen._total_completion_tokens == 485

    def test_missing_api_key_raises(self) -> None:
        from ai.briefs.generator import BriefGenerator

        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="No OpenAI API key"):
                BriefGenerator(pg_url="postgresql://fake:fake@localhost/fake", api_key="")

    def test_build_prompt_handles_missing_fields(self, mock_openai: MagicMock) -> None:
        from ai.briefs.generator import BriefGenerator

        gen = BriefGenerator(pg_url="postgresql://fake:fake@localhost/fake", api_key="sk-fake")

        sparse_row: dict[str, Any] = {
            "county_fips": "99999",
            "county_name": "Mystery County",
        }
        brief = gen.generate_for_county(sparse_row)
        assert isinstance(brief, str)


# ---------------------------------------------------------------------------
# Rate limiting behavior
# ---------------------------------------------------------------------------

class TestBriefRateLimiting:

    def test_batch_sleep_is_configurable(self, mock_openai: MagicMock) -> None:
        from ai.briefs.generator import BriefGenerator

        gen = BriefGenerator(
            pg_url="postgresql://fake:fake@localhost/fake",
            api_key="sk-fake",
            batch_sleep=0.1,
            batch_size=5,
        )
        assert gen._batch_sleep == 0.1
        assert gen._batch_size == 5


# ---------------------------------------------------------------------------
# Live test (skipped by default)
# ---------------------------------------------------------------------------

@pytest.mark.skip(reason="flaky -- depends on OpenAI API availability")
class TestBriefGeneratorLive:

    def test_real_api_call(self) -> None:
        import os

        from ai.briefs.generator import BriefGenerator

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set")

        gen = BriefGenerator(
            pg_url="postgresql://fake:fake@localhost/fake",
            api_key=api_key,
        )
        brief = gen.generate_for_county({
            "county_fips": "17031",
            "county_name": "Cook County",
            "state_name": "Illinois",
            "school_count": 1847,
            "avg_wellbeing_score": 52.7,
            "total_population": 5275541,
        })
        assert len(brief) > 100
        assert "Cook" in brief
