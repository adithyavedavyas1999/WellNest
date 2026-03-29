"""
Prompt templates for the AI module.

Keeping prompts in their own file because they change more often than the
surrounding code and it's easier to review diffs this way.  Also makes it
simpler for the policy team to suggest wording changes without wading
through Python.

Prompt engineering notes:
  - System prompts are kept short — GPT-4o-mini responds better to concise
    persona descriptions than to long instruction blocks.  We tried a 500-word
    system prompt early on and the outputs got *worse* (more hedging, more
    generic filler).
  - User prompts use explicit format instructions because structured output
    mode is unreliable for longer free-text generation.  For JSON responses
    we use response_format=json_object instead.
  - Temperature 0.4 for briefs balances variety with factual grounding.
    0.7 produced too many "creative" interpretations of the data, 0.2 made
    every county brief sound identical.
  - Max tokens for briefs is set to 600 — that's roughly 200 words plus some
    markdown formatting overhead.  Going higher just adds padding.

Token cost estimates (gpt-4o-mini, 2025-01 pricing):
  - County brief: ~800 input + ~500 output = ~1,300 tokens per county
  - Anomaly narrative: ~600 input + ~300 output = ~900 tokens
  - Data quality review: ~2,000 input + ~400 output = ~2,400 tokens
  - Full brief run (3,200 counties): ~4.2M tokens ≈ $0.63 input + $2.10 output
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# County community briefs
# ---------------------------------------------------------------------------

COUNTY_BRIEF_SYSTEM: str = (
    "You are a senior education and public health policy analyst writing for "
    "the Chicago Education & Analytics Collaborative (ChiEAC).  You write "
    "clearly and concisely for an audience of school administrators, county "
    "officials, and NGO grant reviewers.  Avoid jargon.  Use data to support "
    "every claim.  Never speculate beyond what the numbers show."
)

# {county_name}, {state_name}, {fips_code}, {population}, {school_count},
# {avg_wellbeing_score}, {education_score}, {health_score}, {environment_score},
# {safety_score}, {economic_score}, {top_concern}, {top_strength},
# {yoy_change_pct}
COUNTY_BRIEF_USER: str = """\
Write a 200-word community wellbeing brief for {county_name}, {state_name} \
(FIPS {fips_code}).

This brief will be included in an NGO grant proposal, so it should be \
factual, specific, and actionable.

County data:
- Population: {population:,}
- Number of schools assessed: {school_count}
- Average Child Wellbeing Index score: {avg_wellbeing_score:.1f}/100
- Education pillar: {education_score:.1f}/100
- Health pillar: {health_score:.1f}/100
- Environment pillar: {environment_score:.1f}/100
- Safety pillar: {safety_score:.1f}/100
- Economic pillar: {economic_score:.1f}/100
- Strongest area: {top_strength}
- Greatest concern: {top_concern}
- Year-over-year score change: {yoy_change_pct:+.1f}%

Structure your response as:
1. **Overview** — 2-3 sentences summarizing the county's child wellbeing status
2. **Key strengths** — what's working, backed by the scores
3. **Areas of concern** — what needs attention, with specific numbers
4. **Recommendation** — one concrete, actionable next step

Use markdown formatting.  Do not include a title (the report template adds one).\
"""


# ---------------------------------------------------------------------------
# Anomaly narrative prompts
# ---------------------------------------------------------------------------

ANOMALY_NARRATIVE_SYSTEM: str = (
    "You are a data analyst explaining statistical anomalies in school-level "
    "wellbeing data to non-technical stakeholders.  Be direct and specific.  "
    "If the anomaly could be a data error, say so.  If it could reflect a "
    "real event (funding change, school closure, redistricting), mention that "
    "as a possibility.  Keep explanations under 100 words."
)

# {school_name}, {nces_id}, {county_name}, {state_abbr},
# {wellbeing_score}, {education_score}, {health_score},
# {environment_score}, {safety_score}, {detection_method},
# {anomaly_detail}
ANOMALY_NARRATIVE_USER: str = """\
Explain this anomaly for a school administrator:

School: {school_name} (NCES ID: {nces_id})
Location: {county_name}, {state_abbr}
Overall wellbeing score: {wellbeing_score:.1f}/100

Pillar scores:
  Education: {education_score:.1f}
  Health: {health_score:.1f}
  Environment: {environment_score:.1f}
  Safety: {safety_score:.1f}

Detection method: {detection_method}
Details: {anomaly_detail}

In 2-3 sentences, explain what's unusual about this school's data and \
suggest what might be causing it.  If this could be a data quality issue, \
say so explicitly.\
"""


# ---------------------------------------------------------------------------
# Data quality review prompts
# ---------------------------------------------------------------------------

DATA_QUALITY_SYSTEM: str = (
    "You are a data quality engineer reviewing records from an education and "
    "health dataset covering US schools and counties.  Your job is to flag "
    "records that look wrong — impossible values, suspicious patterns, likely "
    "data entry errors.  Be precise about what's wrong and why.  Respond in "
    "JSON format."
)

# {record_count}, {records_json}
DATA_QUALITY_USER: str = """\
Review these {record_count} records for data quality issues.

Each record has a composite wellbeing_score (0-100) built from five pillar \
scores (education, health, environment, safety, economic — each 0-100).

Flag records where:
- Any score is outside the valid 0-100 range
- The composite score is mathematically inconsistent with pillar scores \
  (should be roughly the weighted average)
- Year-over-year changes exceed 30 points (possible but suspicious)
- Multiple pillars are exactly 50.0 (likely imputed defaults)
- A school has >5,000 enrollment (verify — could be a district aggregate)

Records:
{records_json}

Respond with JSON:
{{
  "reviews": [
    {{
      "record_id": "...",
      "verdict": "valid" | "suspicious" | "invalid",
      "confidence": 0.0-1.0,
      "reason": "short explanation"
    }}
  ],
  "summary": "1-2 sentence overall assessment"
}}\
"""
