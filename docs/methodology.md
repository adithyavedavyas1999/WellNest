# Scoring Methodology

How we calculate the Child Wellbeing Score and what it means. This document is the canonical reference -- if the dbt SQL and this doc disagree, this doc is wrong and should be updated to match the code.

## Overview

Every scored school gets a **Child Wellbeing Score** from 0 to 100. It's a weighted composite of four pillar scores (Education, Health & Resources, Environment, Safety), each also 0-100. Higher is better.

The score is intentionally contextual, not absolute. A score of 65 means "this school's surrounding community conditions are better than roughly 65% of schools nationally." It does not mean "65% of children are thriving." This distinction matters and we should be careful about it in any public-facing materials.

## Score Categories

| Range | Category | Interpretation |
|-------|----------|---------------|
| 76-100 | Thriving | Community conditions strongly support child wellbeing |
| 51-75 | Moderate | Adequate conditions with some areas for improvement |
| 26-50 | At Risk | Multiple indicators suggest children face significant barriers |
| 0-25 | Critical | Severe deficits across most wellbeing dimensions |

There's also an "Insufficient Data" category for schools where we couldn't compute a score due to missing data. About 2-4% of schools fall into this bucket, mostly very small rural schools where Census data is suppressed.

## The Four Pillars

### Education Pillar (30% of composite)

Measures academic outcomes and school characteristics.

| Sub-metric | Weight | Source | Direction |
|-----------|--------|--------|-----------|
| Math proficiency rate (vs state avg) | 30% | NCES CCD | Higher = better |
| Reading proficiency rate (vs state avg) | 30% | NCES CCD | Higher = better |
| Chronic absenteeism rate | 20% | NCES CCD | Lower = better (inverted) |
| Student-teacher ratio | 10% | NCES CCD | Lower = better (inverted) |
| Title I status | 10% | NCES CCD | Not Title I = better (binary) |

**Why proficiency "vs state average":** Raw proficiency rates aren't comparable across states because each state sets its own proficiency standards. Massachusetts has much harder tests than Mississippi. We normalize by comparing each school to its state average, which makes the metric more apples-to-apples. This isn't perfect -- within-state variation in test difficulty still exists -- but it's the best we can do with publicly available data.

**Why Title I is scored as negative:** Title I designation means the school has a high concentration of low-income students. It's technically a *resource* (Title I schools receive additional federal funding), but it's also a strong indicator of socioeconomic disadvantage. We weight it at only 10% and may revisit this.

### Health & Resources Pillar (30% of composite)

Measures healthcare access and community health indicators.

| Sub-metric | Weight | Source | Direction |
|-----------|--------|--------|-----------|
| Child poverty rate | 25% | Census ACS | Lower = better (inverted) |
| Uninsured children rate | 20% | Census ACS | Lower = better (inverted) |
| HPSA designation (primary + mental health) | 20% | HRSA | Not designated = better (binary) |
| Food desert flag | 15% | USDA | Not food desert = better (inverted) |
| MUA designation | 10% | HRSA | Not designated = better (binary) |
| Distance to nearest clinic (proxy) | 10% | HRSA HPSA score | Lower score = better |

**Why this is the same weight as Education:** Initially I had Health at 20% and Education at 40%. But feedback from ChiEAC and the literature review convinced me that community health conditions are at least as predictive of long-term child outcomes as current academic metrics. A school with great test scores but 30% child poverty and no nearby clinic is not serving its students well.

**The clinic distance proxy:** We don't have actual clinic locations in the data. Instead, we use the HPSA score (0-25, higher = worse shortage) as a proxy for healthcare access. It's imperfect but correlated. I'd like to integrate actual clinic location data from HRSA at some point, but the download is massive and the join logic is non-trivial.

### Environment Pillar (20% of composite)

Measures environmental quality and natural hazard exposure.

| Sub-metric | Weight | Source | Direction |
|-----------|--------|--------|-----------|
| Air Quality Index (median) | 40% | EPA AQS | Lower = better (inverted) |
| FEMA expected annual loss | 30% | FEMA NRI | Lower = better (inverted) |
| CDC environmental health indicators | 30% | CDC PLACES/Env Health | Lower = better (inverted) |

**Why only 20%:** Environment is important but changes slowly and is less directly actionable at the school/community level than education or health resources. A county's wildfire risk or air quality doesn't change much year to year, and there's not much a local NGO can do about it. We still include it because it materially affects child health outcomes.

**AQI data gaps:** About 40% of US counties have no AQI monitoring stations. For these counties, the AQI score is NULL and excluded from the pillar calculation. The remaining sub-metrics (FEMA and CDC) get re-weighted to compensate.

### Safety Pillar (20% of composite)

Measures community safety conditions.

| Sub-metric | Weight | Source | Direction |
|-----------|--------|--------|-----------|
| Violent crime rate (per 100K) | 50% | FBI UCR | Lower = better (inverted) |
| Social vulnerability index | 30% | FEMA NRI | Lower = better (inverted) |
| Property crime rate (per 100K) | 20% | FBI UCR | Lower = better (inverted) |

**The crime data quality problem:** FBI UCR data depends on voluntary reporting by local agencies. Some counties have near-complete coverage; others have 30% of agencies reporting. We flag counties with `agency_coverage_pct < 60%` as low-confidence in the silver layer. The safety score for these schools should be interpreted with caution.

## Normalization Approach

Each raw metric is normalized to 0-100 using **percentile-bounded min-max scaling**.

We use the 5th and 95th percentiles (p5/p95) as the bounds rather than actual min/max values. This dampens sensitivity to extreme outliers. NYC schools and rural Alaska schools produce values that are 10x the national average for some metrics, and raw min-max scaling would compress the entire middle of the distribution into a tiny band.

The normalization formula (from `macros/scoring.sql`):

```
For regular metrics (higher raw value = higher score):
  score = (value - p5) / (p95 - p5) * 100
  clipped to [0, 100]

For inverted metrics (lower raw value = higher score):
  score = (1 - (value - p5) / (p95 - p5)) * 100
  clipped to [0, 100]
```

Values below p5 get clipped to 0 (or 100 for inverted metrics), and values above p95 get clipped to 100 (or 0 for inverted). This means about 5% of schools will have a perfect 0 or 100 on any given sub-metric, which is intentional -- it captures the tail without letting extreme values dominate the normalization.

## Null Handling

A critical design decision: **missing data does not count as zero.**

If a school has no AQI data (because the county has no monitoring station), the environment pillar excludes the AQI sub-metric and re-weights the remaining sub-metrics proportionally. If the entire environment pillar is null, the composite score excludes that pillar and re-weights the other three.

This is implemented in the `compute_pillar_score` and `compute_composite_score` macros. The math:

```sql
-- only count weights for sub-metrics that have data
adjusted_score = sum(score_i * weight_i for non-null i) /
                 sum(weight_i for non-null i)
```

A school with only 2 of 4 pillars scored still gets a composite score, but the `pillars_with_data` column records how complete the data is. The dashboard and API surface this so users know when a score is based on partial information.

## Pillar Weights Configuration

The pillar weights and sub-metric weights live in a dbt seed file (`seeds/pillar_weights.csv`):

```csv
pillar,sub_metric,weight,pillar_weight
education,math_proficiency,0.3000,0.30
education,reading_proficiency,0.3000,0.30
education,chronic_absenteeism_inv,0.2000,0.30
education,student_teacher_ratio_inv,0.1000,0.30
education,title_i_status,0.1000,0.30
health,child_poverty_inv,0.2500,0.30
health,uninsured_children_inv,0.2000,0.30
...
```

This means you can adjust weights without modifying SQL. Just edit the CSV, run `dbt seed`, and re-run the gold models. The scoring macros read weights from the model parameters, not from the seed directly (the seed documents the intent; the SQL implements it).

## Predictive Model Methodology

### Proficiency Change Prediction

**Target variable:** Year-over-year change in combined education score (math + reading proficiency).

**Why we predict change, not absolute level:** Predicting absolute proficiency is basically predicting poverty. The model just learns "low income = low scores," which we already know. Predicting *change* surfaces which schools are improving or declining relative to their baseline, which is more actionable.

**Features:** 3-year absenteeism trend, poverty rate change, resource access indicators, AQI average, crime trend, enrollment change, Title I status, prior year score. About 25-30 features total after feature engineering.

**Model:** XGBoost regressor with GridSearchCV hyperparameter tuning on a TimeSeriesSplit cross-validation.

**Train/test split:** Strictly temporal. Train on years 1-3 (2019-2021), test on year 4 (2022). No random splitting -- that would introduce look-ahead leakage from autocorrelated school scores.

**Performance:**
- R-squared: 0.25-0.35 (good for education data; most published models get 0.15-0.25)
- MAE: ~4.2 points on a 0-100 scale (target is < 5)
- Cross-validated MAE: ~4.5 points (4-fold TimeSeriesSplit)

**Sanity check:** R-squared above 0.50 triggers a warning in the training code. If you see that, check for target leakage -- `education_score` or similar columns may have leaked into the feature set.

**Fallback:** If XGBoost isn't available, the trainer falls back to LightGBM, then to sklearn's GradientBoostingRegressor.

### Risk Flagging

Schools are flagged as "at risk" if the model predicts a score decline exceeding 5 points. The confidence interval is computed from the cross-validation variance. Schools where the entire confidence interval is below zero get the strongest risk flag.

Top contributing factors are extracted from XGBoost feature importances (gain-based). These are stored as a Postgres array and surfaced in the API response.

## Anomaly Detection Approach

Two complementary methods run on each pipeline execution:

### Isolation Forest

Fits on the 4-dimensional pillar score vector (education, health, environment, safety). Catches schools whose *profile* is unusual -- e.g., safety score of 95 but health score of 12.

- Contamination parameter: 0.05 (flags ~5% of schools)
- 150 estimators
- Features are StandardScaler-normalized before fitting because pillar score distributions differ (safety is left-skewed, health is roughly normal)

### Z-score on Year-over-Year Changes

Computes z-scores on math proficiency change, reading proficiency change, chronic absenteeism change, and enrollment change. Flags any school where `|z| > 2.5` on any metric.

The 2.5 threshold was determined empirically:
- 2.0 flagged ~8% of schools, most of which were normal volatility
- 2.5 flags ~3-5%, with ~70% true positive rate on manual review
- 3.0 missed genuine anomalies in rural districts with small enrollment

### Narrative Generation

Each flagged school gets a template-based narrative (not LLM-generated) describing the anomaly. Example:

> Lincoln Elementary: Large pillar gap -- Education (78) vs Health (23), a 55-point spread. Math proficiency decreased by 12.3 points YoY. Flagged by both Isolation Forest and z-score -- high confidence anomaly.

The LLM-generated narratives (from `ai/briefs/`) are separate and run in the monthly AI pipeline for county-level reports.

## Limitations and Caveats

Things users should know about the scores:

1. **County-level granularity for most context metrics.** Poverty, health, crime, and environment data are at the county level, not the school level. Two schools in the same county get the same context scores even if one is in a wealthy suburb and the other is in an underserved neighborhood. Tract-level data would be better, but the join reliability wasn't good enough (see architecture.md for details).

2. **State-level proficiency comparisons are imperfect.** Each state sets its own proficiency standards. We normalize against the state average, but within-state variation in test difficulty still exists.

3. **Crime data has variable coverage.** The FBI's transition from UCR to NIBRS has created reporting gaps. Some counties show apparent drops in crime that are actually drops in reporting. We flag low-coverage counties but can't fully correct for this.

4. **AQI data missing for ~40% of counties.** No monitoring stations = no data. We don't impute AQI -- the pillar re-weights around the gap. This means the environment pillar is less informative for rural schools.

5. **The score is descriptive, not causal.** A low safety score doesn't mean the school itself is unsafe. It means the school is in a county with higher crime rates. The score describes community conditions, not school quality.

6. **Temporal lag.** Most source data lags 1-2 years behind the current date. The Census ACS 5-year estimates are based on surveys collected over the previous 5 years. Scores reflect recent historical conditions, not real-time status.

7. **Small-school instability.** Schools with very small enrollment (under 50 students) can have volatile proficiency rates from year to year. A single student's test results can swing the percentage significantly. We don't filter these out, but users should interpret small-school scores with more skepticism.

8. **Binary indicators are blunt.** HPSA designation, MUA designation, and food desert flags are binary (yes/no). The reality is a spectrum. A county that barely misses HPSA designation isn't meaningfully different from one that barely qualifies. The binary scoring (0 or 100) creates artificial cliffs that distort the pillar scores for borderline cases.

9. **No school-level spending data.** Per-pupil expenditure would be a valuable input but isn't available at the school level in a nationally comparable dataset. District-level spending data exists but the join to individual schools is ambiguous for multi-school districts.

10. **Model performance is modest.** The XGBoost predictor has an R-squared of ~0.30. This means it explains about 30% of the variance in proficiency changes. The remaining 70% is noise, unmeasured factors, and inherent unpredictability. Predictions should inform, not dictate, decisions.
