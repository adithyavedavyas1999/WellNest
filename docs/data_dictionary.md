# Data Dictionary

This document describes every table in WellNest, from raw ingestion through the gold serving layer. If you're writing a dbt model or debugging a data issue, this is the reference.

## Source Datasets

### 1. NCES Common Core of Data (Schools)

**Raw table:** `raw.nces_schools`
**Source:** https://nces.ed.gov/ccd/files.asp (CSV download, annual)
**Grain:** One row per school per year

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| ncessch | varchar(12) | NCES school identifier | Zero-padded. This is the primary key we use everywhere. |
| sch_name | varchar | School name | Some schools have trailing whitespace -- staging trims it |
| lea_name | varchar | District (LEA) name | |
| state_abbr | varchar(2) | Two-letter state code | |
| county_name | varchar | County name | CDC PLACES has trailing whitespace in county names -- we normalize in staging |
| street_location | varchar | Street address | |
| city_location | varchar | City | |
| zip_location | varchar | ZIP code | Mix of 5-digit and ZIP+4 |
| school_type | int | 1=Regular, 2=Special Ed, 3=Vocational, 4=Alternative | We only score type 1 and 3 |
| school_level | int | 1=Primary, 2=Middle, 3=High, 4=Other | |
| total_students | int | Total enrollment | -1 or -2 means suppressed (small count) |
| free_lunch | int | Free lunch eligible | -1 = suppressed, -2 = not applicable |
| reduced_lunch | int | Reduced-price lunch eligible | Same suppression codes |
| title_i_eligible | varchar | Title I eligibility status | 'Yes', 'No', or 'Missing' |
| student_teacher_ratio | float | Student-teacher ratio | Occasionally null for very small schools |
| latitude | float | School latitude | From NCES EDGE merge |
| longitude | float | School longitude | From NCES EDGE merge |
| school_year | varchar | School year (e.g. '2022-23') | |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- Enrollment of `-1` or `-2` means the value is suppressed (small N). We treat these as NULL.
- About 300 schools have latitude/longitude of `0,0` -- obviously wrong. Staging filters these out.
- School names sometimes include "SCHOOL" in all caps, sometimes mixed case. We don't normalize this since it's how NCES stores it.

### 2. NCES EDGE School Locations

**Raw table:** Merged into `raw.nces_schools` during ingestion
**Source:** https://nces.ed.gov/programs/edge/Geographic/SchoolLocations
**Grain:** One row per school

Provides precise lat/lon coordinates. We merge these onto the CCD data during ingestion since they share the `ncessch` key. About 98.5% of schools have a match.

### 3. CDC PLACES

**Raw table:** `raw.cdc_places`
**Source:** Socrata API (https://data.cdc.gov/resource/swc5-untb.json)
**Grain:** One row per measure per geographic unit (county or tract)

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| locationid | varchar | County FIPS or census tract FIPS | 5-digit for county, 11-digit for tract |
| locationname | varchar | Location name | Has trailing whitespace -- staging trims |
| measureid | varchar | Measure code (e.g. 'CASTHMA', 'OBESITY') | |
| measure | varchar | Full measure name | |
| data_value | float | Prevalence estimate (percent) | Null if suppressed |
| data_value_type | varchar | 'Crude prevalence' or 'Age-adjusted prevalence' | We use age-adjusted where available |
| totalpopulation | int | Population denominator | |
| year | int | Data year | |
| _loaded_at | timestamp | Ingestion timestamp | |

**Measures we use:**

| measureid | Measure | Used For |
|-----------|---------|----------|
| CASTHMA | Current asthma prevalence | Health pillar, environment proxy |
| OBESITY | Obesity prevalence | Health pillar context |
| MHLTH | Mental health not good (14+ days) | Health pillar |
| ACCESS2 | Current lack of health insurance | Health pillar cross-check |
| BPHIGH | High blood pressure | Health context |
| DEPRESSION | Depression prevalence | Health context |

**Known quirks:**
- County names have trailing whitespace (e.g. `"Cook County "` instead of `"Cook County"`). We trim in staging.
- Some rural counties have suppressed values (null data_value) for small populations.
- The API paginates at 50,000 rows. We use `$limit` and `$offset` to fetch everything.

### 4. CDC Environmental Health Tracking

**Raw table:** Ingested as part of CDC PLACES pipeline
**Source:** https://ephtracking.cdc.gov/apigateway/api/v1/
**Grain:** County-level indicators

Used primarily for childhood lead poisoning rates and asthma ED visit rates. The API format is different from PLACES (REST rather than Socrata), but we normalize during ingestion.

### 5. Census ACS 5-Year Estimates

**Raw table:** `raw.census_acs`
**Source:** Census API (https://api.census.gov/data/{year}/acs/acs5)
**Grain:** One row per census tract

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| state_fips | varchar(2) | State FIPS code | Zero-padded |
| county_fips | varchar(3) | County FIPS code | Within state |
| tract_fips | varchar(6) | Census tract code | |
| full_fips | varchar(11) | Full state+county+tract FIPS | Concatenated |
| total_population | int | Total tract population | |
| poverty_rate | float | Percent below poverty line | Derived: B17001_002E / B17001_001E |
| median_hh_income | int | Median household income ($) | |
| uninsured_children | int | Count of uninsured under-19 | Sum of four age/sex buckets |
| pct_bachelors_plus | float | Percent with bachelor's+ | Derived from B15003 table |
| race_white_alone | int | White alone population | |
| race_black_alone | int | Black alone population | |
| race_hispanic | int | Hispanic population | |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- The API returns `-666666666` for missing/suppressed values. This is undocumented in the official API docs -- I found it in a Census Bureau forum post from 2014. Staging replaces with NULL.
- Header row is the first element in the JSON response. Every other Census API returns data on the first row.
- Hard limit of 50 variables per request. We need ~25, so we're fine.
- Requesting all states at once causes timeouts ~40% of the time. We iterate state by state.
- The 5-year estimates lag by about 2 years. ACS 2022 data released in December 2023.

### 6. EPA AQI Data

**Raw table:** `raw.epa_aqi`
**Source:** AQS bulk downloads (https://aqs.epa.gov/aqsweb/airdata/download_files.html)
**Grain:** One row per county per year

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| state_code | varchar(2) | State FIPS | |
| county_code | varchar(3) | County FIPS | |
| county_name | varchar | County name | |
| year | int | Calendar year | |
| days_with_aqi | int | Number of days AQI was measured | |
| good_days | int | Days with AQI 0-50 | |
| moderate_days | int | Days with AQI 51-100 | |
| unhealthy_sensitive_days | int | Days with AQI 101-150 | |
| unhealthy_days | int | Days with AQI 151-200 | |
| very_unhealthy_days | int | Days with AQI 201-300 | |
| hazardous_days | int | Days with AQI 301+ | |
| max_aqi | int | Maximum AQI recorded | |
| ninetieth_pctile_aqi | int | 90th percentile AQI | |
| median_aqi | int | Median AQI | This is what we use for scoring |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- Not all counties have monitoring stations. About 40% of US counties have no AQI data at all. We leave these NULL rather than imputing -- better to say "no data" than guess.
- Real-time AQI from AirNow API supplements the bulk annual data for weather alerts but isn't used in scoring.

### 7. HRSA HPSA (Health Professional Shortage Areas)

**Raw table:** `raw.hrsa_hpsa`
**Source:** https://data.hrsa.gov/data/download
**Grain:** One row per shortage area designation

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| hpsa_id | varchar | Unique designation ID | |
| hpsa_name | varchar | Designation name | |
| hpsa_score | int | Shortage severity (0-25) | Higher = more severe |
| designation_type | varchar | Geographic, Population, or Facility | We use Geographic |
| discipline | varchar | Primary Care, Dental, Mental Health | |
| hpsa_status | varchar | Designated, Withdrawn, Proposed | We only use Designated |
| state_abbr | varchar(2) | State | |
| county_fips | varchar(5) | County FIPS (when applicable) | Not all designations are county-level |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- Some HPSA designations are at the sub-county level (geographic or population-based). We aggregate to county level in silver using `max(hpsa_score)` per discipline per county.
- The data refreshes daily but we pull weekly. Designations rarely change faster than that.

### 8. HRSA MUA/P (Medically Underserved Areas/Populations)

**Raw table:** `raw.hrsa_mua`
**Source:** Same download portal as HPSA
**Grain:** One row per designation

Similar structure to HPSA. We use this as a binary flag (is the county designated as medically underserved?) rather than a continuous score.

### 9. USDA Food Access Research Atlas

**Raw table:** `raw.usda_food_access`
**Source:** https://www.ers.usda.gov/data-products/food-access-research-atlas/
**Grain:** One row per census tract

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| census_tract | varchar(11) | Full FIPS tract code | |
| state | varchar | State name (full) | |
| county | varchar | County name | |
| urban | int | 1=Urban, 0=Rural | |
| la_1and10 | int | Low access at 1mi (urban) or 10mi (rural) | |
| lalowi_1and10 | int | Low access AND low income | This is the "food desert" definition we use |
| la_pop_1and10 | int | Population with low access | |
| pct_lalowi | float | Percent low access + low income | |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- Updated only when new Census data comes out (~every 5 years). The current data is from 2019 vintage.
- The 1-mile / 10-mile threshold changes based on urban/rural classification. We use the combined `la_1and10` flag that handles this automatically.

### 10. FEMA National Risk Index

**Raw table:** `raw.fema_nri`
**Source:** https://hazards.fema.gov/nri/data-resources
**Grain:** One row per county

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| stcofips | varchar(5) | State+county FIPS | |
| county | varchar | County name | |
| state | varchar | State name | |
| eal_valt | float | Expected Annual Loss -- total ($) | Sum across all 18 hazard types |
| sovi_score | float | Social Vulnerability score | Higher = more vulnerable |
| resl_score | float | Community Resilience score | Higher = more resilient |
| risk_score | float | Overall risk index | Composite of EAL + SoVI + Resilience |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- The county FIPS in this dataset uses `stcofips` format (5 characters, zero-padded). Some other datasets split state and county FIPS into separate columns. We normalize in staging.
- EAL values span several orders of magnitude (from ~$0 in some rural counties to billions in coastal counties). We use percentile normalization to handle this.

### 11. NOAA/NWS Weather Alerts

**Raw table:** `raw.noaa_nws_alerts`
**Source:** https://api.weather.gov/alerts/active (GeoJSON)
**Grain:** One row per active alert (snapshot at time of ingestion)

Captures active weather alerts for contextual awareness. Not used in scoring directly -- it's more of a real-time situational awareness layer for the dashboard.

### 12. FBI UCR Crime Data

**Raw table:** `raw.fbi_crime`
**Source:** https://cde.ucr.cjis.gov/ (CSV download)
**Grain:** One row per county per year

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| fips_state | varchar(2) | State FIPS | |
| fips_county | varchar(3) | County FIPS | |
| county_name | varchar | County name | |
| population | int | County population (for rate calculation) | |
| violent_crime | int | Total violent crimes | |
| property_crime | int | Total property crimes | |
| violent_crime_rate | float | Violent crimes per 100K population | Derived |
| property_crime_rate | float | Property crimes per 100K population | Derived |
| agency_coverage_pct | float | Percentage of agencies reporting | Low values mean unreliable data |
| year | int | Calendar year | |
| _loaded_at | timestamp | Ingestion timestamp | |

**Known quirks:**
- Agency coverage varies wildly. Some counties have 100% of agencies reporting, others have 30%. We include an `agency_coverage_pct` column and flag low-confidence counties in silver.
- The transition from UCR to NIBRS reporting has caused discontinuities in some counties. We don't try to adjust for this -- just note it as a data quality caveat.

---

## Staging Tables

Staging models are 1:1 with raw sources. They handle type casting, column renaming, null handling, and basic cleaning. All live in the `staging` schema (or the default schema with `stg_` prefix in dbt).

| Model | Source | Key Transformations |
|-------|--------|-------------------|
| `stg_nces_schools` | `raw.nces_schools` | Trim whitespace, normalize school_type, filter to open schools, replace -1/-2 enrollment with NULL |
| `stg_cdc_places` | `raw.cdc_places` | Trim county names, pivot measures to columns, filter to age-adjusted values |
| `stg_census_acs` | `raw.census_acs` | Replace -666666666 with NULL, build full FIPS, compute poverty_rate and uninsured_children |
| `stg_epa_aqi` | `raw.epa_aqi` | Concatenate state+county FIPS, compute median_aqi per county for most recent year |
| `stg_hrsa_hpsa` | `raw.hrsa_hpsa` | Filter to Designated status, aggregate to county level (max HPSA score per discipline) |
| `stg_usda_food` | `raw.usda_food_access` | Aggregate tract-level food desert flags to county level (percent of tracts that are food deserts) |
| `stg_fema_nri` | `raw.fema_nri` | Normalize FIPS codes, select scoring columns |
| `stg_fbi_crime` | `raw.fbi_crime` | Concatenate FIPS, compute per-capita rates, flag low-coverage counties |

---

## Silver Tables

Silver models join the staging data together with `school_profiles` as the spine. All live in the `silver` schema.

### silver.school_profiles

The core school table. One row per open school for the current year.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | Primary key (zero-padded NCES ID) |
| school_name | varchar | Cleaned school name |
| state_abbr | varchar(2) | Two-letter state code |
| county_fips | varchar(5) | 5-digit county FIPS |
| county_name | varchar | County name |
| city | varchar | City |
| zip_code | varchar | ZIP code |
| latitude | float | School latitude |
| longitude | float | School longitude |
| school_type | varchar | Regular, Vocational, etc. |
| grade_range | varchar | Grade levels served |
| total_enrollment | int | Current enrollment |
| free_reduced_lunch_pct | float | Free/reduced lunch percentage |
| is_title_i | boolean | Title I eligible |
| student_teacher_ratio | float | Student-teacher ratio |
| math_proficiency_pct | float | Math proficiency rate (vs state avg) |
| reading_proficiency_pct | float | Reading proficiency rate (vs state avg) |
| chronic_absenteeism_pct | float | Chronic absenteeism rate |

### silver.school_health_context

Schools enriched with health and demographic indicators at the county level.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | FK to school_profiles |
| child_poverty_rate | float | County avg child poverty rate (Census) |
| uninsured_children_rate | float | County avg uninsured children rate |
| asthma_pct | float | County current asthma prevalence (CDC PLACES) |
| obesity_pct | float | County obesity prevalence |
| mental_health_pct | float | County mental health not good prevalence |
| median_hh_income | int | County median household income |

### silver.school_environment

Schools enriched with environmental quality data.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | FK to school_profiles |
| median_aqi | int | County median AQI |
| good_days_pct | float | Percentage of days with good AQI |
| expected_annual_loss | float | FEMA expected annual loss ($) |
| risk_score | float | FEMA overall risk index |
| community_resilience | float | FEMA community resilience score |

### silver.school_safety

Schools enriched with crime and social vulnerability data.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | FK to school_profiles |
| violent_crime_rate | float | Violent crimes per 100K (county) |
| property_crime_rate | float | Property crimes per 100K (county) |
| social_vulnerability_score | float | FEMA SoVI score |
| low_confidence_crime | boolean | True if agency_coverage_pct < 60% |

### silver.school_resources

Schools enriched with healthcare access and food access data.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | FK to school_profiles |
| is_hpsa_designated | boolean | County has primary care or mental health shortage |
| hpsa_primary_care_score | int | HPSA primary care score (0-25) |
| hpsa_mental_health_score | int | HPSA mental health score (0-25) |
| hpsa_dental_score | int | HPSA dental score (0-25) |
| has_primary_care_shortage | boolean | MUA designated |
| pct_tracts_food_desert | float | Percent of county tracts that are food deserts |

### silver.community_areas

County-level aggregation of all indicators. Used for the county summary and AI briefs.

| Column | Type | Description |
|--------|------|-------------|
| county_fips | varchar(5) | Primary key |
| county_name | varchar | County name |
| state_abbr | varchar(2) | State |
| school_count | int | Number of scored schools in county |
| avg_enrollment | float | Average enrollment per school |
| avg_poverty_rate | float | Average child poverty rate |
| avg_math_proficiency | float | Average math proficiency |
| median_aqi | int | County median AQI |
| violent_crime_rate | float | Violent crime rate |
| is_hpsa | boolean | Any HPSA designation |
| pct_food_desert | float | Percent food desert tracts |

---

## Gold Tables

Gold models are the analytical output layer. These power the API and dashboard directly.

### gold.child_wellbeing_score

The main event. Composite Child Wellbeing Score (0-100) for every scored school.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | PK, unique, not null |
| school_name | varchar | School name |
| state_abbr | varchar(2) | State |
| county_fips | varchar(5) | County FIPS |
| county_name | varchar | County name |
| latitude | float | School latitude |
| longitude | float | School longitude |
| total_enrollment | int | Enrollment |
| school_type | varchar | School type |
| education_score | float | Education pillar (0-100) |
| health_score | float | Health pillar (0-100) |
| environment_score | float | Environment pillar (0-100) |
| safety_score | float | Safety pillar (0-100) |
| wellbeing_score | float | Composite score (0-100), not null |
| wellbeing_category | varchar | Thriving, Moderate, At Risk, Critical, Insufficient Data |
| education_category | varchar | Category for education pillar |
| health_category | varchar | Category for health pillar |
| environment_category | varchar | Category for environment pillar |
| safety_category | varchar | Category for safety pillar |
| math_proficiency_score | float | Normalized math score (0-100) |
| reading_proficiency_score | float | Normalized reading score (0-100) |
| chronic_absenteeism_score | float | Normalized absenteeism (inverted, 0-100) |
| child_poverty_score | float | Normalized poverty (inverted, 0-100) |
| uninsured_children_score | float | Normalized uninsured (inverted, 0-100) |
| aqi_score | float | Normalized AQI (inverted, 0-100) |
| violent_crime_score | float | Normalized crime (inverted, 0-100) |
| pillars_with_data | int | Count of non-null pillars (1-4) |
| scored_at | timestamp | When the score was computed |

See `methodology.md` for the full scoring breakdown.

### gold.school_rankings

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | PK, FK to child_wellbeing_score |
| national_rank | int | Rank among all scored schools |
| national_total | int | Total scored schools nationally |
| state_rank | int | Rank within state |
| state_total | int | Total scored schools in state |
| county_rank | int | Rank within county |
| county_total | int | Total scored schools in county |
| national_percentile_pct | float | National percentile (0-100) |
| state_percentile_pct | float | State percentile (0-100) |
| education_national_rank | int | National rank by education score |
| health_national_rank | int | National rank by health score |
| environment_national_rank | int | National rank by environment score |
| safety_national_rank | int | National rank by safety score |

### gold.resource_gaps

Schools with critical resource gaps suitable for intervention targeting.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | PK |
| school_name | varchar | School name |
| state_abbr | varchar(2) | State |
| county_fips | varchar(5) | County FIPS |
| gap_count | int | Number of pillars in bottom quartile (1-4) |
| pillars_above_median | int | Number of pillars above median (shows potential) |
| weakest_pillar | varchar | Education, Health, Environment, or Safety |
| pillar_spread | float | Gap between strongest and weakest pillar |
| intervention_priority | varchar | High Priority, Critical Need, or Moderate Need |
| has_education_gap | boolean | Education in bottom quartile |
| has_health_gap | boolean | Health in bottom quartile |
| has_environment_gap | boolean | Environment in bottom quartile |
| has_safety_gap | boolean | Safety in bottom quartile |

### gold.trend_metrics

Year-over-year score changes with anomaly flags.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | PK |
| math_yoy_change | float | Math proficiency change |
| reading_yoy_change | float | Reading proficiency change |
| math_trend | varchar | Improving, Declining, or Stable |
| reading_trend | varchar | Improving, Declining, or Stable |
| education_zscore | float | Z-score of education change |
| is_anomalous | boolean | abs(z-score) > 2.5 |

### gold.county_summary

County-level aggregation for maps and reports.

| Column | Type | Description |
|--------|------|-------------|
| county_fips | varchar(5) | PK |
| county_name | varchar | County name |
| state | varchar(2) | State |
| composite_score | float | Enrollment-weighted avg wellbeing score |
| county_category | varchar | Thriving, Moderate, At Risk, Critical |
| school_count | int | Scored schools in county |
| population | int | County population (Census) |
| education_score | float | Avg education pillar |
| health_score | float | Avg health pillar |
| environment_score | float | Avg environment pillar |
| safety_score | float | Avg safety pillar |
| avg_poverty_rate | float | Avg child poverty rate |
| avg_chronic_absenteeism | float | Avg chronic absenteeism |
| pct_title_i | float | Percent of schools that are Title I |
| score_change_1y | float | Year-over-year score change |

---

## ML/AI Tables

### gold.school_predictions

XGBoost model predictions for next-year proficiency change.

| Column | Type | Description |
|--------|------|-------------|
| nces_id | varchar(12) | PK |
| predicted_score_change | float | Predicted education score delta |
| confidence_interval_low | float | 95% CI lower bound |
| confidence_interval_high | float | 95% CI upper bound |
| risk_flag | boolean | True if model predicts significant decline |
| top_contributing_factors | text[] | Postgres array of top feature names |
| model_version | varchar | Model identifier (e.g. 'xgboost_v1.2') |
| predicted_at | date | When prediction was generated |

### gold.county_ai_briefs

GPT-4o-mini generated community briefs for counties.

| Column | Type | Description |
|--------|------|-------------|
| fips | varchar(5) | PK, FK to county_summary |
| county_name | varchar | County name |
| state | varchar(2) | State |
| brief | text | ~200-word community brief |
| generated_at | timestamp | When the brief was generated |

### ml.anomalies

Anomaly detection results from Isolation Forest and z-score methods.

| Column | Type | Description |
|--------|------|-------------|
| nces_school_id | varchar(12) | School ID |
| school_name | varchar | School name |
| state_abbr | varchar(2) | State |
| iforest_score | float | Isolation Forest anomaly score |
| zscore_worst | float | Worst z-score across change metrics |
| zscore_trigger_col | varchar | Which metric triggered the z-score flag |
| detection_method | varchar | isolation_forest, zscore, or both |
| severity | varchar | both_methods, iforest_only, zscore_only |
| narrative | text | Human-readable explanation |
| detected_at | timestamp | Detection timestamp |

---

## Scoring Methodology Reference

For the full scoring breakdown including pillar weights, sub-metric weights, and normalization approach, see `methodology.md`.

Quick reference for pillar weights (from `pillar_weights.csv`):

| Pillar | Weight | Sub-metrics |
|--------|--------|-------------|
| Education | 30% | Math proficiency (30%), reading proficiency (30%), chronic absenteeism inv. (20%), student-teacher ratio inv. (10%), Title I status (10%) |
| Health & Resources | 30% | Child poverty inv. (25%), uninsured children inv. (20%), HPSA designation (20%), food desert inv. (15%), MUA designation (10%), clinic distance inv. (10%) |
| Environment | 20% | AQI inv. (40%), FEMA hazard loss inv. (30%), CDC env health (30%) |
| Safety | 20% | Violent crime inv. (50%), social vulnerability inv. (30%), property crime inv. (20%) |

---

## Data Types and Constraints

All ID columns (NCES IDs, FIPS codes) are stored as `varchar` rather than `integer` because they have leading zeros. FIPS code `06037` (Los Angeles County, CA) would become `6037` if stored as an integer, breaking joins.

Score columns are `float` (specifically `numeric` in Postgres) with CHECK constraints ensuring they stay within 0-100. The dbt tests also enforce this.

Boolean columns use Postgres `boolean` type. In the raw layer, some sources represent booleans as 'Yes'/'No' strings, '1'/'0' integers, or 'true'/'false' strings. Staging normalizes all of these.

Timestamps use `timestamp with time zone` in Postgres. The `_loaded_at` column on raw tables uses `now()` at insert time.
