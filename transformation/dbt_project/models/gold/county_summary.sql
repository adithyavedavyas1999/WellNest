-- County-level summary stats for the dashboard and API.
-- Aggregates school-level wellbeing scores to county, adds context
-- from the community_areas model, and ranks counties.

{{ config(
    materialized='table',
    tags=['gold', 'summary']
) }}

with school_scores as (

    select
        county_fips,
        count(*) as scored_school_count,
        round(avg(wellbeing_score), 2) as avg_wellbeing_score,
        round(min(wellbeing_score), 2) as min_wellbeing_score,
        round(max(wellbeing_score), 2) as max_wellbeing_score,
        round(stddev(wellbeing_score), 2) as stddev_wellbeing_score,
        round(avg(education_score), 2) as avg_education_score,
        round(avg(health_score), 2) as avg_health_score,
        round(avg(environment_score), 2) as avg_environment_score,
        round(avg(safety_score), 2) as avg_safety_score,
        -- distribution of categories
        sum(case when wellbeing_category = 'Thriving' then 1 else 0 end) as thriving_count,
        sum(case when wellbeing_category = 'Moderate' then 1 else 0 end) as moderate_count,
        sum(case when wellbeing_category = 'At Risk' then 1 else 0 end) as at_risk_count,
        sum(case when wellbeing_category = 'Critical' then 1 else 0 end) as critical_count
    from {{ ref('child_wellbeing_score') }}
    group by county_fips

),

community as (

    select
        county_fips,
        state_abbr,
        county_name,
        state_name,
        school_count as total_school_count,
        total_enrollment,
        avg_math_proficiency,
        avg_reading_proficiency,
        avg_chronic_absenteeism,
        avg_frpl_pct,
        avg_poverty_rate,
        avg_child_poverty_rate,
        avg_uninsured_rate,
        avg_median_income,
        total_population,
        median_aqi,
        nri_risk_score,
        social_vulnerability_score,
        violent_crime_rate,
        property_crime_rate,
        has_primary_care_shortage,
        has_mental_health_shortage,
        pct_food_desert,
        avg_math_change,
        avg_reading_change
    from {{ ref('community_areas') }}

),

gaps as (

    select
        county_fips,
        count(*) as schools_with_gaps,
        avg(gap_count) as avg_gap_count
    from {{ ref('resource_gaps') }}
    group by county_fips

),

combined as (

    select
        c.county_fips,
        c.state_abbr,
        c.county_name,
        c.state_name,
        c.total_school_count,
        ss.scored_school_count,
        c.total_enrollment,
        c.total_population,
        -- wellbeing scores
        ss.avg_wellbeing_score,
        ss.min_wellbeing_score,
        ss.max_wellbeing_score,
        ss.stddev_wellbeing_score,
        {{ score_category('ss.avg_wellbeing_score') }} as county_category,
        -- pillar averages
        ss.avg_education_score,
        ss.avg_health_score,
        ss.avg_environment_score,
        ss.avg_safety_score,
        -- category distribution
        ss.thriving_count,
        ss.moderate_count,
        ss.at_risk_count,
        ss.critical_count,
        -- context
        c.avg_math_proficiency,
        c.avg_reading_proficiency,
        c.avg_chronic_absenteeism,
        c.avg_frpl_pct,
        c.avg_poverty_rate,
        c.avg_child_poverty_rate,
        c.avg_median_income,
        c.median_aqi,
        c.nri_risk_score,
        c.social_vulnerability_score,
        c.violent_crime_rate,
        c.has_primary_care_shortage,
        c.has_mental_health_shortage,
        c.pct_food_desert,
        -- resource gaps
        coalesce(g.schools_with_gaps, 0) as schools_with_gaps,
        coalesce(g.avg_gap_count, 0) as avg_gap_count,
        -- trends
        c.avg_math_change,
        c.avg_reading_change

    from community c
    inner join school_scores ss
        on c.county_fips = ss.county_fips
    left join gaps g
        on c.county_fips = g.county_fips

),

ranked as (

    select
        *,
        rank() over (order by avg_wellbeing_score desc) as national_rank,
        count(*) over () as total_counties,
        rank() over (
            partition by state_abbr
            order by avg_wellbeing_score desc
        ) as state_rank,
        round(
            percent_rank() over (order by avg_wellbeing_score) * 100, 1
        ) as national_percentile
    from combined

)

select * from ranked
