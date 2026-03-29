-- Identify schools with critical resource gaps that funders should prioritize.
-- A "gap" is defined as a pillar or sub-metric scoring in the bottom quartile
-- nationally while at least one other pillar scores above median.
-- The idea: schools with uneven profiles are better intervention targets
-- than schools that are uniformly low across everything.

{{ config(
    materialized='table',
    tags=['gold', 'gaps']
) }}

with scores as (

    select
        nces_school_id,
        school_name,
        state_abbr,
        county_fips,
        county_name,
        total_enrollment,
        wellbeing_score,
        wellbeing_category,
        education_score,
        health_score,
        environment_score,
        safety_score,
        -- sub-metrics for specific gap identification
        math_proficiency_score,
        reading_proficiency_score,
        child_poverty_score,
        uninsured_children_score,
        aqi_score,
        violent_crime_score
    from {{ ref('child_wellbeing_score') }}

),

-- national quartile thresholds
quartiles as (

    select
        percentile_cont(0.25) within group (order by education_score) as edu_q1,
        percentile_cont(0.50) within group (order by education_score) as edu_median,
        percentile_cont(0.25) within group (order by health_score) as health_q1,
        percentile_cont(0.50) within group (order by health_score) as health_median,
        percentile_cont(0.25) within group (order by environment_score) as env_q1,
        percentile_cont(0.50) within group (order by environment_score) as env_median,
        percentile_cont(0.25) within group (order by safety_score) as safety_q1,
        percentile_cont(0.50) within group (order by safety_score) as safety_median
    from scores

),

gap_flags as (

    select
        s.*,
        -- flag each pillar as a "gap" if it's in bottom 25% nationally
        s.education_score < q.edu_q1 as has_education_gap,
        s.health_score < q.health_q1 as has_health_gap,
        s.environment_score < q.env_q1 as has_environment_gap,
        s.safety_score < q.safety_q1 as has_safety_gap,

        -- count how many pillars are above median (shows "potential")
        (case when s.education_score >= q.edu_median then 1 else 0 end
         + case when s.health_score >= q.health_median then 1 else 0 end
         + case when s.environment_score >= q.env_median then 1 else 0 end
         + case when s.safety_score >= q.safety_median then 1 else 0 end
        ) as pillars_above_median,

        -- total gap count
        (case when s.education_score < q.edu_q1 then 1 else 0 end
         + case when s.health_score < q.health_q1 then 1 else 0 end
         + case when s.environment_score < q.env_q1 then 1 else 0 end
         + case when s.safety_score < q.safety_q1 then 1 else 0 end
        ) as gap_count,

        -- which pillar is the weakest?
        case
            when least(
                coalesce(s.education_score, 999),
                coalesce(s.health_score, 999),
                coalesce(s.environment_score, 999),
                coalesce(s.safety_score, 999)
            ) = coalesce(s.education_score, 999) then 'Education'
            when least(
                coalesce(s.education_score, 999),
                coalesce(s.health_score, 999),
                coalesce(s.environment_score, 999),
                coalesce(s.safety_score, 999)
            ) = coalesce(s.health_score, 999) then 'Health'
            when least(
                coalesce(s.education_score, 999),
                coalesce(s.health_score, 999),
                coalesce(s.environment_score, 999),
                coalesce(s.safety_score, 999)
            ) = coalesce(s.environment_score, 999) then 'Environment'
            else 'Safety'
        end as weakest_pillar,

        -- gap severity: difference between best and worst pillar
        greatest(
            coalesce(s.education_score, 0),
            coalesce(s.health_score, 0),
            coalesce(s.environment_score, 0),
            coalesce(s.safety_score, 0)
        ) - least(
            coalesce(s.education_score, 100),
            coalesce(s.health_score, 100),
            coalesce(s.environment_score, 100),
            coalesce(s.safety_score, 100)
        ) as pillar_spread

    from scores s
    cross join quartiles q

)

select
    nces_school_id,
    school_name,
    state_abbr,
    county_fips,
    county_name,
    total_enrollment,
    wellbeing_score,
    wellbeing_category,
    education_score,
    health_score,
    environment_score,
    safety_score,
    has_education_gap,
    has_health_gap,
    has_environment_gap,
    has_safety_gap,
    gap_count,
    pillars_above_median,
    weakest_pillar,
    round(pillar_spread, 1) as pillar_spread,
    -- priority score: schools with gaps AND strengths are best targets
    -- (they have something to build on)
    case
        when gap_count >= 1 and pillars_above_median >= 1
        then 'High Priority'
        when gap_count >= 2
        then 'Critical Need'
        when gap_count = 1
        then 'Moderate Need'
        else 'No Gaps Identified'
    end as intervention_priority
from gap_flags
where gap_count >= 1
order by gap_count desc, pillar_spread desc
