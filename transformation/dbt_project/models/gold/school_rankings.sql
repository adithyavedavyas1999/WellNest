-- National, state, and county rankings for every scored school.
-- Three separate rank columns so the frontend can toggle between views.

{{ config(
    materialized='table',
    tags=['gold', 'rankings']
) }}

with scores as (

    select
        nces_school_id,
        school_name,
        state_abbr,
        county_fips,
        county_name,
        total_enrollment,
        school_type,
        wellbeing_score,
        wellbeing_category,
        education_score,
        health_score,
        environment_score,
        safety_score,
        pillars_with_data
    from {{ ref('child_wellbeing_score') }}

),

ranked as (

    select
        s.*,

        -- national rank
        rank() over (
            order by wellbeing_score desc
        ) as national_rank,
        count(*) over () as national_total,

        -- state rank
        rank() over (
            partition by state_abbr
            order by wellbeing_score desc
        ) as state_rank,
        count(*) over (partition by state_abbr) as state_total,

        -- county rank
        rank() over (
            partition by county_fips
            order by wellbeing_score desc
        ) as county_rank,
        count(*) over (partition by county_fips) as county_total,

        -- percentiles
        percent_rank() over (
            order by wellbeing_score
        ) as national_percentile,
        percent_rank() over (
            partition by state_abbr
            order by wellbeing_score
        ) as state_percentile,

        -- pillar-specific national ranks
        rank() over (order by education_score desc nulls last) as education_national_rank,
        rank() over (order by health_score desc nulls last) as health_national_rank,
        rank() over (order by environment_score desc nulls last) as environment_national_rank,
        rank() over (order by safety_score desc nulls last) as safety_national_rank

    from scores s

)

select
    *,
    round(national_percentile * 100, 1) as national_percentile_pct,
    round(state_percentile * 100, 1) as state_percentile_pct
from ranked
