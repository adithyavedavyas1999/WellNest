-- Year-over-year score changes at the school level.
-- We compare the current year's wellbeing scores with what we can compute
-- from prior year data. This powers the "Trends" page in the dashboard.
-- NOTE: trend accuracy depends on having stable data across years, which
-- is dicey for some sources (FBI crime especially).

{{ config(
    materialized='table',
    tags=['gold', 'trends']
) }}

with current_scores as (

    select
        nces_school_id,
        school_name,
        state_abbr,
        county_fips,
        county_name,
        wellbeing_score,
        education_score,
        health_score,
        environment_score,
        safety_score,
        wellbeing_category,
        scored_at
    from {{ ref('child_wellbeing_score') }}

),

-- pull year-over-year changes from the profile
profile_changes as (

    select
        nces_school_id,
        math_proficiency_change,
        reading_proficiency_change,
        absenteeism_change,
        enrollment_change
    from {{ ref('school_profiles') }}

),

trended as (

    select
        cs.nces_school_id,
        cs.school_name,
        cs.state_abbr,
        cs.county_fips,
        cs.county_name,
        cs.wellbeing_score,
        cs.education_score,
        cs.health_score,
        cs.environment_score,
        cs.safety_score,
        cs.wellbeing_category,

        -- raw changes from proficiency data
        pc.math_proficiency_change,
        pc.reading_proficiency_change,
        pc.absenteeism_change,
        pc.enrollment_change,

        -- direction indicators
        case
            when pc.math_proficiency_change > 2 then 'Improving'
            when pc.math_proficiency_change < -2 then 'Declining'
            else 'Stable'
        end as math_trend,

        case
            when pc.reading_proficiency_change > 2 then 'Improving'
            when pc.reading_proficiency_change < -2 then 'Declining'
            else 'Stable'
        end as reading_trend,

        -- absenteeism: decrease is good
        case
            when pc.absenteeism_change < -2 then 'Improving'
            when pc.absenteeism_change > 2 then 'Declining'
            else 'Stable'
        end as absenteeism_trend,

        -- overall education trajectory
        case
            when coalesce(pc.math_proficiency_change, 0)
                + coalesce(pc.reading_proficiency_change, 0) > 4
            then 'Improving'
            when coalesce(pc.math_proficiency_change, 0)
                + coalesce(pc.reading_proficiency_change, 0) < -4
            then 'Declining'
            else 'Stable'
        end as education_trend,

        -- z-score for anomaly detection: flag schools with unusual score changes
        -- (deferred to the ML layer for the full implementation, but this
        -- gives us a rough cut)
        coalesce(pc.math_proficiency_change, 0) as _math_chg,
        coalesce(pc.reading_proficiency_change, 0) as _read_chg

    from current_scores cs
    left join profile_changes pc
        on cs.nces_school_id = pc.nces_school_id

),

-- compute z-scores for the combined education change
with_zscore as (

    select
        t.*,
        (_math_chg + _read_chg) as combined_education_change,
        avg(_math_chg + _read_chg) over () as mean_edu_change,
        stddev(_math_chg + _read_chg) over () as stddev_edu_change
    from trended t

)

select
    nces_school_id,
    school_name,
    state_abbr,
    county_fips,
    county_name,
    wellbeing_score,
    education_score,
    health_score,
    environment_score,
    safety_score,
    wellbeing_category,
    math_proficiency_change,
    reading_proficiency_change,
    absenteeism_change,
    enrollment_change,
    math_trend,
    reading_trend,
    absenteeism_trend,
    education_trend,
    combined_education_change,
    -- z-score for anomaly flagging
    case
        when stddev_edu_change > 0
        then round((combined_education_change - mean_edu_change) / stddev_edu_change, 2)
        else 0
    end as education_change_zscore,
    -- flag for the anomaly detection pipeline
    case
        when stddev_edu_change > 0
            and abs((combined_education_change - mean_edu_change) / stddev_edu_change) > 2.5
        then true
        else false
    end as is_anomalous
from with_zscore
