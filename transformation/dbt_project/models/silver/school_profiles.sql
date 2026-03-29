-- Core school profile: NCES directory + geographic context.
-- This is the spine table that all other silver models join back to.

with schools as (

    select * from {{ ref('stg_nces_schools') }}
    where school_year = '{{ var("school_year") }}'

),

state_lookup as (

    select * from {{ ref('state_fips_codes') }}

),

-- grab previous year for trend calculations
prior_year as (

    select
        nces_school_id,
        total_enrollment as prior_enrollment,
        math_proficiency_pct as prior_math_proficiency,
        reading_proficiency_pct as prior_reading_proficiency,
        chronic_absenteeism_pct as prior_absenteeism
    from {{ ref('stg_nces_schools') }}
    where school_year = '{{ var("prior_school_year") }}'

),

joined as (

    select
        s.nces_school_id,
        s.school_name,
        s.district_name,
        s.district_id,
        s.state_fips,
        s.state_abbr,
        coalesce(sl.state_name, s.state_abbr) as state_name,
        s.county_fips,
        s.county_name,
        s.latitude,
        s.longitude,
        s.total_enrollment,
        s.free_lunch_eligible,
        s.reduced_lunch_eligible,
        -- combined FRPL for the Title I proxy
        coalesce(s.free_lunch_eligible, 0)
            + coalesce(s.reduced_lunch_eligible, 0) as frpl_eligible,
        case
            when s.total_enrollment > 0
            then round(
                (coalesce(s.free_lunch_eligible, 0) + coalesce(s.reduced_lunch_eligible, 0))::numeric
                / s.total_enrollment * 100, 1
            )
            else null
        end as frpl_pct,
        s.student_teacher_ratio,
        s.is_title_i,
        s.school_type_code,
        s.school_type,
        s.lowest_grade,
        s.highest_grade,
        s.math_proficiency_pct,
        s.reading_proficiency_pct,
        s.chronic_absenteeism_pct,
        -- year-over-year deltas
        s.total_enrollment - py.prior_enrollment as enrollment_change,
        s.math_proficiency_pct - py.prior_math_proficiency as math_proficiency_change,
        s.reading_proficiency_pct - py.prior_reading_proficiency as reading_proficiency_change,
        s.chronic_absenteeism_pct - py.prior_absenteeism as absenteeism_change,
        s.school_year,
        s._loaded_at

    from schools s
    left join state_lookup sl
        on s.state_fips = sl.fips_code
    left join prior_year py
        on s.nces_school_id = py.nces_school_id

)

select * from joined
