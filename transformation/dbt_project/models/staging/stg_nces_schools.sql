-- NCES Common Core of Data + EDGE locations
-- One row per school per year. This is the spine everything else joins to.

with source as (

    select * from {{ source('raw', 'nces_schools') }}

),

cleaned as (

    select
        -- NCESSCH is the universal school ID, always 12 chars
        lpad(ncessch::text, 12, '0')              as nces_school_id,
        sch_name                                    as school_name,
        lea_name                                    as district_name,
        lpad(leaid::text, 7, '0')                  as district_id,
        lpad(fipst::text, 2, '0')                  as state_fips,
        upper(trim(stabr))                          as state_abbr,
        -- county FIPS sometimes comes as numeric, sometimes with leading zeros stripped
        lpad(cnty::text, 5, '0')                    as county_fips,
        trim(coname)                                as county_name,
        latitude                                    as latitude,
        longitude                                   as longitude,
        -- enrollment can be -1 or -2 in CCD for "not reported" / "not applicable"
        case
            when member::int >= 0 then member::int
            else null
        end                                         as total_enrollment,
        case
            when free_lunch::int >= 0 then free_lunch::int
            else null
        end                                         as free_lunch_eligible,
        case
            when reduced_lunch::int >= 0 then reduced_lunch::int
            else null
        end                                         as reduced_lunch_eligible,
        case
            when total_staff::numeric > 0
                and member::int > 0
            then round(member::numeric / total_staff::numeric, 1)
            else null
        end                                         as student_teacher_ratio,
        -- title_i: 1=Yes, 2=No, everything else is missing/not applicable
        case
            when title_i_status::int = 1 then true
            when title_i_status::int = 2 then false
            else null
        end                                         as is_title_i,
        -- school type code: 1=regular, 2=special ed, 3=vocational, 4=alternative
        sch_type::int                               as school_type_code,
        case sch_type::int
            when 1 then 'Regular'
            when 2 then 'Special Education'
            when 3 then 'Vocational'
            when 4 then 'Alternative'
            else 'Other'
        end                                         as school_type,
        -- only keep open, operating schools
        case
            when updated_status::int in (1, 3) then 'Open'
            when updated_status::int = 2 then 'Closed'
            when updated_status::int = 6 then 'Temporarily Closed'
            else 'Unknown'
        end                                         as operational_status,
        -- grade range for display
        coalesce(g_lograde, 'PK')                   as lowest_grade,
        coalesce(g_higrade, '12')                   as highest_grade,
        school_year                                  as school_year,
        -- math and reading proficiency come from the assessment file
        -- joined during ingestion. These are percentages (0-100).
        nullif(math_proficiency, -1)::numeric       as math_proficiency_pct,
        nullif(reading_proficiency, -1)::numeric    as reading_proficiency_pct,
        nullif(chronic_absenteeism_rate, -1)::numeric as chronic_absenteeism_pct,
        _loaded_at                                   as _loaded_at

    from source
    where ncessch is not null

)

select * from cleaned
where operational_status = 'Open'
