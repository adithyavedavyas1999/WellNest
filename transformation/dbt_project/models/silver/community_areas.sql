-- County-level aggregation of all indicators for the community view.
-- This rolls up school-level data and combines it with the county-level
-- context tables. Used by the county summary gold model and community briefs.

with school_stats as (

    select
        county_fips,
        state_abbr,
        county_name,
        state_name,
        count(*) as school_count,
        sum(total_enrollment) as total_enrollment,
        avg(math_proficiency_pct) as avg_math_proficiency,
        avg(reading_proficiency_pct) as avg_reading_proficiency,
        avg(chronic_absenteeism_pct) as avg_chronic_absenteeism,
        avg(student_teacher_ratio) as avg_student_teacher_ratio,
        avg(frpl_pct) as avg_frpl_pct,
        sum(case when is_title_i then 1 else 0 end) as title_i_count,
        -- year-over-year changes averaged across schools
        avg(math_proficiency_change) as avg_math_change,
        avg(reading_proficiency_change) as avg_reading_change,
        avg(absenteeism_change) as avg_absenteeism_change
    from {{ ref('school_profiles') }}
    group by county_fips, state_abbr, county_name, state_name

),

census as (

    select
        county_fips,
        avg(poverty_rate) as avg_poverty_rate,
        avg(child_poverty_rate) as avg_child_poverty_rate,
        avg(uninsured_children_rate) as avg_uninsured_rate,
        avg(median_household_income) as avg_median_income,
        sum(total_population) as total_population
    from {{ ref('stg_census_acs') }}
    group by county_fips

),

aqi as (

    select
        county_fips,
        median_aqi,
        good_days,
        unhealthy_days + very_unhealthy_days + hazardous_days as bad_air_days
    from {{ ref('stg_epa_aqi') }}
    -- latest year per county
    qualify row_number() over (partition by county_fips order by data_year desc) = 1

),

fema as (

    select
        county_fips,
        nri_risk_score,
        social_vulnerability_score,
        community_resilience_score,
        expected_annual_loss
    from {{ ref('stg_fema_nri') }}

),

crime as (

    select
        county_fips,
        violent_crime_rate,
        property_crime_rate
    from {{ ref('stg_fbi_crime') }}
    qualify row_number() over (partition by county_fips order by data_year desc) = 1

),

hpsa as (

    select
        county_fips,
        bool_or(discipline_type = 'primary care') as has_primary_care_shortage,
        bool_or(discipline_type = 'mental health') as has_mental_health_shortage,
        max(hpsa_score) as max_hpsa_score
    from {{ ref('stg_hrsa_hpsa') }}
    group by county_fips

),

food as (

    select
        county_fips,
        round(avg(case when is_food_desert then 1.0 else 0.0 end) * 100, 1) as pct_food_desert
    from {{ ref('stg_usda_food') }}
    group by county_fips

),

combined as (

    select
        ss.county_fips,
        ss.state_abbr,
        ss.county_name,
        ss.state_name,
        ss.school_count,
        ss.total_enrollment,
        ss.avg_math_proficiency,
        ss.avg_reading_proficiency,
        ss.avg_chronic_absenteeism,
        ss.avg_student_teacher_ratio,
        ss.avg_frpl_pct,
        ss.title_i_count,
        ss.avg_math_change,
        ss.avg_reading_change,
        -- census
        c.avg_poverty_rate,
        c.avg_child_poverty_rate,
        c.avg_uninsured_rate,
        c.avg_median_income,
        c.total_population,
        -- environment
        a.median_aqi,
        a.bad_air_days,
        f.nri_risk_score,
        f.social_vulnerability_score,
        f.community_resilience_score,
        f.expected_annual_loss,
        -- safety
        cr.violent_crime_rate,
        cr.property_crime_rate,
        -- resources
        coalesce(h.has_primary_care_shortage, false) as has_primary_care_shortage,
        coalesce(h.has_mental_health_shortage, false) as has_mental_health_shortage,
        h.max_hpsa_score,
        fd.pct_food_desert

    from school_stats ss
    left join census c on ss.county_fips = c.county_fips
    left join aqi a on ss.county_fips = a.county_fips
    left join fema f on ss.county_fips = f.county_fips
    left join crime cr on ss.county_fips = cr.county_fips
    left join hpsa h on ss.county_fips = h.county_fips
    left join food fd on ss.county_fips = fd.county_fips

)

select * from combined
