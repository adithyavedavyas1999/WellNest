-- Schools joined with environmental context: EPA AQI + FEMA risk data.
-- AQI is at the county level (annual summary), and FEMA NRI is also county-level,
-- so the join is straightforward here compared to the health context model.

with schools as (

    select
        nces_school_id,
        county_fips,
        state_abbr
    from {{ ref('school_profiles') }}

),

-- most recent year of AQI data per county
aqi_latest as (

    select
        county_fips,
        median_aqi,
        p90_aqi,
        good_days,
        moderate_days,
        unhealthy_sensitive_days,
        unhealthy_days + very_unhealthy_days + hazardous_days as bad_air_days,
        days_with_aqi,
        dominant_pollutant,
        data_year as aqi_year,
        row_number() over (
            partition by county_fips
            order by data_year desc
        ) as rn
    from {{ ref('stg_epa_aqi') }}

),

aqi as (

    select * from aqi_latest where rn = 1

),

fema as (

    select
        county_fips,
        nri_risk_score,
        nri_risk_rating,
        expected_annual_loss,
        social_vulnerability_score,
        social_vulnerability_rating,
        community_resilience_score,
        community_resilience_rating,
        heatwave_loss,
        tornado_loss,
        wildfire_loss
    from {{ ref('stg_fema_nri') }}

),

joined as (

    select
        s.nces_school_id,
        -- air quality
        a.median_aqi,
        a.p90_aqi,
        a.good_days as aqi_good_days,
        a.bad_air_days,
        a.days_with_aqi,
        a.dominant_pollutant,
        a.aqi_year,
        -- FEMA risk
        f.nri_risk_score,
        f.nri_risk_rating,
        f.expected_annual_loss,
        f.social_vulnerability_score,
        f.social_vulnerability_rating,
        f.community_resilience_score,
        -- top hazards for narrative context
        f.heatwave_loss,
        f.tornado_loss,
        f.wildfire_loss

    from schools s
    left join aqi a
        on s.county_fips = a.county_fips
    left join fema f
        on s.county_fips = f.county_fips

)

select * from joined
