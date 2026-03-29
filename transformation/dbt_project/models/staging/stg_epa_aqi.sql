-- EPA AQI: annual summary by county from the AQS bulk data files.
-- Real-time API data is too volatile for a yearly score, so we use
-- the annual summaries which give median AQI, days in each category, etc.

with source as (

    select * from {{ source('raw', 'epa_aqi') }}

),

cleaned as (

    select
        lpad(state_code::text, 2, '0')
            || lpad(county_code::text, 3, '0')        as county_fips,
        trim(county_name)                              as county_name,
        trim(state_abbr)                               as state_abbr,
        year::int                                      as data_year,
        -- main AQI values
        median_aqi::int                                as median_aqi,
        percentile_90_aqi::int                         as p90_aqi,
        max_aqi::int                                   as max_aqi,
        -- days in each AQI category
        good_days::int                                 as good_days,
        moderate_days::int                             as moderate_days,
        unhealthy_sensitive_days::int                  as unhealthy_sensitive_days,
        unhealthy_days::int                            as unhealthy_days,
        very_unhealthy_days::int                       as very_unhealthy_days,
        hazardous_days::int                            as hazardous_days,
        days_with_aqi::int                             as days_with_aqi,
        -- dominant pollutant
        coalesce(
            nullif(trim(main_pollutant), ''),
            'Unknown'
        )                                              as dominant_pollutant,
        _loaded_at                                     as _loaded_at

    from source
    where state_code is not null
        and county_code is not null
        and median_aqi is not null

)

select * from cleaned
