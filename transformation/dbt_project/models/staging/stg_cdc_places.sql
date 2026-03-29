-- CDC PLACES: census tract-level health indicators
-- Pulled from Socrata API. Tract-level gives us the best
-- granularity for school-to-health joins.
-- Watch out: county names have trailing whitespace in some records

with source as (

    select * from {{ source('raw', 'cdc_places') }}

),

cleaned as (

    select
        lpad(locationid::text, 11, '0')             as census_tract_fips,
        {{ tract_to_county('locationid') }}          as county_fips,
        trim(locationname)                           as location_name,
        upper(trim(stateabbr))                       as state_abbr,
        lower(trim(measureid))                       as measure_id,
        trim(measure)                                as measure_name,
        data_value::numeric                          as data_value,
        low_confidence_limit::numeric                as ci_low,
        high_confidence_limit::numeric               as ci_high,
        totalpopulation::int                         as total_population,
        data_value_type                              as value_type,
        year::int                                    as data_year,
        category                                     as measure_category,
        _loaded_at                                   as _loaded_at

    from source
    -- filter to tract-level rows only; source sometimes mixes geographies
    where locationid is not null
        and length(locationid::text) >= 10
        and data_value is not null

)

select * from cleaned
