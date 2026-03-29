-- USDA Food Access Research Atlas.
-- Tract-level food desert indicators.
-- "Low access" means >1 mile from supermarket in urban, >10 in rural.

with source as (

    select * from {{ source('raw', 'usda_food_access') }}

),

cleaned as (

    select
        lpad(census_tract::text, 11, '0')              as census_tract_fips,
        {{ tract_to_county('census_tract') }}           as county_fips,
        lpad(state_fips::text, 2, '0')                 as state_fips,
        upper(trim(state_abbr))                         as state_abbr,
        trim(county_name)                               as county_name,
        -- core desert indicators
        coalesce(low_income_flag::int, 0) = 1           as is_low_income,
        coalesce(la_flag_1mile::int, 0) = 1             as is_low_access_1mi,
        coalesce(la_flag_10mile::int, 0) = 1            as is_low_access_10mi,
        coalesce(la_flag_half_mile::int, 0) = 1         as is_low_access_half_mi,
        -- a tract is a "food desert" when it's both low-income AND low-access
        (coalesce(low_income_flag::int, 0) = 1
            and coalesce(la_flag_1mile::int, 0) = 1)    as is_food_desert,
        -- population counts
        population::int                                 as tract_population,
        low_access_pop_1mile::int                       as low_access_pop_1mi,
        low_access_pop_10mile::int                      as low_access_pop_10mi,
        -- urban/rural (useful for deciding which access threshold to apply)
        coalesce(urban_flag::int, 0) = 1                as is_urban,
        _loaded_at                                      as _loaded_at

    from source
    where census_tract is not null

)

select * from cleaned
