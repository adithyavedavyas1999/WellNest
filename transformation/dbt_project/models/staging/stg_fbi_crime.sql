-- FBI UCR / county-level crime data.
-- This data is notoriously patchy -- not all agencies report every year,
-- and some counties have zero coverage in certain years.
-- We keep the coverage indicator so downstream models can flag low-confidence joins.

with source as (

    select * from {{ source('raw', 'fbi_crime') }}

),

cleaned as (

    select
        lpad(fips_state::text, 2, '0')
            || lpad(fips_county::text, 3, '0')          as county_fips,
        trim(county_name)                               as county_name,
        upper(trim(state_abbr))                         as state_abbr,
        year::int                                       as data_year,
        -- per-100k rates (already computed by source)
        violent_crime_rate::numeric                     as violent_crime_rate,
        property_crime_rate::numeric                    as property_crime_rate,
        -- raw counts for context
        violent_crime_count::int                        as violent_crime_count,
        property_crime_count::int                       as property_crime_count,
        population::int                                 as population,
        -- how much of the county's population is covered by reporting agencies
        -- below 70% or so, the rates are extrapolated and less reliable
        case
            when coverage_pct::numeric > 0
            then coverage_pct::numeric
            else null
        end                                             as agency_coverage_pct,
        _loaded_at                                      as _loaded_at

    from source
    where fips_state is not null
        and fips_county is not null
        and violent_crime_rate is not null

)

select * from cleaned
