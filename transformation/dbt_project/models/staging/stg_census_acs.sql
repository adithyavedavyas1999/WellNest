-- Census ACS 5-Year estimates at the tract level.
-- The Census API returns -666666666 for missing values -- lovely.

with source as (

    select * from {{ source('raw', 'census_acs') }}

),

nullify_sentinel as (

    select
        lpad(state_fips::text, 2, '0')
            || lpad(county_fips::text, 3, '0')
            || lpad(tract_fips::text, 6, '0')        as census_tract_fips,
        lpad(state_fips::text, 2, '0')
            || lpad(county_fips::text, 3, '0')        as county_fips,
        lpad(state_fips::text, 2, '0')                as state_fips,
        -- poverty: B17001_002 (below poverty) / B17001_001 (total for whom determined)
        case
            when total_poverty_universe::bigint in (-666666666, 0)
            then null
            else round(
                below_poverty_level::numeric / total_poverty_universe::numeric * 100, 2
            )
        end                                           as poverty_rate,
        -- child poverty: use the under-18 poverty counts
        case
            when child_poverty_universe::bigint in (-666666666, 0)
            then null
            else round(
                children_below_poverty::numeric / child_poverty_universe::numeric * 100, 2
            )
        end                                           as child_poverty_rate,
        -- uninsured children under 19
        case
            when child_insurance_universe::bigint in (-666666666, 0)
            then null
            else round(
                children_uninsured::numeric / child_insurance_universe::numeric * 100, 2
            )
        end                                           as uninsured_children_rate,
        case
            when median_household_income::bigint = -666666666
            then null
            else median_household_income::int
        end                                           as median_household_income,
        case
            when total_population::bigint in (-666666666, 0)
            then null
            else total_population::int
        end                                           as total_population,
        acs_year::int                                 as acs_year,
        _loaded_at                                    as _loaded_at

    from source
    where state_fips is not null

)

select * from nullify_sentinel
