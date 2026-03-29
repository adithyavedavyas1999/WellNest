-- Schools joined with tract-level health data from CDC PLACES + Census ACS.
-- The join strategy: school -> census tract (via county_fips + spatial proximity).
-- Since we don't have PostGIS in dbt, we join on the county FIPS and pick
-- the tract-level aggregate. For schools in larger counties this is an
-- approximation, but it's the best we can do without a spatial join in SQL.

with schools as (

    select
        nces_school_id,
        county_fips,
        state_abbr
    from {{ ref('school_profiles') }}

),

-- pivot CDC PLACES from long to wide for the measures we care about
cdc_tract_wide as (

    select
        county_fips,
        -- we aggregate to county because matching schools to specific tracts
        -- requires the spatial join done in the ingestion layer
        avg(case when measure_id = 'obesity' then data_value end) as obesity_pct,
        avg(case when measure_id = 'casthma' then data_value end) as asthma_pct,
        avg(case when measure_id = 'mhlth' then data_value end) as poor_mental_health_pct,
        avg(case when measure_id = 'access2' then data_value end) as lack_health_insurance_pct,
        avg(case when measure_id = 'checkup' then data_value end) as annual_checkup_pct,
        avg(case when measure_id = 'dental' then data_value end) as dental_visit_pct,
        avg(case when measure_id = 'depression' then data_value end) as depression_pct,
        avg(case when measure_id = 'disability' then data_value end) as disability_pct
    from {{ ref('stg_cdc_places') }}
    group by county_fips

),

-- census tract aggregated to county
census_county as (

    select
        county_fips,
        avg(poverty_rate) as avg_poverty_rate,
        avg(child_poverty_rate) as avg_child_poverty_rate,
        avg(uninsured_children_rate) as avg_uninsured_children_rate,
        avg(median_household_income) as avg_median_income,
        sum(total_population) as county_population
    from {{ ref('stg_census_acs') }}
    group by county_fips

),

joined as (

    select
        s.nces_school_id,
        -- census demographics
        c.avg_poverty_rate as poverty_rate,
        c.avg_child_poverty_rate as child_poverty_rate,
        c.avg_uninsured_children_rate as uninsured_children_rate,
        c.avg_median_income as median_household_income,
        c.county_population,
        -- CDC health indicators
        cdc.obesity_pct,
        cdc.asthma_pct,
        cdc.poor_mental_health_pct,
        cdc.lack_health_insurance_pct,
        cdc.annual_checkup_pct,
        cdc.dental_visit_pct,
        cdc.depression_pct,
        cdc.disability_pct

    from schools s
    left join census_county c
        on s.county_fips = c.county_fips
    left join cdc_tract_wide cdc
        on s.county_fips = cdc.county_fips

)

select * from joined
