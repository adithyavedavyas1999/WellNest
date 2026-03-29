-- HRSA Health Professional Shortage Area designations.
-- Covers primary care, dental, and mental health.
-- Designation types: geographic, population, facility.
-- HPSA score ranges 0-25 (higher = more severe shortage).

with source as (

    select * from {{ source('raw', 'hrsa_hpsa') }}

),

cleaned as (

    select
        hpsa_id::text                                  as hpsa_id,
        trim(hpsa_name)                                as hpsa_name,
        -- discipline: primary care, dental health, mental health
        lower(trim(discipline_type))                   as discipline_type,
        lower(trim(hpsa_type))                         as designation_type,
        hpsa_score::int                                as hpsa_score,
        lower(trim(hpsa_status))                       as designation_status,
        lpad(state_fips::text, 2, '0')                 as state_fips,
        lpad(county_fips::text, 5, '0')                as county_fips,
        upper(trim(state_abbr))                        as state_abbr,
        trim(county_name)                              as county_name,
        -- provider ratio for gauging severity
        nullif(provider_ratio, 0)::numeric             as provider_ratio,
        designation_date::date                         as designation_date,
        _loaded_at                                     as _loaded_at

    from source
    where hpsa_status is not null
        -- only keep designated areas, not withdrawn ones
        and lower(trim(hpsa_status)) = 'designated'

)

select * from cleaned
