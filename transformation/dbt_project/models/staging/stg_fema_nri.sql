-- FEMA National Risk Index at the county level.
-- Expected annual loss across 18 hazard types, plus social vulnerability
-- and community resilience scores.

with source as (

    select * from {{ source('raw', 'fema_nri') }}

),

cleaned as (

    select
        lpad(stcofips::text, 5, '0')                   as county_fips,
        lpad(left(lpad(stcofips::text, 5, '0'), 2), 2, '0') as state_fips,
        trim(county)                                    as county_name,
        trim(state)                                     as state_name,
        -- overall risk
        risk_score::numeric                             as nri_risk_score,
        risk_rating                                     as nri_risk_rating,
        -- expected annual loss from all hazards (in dollars)
        eal_valt::numeric                               as expected_annual_loss,
        -- social vulnerability (higher = more vulnerable)
        sovi_score::numeric                             as social_vulnerability_score,
        sovi_ratng                                      as social_vulnerability_rating,
        -- community resilience (higher = more resilient)
        resl_score::numeric                             as community_resilience_score,
        resl_ratng                                      as community_resilience_rating,
        -- top individual hazard losses for analysis
        hwav_ealv::numeric                              as heatwave_loss,
        trnd_ealv::numeric                              as tornado_loss,
        rfld_ealv::numeric                              as riverine_flood_loss,
        hrcn_ealv::numeric                              as hurricane_loss,
        wfir_ealv::numeric                              as wildfire_loss,
        population::int                                 as population,
        _loaded_at                                      as _loaded_at

    from source
    where stcofips is not null
        and risk_score is not null

)

select * from cleaned
