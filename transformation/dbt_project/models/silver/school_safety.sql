-- Schools joined with safety indicators: FBI crime + FEMA social vulnerability.
-- Crime data coverage is spotty in some rural counties -- we carry the coverage
-- percentage through so the scoring layer can downweight low-confidence values.

with schools as (

    select
        nces_school_id,
        county_fips,
        state_abbr
    from {{ ref('school_profiles') }}

),

-- most recent crime data per county, preferring years with decent coverage
crime_latest as (

    select
        county_fips,
        violent_crime_rate,
        property_crime_rate,
        violent_crime_count,
        property_crime_count,
        population,
        agency_coverage_pct,
        data_year as crime_year,
        row_number() over (
            partition by county_fips
            order by data_year desc
        ) as rn
    from {{ ref('stg_fbi_crime') }}
    -- exclude years with really thin coverage
    where agency_coverage_pct is null
        or agency_coverage_pct >= 40

),

crime as (

    select * from crime_latest where rn = 1

),

fema_svi as (

    select
        county_fips,
        social_vulnerability_score,
        social_vulnerability_rating,
        community_resilience_score
    from {{ ref('stg_fema_nri') }}

),

joined as (

    select
        s.nces_school_id,
        -- crime
        cr.violent_crime_rate,
        cr.property_crime_rate,
        cr.violent_crime_count,
        cr.property_crime_count,
        cr.agency_coverage_pct as crime_coverage_pct,
        cr.crime_year,
        -- social vulnerability from FEMA (also used in environment, but
        -- it's a key safety input too -- the pillar scoring uses it)
        f.social_vulnerability_score,
        f.social_vulnerability_rating,
        f.community_resilience_score,
        -- flag for low-confidence crime data
        case
            when cr.violent_crime_rate is null then true
            when cr.agency_coverage_pct < 60 then true
            else false
        end as low_confidence_crime

    from schools s
    left join crime cr
        on s.county_fips = cr.county_fips
    left join fema_svi f
        on s.county_fips = f.county_fips

)

select * from joined
