-- Schools joined with resource access: HRSA HPSA + MUA + USDA food access.
-- HPSA designations exist at multiple levels (county, geographic, population)
-- so we aggregate to county and take the max score for each discipline type.

with schools as (

    select
        nces_school_id,
        county_fips,
        state_abbr
    from {{ ref('school_profiles') }}

),

-- summarize HPSA to one row per county with max scores per discipline
hpsa_county as (

    select
        county_fips,
        max(case when discipline_type = 'primary care' then hpsa_score end) as hpsa_primary_care_score,
        max(case when discipline_type = 'mental health' then hpsa_score end) as hpsa_mental_health_score,
        max(case when discipline_type = 'dental health' then hpsa_score end) as hpsa_dental_score,
        bool_or(discipline_type = 'primary care') as has_primary_care_shortage,
        bool_or(discipline_type = 'mental health') as has_mental_health_shortage,
        bool_or(discipline_type = 'dental health') as has_dental_shortage
    from {{ ref('stg_hrsa_hpsa') }}
    group by county_fips

),

-- food access aggregated to county
food_county as (

    select
        county_fips,
        -- fraction of tracts in the county that are food deserts
        round(
            avg(case when is_food_desert then 1.0 else 0.0 end) * 100, 1
        ) as pct_tracts_food_desert,
        sum(case when is_food_desert then tract_population else 0 end) as food_desert_population,
        sum(tract_population) as total_population,
        count(*) as tract_count,
        sum(case when is_food_desert then 1 else 0 end) as food_desert_tract_count
    from {{ ref('stg_usda_food') }}
    group by county_fips

),

joined as (

    select
        s.nces_school_id,
        -- HPSA indicators
        h.hpsa_primary_care_score,
        h.hpsa_mental_health_score,
        h.hpsa_dental_score,
        coalesce(h.has_primary_care_shortage, false) as has_primary_care_shortage,
        coalesce(h.has_mental_health_shortage, false) as has_mental_health_shortage,
        coalesce(h.has_dental_shortage, false) as has_dental_shortage,
        -- combined HPSA flag for scoring: any shortage = designated
        coalesce(h.has_primary_care_shortage, false)
            or coalesce(h.has_mental_health_shortage, false) as is_hpsa_designated,
        -- food access
        f.pct_tracts_food_desert,
        f.food_desert_population,
        f.food_desert_tract_count,
        f.tract_count as county_tract_count,
        -- is this a food-desert-heavy county?
        coalesce(f.pct_tracts_food_desert, 0) > 30 as is_food_desert_area

    from schools s
    left join hpsa_county h
        on s.county_fips = h.county_fips
    left join food_county f
        on s.county_fips = f.county_fips

)

select * from joined
