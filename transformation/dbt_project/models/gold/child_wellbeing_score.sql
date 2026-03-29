-- The main event: composite Child Wellbeing Score for every school.
-- Four pillars (education, health, environment, safety) with configurable
-- weights from the pillar_weights seed. Each raw metric is normalized
-- to 0-100 using national percentiles, then rolled into pillar scores,
-- then into a final composite.

{{ config(
    materialized='table',
    tags=['gold', 'scoring']
) }}

-- percentile boundaries computed across all schools for min-max normalization
-- we use percentiles (p5/p95) instead of actual min/max to reduce
-- sensitivity to outliers (looking at you, NYC and rural Alaska)
with national_stats as (

    select
        -- education
        percentile_cont(0.05) within group (order by sp.math_proficiency_pct) as math_p5,
        percentile_cont(0.95) within group (order by sp.math_proficiency_pct) as math_p95,
        percentile_cont(0.05) within group (order by sp.reading_proficiency_pct) as read_p5,
        percentile_cont(0.95) within group (order by sp.reading_proficiency_pct) as read_p95,
        percentile_cont(0.05) within group (order by sp.chronic_absenteeism_pct) as absent_p5,
        percentile_cont(0.95) within group (order by sp.chronic_absenteeism_pct) as absent_p95,
        percentile_cont(0.05) within group (order by sp.student_teacher_ratio) as str_p5,
        percentile_cont(0.95) within group (order by sp.student_teacher_ratio) as str_p95,
        -- health
        percentile_cont(0.05) within group (order by hc.child_poverty_rate) as cpov_p5,
        percentile_cont(0.95) within group (order by hc.child_poverty_rate) as cpov_p95,
        percentile_cont(0.05) within group (order by hc.uninsured_children_rate) as unins_p5,
        percentile_cont(0.95) within group (order by hc.uninsured_children_rate) as unins_p95,
        percentile_cont(0.05) within group (order by sr.pct_tracts_food_desert) as food_p5,
        percentile_cont(0.95) within group (order by sr.pct_tracts_food_desert) as food_p95,
        -- environment
        percentile_cont(0.05) within group (order by se.median_aqi) as aqi_p5,
        percentile_cont(0.95) within group (order by se.median_aqi) as aqi_p95,
        percentile_cont(0.05) within group (order by se.expected_annual_loss) as eal_p5,
        percentile_cont(0.95) within group (order by se.expected_annual_loss) as eal_p95,
        -- safety
        percentile_cont(0.05) within group (order by ss.violent_crime_rate) as vcrime_p5,
        percentile_cont(0.95) within group (order by ss.violent_crime_rate) as vcrime_p95,
        percentile_cont(0.05) within group (order by ss.property_crime_rate) as pcrime_p5,
        percentile_cont(0.95) within group (order by ss.property_crime_rate) as pcrime_p95,
        percentile_cont(0.05) within group (order by ss.social_vulnerability_score) as sovi_p5,
        percentile_cont(0.95) within group (order by ss.social_vulnerability_score) as sovi_p95

    from {{ ref('school_profiles') }} sp
    left join {{ ref('school_health_context') }} hc on sp.nces_school_id = hc.nces_school_id
    left join {{ ref('school_environment') }} se on sp.nces_school_id = se.nces_school_id
    left join {{ ref('school_safety') }} ss on sp.nces_school_id = ss.nces_school_id
    left join {{ ref('school_resources') }} sr on sp.nces_school_id = sr.nces_school_id

),

-- normalize each metric to 0-100
normalized as (

    select
        sp.nces_school_id,
        sp.school_name,
        sp.state_abbr,
        sp.county_fips,
        sp.county_name,
        sp.latitude,
        sp.longitude,
        sp.total_enrollment,
        sp.school_type,

        -- education sub-metrics
        {{ normalize_metric('sp.math_proficiency_pct', 'ns.math_p5', 'ns.math_p95') }}
            as math_proficiency_score,
        {{ normalize_metric('sp.reading_proficiency_pct', 'ns.read_p5', 'ns.read_p95') }}
            as reading_proficiency_score,
        {{ normalize_metric('sp.chronic_absenteeism_pct', 'ns.absent_p5', 'ns.absent_p95', invert=true) }}
            as chronic_absenteeism_score,
        {{ normalize_metric('sp.student_teacher_ratio', 'ns.str_p5', 'ns.str_p95', invert=true) }}
            as student_teacher_ratio_score,
        case when sp.is_title_i then 0.0 else 100.0 end
            as title_i_score,

        -- health sub-metrics
        {{ normalize_metric('hc.child_poverty_rate', 'ns.cpov_p5', 'ns.cpov_p95', invert=true) }}
            as child_poverty_score,
        {{ normalize_metric('hc.uninsured_children_rate', 'ns.unins_p5', 'ns.unins_p95', invert=true) }}
            as uninsured_children_score,
        case when sr.is_hpsa_designated then 0.0 else 100.0 end
            as hpsa_score,
        {{ normalize_metric('sr.pct_tracts_food_desert', 'ns.food_p5', 'ns.food_p95', invert=true) }}
            as food_desert_score,
        -- MUA is binary: designated or not
        case when sr.has_primary_care_shortage then 0.0 else 100.0 end
            as mua_score,
        -- clinic distance: we don't have it directly, proxy with HPSA score
        -- (higher HPSA score = worse access)
        case
            when sr.hpsa_primary_care_score is not null
            then greatest(0.0, 100.0 - sr.hpsa_primary_care_score * 4.0)
            else null
        end as clinic_distance_score,

        -- environment sub-metrics
        {{ normalize_metric('se.median_aqi', 'ns.aqi_p5', 'ns.aqi_p95', invert=true) }}
            as aqi_score,
        {{ normalize_metric('se.expected_annual_loss', 'ns.eal_p5', 'ns.eal_p95', invert=true) }}
            as fema_hazard_score,
        -- CDC env health: proxy from asthma and poor mental health rates
        -- (lower rates = healthier environment)
        case
            when hc.asthma_pct is not null
            then greatest(0.0, 100.0 - hc.asthma_pct)
            else null
        end as cdc_env_health_score,

        -- safety sub-metrics
        {{ normalize_metric('ss.violent_crime_rate', 'ns.vcrime_p5', 'ns.vcrime_p95', invert=true) }}
            as violent_crime_score,
        {{ normalize_metric('ss.social_vulnerability_score', 'ns.sovi_p5', 'ns.sovi_p95', invert=true) }}
            as social_vulnerability_inv_score,
        {{ normalize_metric('ss.property_crime_rate', 'ns.pcrime_p5', 'ns.pcrime_p95', invert=true) }}
            as property_crime_score

    from {{ ref('school_profiles') }} sp
    cross join national_stats ns
    left join {{ ref('school_health_context') }} hc on sp.nces_school_id = hc.nces_school_id
    left join {{ ref('school_environment') }} se on sp.nces_school_id = se.nces_school_id
    left join {{ ref('school_safety') }} ss on sp.nces_school_id = ss.nces_school_id
    left join {{ ref('school_resources') }} sr on sp.nces_school_id = sr.nces_school_id

),

-- compute pillar scores from sub-metrics
pillars as (

    select
        n.*,

        -- Education Pillar (30%)
        {{ compute_pillar_score([
            ('n.math_proficiency_score', 0.30),
            ('n.reading_proficiency_score', 0.30),
            ('n.chronic_absenteeism_score', 0.20),
            ('n.student_teacher_ratio_score', 0.10),
            ('n.title_i_score', 0.10)
        ]) }} as education_score,

        -- Health Pillar (30%)
        {{ compute_pillar_score([
            ('n.child_poverty_score', 0.25),
            ('n.uninsured_children_score', 0.20),
            ('n.hpsa_score', 0.20),
            ('n.food_desert_score', 0.15),
            ('n.mua_score', 0.10),
            ('n.clinic_distance_score', 0.10)
        ]) }} as health_score,

        -- Environment Pillar (20%)
        {{ compute_pillar_score([
            ('n.aqi_score', 0.40),
            ('n.fema_hazard_score', 0.30),
            ('n.cdc_env_health_score', 0.30)
        ]) }} as environment_score,

        -- Safety Pillar (20%)
        {{ compute_pillar_score([
            ('n.violent_crime_score', 0.50),
            ('n.social_vulnerability_inv_score', 0.30),
            ('n.property_crime_score', 0.20)
        ]) }} as safety_score

    from normalized n

),

-- final composite
scored as (

    select
        p.nces_school_id,
        p.school_name,
        p.state_abbr,
        p.county_fips,
        p.county_name,
        p.latitude,
        p.longitude,
        p.total_enrollment,
        p.school_type,

        -- pillar scores
        p.education_score,
        p.health_score,
        p.environment_score,
        p.safety_score,

        -- composite
        {{ compute_composite_score(
            'p.education_score',
            'p.health_score',
            'p.environment_score',
            'p.safety_score'
        ) }} as wellbeing_score,

        -- category label
        {{ score_category(
            compute_composite_score(
                'p.education_score',
                'p.health_score',
                'p.environment_score',
                'p.safety_score'
            )
        ) }} as wellbeing_category,

        -- pillar categories for breakdown
        {{ score_category('p.education_score') }} as education_category,
        {{ score_category('p.health_score') }} as health_category,
        {{ score_category('p.environment_score') }} as environment_category,
        {{ score_category('p.safety_score') }} as safety_category,

        -- sub-metric scores for detailed drilldown
        p.math_proficiency_score,
        p.reading_proficiency_score,
        p.chronic_absenteeism_score,
        p.child_poverty_score,
        p.uninsured_children_score,
        p.aqi_score,
        p.violent_crime_score,

        -- count how many pillars have data
        (case when p.education_score is not null then 1 else 0 end
         + case when p.health_score is not null then 1 else 0 end
         + case when p.environment_score is not null then 1 else 0 end
         + case when p.safety_score is not null then 1 else 0 end
        ) as pillars_with_data,

        current_timestamp as scored_at

    from pillars p

)

select * from scored
where wellbeing_score is not null
