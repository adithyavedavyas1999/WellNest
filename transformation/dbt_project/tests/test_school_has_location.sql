-- Every scored school should have lat/lon for the map views.
-- Some NCES records are missing coordinates (mostly new schools
-- that haven't been geocoded yet). We want to keep noise low
-- so this is a warn-level issue, but we track how many.

{{ config(severity='warn') }}

select
    nces_school_id,
    school_name,
    state_abbr
from {{ ref('child_wellbeing_score') }}
where latitude is null
    or longitude is null
