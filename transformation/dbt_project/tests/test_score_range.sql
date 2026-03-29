-- Every wellbeing score and pillar score should be between 0 and 100.
-- This catches normalization bugs and rounding edge cases.

select
    nces_school_id,
    wellbeing_score,
    education_score,
    health_score,
    environment_score,
    safety_score
from {{ ref('child_wellbeing_score') }}
where wellbeing_score < 0
    or wellbeing_score > 100
    or education_score < 0
    or education_score > 100
    or health_score < 0
    or health_score > 100
    or environment_score < 0
    or environment_score > 100
    or safety_score < 0
    or safety_score > 100
