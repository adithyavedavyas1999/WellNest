-- Min-max normalize a metric to 0-100 range.
-- Handles nulls gracefully (returns null, not 0) and clips to bounds.
-- Set invert=true for metrics where lower raw values are better
-- (e.g., crime rate, poverty rate, AQI).
{% macro normalize_metric(column, min_val, max_val, invert=false) %}
    case
        when {{ column }} is null then null
        {% if invert %}
        when {{ column }} <= {{ min_val }} then 100.0
        when {{ column }} >= {{ max_val }} then 0.0
        else round(
            (1.0 - ({{ column }}::numeric - {{ min_val }}) / nullif({{ max_val }} - {{ min_val }}, 0)) * 100.0,
            2
        )
        {% else %}
        when {{ column }} <= {{ min_val }} then 0.0
        when {{ column }} >= {{ max_val }} then 100.0
        else round(
            ({{ column }}::numeric - {{ min_val }}) / nullif({{ max_val }} - {{ min_val }}, 0) * 100.0,
            2
        )
        {% endif %}
    end
{% endmacro %}


-- Compute a pillar score from sub-metric scores and their weights.
-- Expects a list of (score_column, weight) tuples.
-- Null sub-metrics get excluded and remaining weights are re-normalized,
-- so a school missing one indicator doesn't get penalized to zero.
{% macro compute_pillar_score(metric_weight_pairs) %}
    case
        when (
            {% for pair in metric_weight_pairs %}
                {% if not loop.first %} + {% endif %}
                case when {{ pair[0] }} is not null then 1 else 0 end
            {% endfor %}
        ) = 0 then null
        else round(
            (
                {% for pair in metric_weight_pairs %}
                    {% if not loop.first %} + {% endif %}
                    coalesce({{ pair[0] }}, 0) * {{ pair[1] }}
                {% endfor %}
            ) / nullif(
                {% for pair in metric_weight_pairs %}
                    {% if not loop.first %} + {% endif %}
                    case when {{ pair[0] }} is not null then {{ pair[1] }} else 0 end
                {% endfor %}
            , 0),
            2
        )
    end
{% endmacro %}


-- Final composite score: weighted average of the four pillars.
-- Same null-safe logic as pillar scoring -- if a school has no
-- environment data, we don't want it dragged to zero.
{% macro compute_composite_score(edu_score, health_score, env_score, safety_score, edu_w=0.30, health_w=0.30, env_w=0.20, safety_w=0.20) %}
    case
        when {{ edu_score }} is null
            and {{ health_score }} is null
            and {{ env_score }} is null
            and {{ safety_score }} is null
        then null
        else round(
            (
                coalesce({{ edu_score }}, 0) * {{ edu_w }}
                + coalesce({{ health_score }}, 0) * {{ health_w }}
                + coalesce({{ env_score }}, 0) * {{ env_w }}
                + coalesce({{ safety_score }}, 0) * {{ safety_w }}
            ) / nullif(
                case when {{ edu_score }} is not null then {{ edu_w }} else 0 end
                + case when {{ health_score }} is not null then {{ health_w }} else 0 end
                + case when {{ env_score }} is not null then {{ env_w }} else 0 end
                + case when {{ safety_score }} is not null then {{ safety_w }} else 0 end
            , 0),
            2
        )
    end
{% endmacro %}


-- Map a 0-100 score to a human-readable category.
-- Matches the methodology doc thresholds.
{% macro score_category(score_column) %}
    case
        when {{ score_column }} is null then 'Insufficient Data'
        when {{ score_column }} >= 76 then 'Thriving'
        when {{ score_column }} >= 51 then 'Moderate'
        when {{ score_column }} >= 26 then 'At Risk'
        else 'Critical'
    end
{% endmacro %}
