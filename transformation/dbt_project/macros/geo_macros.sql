-- Convert a 2-digit FIPS code to state abbreviation using our seed table.
-- Falls back to null if the code isn't in our lookup (shouldn't happen
-- for the 50 states + DC, but some territories slip through in NCES data).
{% macro fips_to_state(fips_column) %}
    (
        select sf.state_abbr
        from {{ ref('state_fips_codes') }} sf
        where sf.fips_code = lpad({{ fips_column }}::text, 2, '0')
        limit 1
    )
{% endmacro %}


-- Standardize county FIPS to 5 digits (state 2 + county 3).
-- Census data sometimes drops leading zeros, FEMA pads them -- normalize here.
{% macro county_fips_format(fips_column) %}
    lpad({{ fips_column }}::text, 5, '0')
{% endmacro %}


-- Extract county FIPS (5-digit) from an 11-digit census tract code.
-- Tract FIPS = state(2) + county(3) + tract(6).
-- Some CDC data ships the full 11-char tract, some ship it as bigint
-- which chops the leading zero. We handle both.
{% macro tract_to_county(tract_column) %}
    lpad(left(lpad({{ tract_column }}::text, 11, '0'), 5), 5, '0')
{% endmacro %}
