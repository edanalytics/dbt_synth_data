{% macro synth_distribution_benfordize(distribution, type="double", probabilities={
    "1":0.301, "2":0.176, "3":0.125, "4":0.097, "5":0.079, "6":0.067, "7":0.058, "8":0.051, "9":0.046
}) %}
    {{ return(adapter.dispatch('synth_distribution_benfordize')(distribution, type, probabilities)) }}
{% endmacro %}

{% macro default__synth_distribution_discretize_floor(distribution, type, probabilities) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_discretize_floor(distribution, type, probabilities) %}
    concat(
        {{synth_distribution_discrete_probabilities(probabilities=probabilities)}},
        substr(printf('%.12e', {{distribution}}), 2)
    )::{{type}}
{% endmacro %}

{% macro duckdb__synth_distribution_discretize_floor(distribution, type, probabilities) %}
    concat(
        {{synth_distribution_discrete_probabilities(probabilities=probabilities)}},
        substring(format('{:E}', {{distribution}}), 2)
    )::{{type}}
{% endmacro %}

{% macro postgres__synth_distribution_discretize_floor(distribution, type, probabilities) %}
    concat(
        {{synth_distribution_discrete_probabilities(probabilities=probabilities)}},
        substring(to_char({{distribution}}, '9.9999999999999999999EEEE') from 2)
    )::{{type}}
{% endmacro %}

{% macro snowflake__synth_distribution_discretize_floor(distribution, type, probabilities) %}
    concat(
        {{synth_distribution_discrete_probabilities(probabilities=probabilities)}},
        substring(to_varchar({{distribution}}, 'TME')::varchar, 2)
    {# see https://docs.snowflake.com/en/sql-reference/sql-format-models#text-minimal-format-elements #}
    )::{{type}}
{% endmacro%}
