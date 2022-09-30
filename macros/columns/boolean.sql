{% macro column_boolean(name, pct_true) -%}
    {{ return(adapter.dispatch('column_boolean')(get_randseed(), name, pct_true)) }}
{%- endmacro %}

{% macro default__column_boolean(randseed, name, pct_true) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_boolean(randseed, name, pct_true) %}
    CASE 
        WHEN RANDOM() < {{pct_true}} THEN TRUE
        ELSE FALSE
    END AS {{name}}
{% endmacro %}

{% macro snowflake__column_boolean(randseed, name, pct_true) %}
    CASE 
        WHEN UNIFORM(0::float, 1::float, RANDOM( {{randseed}} )) < {{pct_true}} THEN TRUE
        ELSE FALSE
    END AS {{name}}
{% endmacro%}