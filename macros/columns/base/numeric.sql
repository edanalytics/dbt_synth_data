{% macro column_numeric(name, min, max, precision=5) -%}
    {{ return(adapter.dispatch('column_numeric')(get_randseed(), name, min, max, precision)) }}
{%- endmacro %}

{% macro default__column_numeric(randseed, name, min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_numeric(randseed, name, min, max, precision) %}
    round( (RANDOM() * ({{max}}-{{min}}) + {{min}})::numeric , {{precision}}) as {{name}}
{% endmacro %}

{% macro snowflake__column_numeric(randseed, name, min, max, precision) %}
    ROUND( UNIFORM({{min}}::float, {{max}}::float, RANDOM( {{randseed}} )) , {{precision}} ) AS {{name}}
{% endmacro%}