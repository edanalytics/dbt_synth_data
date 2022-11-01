{% macro column_integer(name, min, max, distribution) -%}
    {{ return(adapter.dispatch('column_integer')(get_randseed(), name, min, max, distribution)) }}
{%- endmacro %}

{% macro default__column_integer(randseed, name, min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_integer(randseed, name, min, max, distribution) %}
    round( (RANDOM() * ({{max}}-{{min}})) + {{min}}) as {{name}}
{% endmacro %}

{% macro snowflake__column_integer(randseed, name, min, max, distribution) %}
    UNIFORM({{min}}, {{max}}, RANDOM( {{randseed}} )) AS {{name}}
{% endmacro%}