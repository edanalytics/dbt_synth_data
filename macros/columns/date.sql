{% macro column_date(name, min='1990-01-01', max='', distribution='uniform') -%}
    {{ return(adapter.dispatch('column_date')(get_randseed(), name, min, max, distribution)) }}
{%- endmacro %}

{% macro default__column_date(randseed, name, min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_date(randseed, name, min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int as {{name}}
{% endmacro %}

{% macro snowflake__column_date(randseed, name, min, max, distribution) %}
    UNIFORM({{min}}, {{max}}, RANDOM( {{randseed}} )) AS {{name}}
{% endmacro%}