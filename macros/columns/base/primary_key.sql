{% macro column_primary_key(name) -%}
    {{ return(adapter.dispatch('column_primary_key')(name)) }}
{%- endmacro %}

{% macro default__column_primary_key(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_primary_key(name) %}
    MD5( s.idx::varchar ) AS {{name}}
{% endmacro %}

{% macro snowflake__column_primary_key(name) %}
    MD5( seq8() ) AS {{name}}
{% endmacro%}