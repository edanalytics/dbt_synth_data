{% macro column_primary_key(name) -%}
    {{ return(adapter.dispatch('column_primary_key')(name)) }}
{%- endmacro %}

{% macro default__column_primary_key(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_primary_key(name) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_primary_key(name) %}
    MD5( RANDOM( {{get_randseed()}} ) ) AS {{name}}
{% endmacro%}