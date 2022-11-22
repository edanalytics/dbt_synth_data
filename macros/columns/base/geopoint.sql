{% macro column_geopoint(name) -%}
    {{ return(adapter.dispatch('column_geopoint')(get_randseed(), name)) }}
{%- endmacro %}

{% macro default__column_geopoint(randseed, name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_geopoint(randseed, name) %}
    ST_MAKEPOINT(
        RANDOM()*360.0 - 180.0,
        RANDOM()*180.0 - 90.0
    ) AS {{name}}
{% endmacro %}

{% macro snowflake__column_geopoint(randseed, name) %}
    ST_MAKEPOINT(
        UNIFORM(-180.0, 180.0, RANDOM( {{randseed}} )),
        UNIFORM(-90.0, 90.0, RANDOM( {{randseed}} ))
    ) AS {{name}}
{% endmacro%}