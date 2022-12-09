{% macro synth_column_geopoint(name) -%}
    {{ return(adapter.dispatch('synth_column_geopoint')(name)) }}
{%- endmacro %}

{% macro default__synth_column_geopoint(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_geopoint(name) %}
    ST_MAKEPOINT(
        RANDOM()*360.0 - 180.0,
        RANDOM()*180.0 - 90.0
    ) AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_geopoint(name) %}
    ST_MAKEPOINT(
        UNIFORM(-180.0, 180.0, RANDOM( synth_get_randseed() )),
        UNIFORM(-90.0, 90.0, RANDOM( synth_get_randseed() ))
    ) AS {{name}}
{% endmacro%}