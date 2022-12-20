{% macro synth_geopoint() -%}
    {{ return(adapter.dispatch('synth_geopoint')()) }}
{%- endmacro %}

{% macro default__synth_geopoint() -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_geopoint() %}
    ST_MAKEPOINT(
        RANDOM()*360.0 - 180.0,
        RANDOM()*180.0 - 90.0
    )
{% endmacro %}

{% macro snowflake__synth_geopoint() %}
    ST_MAKEPOINT(
        UNIFORM(-180.0, 180.0, RANDOM( synth_get_randseed() )),
        UNIFORM(-90.0, 90.0, RANDOM( synth_get_randseed() ))
    )
{% endmacro%}