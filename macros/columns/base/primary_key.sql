{% macro synth_column_primary_key(name) -%}
    {{ return(adapter.dispatch('synth_column_primary_key')(name)) }}
{%- endmacro %}

{% macro default__synth_column_primary_key(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_primary_key(name) %}
    MD5( s.idx::varchar ) as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_primary_key(name) %}
    MD5( seq8() ) as {{name}}
{% endmacro%}