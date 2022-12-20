{% macro synth_primary_key() -%}
    {{ return(adapter.dispatch('synth_primary_key')()) }}
{%- endmacro %}

{% macro default__synth_primary_key() -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_primary_key() %}
    MD5( s.idx::varchar )
{% endmacro %}

{% macro snowflake__synth_primary_key() %}
    MD5( seq8() )
{% endmacro%}