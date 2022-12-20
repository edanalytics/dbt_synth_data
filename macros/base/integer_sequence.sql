{% macro synth_integer_sequence(step=1, start=0) -%}
    {{ return(adapter.dispatch('synth_integer_sequence')(step, start)) }}
{%- endmacro %}

{% macro default__synth_integer_sequence(step=1, start=0) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_integer_sequence(step=1, start=0) %}
    ( {{start}} + s.idx * {{step}} )
{% endmacro %}

{% macro snowflake__synth_integer_sequence(step=1, start=0) %}
    ( {{start}} + seq8() * {{step}} )
{% endmacro%}