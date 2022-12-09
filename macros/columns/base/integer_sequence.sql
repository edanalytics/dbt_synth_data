{% macro synth_column_integer_sequence(name, step=1, start=0) -%}
    {{ return(adapter.dispatch('synth_column_integer_sequence')(name, step, start)) }}
{%- endmacro %}

{% macro default__synth_column_integer_sequence(name, step=1, start=0) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_integer_sequence(name, step=1, start=0) %}
    ( {{start}} + s.idx * {{step}} ) AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_integer_sequence(name, step=1, start=0) %}
    ( {{start}} + seq8() * {{step}} ) AS {{name}}
{% endmacro%}