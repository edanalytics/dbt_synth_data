{% macro synth_column_integer_sequence(name, step=1, start=0) -%}
    {{ return(adapter.dispatch('synth_column_integer_sequence')(name, step, start)) }}
{%- endmacro %}

{% macro default__synth_column_integer_sequence(name, step, start) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_integer_sequence(name, step, start) %}
    ( {{start}} + s.idx * {{step}} ) as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_integer_sequence(name, step, start) %}
    ( {{start}} + seq8() * {{step}} ) as {{name}}
{% endmacro%}