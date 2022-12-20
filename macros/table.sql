{% macro synth_table(rows=1000) -%}
    {{ return(adapter.dispatch('synth_table')(rows)) }}
{%- endmacro %}


{% macro default__synth_table(rows=1000) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_table(rows=1000) %}
    generate_series(1,{{rows}}) as s(idx)
{% endmacro %}

{% macro snowflake__synth_table(rows=1000) %}
    table(generator(rowcount => {{rows}}))
{% endmacro %}