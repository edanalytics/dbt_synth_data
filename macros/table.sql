{% macro synth_table(rows=1000, columns=[]) -%}
    {{ return(adapter.dispatch('synth_table')(rows, columns)) }}
{%- endmacro %}


{% macro default__synth_table(rows=1000, columns=[]) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}


{% macro postgres__synth_table(rows=1000, columns=[]) %}
    select
        {%- for column in columns-%}
        {{ column }} {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    from generate_series(1,{{rows}}) as s(idx)
{% endmacro %}


{% macro snowflake__synth_table(rows=1000, columns=[]) %}
    select
        {%- for column in columns-%}
        {{ column }} {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    from table(generator(rowcount => {{rows}}))
{% endmacro %}