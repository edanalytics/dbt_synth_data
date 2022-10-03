{% macro table(rows=1000, columns=[]) -%}
    {{ return(adapter.dispatch('table')(rows, columns)) }}
{%- endmacro %}


{% macro default__table(rows=1000, columns=[]) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}


{% macro postgres__table(rows=1000, columns=[]) %}
    select
        {%- for column in columns-%}
        {{ column }} {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    from generate_series(1,{{rows}}) as s(idx)
{% endmacro %}


{% macro snowflake__table(rows=1000, columns=[]) %}
    select
        {%- for column in columns-%}
        {{ column }} {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    from table(generator(rowcount => {{rows}}))
{% endmacro %}