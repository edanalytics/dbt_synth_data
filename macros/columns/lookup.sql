{% macro column_lookup(name, value_col, lookup_table, from_col, to_col) -%}
    {{ return(adapter.dispatch('column_lookup')(name, value_col, lookup_table, from_col, to_col)) }}
{%- endmacro %}

{% macro default__column_lookup(name, value_col, lookup_table, from_col, to_col) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_lookup(name, value_col, lookup_table, from_col, to_col) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_lookup(name, value_col, lookup_table, from_col, to_col) %}
    {{ dbt_synth.add_post_hook(lookup_update(name, value_col, lookup_table, from_col, to_col)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro%}

{% macro lookup_update(name, value_col, lookup_table, from_col, to_col) %}
update {{ this }} x set x.{{name}}=y.{{to_col}} from (
  select {{from_col}}, {{to_col}}
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
) as y where {{value_col}}=y.{{from_col}}
{% endmacro %}