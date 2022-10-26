{% macro column_lookup(name, value_col, lookup_table, from_col, to_col, funcs=[]) -%}
    {{ return(adapter.dispatch('column_lookup')(name, value_col, lookup_table, from_col, to_col, funcs)) }}
{%- endmacro %}

{% macro default__column_lookup(name, value_col, lookup_table, from_col, to_col, funcs) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_lookup(name, value_col, lookup_table, from_col, to_col, funcs) %}
    {{ dbt_synth.add_update_hook(postgres__lookup_update(name, value_col, lookup_table, from_col, to_col, funcs)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro %}

{% macro postgres__lookup_update(name, value_col, lookup_table, from_col, to_col, funcs) %}
update {{ this }} set {{name}}=y.{{to_col}} from (
  select {{from_col}}, {{to_col}}
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
) as y where {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %}=y.{{from_col}}
{% endmacro %}

{% macro snowflake__column_lookup(name, value_col, lookup_table, from_col, to_col, funcs) %}
    {{ dbt_synth.add_update_hook(snowflake__lookup_update(name, value_col, lookup_table, from_col, to_col, funcs)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro%}

{% macro snowflake__lookup_update(name, value_col, lookup_table, from_col, to_col, funcs) %}
update {{ this }} x set x.{{name}}=y.{{to_col}} from (
  select {{from_col}}, {{to_col}}
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
) as y where {% for f in funcs %}{{f}}({% endfor %}x.{{value_col}}{% for f in funcs %}){% endfor %}=y.{{from_col}}
{% endmacro %}