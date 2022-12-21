{% macro synth_lookup(name, value_col, lookup_table, from_col, to_col, funcs=[]) -%}
    {{ return(adapter.dispatch('synth_lookup')(name, value_col, lookup_table, from_col, to_col, funcs)) }}
{%- endmacro %}

{% macro default__synth_lookup(name, value_col, lookup_table, from_col, to_col, funcs) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_lookup(name, value_col, lookup_table, from_col, to_col, funcs) %}
    {{ synth_add_update_hook(postgres__synth_lookup_update(name, value_col, lookup_table, from_col, to_col, funcs)) or "" }}
    
    ''::varchar
{% endmacro %}

{% macro postgres__synth_lookup_update(name, value_col, lookup_table, from_col, to_col, funcs) %}
update {{ this }} x set {{name}}=y.{{to_col}} from (
  select {{from_col}}, {{to_col}}
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
) as y where {% for f in funcs %}{{f}}({% endfor %}x.{{value_col}}{% for f in funcs %}){% endfor %}=y.{{from_col}}
{% endmacro %}

{% macro snowflake__synth_lookup(name, value_col, lookup_table, from_col, to_col, funcs) %}
    {{ synth_add_update_hook(snowflake__synth_lookup_update(name, value_col, lookup_table, from_col, to_col, funcs)) or "" }}
    
    ''::varchar
{% endmacro%}

{% macro snowflake__synth_lookup_update(name, value_col, lookup_table, from_col, to_col, funcs) %}
update {{ this }} x set x.{{name}}=y.{{to_col}} from (
  select {{from_col}}, {{to_col}}
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
) as y where {% for f in funcs %}{{f}}({% endfor %}x.{{value_col}}{% for f in funcs %}){% endfor %}=y.{{from_col}}
{% endmacro %}