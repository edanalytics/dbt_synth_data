{% macro column_select(name, value_col, lookup_table, distribution="uniform", weight_col="", filter="", funcs=[]) -%}
    
    {% if distribution=='uniform' %}
        {{ return(adapter.dispatch('column_select_uniform')(name, value_col, lookup_table, filter, funcs)) }}
    
    {% elif distribution=='weighted' %}
        {% if weight_col=='' %}
            {{ exceptions.raise_compiler_error("`weight_col` is required when `distribution` for select column `" ~ name ~ "` is `weighted`.") }}
        {% endif %}
        {{ return(adapter.dispatch('column_select_weighted')(name, value_col, lookup_table, weight_col, filter, funcs)) }}
    
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `distribution` " ~ distribution ~ " for select column `" ~ name ~ "`: should be `uniform` (default) or `weighted`.") }}
    {% endif %}

{%- endmacro %}

{% macro default__column_select_uniform(name, value_col, lookup_table, filter, funcs) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro default__column_select_weighted(name, value_col, lookup_table, weight_col, filter, funcs) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}



{% macro postgres__column_select_uniform(name, value_col, lookup_table, filter, funcs) %}
    {{ dbt_synth.add_update_hook(postgres__select_uniform_update(name, value_col, lookup_table, filter, funcs)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__select_cleanup(name)) or "" }}
    
    RANDOM() as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro %}

{% macro postgres__select_uniform_update(name, value_col, lookup_table, filter, funcs) %}
update {{ this }} set {{name}}=y.{{value_col}} from (
  select
    {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %} as {{value_col}},
    ( (row_number() over (order by {{value_col}} asc)) - 1 )::double precision / count(*) over () as from_val,
    ( row_number() over (order by {{value_col}} asc) )::double precision / count(*) over () as to_val
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
  {% if filter|trim|length %}
  where {{filter}}
  {% endif %}
) as y where {{name}}_rand>=y.from_val and {{name}}_rand<y.to_val+0.0001
{% endmacro %}

{% macro postgres__column_select_weighted(name, value_col, lookup_table, weight_col, filter, funcs) %}
    {{ dbt_synth.add_update_hook(postgres__select_weighted_update(name, value_col, lookup_table, weight_col, filter, funcs)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__select_cleanup(name)) or "" }}
    
    RANDOM() as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro %}

{% macro postgres__select_weighted_update(name, value_col, lookup_table, weight_col, filter, funcs) %}
update {{this}} set {{name}}=y.{{value_col}} from(
  select
    {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %} as {{value_col}},
    {{weight_col}},
    ((sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc)) - {{weight_col}}::double precision) / sum({{weight_col}}::double precision) over () as from_freq,
    (sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc)) / sum({{weight_col}}::double precision) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
  {% if filter|trim|length %}
  where {{filter}}
  {% endif %}
  order by from_freq asc, to_freq asc
) as y where {{name}}_rand>=y.from_freq and {{name}}_rand<y.to_freq+0.0001
{% endmacro %}

{% macro postgres__select_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}



{% macro snowflake__column_select_uniform(name, value_col, lookup_table, filter, funcs) %}
    {{ dbt_synth.add_update_hook(snowflake__select_uniform_update(name, value_col, lookup_table, filter, funcs)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__select_cleanup(name)) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{randseed}})) as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro %}

{% macro snowflake__select_uniform_update(name, value_col, lookup_table, filter, funcs) %}
update {{ this }} x set x.{{name}}=y.{{value_col}} from (
  select
    {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %} as {{value_col}},
    ( (row_number() over (order by {{value_col}} asc)) - 1 )::double precision / count(*) over () as from_val,
    ( row_number() over (order by {{value_col}} asc) )::double precision / count(*) over () as to_val
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
  {% if filter|trim|length %}
  where {{filter}}
  {% endif %}
) as y where x.{{name}}_rand>=y.from_val and x.{{name}}_rand<y.to_val+0.0001
{% endmacro %}

{% macro snowflake__column_select_weighted(name, value_col, lookup_table, weight_col, filter, funcs) %}
    {{ dbt_synth.add_update_hook(snowflake__select_weighted_update(name, value_col, lookup_table, weight_col, filter, funcs)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__select_cleanup(name)) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{randseed}})) as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro %}

{% macro snowflake__select_weighted_update(name, value_col, lookup_table, weight_col, filter, funcs) %}
update {{this}} x set x.{{name}}=y.{{value_col}} from(
  select
    {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %} as {{value_col}},
    {{weight_col}},
    ((sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc)) - {{weight_col}}::double precision) / sum({{prevalence_col}}::double precision) over () as from_freq,
    (sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc)) / sum({{weight_col}}::double precision) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
  {% if filter|trim|length %}
  where {{filter}}
  {% endif %}
  order by from_freq asc, to_freq asc
) as y where x.{{name}}_rand>=y.from_freq and x.{{name}}_rand<y.to_freq+0.0001
{% endmacro %}

{% macro snowflake__select_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}
