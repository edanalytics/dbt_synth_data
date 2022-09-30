{% macro column_foreign_key(name, table, column) -%}
    {{ return(adapter.dispatch('column_foreign_key')(name, table, column)) }}
{%- endmacro %}

{% macro default__column_foreign_key(name, table, column) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}



{% macro postgres__column_foreign_key(name, table, column) %}
    {{ dbt_synth.add_post_hook(postgres__foreign_key_update(name, table, column)) or "" }}
    {{ dbt_synth.add_post_hook(postgres__foreign_key_cleanup(name)) or "" }}
    
    ''::varchar AS {{name}},
    RANDOM() AS {{name}}_rand
{% endmacro %}

{% macro postgres__foreign_key_update(name, table, column) %}
update {{ this }} set {{name}}=y.val from (
  select
    {{column}} as val,
    ( (row_number() over (order by {{column}} asc)) - 1 )::double precision / count(*) over () as from_val,
    ( row_number() over (order by {{column}} asc) )::double precision / count(*) over () as to_val
  from {{ this.database }}.{{ this.schema }}.{{ table }}
  order by {{column}}
) as y where {{name}}_rand>=y.from_val and {{name}}_rand<y.to_val
{% endmacro %}

{% macro postgres__foreign_key_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}



{% macro snowflake__column_foreign_key(name, table, column) %}
    {{ dbt_synth.add_post_hook(snowflake__foreign_key_update(name, table, column)) or "" }}
    {{ dbt_synth.add_post_hook(snowflake__foreign_key_cleanup(name)) or "" }}
    
    ''::varchar AS {{name}},
    UNIFORM(0::float, 1::float, RANDOM( {{get_randseed()}} )) AS {{name}}_rand
{% endmacro%}

{% macro snowflake__foreign_key_update(name, table, column) %}
update {{ this }} x set x.{{name}}=y.val from (
  select
    {{column}} as val,
    ( (row_number() over (order by {{column}} asc)) - 1 ) / count(*) over () as from_val,
    ( row_number() over (order by {{column}} asc) ) / count(*) over () as to_val
  from {{ this.database }}.{{ this.schema }}.{{ table }}
  order by {{column}}
) as y where x.{{name}}_rand>=y.from_val and x.{{name}}_rand<y.to_val
{% endmacro %}

{% macro snowflake__foreign_key_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}