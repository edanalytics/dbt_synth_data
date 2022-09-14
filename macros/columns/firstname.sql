{% macro column_firstname(name, distribution) -%}
    {{ return(adapter.dispatch('column_firstname')(get_randseed(), name, distribution)) }}
{%- endmacro %}

{% macro default__column_firstname(randseed, name, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_firstname(randseed, name, distribution) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_firstname(randseed, name, distribution) %}
    {{ dbt_synth.add_post_hook(frequency_firstnames_update(name)) or "" }}
    {{ dbt_synth.add_post_hook(frequency_firstnames_cleanup()) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{randseed}})) as firstname_rand,
    ''::varchar AS {{name}}
{% endmacro%}

{% macro frequency_firstnames_update(name) %}
update {{this}} x set x.{{name}}=y.the_name from(
  select
    INITCAP(name) as the_name,
    frequency,
    ((sum(frequency::double) over (order by frequency desc, name asc)) - frequency::double) / sum(frequency::double) over () as from_freq,
    (sum(frequency::double) over (order by frequency desc, name asc)) / sum(frequency::double) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.synth_firstnames
  order by from_freq asc, to_freq asc
) as y where x.firstname_rand>=y.from_freq and x.firstname_rand<y.to_freq
{% endmacro %}

{% macro frequency_firstnames_cleanup() %}
alter table {{ this }} drop column firstname_rand
{% endmacro %}