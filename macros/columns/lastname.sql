{% macro column_lastname(name, distribution) -%}
    {{ return(adapter.dispatch('column_lastname')(get_randseed(), name, distribution)) }}
{%- endmacro %}

{% macro default__column_lastname(randseed, name, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_lastname(randseed, name, distribution) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_lastname(randseed, name, distribution) %}
    {{ dbt_synth.add_post_hook(frequency_lastnames_update(name)) or "" }}
    {{ dbt_synth.add_post_hook(frequency_lastnames_cleanup()) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{randseed}})) as lastname_rand,
    ''::varchar AS {{name}}
{% endmacro%}

{% macro frequency_lastnames_update(name) %}
update {{ this }} x set x.{{name}}=y.the_name from(
  select
    INITCAP(name) as the_name,
    frequency,
    ((sum(frequency::double) over (order by frequency desc, name asc)) - frequency::double) / sum(frequency::double) over () as from_freq,
    (sum(frequency::double) over (order by frequency desc, name asc)) / sum(frequency::double) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.synth_lastnames
  order by from_freq asc, to_freq asc
) as y where x.lastname_rand>=y.from_freq and x.lastname_rand<y.to_freq
{% endmacro %}

{% macro frequency_lastnames_cleanup() %}
alter table {{ this }} drop column lastname_rand
{% endmacro %}