{% macro column_lastname(name, distribution) -%}
    {{ return(adapter.dispatch('column_lastname')(get_randseed(), name, distribution)) }}
{%- endmacro %}

{% macro default__column_lastname(randseed, name, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_lastname(randseed, name, distribution) %}
    {{ dbt_synth.add_post_hook(postgres__frequency_lastnames_update(name)) or "" }}
    {{ dbt_synth.add_post_hook(postgres__frequency_lastnames_cleanup(name)) or "" }}
    
    RANDOM() as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro %}

{% macro postgres__frequency_lastnames_update(name) %}
update {{ this }} set {{name}}=y.the_name from(
  select
    INITCAP(name) as the_name,
    frequency,
    ((sum(frequency::double precision) over (order by frequency desc, name asc)) - frequency::double precision) / sum(frequency::double precision) over () as from_freq,
    (sum(frequency::double precision) over (order by frequency desc, name asc)) / sum(frequency::double precision) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.synth_lastnames
  order by from_freq asc, to_freq asc
) as y where {{name}}_rand>=y.from_freq and {{name}}_rand<y.to_freq+0.0001
{% endmacro %}

{% macro postgres__frequency_lastnames_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}

{% macro snowflake__column_lastname(randseed, name, distribution) %}
    {{ dbt_synth.add_post_hook(snowflake__frequency_lastnames_update(name)) or "" }}
    {{ dbt_synth.add_post_hook(snowflake__frequency_lastnames_cleanup(name)) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{randseed}})) as {{name}}_rand,
    ''::varchar AS {{name}}
{% endmacro%}

{% macro snowflake__frequency_lastnames_update(name) %}
update {{ this }} x set x.{{name}}=y.the_name from(
  select
    INITCAP(name) as the_name,
    frequency,
    ((sum(frequency::double) over (order by frequency desc, name asc)) - frequency::double) / sum(frequency::double) over () as from_freq,
    (sum(frequency::double) over (order by frequency desc, name asc)) / sum(frequency::double) over () as to_freq
  from {{ this.database }}.{{ this.schema }}.synth_lastnames
  order by from_freq asc, to_freq asc
) as y where x.{{name}}_rand>=y.from_freq and x.{{name}}_rand<y.to_freq
{% endmacro %}

{% macro snowflake__frequency_lastnames_cleanup(name) %}
alter table {{ this }} drop column {{name}}_rand
{% endmacro %}