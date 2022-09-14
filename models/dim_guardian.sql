-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}
-- depends_on: {{ ref('synth_words') }}
-- depends_on: {{ ref('synth_cities') }}

{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = 100000,
  columns = [
    dbt_synth.column_primary_key(name='k_guardian'),
    dbt_synth.column_firstname(name='first_name'),
    dbt_synth.column_lastname(name='last_name'),
    dbt_synth.column_expression(name='display_name', expression="last_name || ', ' || first_name"),
    dbt_synth.column_lookup(name='gender', value_col='UPPER(first_name)', lookup_table='synth_firstnames', from_col='name', to_col='gender'),
    dbt_synth.column_address(name='address', countries=['United States of America'], geo_regions=['AL'], address_types=['house','apartment']),
    dbt_synth.column_phone_number(name='phone'),
  ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}

    {# dbt_synth.column_email(name='email'), #}
