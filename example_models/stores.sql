{{ config(materialized='table') }}

{{ synth_column_primary_key(name='k_store') }}
{{ synth_column_date(name="date_opened", min='1998-01-01', max='2023-01-01') }}
{{ synth_column_boolean(name="is_active", pct_true=0.96) }}
{{ synth_column_address(name='physical_address', address_types=['house'], countries=['United States'], parts=['street_address', 'city', 'geo_region_abbr', 'postal_code']) }}
{{ synth_column_phone_number(name='phone_number') }}

{{ synth_table(rows=2) }}