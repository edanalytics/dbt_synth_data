{{ config(materialized='table') }}

{{ synth_column_primary_key(name='k_customer') }}
{{ synth_column_firstname(name='first_name') }}
{{ synth_column_lastname(name='last_name') }}
{{ synth_column_expression(name='full_name', expression="first_name || ' ' || last_name") }}
{{ synth_column_expression(name='sort_name', expression="last_name || ', ' || first_name") }}
{{ synth_column_date(name="birth_date", min='1938-01-01', max='1994-12-31') }}
{{ synth_column_address(name='shipping_address', countries=['United States'], parts=['street_address', 'city', 'geo_region_abbr', 'postal_code']) }}
{{ synth_column_phone_number(name='phone_number') }}

{{ synth_table(rows=100) }}