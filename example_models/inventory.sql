-- depends_on: {{ ref('products') }}
-- depends_on: {{ ref('stores') }}
{{ config(materialized='table') }}

{{ synth_column_primary_key(name='k_inventory') }}
{{ synth_column_select(name='k_product', lookup_table="products", value_col="k_product") }}
{{ synth_column_select(name='k_store', lookup_table="stores", value_col="k_store") }}
{{ synth_column_integer(name='stock_count', min=0, max=500) }}

{{ synth_table(rows=100) }}