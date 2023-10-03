-- depends_on: {{ ref('products') }}
-- depends_on: {{ ref('stores') }}
{{ config(materialized='table') }}
with
{{ synth_column_primary_key(name='k_inventory') }}
{{ synth_column_select(name='k_product', model_name="products", value_cols="k_product") }}
{{ synth_column_select(name='k_store', model_name="stores", value_cols="k_store") }}
{{ synth_column_integer(name='stock_count', min=0, max=500) }}
{{ synth_table(rows=100000000) }}

select * from synth_table