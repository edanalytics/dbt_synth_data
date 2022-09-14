-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_guardian') }}

{# 100K k_student, k_guardian -> 200k fct_family_relationship rows in ~14 mins#}
{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = 200000,
  columns = [
    dbt_synth.column_foreign_key(name='k_student', table='dim_student', column='k_student'),
    dbt_synth.column_foreign_key(name='k_guardian', table='dim_guardian', column='k_guardian'),
  ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}