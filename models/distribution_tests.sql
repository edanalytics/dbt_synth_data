{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = 100000,
  columns = [
    dbt_synth.distribution(name='continuous_uniform_0_1', class='continuous', type='uniform',       min=0, max=1),
    dbt_synth.distribution(name='continuous_normal',      class='continuous', type='normal' ),
    dbt_synth.distribution(name='discrete_uniform_0_10',  class='discrete',   type='uniform',       min=0, max=9),
    dbt_synth.distribution(name='discrete_normal',        class='discrete',   type='normal',        mean=0, stddev=5),
    dbt_synth.distribution(name='discrete_bernoulli',     class='discrete',   type='bernoulli' ),
    dbt_synth.distribution(name='discrete_binomial',      class='discrete',   type='binomial',      n=100, p=0.3),
    dbt_synth.distribution(name='discrete_probability',   class='discrete',   type='probabilities', probabilities=[0.05, 0.15, 0.25, 0.35, 0.2]),
  ]
) }}
{#-
    Test by wrapping compiled output in something like
    with rand_data as (
        select * from [db.schema.]distribution_tests
    )
    --select round(continuous_uniform_0_1::numeric, 1) as continuous_uniform_0_1, count(*)
    --select round(continuous_normal::numeric, 1) as continuous_normal, count(*)
    --select discrete_uniform_0_10, count(*)
    --select discrete_normal, count(*)
    --select discrete_bernoulli, count(*)
    --select discrete_binomial, count(*)
    select discrete_probability, count(*)
    from rand_data 
    group by 1
    order by 1;
-#}