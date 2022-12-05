{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = 100000,
  columns = [
    dbt_synth.distribution(name='continuous_uniform_0_1', class='continuous', type='uniform',       min=0, max=1),
    dbt_synth.distribution(name='continuous_normal',      class='continuous', type='normal' ),
    
    dbt_synth.distribution(name='normal_0',               class='continuous', type='normal',    mean=5.0, stddev=1.0),
    dbt_synth.distribution(name='normal_1',               class='continuous', type='normal',    mean=8.0, stddev=1.0),
    dbt_synth.distribution(name='which_one',              class='discrete',   type='bernoulli', p=0.35),
    dbt_synth.column_expression(name='continuous_bimodal',
        expression='(case when which_one=0 then normal_0 else normal_1 end)'),
    
    dbt_synth.distribution(name='discrete_uniform_0_10',  class='discrete',   type='uniform',       min=0, max=9),
    dbt_synth.distribution(name='discrete_normal',        class='discrete',   type='normal',        mean=0, stddev=5),
    dbt_synth.distribution(name='discrete_bernoulli',     class='discrete',   type='bernoulli' ),
    dbt_synth.distribution(name='discrete_binomial',      class='discrete',   type='binomial',      n=100, p=0.3),
    dbt_synth.distribution(name='discrete_probability',   class='discrete',   type='probabilities', probabilities=[0.05, 0.15, 0.25, 0.35, 0.2]),
  ]
) }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column which_one") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column normal_0") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column normal_1") or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
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