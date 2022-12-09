{{ config(materialized='table', custom_description='HEADER!') }}
{{ table(
  rows = 1000000,
  columns = [
    column_distribution(name='continuous_uniform_0_1',
        distribution=distribution(class='continuous', type='uniform', min=0, max=1)
    ),
    column_distribution(name='continuous_normal',
        distribution=distribution(class='continuous', type='normal')
    ),
    column_distribution(name='continuous_bimodal',
        distribution=distribution_union(
            distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
            distribution(class='continuous', type='normal', mean=8.0, stddev=1.0),
            weights=[1, 2]
        )
    ),
    column_distribution(name='continuous_trimodal',
        distribution=distribution_union(
            distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
            distribution(class='continuous', type='normal', mean=10.0, stddev=2.0),
            distribution(class='continuous', type='normal', mean=15.0, stddev=1.0),
            weights=[1, 2, 3]
        )
    ),
    column_distribution(name='continuous_average',
        distribution=distribution_average(
            distribution(class='continuous', type='exponential', lambda=0.1),
            distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
            weights=[1,4]
        )
    ),
    column_distribution(name='interesting_wave',
        distribution=distribution_union(
            distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
            distribution(class='continuous', type='normal', mean=4.0, stddev=1.0),
            distribution(class='continuous', type='normal', mean=6.0, stddev=1.0),
            weights=[1,2,3]
        )
    ),
    column_distribution(name='steps',
        distribution=distribution_union(
            distribution(class='continuous', type='uniform', min=0, max=1),
            distribution(class='continuous', type='uniform', min=1, max=2),
            distribution(class='continuous', type='uniform', min=2, max=3),
            weights=[1,2,3]
        )
    ),
    column_distribution(name='continuous_exponential',
        distribution=distribution(class='continuous', type='exponential', lambda=0.1)
    ),
    column_distribution(name='discrete_uniform_0_10',
        distribution=distribution(class='discrete', type='uniform', min=0, max=9, precision=1)
    ),
    column_distribution(name='discrete_normal',
        distribution=distribution(class='discrete', type='normal', mean=0, stddev=5)
    ),
    column_distribution(name='discrete_exponential',
        distribution=distribution(class='discrete', type='exponential', lambda=0.5)
    ),
    column_distribution(name='discrete_bernoulli',
        distribution=distribution(class='discrete', type='bernoulli')
    ),
    column_distribution(name='discrete_binomial',
        distribution=distribution(class='discrete', type='binomial', n=100000, p=0.02)
    ),
    column_distribution(name='discrete_probability',
        distribution=distribution(class='discrete', type='probabilities',
            probabilities={"cat":0.3, "dog":0.5, "parrot":0.2}, wrap="'")
    ),
    column_distribution(name='discrete_weights',
        distribution=distribution(class='discrete', type='weights',
            values=["cat", "dog", "parrot"], weights=[3, 6, 1])
    ),
  ]
) }}
{# { "5":0.05, "7":0.15, "11":0.25, "13":0.35, "17":0.2} #}
{# [0.05, 0.15, 0.25, 0.35, 0.2] #}
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