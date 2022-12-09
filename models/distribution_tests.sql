{{ config(materialized='table', custom_description='HEADER!') }}
{{ synth_table(
  rows = 1000000,
  columns = [
    synth_column_distribution(name='continuous_uniform_0_1',
        distribution=synth_distribution(class='continuous', type='uniform', min=0, max=1)
    ),
    synth_column_distribution(name='continuous_normal',
        distribution=synth_distribution(class='continuous', type='normal')
    ),
    synth_column_distribution(name='continuous_bimodal',
        distribution=synth_distribution_union(
            synth_distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
            synth_distribution(class='continuous', type='normal', mean=8.0, stddev=1.0),
            weights=[1, 2]
        )
    ),
    synth_column_distribution(name='continuous_trimodal',
        distribution=synth_distribution_union(
            synth_distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
            synth_distribution(class='continuous', type='normal', mean=10.0, stddev=2.0),
            synth_distribution(class='continuous', type='normal', mean=15.0, stddev=1.0),
            weights=[1, 2, 3]
        )
    ),
    synth_column_distribution(name='continuous_average',
        distribution=synth_distribution_average(
            synth_distribution(class='continuous', type='exponential', lambda=0.1),
            synth_distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
            weights=[1,4]
        )
    ),
    synth_column_distribution(name='interesting_wave',
        distribution=synth_distribution_union(
            synth_distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
            synth_distribution(class='continuous', type='normal', mean=4.0, stddev=1.0),
            synth_distribution(class='continuous', type='normal', mean=6.0, stddev=1.0),
            weights=[1,2,3]
        )
    ),
    synth_column_distribution(name='steps',
        distribution=synth_distribution_union(
            synth_distribution(class='continuous', type='uniform', min=0, max=1),
            synth_distribution(class='continuous', type='uniform', min=1, max=2),
            synth_distribution(class='continuous', type='uniform', min=2, max=3),
            weights=[1,2,3]
        )
    ),
    synth_column_distribution(name='continuous_exponential',
        distribution=synth_distribution(class='continuous', type='exponential', lambda=0.1)
    ),
    synth_column_distribution(name='discrete_uniform_0_10',
        distribution=synth_distribution(class='discrete', type='uniform', min=0, max=9, precision=1)
    ),
    synth_column_distribution(name='discrete_normal',
        distribution=synth_distribution(class='discrete', type='normal', mean=0, stddev=5)
    ),
    synth_column_distribution(name='discrete_exponential',
        distribution=synth_distribution(class='discrete', type='exponential', lambda=0.5)
    ),
    synth_column_distribution(name='discrete_bernoulli',
        distribution=synth_distribution(class='discrete', type='bernoulli')
    ),
    synth_column_distribution(name='discrete_binomial',
        distribution=synth_distribution(class='discrete', type='binomial', n=100000, p=0.02)
    ),
    synth_column_distribution(name='discrete_probability',
        distribution=synth_distribution(class='discrete', type='probabilities',
            probabilities={"cat":0.3, "dog":0.5, "parrot":0.2}, wrap="'")
    ),
    synth_column_distribution(name='discrete_weights',
        distribution=synth_distribution(class='discrete', type='weights',
            values=["cat", "dog", "parrot"], weights=[3, 6, 1])
    ),
  ]
) }}
{# { "5":0.05, "7":0.15, "11":0.25, "13":0.35, "17":0.2} #}
{# [0.05, 0.15, 0.25, 0.35, 0.2] #}
{{ config(post_hook=synth_get_post_hooks())}}
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