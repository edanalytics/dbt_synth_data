{{ config(materialized='table') }}

with
{{ synth_column_distribution(name="continuous_uniform",
    distribution=synth_distribution(class='continuous', type='uniform', min=0, max=1)
) }}
{{ synth_column_distribution(name="continuous_normal",
    distribution=synth_distribution(class='continuous', type='normal')
) }}
{{ synth_column_distribution(name="continuous_exponential",
    distribution=synth_distribution(class='continuous', type='exponential', lambda=0.1)
) }}
{{ synth_column_distribution(name="laplace",
    distribution=synth_distribution_continuous_laplace()
) }}
{{ synth_column_distribution(name="cauchy",
    distribution=synth_distribution_continuous_cauchy(x0=0.0, gamma=0.5)
) }}
{{ synth_column_distribution(name="continuous_bimodal",
    distribution=synth_distribution_union(
        synth_distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
        synth_distribution(class='continuous', type='normal', mean=8.0, stddev=1.0),
        weights=[1, 2]
    )
) }}
{{ synth_column_distribution(name="continuous_trimodal",
    distribution=synth_distribution_union(
        synth_distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
        synth_distribution(class='continuous', type='normal', mean=10.0, stddev=2.0),
        synth_distribution(class='continuous', type='normal', mean=15.0, stddev=1.0),
        weights=[1, 2, 3]
    )
) }}
{{ synth_column_distribution(name="continuous_average",
    distribution=synth_distribution_average(
        synth_distribution(class='continuous', type='exponential', lambda=0.1),
        synth_distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
        weights=[1,4]
    )
) }}
{{ synth_column_distribution(name="interesting_wave",
    distribution=synth_distribution_union(
        synth_distribution(class='continuous', type='normal', mean=2.0, stddev=1.0),
        synth_distribution(class='continuous', type='normal', mean=4.0, stddev=1.0),
        synth_distribution(class='continuous', type='normal', mean=6.0, stddev=1.0),
        weights=[1,2,3]
    )
) }}
{{ synth_column_distribution(name="steps",
    distribution=synth_distribution_union(
        synth_distribution(class='continuous', type='uniform', min=0, max=1),
        synth_distribution(class='continuous', type='uniform', min=1, max=2),
        synth_distribution(class='continuous', type='uniform', min=2, max=3),
        weights=[1,2,3]
    )
) }}
{%- if target.type not in ['sqlite', 'duckdb'] -%}
{{ synth_column_distribution(name="discretized_uniform",
    distribution=synth_distribution_discretize_width_bucket(
        distribution=synth_distribution(class='continuous', type='uniform', min=0, max=10),
        from=0, to=10, strict_bounds=True, count=5, labels='bucket_range'
    )
) }}
{{ synth_column_distribution(name="discretized_normal",
    distribution=synth_distribution_discretize_width_bucket(
        distribution=synth_distribution(class='continuous', type='normal', mean=0.0, stddev=1.0),
        from=-2.2, to=2.2, strict_bounds=False, count=24, labels='bucket_range'
    )
) }}
{% endif %}
{{ synth_column_distribution(name="discretized_exponential",
    distribution=synth_distribution_discretize_floor(
        distribution=synth_distribution(class='continuous', type='exponential', lambda=0.1)
    )
) }}
{{ synth_column_distribution(name="discrete_bernoulli",
    distribution=synth_distribution(class='discrete', type='bernoulli')
) }}
{{ synth_column_distribution(name="discrete_binomial",
    distribution=synth_distribution(class='discrete', type='binomial', n=10, p=0.03)
) }}
{{ synth_column_distribution(name="discrete_probability",
    distribution=synth_distribution(class='discrete', type='probabilities',
        probabilities={"cat":0.3, "dog":0.5, "parrot":0.2}
    )
) }}
{{ synth_column_distribution(name="discrete_weights",
    distribution=synth_distribution(class='discrete', type='weights',
        values=["cat", "dog", "parrot"], weights=[3, 6, 1]
    )
) }}
{{ synth_table(rows=10000) }}

select * from synth_table

{#-
    Test this data with something like
    ```sql
    with rand_data as (
        select * from [database].[schema].distributions
    )
    select round(continuous_uniform::numeric, 1) as continuous_uniform_0_1, count(*)
    --select round(continuous_normal::numeric, 1) as continuous_normal, count(*)
    --select round(continuous_exponential::numeric, 1) as continuous_exponential, count(*)
    --select round(continuous_bimodal::numeric, 1) as continuous_bimodal, count(*)
    --select round(continuous_trimodal::numeric, 1) as continuous_trimodal, count(*)
    --select round(continuous_average::numeric, 1) as continuous_intersection, count(*)
    --select round(interesting_wave::numeric, 1) as interesting_wave, count(*)
    --select round(steps::numeric, 1) as steps, count(*)
    --select discretized_uniform, count(*)
    --select discretized_normal, count(*)
    --select discretized_exponential, count(*)
    --select discrete_bernoulli, count(*)
    --select discrete_binomial, count(*)
    --select discrete_probability, count(*)
    --select discrete_weights, count(*)
    from rand_data
    group by 1
    order by 1;
    ```
    or with DuckDB, you can do
    ```sql
    select histogram(discrete_weights), max(stats(discrete_weights))
    from [database].[schema].distributions
    ```
-#}