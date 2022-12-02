{#- This macro simply does routing, so you can call distribution_discrete('normal', ...) or distribution_discrete_normal(...) -#}
{% macro distribution_discrete(type, min=0, max=1, mean=0, stddev=1, precision=-1, n=10, p=0.5, probabilities=None) -%}
    {%- if type=='uniform' -%}
    {{ dbt_synth.distribution_discrete_uniform(min=min, max=max) }}
    {%- elif type=='normal' -%}
    {{ dbt_synth.distribution_discrete_normal(mean=mean, stddev=stddev) }}
    {%- elif type=='bernoulli' -%}
    {{ dbt_synth.distribution_discrete_bernoulli(p=p) }}
    {%- elif type=='binomial' -%}
    {{ dbt_synth.distribution_discrete_binomial(n=n, p=p) }}
    {%- elif type=='probabilities' -%}
    {{ dbt_synth.distribution_discrete_probabilities(probabilities=probabilities) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid discrete distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}