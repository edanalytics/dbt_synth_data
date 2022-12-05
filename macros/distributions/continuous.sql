{#- This macro simply does routing, so you can call distribution_continuous('normal', ...) or distribution_continuous_normal(...) -#}
{% macro distribution_continuous(type, min=0, max=1, mean=0, stddev=1, lambda=1.0, precision=-1) -%}
    {%- if type=='uniform' -%}
    {{ dbt_synth.distribution_continuous_uniform(min=min, max=max) }}
    {%- elif type=='normal' -%}
    {{ dbt_synth.distribution_continuous_normal(mean=mean, stddev=stddev) }}
    {%- elif type=='exponential' -%}
    {{ dbt_synth.distribution_continuous_exponential(lambda=lambda) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid continuous distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}