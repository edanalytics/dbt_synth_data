{#- This macro simply does routing, so you can call distribution_continuous('normal', ...) or distribution_continuous_normal(...) -#}
{% macro distribution_continuous(type, min=0, max=1, mean=0, stddev=1, precision=-1) -%}
    {%- if type=='uniform' -%}
    {{ dbt_synth.distribution_continuous_uniform(min=min, max=max) }}
    {%- elif type=='normal' -%}
    {{ dbt_synth.distribution_continuous_normal(mean=mean, stddev=stddev) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid continuous distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}