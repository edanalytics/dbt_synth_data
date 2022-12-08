{% macro distribution_discrete_normal(mean=0, stddev=1, precision=0) -%}
    {{ dbt_synth.distribution_continuous_normal(mean=mean, stddev=stddev, precision=precision) }}
{%- endmacro %}