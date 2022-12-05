{% macro distribution_discrete_exponential(lambda=1.0) -%}
    floor( {{ dbt_synth.distribution_continuous_exponential(lambda=lambda) }} )
{%- endmacro %}