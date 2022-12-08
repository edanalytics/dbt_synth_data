{% macro distribution_discrete_uniform(min=0, max=1, precision=0) -%}
    floor( {{ dbt_synth.distribution_continuous_uniform(min=min, max=max+1, precision=precision) }} )
{%- endmacro %}