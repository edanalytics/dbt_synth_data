{% macro synth_distribution_continuous_cauchy(x0=0.0, gamma=1.0) -%}
   {{ dbt_synth_data.synth_distribution_continuous_normal(mean=x0, stddev=gamma) }}
   / {{ dbt_synth_data.synth_distribution_continuous_normal(mean=x0, stddev=1.0) }}
{%- endmacro %}