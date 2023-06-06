{% macro synth_distribution_continuous_laplace(mu=0.0, b=1.0) -%}
   {{mu}}
   + {{ dbt_synth_data.synth_distribution_continuous_exponential(lambda=1/b) }}
   - {{ dbt_synth_data.synth_distribution_continuous_exponential(lambda=1/b) }}
{%- endmacro %}