{% macro distribution_discrete_exponential(lambda=1.0, precision=0) -%}
    {% if precision==0 %}
        floor( {{ dbt_synth.distribution_continuous_exponential(lambda=lambda) }} )
    {% else %}
        {{ dbt_synth.distribution_continuous_exponential(lambda=lambda, precision=precision) }}
    {% endif %}
{%- endmacro %}