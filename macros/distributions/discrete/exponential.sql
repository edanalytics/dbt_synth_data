{% macro synth_distribution_discrete_exponential(lambda=1.0, precision=0) -%}
    {% if precision==0 %}
        floor( {{ synth_distribution_continuous_exponential(lambda=lambda) }} )
    {% else %}
        {{ synth_distribution_continuous_exponential(lambda=lambda, precision=precision) }}
    {% endif %}
{%- endmacro %}