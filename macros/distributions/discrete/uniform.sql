{% macro synth_distribution_discrete_uniform(min=0, max=1, precision=0) -%}
    {% if precision==0 %}
        floor( {{ synth_distribution_continuous_uniform(min=min, max=max+1) }} )
    {% else %}
        {{ synth_distribution_continuous_uniform(min=min, max=max, precision=precision) }}
    {% endif %}
{%- endmacro %}