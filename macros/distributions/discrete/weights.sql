{% macro distribution_discrete_weights(values, weights=None) -%}
    {# Assume uniform/equal weights if weights are not specified:  #}
    {% if weights is none %}
        {% set weights=[1] * values|length %}
    {% endif %}

    {# Determine generated value types: #}
    {%- if values[0] is number -%}
        {% set wrap = "" %}
    {% elif values[0] is string %}
        {% set wrap = "'" %}
    {% else %}
        {{ exceptions.raise_compiler_error("`values` must be strings or numbers") }}
    {% endif %}

    case {{ dbt_synth.distribution_discrete_uniform(min=0, max=weights|sum - 1) }}
    {% for v in range(0, values|length) %}
        {% for w in range(0, weights[v]) %}
        when {{ weights[:v]|sum + w }} then {{wrap}}{{values[v]}}{{wrap}}
        {% endfor %}
    {% endfor %}
    end
{%- endmacro %}