{% macro synth_column_values(name, values, probabilities=None) -%}
    {# Determine generated value types: #}
    {%- if values[0] is number -%}
        {% set wrap = "" %}
    {% elif values[0] is string %}
        {% set wrap = "'" %}
    {% else %}
        {{ exceptions.raise_compiler_error("`values` must be strings or numbers") }}
    {% endif %}

    {% if values|length==1 %}
        {{wrap}}{{values[0]}}{{wrap}} as {{name}}
    {% else %}
        {# Construct probabilities if none specified: #}
        {%- if probabilities is none -%}
        {% set probabilities = [] %}
        {% for i in range(values|length-1) %}
            {% do probabilities.append( (1 / values|length)|round(3) ) %}
        {% endfor %}
        {% do probabilities.append( (1.0 - probabilities|sum)|round(3) ) %}
        {% endif %}

        {{ synth_distribution_discrete_probabilities(probabilities=synth_zip(values, probabilities), wrap=wrap) }} as {{name}}
    {% endif %}
{%- endmacro %}