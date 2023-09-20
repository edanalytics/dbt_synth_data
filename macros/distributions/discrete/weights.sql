{% macro synth_distribution_discrete_weights(values, weights=None) -%}
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

    {% if target.type == 'duckdb' %}
        {%- set ns = namespace(curr_idx=0, curr_threshold=0) -%}
        {# DuckDB requires some gymnastics to prevent NULL values #}
        {% set value_list = [] %}
        {% for i in range(0, weights|length) %}
            {% for j in range(0, weights[i]) %}
                {% do value_list.append(values[i]) %}
            {% endfor %}
        {% endfor %}
        ifnull(
            ({{ value_list }})[
                {{ dbt_synth_data.synth_distribution_discretize_floor(
                    distribution=dbt_synth_data.synth_distribution_continuous_uniform(
                        min=0,
                        max=weights|sum
                    )
                ) }}
            ],
            {{wrap}}{{value_list[value_list|length - 1]}}{{wrap}}
        )
    {% else %}
        case {{ dbt_synth_data.synth_distribution_discretize_floor( distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=weights|sum) ) }}
        {% for v in range(0, values|length) %}
            {% for w in range(0, weights[v]) %}
            when {{ weights[:v]|sum + w }} then {{wrap}}{{values[v]}}{{wrap}}
            {% endfor %}
        {% endfor %}
        end
    {% endif %}
{%- endmacro %}