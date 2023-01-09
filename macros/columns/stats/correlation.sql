{% macro synth_column_correlation(name, data, column=1) -%}
    {% set base_field %}
        {{ synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__{{column}}__rand
    {% endset %}
    {{ synth_store("base_fields", name+"__"+column+"__rand", base_field) }}

    {%- set hypercube_shape = [] -%}
    {%- set ns = namespace(column_index=0) -%}
    {%- set ns2 = namespace(counter=0) -%}
    {%- for col, vals in data.columns.items() -%}
        {% if col==column %}{% set ns.column_index = ns2.counter %}{% endif %}
        {%- do hypercube_shape.append(vals|length) -%}
        {%- set ns2.counter = ns2.counter + 1 -%}
    {% endfor %}
    {%- set hypercube_dimension = data.columns|length -%}
    {% set iterator = synth_hypercube_iterator(hypercube_shape) %}
    {% set ns_from = namespace(threshhold=0.0) %}
    {% set ns_to = namespace(threshhold=0.0) %}
    {% set ns_field = namespace(field='CASE ') %} 
    {% for indices_string in iterator %}
        {%- set indices = indices_string.split('.') -%}
        {%- set this_probability = synth_hypercube_value_at_indices(data.probabilities, indices) -%}
        {%- if this_probability > 0 -%}
        {%- set value = data.columns[column][(indices[ns.column_index]|int)] %}
        {%- set ns_to.threshhold = ns_from.threshhold + this_probability -%}
        {% set ns_field.field = ns_field.field + 'WHEN ' %}
            {% if not loop.first %}
            {% set ns_field.field = ns_field.field + name + '__' + column + '__rand >= ' + ns_from.threshhold|string %}
            {%- endif %}
            {% if not loop.first and not loop.last %}
            {% set ns_field.field = ns_field.field + ' AND  ' %}
            {%- endif %}
            {%- if not loop.last %}
            {% set ns_field.field = ns_field.field + name + '__' + column + '__rand < ' + ns_to.threshhold|string %}
            {%- endif %}
        {% set ns_field.field = ns_field.field + ' THEN ' %}
        {% if value is string %}
            {% set ns_field.field = ns_field.field + "'" + value + "' " %}
        {% else %}
            {% set ns_field.field = ns_field.field + value|string + " " %}
        {% endif %}
        {% set ns_from.threshhold = ns_to.threshhold -%}
        {% endif -%}
    {% endfor %}
    {% set ns_field.field = ns_field.field + ' END as ' + name %}
    {{ synth_store("final_fields", name, ns_field.field) }}
{%- endmacro %}

{% macro synth_hypercube_iterator(shape, prefix="") %}
    {% set idx_list = [] %}
    {% if shape|length == 1 %}
        {% for idx in range(0, shape[0]) %}
            {% do idx_list.append(prefix + idx|string) %}
        {% endfor %}
    {% elif shape|length > 1 %}
        {% for idx in range(0, shape[0]) %}
            {% set sub_list = synth_hypercube_iterator(shape[1:], idx|string + '.') %}
            {% for value in sub_list %}
                {% do idx_list.append(prefix + value) %}
            {% endfor %}
        {% endfor %}
    {% endif %}
    {{ return(idx_list) }}
{% endmacro %}

{% macro synth_hypercube_value_at_indices(hypercube, indices) %}
    {% if indices|length > 1 %}
        {{ return(synth_hypercube_value_at_indices(hypercube[indices[0]|int], indices[1:])) }}
    {% else %}
        {{ return(hypercube[indices[0]|int]) }}
    {% endif %}
{% endmacro %}