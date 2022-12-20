{% macro synth_correlation(data, column=1) -%}
    {{ return(adapter.dispatch('synth_correlation')(data, column)) }}
{%- endmacro %}

{% macro default__synth_correlation(data, column) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}



{% macro postgres__synth_correlation(data, column) %}
    {{ synth_add_update_hook(postgres__synth_correlation_update(data, column)) or "" }}
    {{ synth_add_cleanup_hook(postgres__synth_correlation_cleanup(column)) or "" }}
    
    RANDOM() as {{column}}_rand,
    {% if data.columns[column][0] is string %} ''::varchar
    {% elif data.columns[column][0] is integer %}0::int
    {% elif data.columns[column][0] is float %}0::float
    {% else %}NULL
    {% endif %} AS {{column}}
{% endmacro %}

{% macro postgres__synth_correlation_update(data, column) %}
    {%- set hypercube_dimension = data.columns|length -%}
    {%- set hypercube_shape = [] -%}
    {%- set ns = namespace(column_index=0) -%}
    {%- set ns2 = namespace(counter=0) -%}
    {%- for col, vals in data.columns.items() -%}
        {% if col==column %}{% set ns.column_index = ns2.counter %}{% endif %}
        {%- do hypercube_shape.append(vals|length) -%}
        {%- set ns2.counter = ns2.counter + 1 -%}
    {% endfor %}
    update {{this}} set {{column}}=(
    CASE
    {% set iterator = synth_hypercube_iterator(hypercube_shape) %}
    {% set ns_from = namespace(threshhold=0.0) %}
    {% set ns_to = namespace(threshhold=0.0) %}
    {% for indices_string in iterator %}
        {%- set indices = indices_string.split('.') -%}
        {%- set this_probability = synth_hypercube_value_at_indices(data.probabilities, indices) -%}
        {%- if this_probability > 0 -%}
        {%- set value = data.columns[column][(indices[ns.column_index]|int)] %}
        {%- set ns_to.threshhold = ns_from.threshhold + this_probability -%}
        WHEN
            {% if not loop.first %}
            {{column}}_rand >= {{ ns_from.threshhold }}
            {%- endif %}
            {% if not loop.first and not loop.last %}
            AND 
            {%- endif %}
            {%- if not loop.last %}
            {{column}}_rand < {{ ns_to.threshhold }}
            {%- endif %}
        THEN {% if value is string %}'{{value}}'{% else %}{{value}}{% endif %}
        {% set ns_from.threshhold = ns_to.threshhold -%}
        {% endif -%}
    {% endfor %}
    END)
{% endmacro %}

{% macro postgres__synth_correlation_cleanup(column) %}
alter table {{ this }} drop column {{column}}_rand
{% endmacro %}



{% macro snowflake__synth_correlation(data, column) %}
    {{ synth_add_update_hook(snowflake__synth_correlation_update(data, column)) or "" }}
    {{ synth_add_cleanup_hook(snowflake__synth_correlation_cleanup(column)) or "" }}
    
    UNIFORM(0::double, 1::double, RANDOM({{data.randseed}})) as {{column}}_rand,
    {% if data.columns[column][0] is string %} ''::varchar
    {% elif data.columns[column][0] is integer %}0::int
    {% elif data.columns[column][0] is float %}0::float
    {% else %}NULL
    {% endif %} AS {{column}}
{% endmacro%}

{% macro snowflake__synth_correlation_update(data, column) %}
    {%- set hypercube_dimension = data.columns|length -%}
    {%- set hypercube_shape = [] -%}
    {%- set ns = namespace(column_index=0) -%}
    {%- set ns2 = namespace(counter=0) -%}
    {%- for col, vals in data.columns.items() -%}
        {% if col==column %}{% set ns.column_index = ns2.counter %}{% endif %}
        {%- do hypercube_shape.append(vals|length) -%}
        {%- set ns2.counter = ns2.counter + 1 -%}
    {% endfor %}
    update {{this}} x set x.{{column}}=(
    CASE
    {% set iterator = synth_hypercube_iterator(hypercube_shape) %}
    {% set ns_from = namespace(threshhold=0.0) %}
    {% set ns_to = namespace(threshhold=0.0) %}
    {% for indices_string in iterator %}
        {%- set indices = indices_string.split('.') -%}
        {%- set this_probability = synth_hypercube_value_at_indices(data.probabilities, indices) -%}
        {%- if this_probability > 0 -%}
        {%- set value = data.columns[column][(indices[ns.column_index]|int)] %}
        {%- set ns_to.threshhold = ns_from.threshhold + this_probability -%}
        WHEN
            {% if not loop.first %}
            {{column}}_rand >= {{ ns_from.threshhold }}
            {%- endif %}
            {% if not loop.first and not loop.last %}
            AND 
            {%- endif %}
            {%- if not loop.last %}
            {{column}}_rand < {{ ns_to.threshhold }}
            {%- endif %}
        THEN {% if value is string %}'{{value}}'{% else %}{{value}}{% endif %}
        {% set ns_from.threshhold = ns_to.threshhold -%}
        {% endif -%}
    {% endfor %}
    END)
{% endmacro %}

{% macro snowflake__synth_correlation_cleanup(column) %}
alter table {{ this }} drop column {{column}}_rand
{% endmacro %}

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