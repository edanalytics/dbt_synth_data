{% macro column_value(name, value='') -%}
    {{ return(adapter.dispatch('column_value')(name, value)) }}
{%- endmacro %}

{% macro default__column_value(name, value) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_value(name, value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %} AS {{name}}
{% endmacro %}

{% macro snowflake__column_value(name, value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %} AS {{name}}
{% endmacro%}