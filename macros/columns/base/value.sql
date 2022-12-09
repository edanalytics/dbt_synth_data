{% macro synth_column_value(name, value='') -%}
    {{ return(adapter.dispatch('synth_column_value')(name, value)) }}
{%- endmacro %}

{% macro default__synth_column_value(name, value) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_value(name, value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %} AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_value(name, value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %} AS {{name}}
{% endmacro%}