{% macro synth_value(value='') -%}
    {{ return(adapter.dispatch('synth_value')(value)) }}
{%- endmacro %}

{% macro default__synth_value(value) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_value(value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %}
{% endmacro %}

{% macro snowflake__synth_value(value) %}
    {% if value is string %} '{{value}}'
    {% elif value is integer %}{{value}}
    {% elif value is float %}{{value}}
    {% elif not value %}NULL
    {% else %}{{value}}
    {% endif %}
{% endmacro%}