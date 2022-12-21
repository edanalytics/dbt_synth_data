{% macro synth_value(value='', type='') -%}
    {{ return(adapter.dispatch('synth_value')(value)) }}
{%- endmacro %}

{% macro default__synth_value(value) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_value(value) %}
    {% if value is string %} '{{value}}'
    {% elif value is numeric %}{{value}}
    {% elif not value %}NULL{% if type %}::{{type}}{% endif %}
    {% else %}{{value}}
    {% endif %}
{% endmacro %}

{% macro snowflake__synth_value(value) %}
    {% if value is string %} '{{value}}'
    {% elif value is numeric %}{{value}}
    {% elif not value %}NULL{% if type %}::{{type}}{% endif %}
    {% else %}{{value}}
    {% endif %}
{% endmacro%}