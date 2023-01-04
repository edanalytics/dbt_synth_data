{% macro synth_column_value(name, value='', type='') -%}
    {{ return(adapter.dispatch('synth_column_value')(name, value, type)) }}
{%- endmacro %}

{% macro default__synth_column_value(name, value, type) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_value(name, value, type) %}
    {% if value is string %} '{{value}}'
    {% elif value is number %}{{value}}
    {% elif not value %}NULL{% if type!='' %}::{{type}}{% endif %}
    {% else %}{{value}}
    {% endif %} as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_value(name, value, type) %}
    {% if value is string %} '{{value}}'
    {% elif value is number %}{{value}}
    {% elif not value %}NULL{% if type!='' %}::{{type}}{% endif %}
    {% else %}{{value}}
    {% endif %} as {{name}}
{% endmacro%}