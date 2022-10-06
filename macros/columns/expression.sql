{% macro column_expression(name, expression, type='varchar') -%}
    {{ return(adapter.dispatch('column_expression')(name, expression, type)) }}
{%- endmacro %}

{% macro default__column_expression(name, expression, type='varchar') -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_expression(name, expression, type='varchar') %}
    {{ dbt_synth.add_update_hook(expression_update(name, expression)) or "" }}
    
    NULL::{{type}} AS {{name}}
{% endmacro %}

{% macro snowflake__column_expression(name, expression, type='varchar') %}
    {{ dbt_synth.add_update_hook(expression_update(name, expression)) or "" }}
    
    NULL::{{type}} AS {{name}}
{% endmacro%}

{% macro expression_update(name, expression) %}
update {{ this }} set {{name}} = {{expression}}
{% endmacro %}