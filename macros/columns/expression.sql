{% macro column_expression(name, expression) -%}
    {{ return(adapter.dispatch('column_expression')(name, expression)) }}
{%- endmacro %}

{% macro default__column_expression(name, expression) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_expression(name, expression) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_expression(name, expression) %}
    {{ dbt_synth.add_post_hook(expression_update(name, expression)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro%}

{% macro expression_update(name, expression) %}
update {{ this }} set {{name}} = ({{expression}})
{% endmacro %}