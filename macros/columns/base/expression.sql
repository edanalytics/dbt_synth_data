{% macro synth_column_expression(name, expression, type='varchar') -%}
    {{ return(adapter.dispatch('synth_column_expression')(name, expression, type)) }}
{%- endmacro %}

{% macro default__synth_column_expression(name, expression, type='varchar') -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_expression(name, expression, type='varchar') %}
    {{ synth_add_update_hook(synth_expression_update(name, expression)) or "" }}
    
    NULL AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_expression(name, expression, type='varchar') %}
    {{ synth_add_update_hook(synth_expression_update(name, expression)) or "" }}
    
    NULL::{{type}} AS {{name}}
{% endmacro%}

{% macro synth_expression_update(name, expression) %}
update {{ this }} set {{name}} = {{expression}}
{% endmacro %}