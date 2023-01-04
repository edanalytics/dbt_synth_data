{% macro synth_column_expression(name, expression, type='varchar') -%}
    {{ return(adapter.dispatch('synth_column_expression')(name, expression, type)) }}
{%- endmacro %}

{% macro default__synth_column_expression(name, expression, type) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_expression(name, expression, type) %}
    {{ synth_add_update_hook(synth_column_expression_update(name, expression)) or "" }}
    
    NULL as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_expression(name, expression, type) %}
    {{ synth_add_update_hook(synth_column_expression_update(name, expression)) or "" }}
    
    NULL::{{type}} as {{name}}
{% endmacro%}

{% macro synth_column_expression_update(name, expression) %}
update {{ this }} set {{name}} = {{expression}}
{% endmacro %}