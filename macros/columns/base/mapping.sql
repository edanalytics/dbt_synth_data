{% macro synth_column_mapping(name, expression='', mapping={}) -%}
    {{ return(adapter.dispatch('synth_column_mapping')(name, expression, mapping)) }}
{%- endmacro %}

{% macro default__synth_column_mapping(name, expression='', mapping={}) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_mapping(name, expression='', mapping={}) %}
    {{ synth_add_update_hook(synth_mapping_update(name, expression, mapping)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_mapping(name, expression='', mapping={}) %}
    {{ synth_add_update_hook(synth_mapping_update(name, expression, mapping)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro%}

{% macro synth_mapping_update(name, expression='', mapping={}) %}
update {{ this }} set {{name}} = (
    case {{expression}}
    {% for k,v in mapping.items() %}
    when '{{k}}' then '{{v}}'
    {% endfor %}
    end
)
{% endmacro %}