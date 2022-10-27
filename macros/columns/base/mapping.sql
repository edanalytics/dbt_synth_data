{% macro column_mapping(name, expression='', mapping={}) -%}
    {{ return(adapter.dispatch('column_mapping')(name, expression, mapping)) }}
{%- endmacro %}

{% macro default__column_mapping(name, expression='', mapping={}) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_mapping(name, expression='', mapping={}) %}
    {{ dbt_synth.add_update_hook(mapping_update(name, expression, mapping)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro %}

{% macro snowflake__column_mapping(name, expression='', mapping={}) %}
    {{ dbt_synth.add_update_hook(mapping_update(name, expression, mapping)) or "" }}
    
    ''::varchar AS {{name}}
{% endmacro%}

{% macro mapping_update(name, expression='', mapping={}) %}
update {{ this }} set {{name}} = (
    case {{expression}}
    {% for k,v in mapping.items() %}
    when '{{k}}' then '{{v}}'
    {% endfor %}
    end
)
{% endmacro %}