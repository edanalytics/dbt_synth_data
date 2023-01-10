{% macro synth_column_mapping(name, expression='', mapping={}) -%}
    {% set final_field %}
        case {{expression}}
        {% for k,v in mapping.items() %}
            when '{{k}}' then '{{v}}'
        {% endfor %}
        end as {{name}}
    {% endset %}
    {{ synth_store('joins', name+'__mapping', {"fields": final_field, "clause": ""}) }}
{%- endmacro %}