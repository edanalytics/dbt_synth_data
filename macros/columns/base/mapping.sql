{% macro synth_column_mapping(name, expression='', mapping={}) -%}
    {% set final_field %}
        case {{expression}}
        {% for k,v in mapping.items() %}
            when {% if k is string %}'{% endif %}{{k}}{% if k is string %}'{% endif %}
                then {% if v is string %}'{% endif %}{{v}}{% if v is string %}'{% endif %}
        {% endfor %}
        end as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('joins', name+'__mapping', {"fields": final_field, "clause": ""}) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}