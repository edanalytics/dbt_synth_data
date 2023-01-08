{% macro synth_column_value(name, value='', type='') -%}
    {% set base_field %}
        {% if value is string %} '{{value}}'
        {% elif value is number %}{{value}}
        {% elif not value %}NULL{% if type!='' %}::{{type}}{% endif %}
        {% else %}{{value}}
        {% endif %} as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{%- endmacro %}