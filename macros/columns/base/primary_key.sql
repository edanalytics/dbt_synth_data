{% macro synth_column_primary_key(name) -%}
    {% set base_field %}
        MD5( '{{this}}' || __row_number::varchar ) as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{%- endmacro %}