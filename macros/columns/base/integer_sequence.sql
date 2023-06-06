{% macro synth_column_integer_sequence(name, step=1, start=0) -%}
    {% set base_field %}
        ( {{start}} + (__row_number) * {{step}} ) as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}