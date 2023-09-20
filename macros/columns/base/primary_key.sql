{% macro synth_column_primary_key(name) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_primary_key')() }} as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_primary_key() -%}
    MD5( '{{this}}' || __row_number::varchar )
{%- endmacro %}

{% macro sqlite__synth_column_primary_key() %}
    {# SQLite doesn't support MD5() out-of-the-box, so just use the row number #}
    __row_number
{% endmacro %}
