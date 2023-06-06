{% macro synth_column_lookup(name, model_name, value_col, from_col, to_col, do_ref=True) -%}
    {# Allow for `value_cols` to be a single (string) column name: #}
    {% if value_cols is string %}{% set value_cols = [value_cols] %}{% endif %}
    
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    {% set join_fields %}
        {{table_name}}__{{name}}__lookup.{{to_col}} as {{name}}
    {% endset %}
    {% set join_clause %}
        left join {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %} {{table_name}}__{{name}}__lookup on ___PREVIOUS_CTE___.{{value_col}}={{table_name}}__{{name}}__lookup.{{from_col}}
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__lookup", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{%- endmacro %}