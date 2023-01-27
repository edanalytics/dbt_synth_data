{% macro synth_column_lookup(name, model_name, value_col, from_col, to_col, ref=True) -%}
    {# Allow for `value_cols` to be a single (string) column name: #}
    {% if value_cols is string %}{% set value_cols = [value_cols] %}{% endif %}
    
    {% set join_fields %}
        {{name}}__lookup.{{to_col}} as {{name}}
    {% endset %}
    {% set join_clause %}
        left join {% if ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %} {{name}}__lookup on ___PREVIOUS_CTE___.{{value_col}}={{name}}__lookup.{{from_col}}
    {% endset %}
    {{ synth_store("joins", name+"__lookup", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{%- endmacro %}