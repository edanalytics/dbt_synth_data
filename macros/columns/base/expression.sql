{% macro synth_column_expression(name, expression) -%}
    {% set join_fields %}
      {{expression}} as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store("joins", name+"__expression", {"fields": join_fields, "clause": ""} ) }}
    
    {% set final_field %}
      {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store("final_fields", name, final_field) }}
{%- endmacro %}