{% macro synth_column_lookup(name, model_name, value_cols, from_col, to_col) -%}
    {# Allow for `value_cols` to be a single (string) column name: #}
    {% if value_cols is string %}{% set value_cols = [value_cols] %}{% endif %}
    
    {% set join_fields %}
        {% for value_col in value_cols %}
        {{name}}_lookup.{{value_col}} as {{name}}
        {% if not loop.last %},{% endif %}
        {% endfor%}
    {% endset %}
    {% set join_clause %}
        left join {{ref(model_name)}} {{name}}_lookup on ___PREVIOUS_CTE___.{{from_col}}={{name}}__lookup.to_col
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% endfor %}
        {% endif %}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{%- endmacro %}