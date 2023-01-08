{% macro synth_column_phone_number(name) -%}
    {% set join_fields %}
        '(' ||
        LPAD( ({{ synth_distribution_discretize_floor(
            distribution=synth_distribution_continuous_uniform(min=100, max=1000)
        ) }})::varchar, 3, '0' )
        || ') ' ||
        LPAD( ({{ synth_distribution_discretize_floor(
            distribution=synth_distribution_continuous_uniform(min=100, max=1000)
        ) }})::varchar, 3, '0' )
        || '-' ||
        LPAD( ({{ synth_distribution_discretize_floor(
            distribution=synth_distribution_continuous_uniform(min=1, max=10000)
        ) }})::varchar, 4, '0' )
        as {{name}}
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": ""} ) }}
    
    {% set final_field %}
      {{name}}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{%- endmacro %}