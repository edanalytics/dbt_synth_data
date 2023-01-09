{% macro synth_column_phone_number(name) -%}
    {% set join_fields %}
        '(' ||
        {{ adapter.dispatch('synth_column_phone_number_chunk')(min=100, max=1000, pad_length=3) }}
        || ') ' ||
        {{ adapter.dispatch('synth_column_phone_number_chunk')(min=100, max=1000, pad_length=3) }}
        || '-' ||
        {{ adapter.dispatch('synth_column_phone_number_chunk')(min=1, max=1000, pad_length=4) }}
        as {{name}}
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": ""} ) }}
    
    {% set final_field %}
      {{name}}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_phone_number_chunk(min, max, pad_length) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_column_phone_number_chunk(min, max, pad_length) %}
    substr( '{{'0' * pad_length}}' || cast ({{ synth_distribution_discretize_floor(
        distribution=synth_distribution_continuous_uniform(min=min, max=max)
    ) }} as int), -1*{{pad_length}}, {{pad_length}})
{% endmacro %}

{% macro postgres__synth_column_phone_number_chunk(min, max, pad_length) %}
    LPAD( ({{ synth_distribution_discretize_floor(
        distribution=synth_distribution_continuous_uniform(min=min, max=max)
    ) }})::varchar, {{pad_length}}, '0' )
{% endmacro %}

{% macro snowflake__synth_column_phone_number_chunk(min, max, pad_length) %}
    LPAD( ({{ synth_distribution_discretize_floor(
        distribution=synth_distribution_continuous_uniform(min=min, max=max)
    ) }})::varchar, {{pad_length}}, '0' )
    
{% endmacro%}