{% macro synth_column_firstname(name, distribution="weighted", filter="") -%}
    {{ dbt_synth_data.synth_column_select(
        name=name+'__int',
        model_name="synth_firstnames",
        value_cols="name",
        distribution=distribution,
        weight_col="frequency",
        filter=filter
    ) }}
    {# Wrap name with INITCAP() to capitalize name: #}
    {{ dbt_synth_data.synth_column_expression(name=name, expression=dbt_synth_data.synth_initcap(name+"__int") ) }}
    {{ dbt_synth_data.synth_remove('final_fields', name+'__int') }}
    {{ return("") }}
{%- endmacro %}