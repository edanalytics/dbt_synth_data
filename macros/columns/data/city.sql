{% macro synth_column_city(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ dbt_synth_data.synth_column_select(
        name=name,
        model_name="synth_cities",
        value_cols="name",
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}