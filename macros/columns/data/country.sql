{% macro synth_column_country(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ synth_column_select(
        name=name,
        model_name="synth_countries",
        value_cols="name",
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}