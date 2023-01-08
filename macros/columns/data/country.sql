{% macro synth_column_country(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ synth_column_select(
        name=name,
        value_col="name",
        lookup_table="synth_countries",
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}