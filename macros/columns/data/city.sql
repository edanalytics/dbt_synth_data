{% macro synth_column_city(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ synth_column_select(
        name=name,
        value_col="name",
        lookup_table="synth_cities",
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}