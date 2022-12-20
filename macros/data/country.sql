{% macro synth_country(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ return(
        synth_select(
            name=name,
            value_col="name",
            lookup_table="synth_countries",
            distribution=distribution,
            weight_col=weight_col,
            filter=filter,
        )
    ) }}
{%- endmacro %}