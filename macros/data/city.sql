{% macro synth_city(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ return(
        synth_select(
            name=name,
            value_col="name",
            lookup_table="synth_cities",
            distribution=distribution,
            weight_col=weight_col,
            filter=filter,
        )
    ) }}
{%- endmacro %}