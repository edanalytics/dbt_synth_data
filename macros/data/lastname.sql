{% macro synth_lastname(name, distribution="weighted", filter="") -%}
    {{ return(
        synth_select(
            name=name,
            value_col="name",
            lookup_table="synth_lastnames",
            distribution=distribution,
            weight_col="frequency",
            filter=filter,
            funcs=["INITCAP"]
        )
    ) }}
{%- endmacro %}