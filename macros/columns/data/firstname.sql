{% macro synth_column_firstname(name, distribution="weighted", filter="") -%}
    {{ return(
        synth_column_select(
            name=name,
            value_col="name",
            lookup_table="synth_firstnames",
            distribution=distribution,
            weight_col="frequency",
            filter=filter,
            funcs=["INITCAP"]
        )
    ) }}
{%- endmacro %}