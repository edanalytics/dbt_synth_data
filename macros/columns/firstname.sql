{% macro column_firstname(name, distribution="prevalence") -%}
    {{ return(
        dbt_synth.column_select(
            name=name,
            value_col="name",
            lookup_table="synth_firstnames",
            distribution=distribution,
            prevalence_col="frequency",
            filter="",
            funcs=["INITCAP"]
        )
    ) }}
{%- endmacro %}