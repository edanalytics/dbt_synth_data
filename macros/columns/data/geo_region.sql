{% macro column_geo_region(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ return(
        dbt_synth.column_select(
            name=name,
            value_col="name",
            lookup_table="synth_geo_regions",
            distribution=distribution,
            weight_col=weight_col,
            filter=filter,
        )
    ) }}
{%- endmacro %}