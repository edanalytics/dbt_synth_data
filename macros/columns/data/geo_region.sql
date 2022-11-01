{% macro column_geo_region(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ return(column_geo_region_type(name, "name", distribution, weight_col, filter)) }}
{%- endmacro %}

{% macro column_geo_region_abbr(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ return(column_geo_region_abbr(name, "abbr", distribution, weight_col, filter)) }}
{%- endmacro %}

{% macro column_geo_region_type(name, type, distribution, weight_col, filter) -%}
{{ return(
        dbt_synth.column_select(
            name=name,
            value_col=("abbr" if type=="abbr" else "name"),
            lookup_table="synth_geo_regions",
            distribution=distribution,
            weight_col=weight_col,
            filter=filter,
        )
    ) }}
{%- endmacro %}