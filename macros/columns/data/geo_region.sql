{% macro synth_column_geo_region(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ dbt_synth_data.synth_column_geo_region_type(name, "name", distribution, weight_col, filter) }}
{%- endmacro %}

{% macro synth_column_geo_region_abbr(name, distribution="weighted", weight_col="population", filter="") -%}
    {{ dbt_synth_data.synth_column_geo_region_type(name, "abbr", distribution, weight_col, filter) }}
{%- endmacro %}

{% macro synth_column_geo_region_type(name, type, distribution, weight_col, filter) -%}
    {{ dbt_synth_data.synth_column_select(
        name=name,
        model_name="synth_geo_regions",
        value_cols=("abbr" if type=="abbr" else "name"),
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}