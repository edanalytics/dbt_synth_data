{% macro synth_column_lastname(name, distribution="weighted", filter="") -%}
    {{ synth_column_select(
        name=name,
        value_col="name",
        lookup_table="synth_lastnames",
        distribution=distribution,
        weight_col="frequency",
        filter=filter,
        funcs=["INITCAP"]
    ) }}
    {{ return("") }}
{%- endmacro %}