{% macro synth_column_language(name, type="name", distribution="weighted", weight_col="speakers", filter="") -%}
    {% if type!="name" and type!="code2" and type!="code3" %}
        {{ exceptions.raise_compiler_error("Language column `" ~ name ~ "` must specify `type` as `name`, `code2`, or `code3`.") }}
    {% endif %}
    {{ return(
        synth_column_select(
            name=name,
            value_col=type,
            lookup_table="synth_languages",
            distribution=distribution,
            weight_col=weight_col,
            filter=filter,
        )
    ) }}
{%- endmacro %}