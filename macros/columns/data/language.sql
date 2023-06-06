{% macro synth_column_language(name, type="name", distribution="weighted", weight_col="speakers", filter="") -%}
    {% if type!="name" and type!="code2" and type!="code3" %}
        {{ exceptions.raise_compiler_error("Language column `" ~ name ~ "` must specify `type` as `name`, `code2`, or `code3`.") }}
    {% endif %}
    
    {{ dbt_synth_data.synth_column_select(
        name=name,
        model_name="synth_languages",
        value_cols=type,
        distribution=distribution,
        weight_col=weight_col,
        filter=filter,
    ) }}
    {{ return("") }}
{%- endmacro %}