{% macro synth_column_foreign_key(name, model_name, column, distribution="uniform", weight_col="", filter="") -%}
    {{ synth_column_select(name, model_name, value_cols=[column], distribution=distribution, weight_col=weightcol, filter=filter) or "" }}
{%- endmacro %}