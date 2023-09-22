-- depends_on: {{ ref('products') }}
{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='k_order') }}
{{ synth_column_select(name='k_product',
    model_name="products", value_cols="k_product", distribution="weighted", weight_col="popularity"
) }}
{{ synth_column_distribution(name="status",
    distribution=synth_distribution(class='discrete', type='probabilities',
        probabilities={"New":0.2, "Shipped":0.5, "Returned":0.2, "Lost":0.1}
    )
) }}
{{ synth_column_integer(name='num_ordered', min=1, max=10) }}
{%- if target.type!='sqlite' -%}
{# SQLite doesn't support dropping columns :( #}
{{ synth_add_cleanup_hook("alter table " + this.schema + ".products drop column popularity") }}
{% endif %}
{{ synth_table(rows=1000) }}

select * from synth_table