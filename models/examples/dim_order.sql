-- depends_on: {{ ref('dim_product') }}
{{ config(materialized='table') }}
{{ synth_table(
    rows = 5000,
    columns = [
        synth_column_primary_key(name='k_order'),
        synth_column_select(name="k_product",
            lookup_table="dim_product", value_col="k_product", distribution="weighted", weight_col="popularity"),
        synth_column_distribution(name='status', 
            distribution=synth_distribution(class='discrete', type='probabilities',
                probabilities={"New":0.2, "Shipped":0.5, "Returned":0.2, "Lost":0.1}
            )
        ),
        synth_column_integer(name='num_ordered', min=1, max=10),
    ]
) }}
{{ synth_add_run_end_hook("alter table " + this.database + "." + this.schema + ".dim_product drop column popularity") }}
{{ config(post_hook=synth_get_post_hooks())}}
