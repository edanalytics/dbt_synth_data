-- depends_on: {{ ref('dim_product') }}
{{ config(materialized='table') }}
select
        {{ synth_primary_key() }} as k_order,
        {{ synth_select(lookup_table="dim_product", 
            value_col="k_product", distribution="weighted", weight_col="popularity") }} as k_product,
        {{ synth_distribution(class='discrete', type='probabilities',
            probabilities={"New":0.2, "Shipped":0.5, "Returned":0.2, "Lost":0.1}
        ) }} as status,
        {{ synth_integer(min=1, max=10) }} as num_ordered
from {{ synth_table(rows=5000) }}

{{ synth_add_run_end_hook("alter table " + this.database + "." + this.schema + ".dim_product drop column popularity") }}
{{ config(post_hook=synth_get_post_hooks())}}
