{{ config(materialized='table') }}
select
    {{ synth_primary_key() }} as k_product,
    {{ synth_string(min_length=10, max_length=20) }} as name,
    {{ synth_distribution( 
        distribution=synth_distribution(class='continuous', type='exponential', lambda=0.05)
    ) }} as popularity
from {{ synth_table(rows=50) }}

{{ config(post_hook=synth_get_post_hooks())}}