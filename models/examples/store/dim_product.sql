{{ config(materialized='table') }}
{{ synth_table(
    rows = 50,
    columns = [
        synth_column_primary_key(name='k_product'),
        synth_column_string(name='name', min_length=10, max_length=20),
        synth_column_distribution(name='popularity', 
            distribution=synth_distribution(class='continuous', type='exponential', lambda=0.05)
        ),
    ]
) }}
{{ config(post_hook=synth_get_post_hooks())}}