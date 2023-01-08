{{ config(materialized='table') }}

{{ synth_column_primary_key(name='k_product') }}
{{ synth_column_words(name='product_name', language_code='en', distribution="uniform", format_strings=[
    "{ADJ} {NOUN} Tool",
    "Tool for {VERB} {NOUN}s"
], funcs=["INITCAP"]) }}
{{ synth_column_numeric(name='shipping_weight', min=0.1, max=30.0, precision=1) }}
{{ synth_column_distribution(name='popularity',
    distribution=synth_distribution(class='continuous', type='exponential', lambda=0.05)
) }}

{{ synth_table(rows=50) }}