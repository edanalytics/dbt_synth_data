{{ config(materialized='table') }}

{# This model includes every column type, as it appears in the documentation (a tests suite of sorts): #}

{% set birthyear_grade_correlations = ({
    "columns": {
        "birthyear": [ 2010, 2009, 2008, 2007, 2006, 2005, 2004 ],
        "grade": [ 'Eighth grade', 'Ninth grade', 'Tenth grade', 'Eleventh grade', 'Twelfth grade' ]
    },
    "probabilities": [
        [ 0.02, 0.00, 0.00, 0.00, 0.00 ],
        [ 0.15, 0.02, 0.00, 0.00, 0.00 ],
        [ 0.03, 0.15, 0.02, 0.00, 0.00 ],
        [ 0.00, 0.03, 0.15, 0.02, 0.00 ],
        [ 0.00, 0.00, 0.03, 0.15, 0.02 ],
        [ 0.00, 0.00, 0.00, 0.03, 0.15 ],
        [ 0.00, 0.00, 0.00, 0.00, 0.03 ]
    ]
    })
%}
with
{{ synth_column_primary_key(name='column_pkey') }}
{{ synth_column_boolean(name="column_boolean", pct_true=0.2) }}
{{ synth_column_integer(name="column_integer", min=2000, max=2020) }}
{{ synth_column_integer_sequence(name="column_intseq", step=1, start=1) }}
{{ synth_column_numeric(name="column_numeric", min=1.99, max=999.99, precision=2) }}
{{ synth_column_string(name="column_string", min_length=10, max_length=20) }}
{{ synth_column_date(name="column_date", min='1938-01-01', max='1994-12-31') }}
{{ synth_column_date_sequence(name="column_dateseq", start_date='2020-08-10', step=3)}}
{{ synth_column_value(name="column_value", value='Yes') }}
{{ synth_column_values(name="column_values",
    values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'],
    probabilities=[0.2, 0.3, 0.15, 0.35]
) }}
{{ synth_column_correlation(name='column_correlation1', data=birthyear_grade_correlations, column='birthyear') }}
{{ synth_column_correlation(name='column_correlation2', data=birthyear_grade_correlations, column='grade') }}
{{ synth_column_expression(name='column_expression', expression="lower(column_string)" ) }}
{{ synth_column_mapping(name='column_mapping', expression='column_boolean', mapping=({ true:'Afghanistan', false:'United States' }) ) }}
{{ synth_column_foreign_key(name='column_fkey', model_name='stores', column='k_store') }}
{# synth_column_lookup(name='column_lookup', model_name='synth_countries', value_cols='column_mapping', from_col='name', to_col='population') #}
{{ synth_column_select(name='column_select',
    model_name="synth_words",
    value_cols="word",
    distribution="weighted",
    weight_col="frequency",
    filter="part_of_speech like '%ADJ%'"
) }}
{{ synth_column_city(name='column_city', distribution="weighted", weight_col="population", filter="timezone like 'Europe/%'") }}
{{ synth_column_geo_region(name='column_georegion', distribution="weighted", weight_col="population", filter="country='United States'") }}
{{ synth_column_country(name='column_country', distribution="weighted", weight_col="population", filter="continent='Europe'") }}
{{ synth_column_firstname(name='column_firstname') }}
{{ synth_column_lastname(name='column_lastname') }}
{{ synth_column_word(name='column_word', language_code="en", distribution="weighted", pos=["NOUN", "VERB"]) }}
{{ synth_column_words(name='column_words1', language_code="en", distribution="uniform", n=5) }}
{{ synth_column_words(name='column_words2', language_code="en", distribution="uniform", format_strings=[
    "{ADV} learning for {ADJ} {NOUN}s",
    "{ADV} {VERB} {NOUN} course"
]) }}
{{ synth_column_language(name='column_language', type="name", distribution="weighted") }}
{{ synth_column_address(name='column_address', countries=['United States'],
    parts=['street_address', 'city', 'geo_region', 'country', 'postal_code']) }}
{{ synth_column_phone_number(name="column_phone") }}

{{ synth_table(rows=1000) }}

select * from synth_table