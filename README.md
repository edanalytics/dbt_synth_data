# dbt_synth_data

This is a dbt package for creating synthetic data. Currently it supports only Snowflake, however we may add Postgresql and/or other backends eventually.

See `models/*` for examples of usage, further documentation will come soon.

## Performance
Creating `models/dim_student.sql` with 100M rows takes 16 minutes. Creating 100K `dim_student`s, 100K `dim_guardian`s, and 200K `fct_family_relationship`s takes around 14 minutes. (This is all with a single Xsmall warehouse.)