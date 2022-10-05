# dbt_synth_data

This is a dbt package for creating synthetic data. Currently it supports only Snowflake, however we may add Postgresql and/or other backends eventually.

See `models/*` for examples of usage, further documentation will come soon.

All the magic happens in `macros/*`.

## Performance
In Snowflake, using a single Xsmall warehouse:

* Creating `models/dim_student.sql` with 100M rows takes 16 minutes

* Creating 100K `dim_student`s takes 20 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 14 mins.

In Postgres, using an AWS RDS small instance:

* Creating `models/dim_student.sql` with 10M rows didn't finish in 3 hours...

* Creating 100K `dim_student`s takes 43 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 76 minutes.


### Todo:
[ ] implement custom distribution for values