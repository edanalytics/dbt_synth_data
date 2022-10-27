# dbt_synth_data

This is a `dbt` package for creating synthetic data. Currently it supports Snowflake and Postgresql. Other backends may be added eventually.

See `models/*` for examples of usage, further documentation will come soon.

`seeds/*` contains various files which are used to generate realistic random values of various types.

All the magic happens in `macros/*`.



## Architecture
Tables are created using Snowflake's `generator()` or Postgres' `generate_series()`. While `select`ing `RANDOM()` for a column inside a `generate`d table results in a random value for each row, unfortunately, due to how database engines / optimizers are designed, *expressions based on or derived from* a `RANDOM()` value are "optimized" so that <ins>every row has the same value</ins>. Therefore this package uses a (slower) multi-step process to generate distinct values for each row:

1. an intermediate column is added to the table containing a `RANDOM()` number
1. the table is `update`d, with a new column being added with values populated based on the `RANDOM()` value from the intermediate column
1. finally, the table is "cleaned up" by removing the intermediate column (and any other temporary columns that were created to build up a more complex value)

These steps are handled using `dbt`'s [post hooks](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook) feature, which is why you *must* include the following at the bottom of every model you build with this package:
```
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
Hook queries (and other package data) are stored in the `dbt` [`builtins` object](https://docs.getdbt.com/reference/dbt-jinja-functions/builtins) during parse/run time, as this is one of few dbt objects that persist and are scoped across `macro`s.



## Simple Example
Consider the example model `orders.sql` below:
```sql
-- depends_on: {{ ref('products') }}
{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = 5000,
    columns = [
        dbt_synth.column_primary_key(name='order_id'),
        dbt_synth.column_foreign_key(name='product_id', table='products', column='product_id'),
        dbt_synth.column_values(name='status', values=["New", "Shipped", "Returned", "Lost"], weights=[0.2, 0.5, 0.2, 0.1]),
        dbt_synth.column_integer(name='num_ordered', min=1, max=10, distribution='uniform'),
    ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
The model begins with a [dependency hint to dbt](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies) for another model `products`. The model is also materialized as a table, to persist the new data in the database. Next, a new table is created with 5000 rows and several columns:
* `order_id` is the primary key on the table - it wil contain a unique hash value per row
* `product_id` is a foreign key to the `products` table - values in this column will be uniformly-distributed, valid primary keys of the `products` table
* each order has a `status` with several possible values - a `weights` array is also supplied, which will determine the prevalence of each `status` value
* `num_ordered` is the count of how many of the product were ordered, a uniformly-distributed integer from 1-10

Finally, the model adds the post hooks required to finish building and clean up the new table.



## Column types
This package provides the following column types:


### Base column types

<details>
<summary><code>boolean</code></summary>

Generates boolean values.
```python
    dbt_synth.column_boolean(name='is_complete', pct_true=0.2),
```
</details>

<details>
<summary><code>integer</code></summary>

Generates integer values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>integer sequence</code></summary>

Generates an integer sequence.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>numeric</code></summary>

Generates numeric values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>date</code></summary>

Generates date values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>date sequence</code></summary>

Generates a date sequence values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>primary key</code></summary>

Generates a primary key column.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>value</code></summary>

Generates a (single, static) value for every row.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>values</code></summary>

Generates values from a list of possible values, with optional weighting.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>mapping</code></summary>

Generates values by mapping from an existing column or expresion to values in a dictionary.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>expression</code></summary>

Generates values based on an expression (which may refer to other columns, or invoke SQL functions).
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>


### Reference column types

<details>
<summary><code>foreign key</code></summary>

Generates values that are a primary key of another table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>lookup</code></summary>

Generates values based on looking up values from one column in another table..
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>select</code></summary>

Generates values by selecting them from another table, optionally weighted using a specified column of the other table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>


### Data column types
Data column types may all specify a `filter`, which is a SQL `where` expression narrowing down the pool of data values that will be used. They may also specify `distribution="weighted"` and `weight_col="population"` (or similar) to skew value distributions.

<details>
<summary><code>city</code></summary>

Generates a city, selected from the `synth_cities` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>country</code></summary>

Generates a country, selected from the `synth_countries` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>geo region</code></summary>

Generates a geo region (state, province, or territory), selected from the `synth_geo_regions` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>first name</code></summary>

Generates a first name, selected from the `synth_firstnames` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>last name</code></summary>

Generates a last name, selected from the `synth_lastnames` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>word</code></summary>

Generates a single word, selected from the `synth_words` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>words</code></summary>

Generates several words, selected from the `synth_words` seed table.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>


### Composite column types
Composite column types put together several other column types into a more complex data type.

<details>
<summary><code>address</code></summary>

Generates an address, based on `city`, `geo region`, `country`, `words`, and other values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>


### Statistical column types
Statistical column types can be used to make advanced statistical relationships between tables and columns.

<details>
<summary><code>correlation</code></summary>

Generates two or more columns with correlated values.
```python
    {% set birthyear_grade_correlations = ({
        "randseed": dbt_synth.get_randseed(),
        "columns": {
            "birth_year": [ 2010, 2009, 2008, 2007, 2006, 2005, 2004 ],
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
    ...
    {{ dbt_synth.table(
        rows = var('num_students'),
        columns = [
            dbt_synth.column_primary_key(name='k_student'),
            dbt_synth.column_correlation(data=birthyear_grade_correlations, column='birth_year'),
            dbt_synth.column_correlation(data=birthyear_grade_correlations, column='grade'),
            ...
        ]
    ) }}
    {{ config(post_hook=dbt_synth.get_post_hooks())}}
```
</details>


## Performance
In Snowflake, using a single Xsmall warehouse:

* Creating `models/dim_student.sql` with 100M rows takes 16 minutes

* Creating 100K `dim_student`s takes 20 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 14 mins.

In Postgres, using an AWS RDS small instance:

* Creating `models/dim_student.sql` with 10M rows didn't finish in 3 hours...

* Creating 100K `dim_student`s takes 43 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 76 minutes.


