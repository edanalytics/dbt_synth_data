<!-- Logo/image -->
![dbt_synth_data](assets/dalle-mini_small_laptop_on_a_white_background_showing_fake_data_in_a_spreadsheet.png)

This is a [`dbt`](https://www.getdbt.com/) package for creating synthetic data. Currently it supports [Snowflake](https://www.snowflake.com/en/), [Postgres](https://www.postgresql.org/), and [SQLite](https://www.sqlite.org/index.html) (with the [`stats` extension](https://docs.getdbt.com/reference/warehouse-setups/sqlite-setup#sqlite-extensions)). Other backends may be added eventually.

All the magic happens in `macros/*`.

# Table of Contents  
* [About](#about)
* [Installation](#installation)
* [Architecture](#architecture)
* [Simple example](#simple-example)
* [Distributions](#distributions)
* [Column types](#column-types)
* [Advanced usage](#advanced-usage)
* [Datasets](#datasets)
* [Performance](#performance)
* [Changelog](#changelog)
* [Contributing](#contributing)
* [License](#license)


# About
Robert Fehrmann (CTO at Snowflake) has a couple good blog posts about [generating random integers and strings](https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-1/) or [dates and times](https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-2/) in Snowflake, which the [base column types](#base-column-types) in this package emulate.

However, creating more realistic synthetic data requires more complex data [column types](#column-types), advanced random [distributions](#distributions), supporting [datasets](#datasets), and [references](#reference-column-types) to other models.

`dbt_synth_data` provides many macros to facilitate building out realistic synthetic data. It builds up a series of CTEs and joins from a base of randomly-generated values - see [Architecture](#architecture) for details.

## Philosophy
There are generally two approaches to creating synthetic or "fake" data:
1. start with real data, de-identify it, and possibly "fuzz" or "jitter" some values
1. start with nothing and synthesize data by describing it, including and distributions and correlations in the data

(Recent research has proposed a hybrid approach, where a "nearby" or similar synthetic data row (2) is selected for each row of a real, de-dentified row (1)... but adequately defining "nearby" is difficult.)

Approach (1) can be dangerous, suscpetible to re-identification and other adversarial attacks. `dbt_synth_data` implements approach (2) *only*.

## Intended Use
Synthetic data generated with `dbt_synth_data` can be useful for testing user interfaces, demoing applications, performance-tuning operational systems, preparing training and other materials with realistic data, and potentially other uses.

## Limitations
The synthetic data created using `dbt_synth_data` should not be mistaken as being fully realistic, reflecting all correlations that may be present in the real world. Therefore **please do not use data generated using this package to train ML models!**


# Installation
1. add `dbt_synth_data` to your `packages.yml`
1. run `dbt deps`
1. run `dbt seed`
1. add `"dbt_packages/dbt_synth/macros"` to your `dbt_project.yml`'s `macro-paths`
1. build your synthetic models as documented below
1. `dbt run`


# Architecture

CTEs, joins, and fields defined by `synth_column_*()` are temporarily stored in `dbt`'s [`target` object](https://docs.getdbt.com/reference/dbt-jinja-functions/target) during parse/run time, as this is one of few dbt objects that persist and are scoped across `macro`s. Finally, `synth_table()` stitches everything together into query of the general form
```sql
-- [CTEs as required for selecting seed data or values from other models]
base as (
    select
        -- base CTE includes a row_number, which facilitates generating integer or date sequences, primary keys, and more
        row_number() over (order by 1) as __row_number
    from table(generator( rowcount => [rows] )) -- snowflake
    -- from generate_series( 1, [rows] ) as s(idx) -- postgres, sqlite
),
join0 as (
     select
        base.__row_number,
        -- randomness source fields, such as
        UNIFORM(0::float, 1::float, RANDOM()) as field1__rand, -- snowflake
        -- RANDOM() as field1__rand, -- postgres
        -- [similar *__rand fields for other columns as required]
    from base
),
-- arbitrarily many further joins to the CTEs defined above
joinN as (
    select
        join[N-1].*, -- all fields from prior joins, plus:
        CTEx.field2,
        CTEx.field3
    from join[N-1]
        left join CTEx on ... -- something involving join[N-1].*__rand
),
synth_table as (
    select
        field1,
        field2,
        field3
        -- only the fields we actually want to keep in the final table
        -- (intermediate fields, including *__rand, are dropped)
    from joinN
)
```


# Simple example
Consider the example model `orders.sql` below:
```sql
-- depends_on: {{ ref('products') }}
with
{{ synth_column_primary_key(name='order_id') }}
{{ synth_column_foreign_key(name='product_id', table='products', column='product_id') }}
{{ synth_column_distribution(name='status', 
    distribution=synth_distribution(class='discrete', type='probabilities',
        probabilities={"New":0.2, "Shipped":0.5, "Returned":0.2, "Lost":0.1}
    )
) }}
{{ synth_column_integer(name='num_ordered', min=1, max=10) }}
{{ synth_table(rows = 5000) }}
select * from synth_table
```
The model begins with a [dependency hint to dbt](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies) for another model `products`. Next, we define the columns we want in the table, including:
* `order_id` is the primary key on the table - it wil contain a unique hash value per row
* `product_id` is a foreign key to the `products` table - values in this column will be uniformly-distributed, valid primary keys of the `products` table
* each order has a `status` with several possible values, whose prevalence/likelihoods are given by a discrete probability distribution
* `num_ordered` is the count of how many of the product were ordered, a uniformly-distributed integer from 1-10

Finally, we materialize the table with 5000 rows of synthetic data and select the result.

Note that the user must provide the opening `with` for CTEs and a final `select * from synth_table` - this allows flexibility to add your own CTEs at the top or bottom of the model, as well as arbitrary post-processing of columns produced by `dbt_synth_data` - see [Advanced Usage](#advanced-usage) for more details.



# Distributions
This package provides the following distributions:

### Continuous Distributions
<details>
<summary><code>uniform</code></summary>

Generates [uniformly-distributed](https://en.wikipedia.org/wiki/Continuous_uniform_distribution) real numbers.
```python
    synth_distribution_continuous_uniform(min=0.6, max=7.9)
```
Default `min` is `0.0`. Default `max` is `1.0`. `min` and `max` are inclusive.

![Example of continuous uniform distribution](/assets/continuous_uniform.png)
**Above:** Histogram of a continuous uniform distribution (1M values).
</details>

<details>
<summary><code>normal</code></summary>

Generates [normally-distributed (Gaussian)](https://en.wikipedia.org/wiki/Normal_distribution) real numbers.
```python
    synth_distribution_continuous_normal(mean=5, stddev=0.5)
```
Default `mean` is `0.0`, default `stddev` is `1.0`.

![Example of continuous uniform distribution](/assets/continuous_normal.png)
**Above:** Histogram of a continuous normal distribution (1M values).
</details>

<details>
<summary><code>exponential</code></summary>

Generates [exponentially-distributed](https://en.wikipedia.org/wiki/Exponential_distribution) real numbers.
```python
    synth_distribution_continuous_exponential(lambda=5.0)
```
Default `lambda` is `1.0`.

![Example of continuous uniform distribution](/assets/continuous_exponential.png)
**Above:** Histogram of a continuous exponential distribution (1M values).
</details>


### Discrete Distributions
<details>
<summary><code>bernoulli</code></summary>

Generates integers (`0` and `1`) according to a [Bernoulli distribution](https://en.wikipedia.org/wiki/Bernoulli_distribution).
```python
    synth_distribution_discrete_bernoulli(p=0.3)
```
Default `p` is `0.5`.
</details>

<details>
<summary><code>binomial</code></summary>

Generates integers according to a [Binomial distribution](https://en.wikipedia.org/wiki/Binomial_distribution).
```python
    synth_distribution_discrete_binomial(n=100, p=0.3)
```
Default `n` is `10`, default `p` is `0.5`.

Note that the implementation is approximate, based on a normal distribution (see [here](https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation)). For small `n` or `p` near `0` or `1`, normally-distributed values may be `< 0` or `> n`, which is impossible in a binomial distribution. These long-tail values are rare, so, while not completely correct, we use
* `abs()` to shift those `< 0`
* `mod(..., n+1)` to shift those `> n`

This may artificially increase small values. However, the approximation is close if `n*p` and `n*(1-p)` are large.
</details>

<details>
<summary><code>weights</code></summary>

Generates discrete values according to a user-defined probability set.
```python
    synth_distribution_discrete_weights(values=[1,3,5,7,9], weights=[1,1,6,3,1])
```
`values` is a required list of strings, floats, or integers; it has no default.

`weights` is an optional list of integers. It's length should be the same the length of `values`. If `weights` is omitted, each of the `values` will be equally likely. Otherwise, the integers indicate likelihood; in the example above, the value `5` will be about six times as prevalent as the value `9`.

Avoid using `weights` with a large sum; this will generate long `case` statements which can run slowly.
</details>

<details>
<summary><code>probabilities</code></summary>

Generates discrete values according to a user-defined probability set.
```python
    synth_distribution_discrete_probabilities(probabilities={"1":0.15, "5":0.5, "8": 0.35})
```
`probabilities` is required and has no default. It may be
* a list (array) such as `[0.05, 0.8, 0.15]`, in which case the (zero-based) indices are the integer values generated
* or a dictionary (key-value) structure such as `{ "1":0.05, "3":0.8, "7":0.15 }` with integer keys (specified as strings in order to be valud JSON), in which case the keys are the integers generated

You may actually specify string or float keys in your `probabilities` dict to generate those values instead of integers, however you must specify the additional parameter `keys_type="varchar"` (or similar) so the the value types are correct. For example:
```python
    synth_distributions_discrete_probabilities(probabilities={"cat":0.3, "dog":0.5, "parrot":0.2}, keys_type="varchar")
```

`probabilities` must sum to `1.0`.

Note that, because values are generated using `case` statements, supplying `probabilities` with many digits of specificity will run slower, i.e., `probabilities=[0.1, 0.3, 0.6]` will generate something like
```sql
case floor( 10*random() )
    when 0 then 0
    when 1 then 1
    when 2 then 1
    when 3 then 1
    when 4 then 2
    when 5 then 2
    when 6 then 2
    when 7 then 2
    when 8 then 2
    when 9 then 2
end
```
while `probabilities=[0.101, 0.301, 0.598]` will generate something like
```sql
case floor( 1000*random() )
    when 0 then 0
    when ...
    when 99 then 0
    when 100 then 0
    when 101 then 1
    when ...
    when 400 then 1
    when 401 then 1
    when 402 then 1
    when 403 then 2
    ...
    when 998 then 2
    when 999 then 2
end
```
which takes longer for the database engine to evaluate.

Really you should avoid specifiying `probabilities` of more than 4 digits at the most.
</details>

## Discretizing Continuous Distributions

Any of the [continuous distributions](#continuous-distributions) listed above can be made discrete using the following mechanisms:

<details>
<summary><code>discretize_floor</code></summary>

Converts values from [continuous distributions](#continuous-distributions) to (discrete) integers by applying the `floor()` function.
```python
    synth_distribution_discretize_floor(
        distribution=synth_distribution(class='...', type='...', ...),
    )
```
</details>

<details>
<summary><code>discretize_ceil</code></summary>

Converts values from [continuous distributions](#continuous-distributions) to (discrete) integers by applying the `ceil()` function.
```python
    synth_distribution_discretize_ceil(
        distribution=synth_distribution(class='...', type='...', ...),
    )
```
</details>

<details>
<summary><code>discretize_round</code></summary>

Converts values from [continuous distributions](#continuous-distributions) to discrete values by applying the `round()` function.
```python
    synth_distribution_discretize_round(
        distribution=synth_distribution(class='...', type='...', ...),
        precision=0
    )
```
`precision` indicates the number of digits to round to.
</details>

<details>
<summary><code>discretize_width_bucket</code></summary>

**Note** that SQLite doesn't support `width_bucket()`; you will get an error if you try to use this function on SQLite.

Converts values from [continuous distributions](#continuous-distributions) to discrete values by bucketing them. Buckets are specified by `from` and `to` bounds and either `count` (the number of buckets) or `size` (the target bucket size).

For some distributions (like `uniform`), the bounds may be strict - values outside the bounds are impossible. For other distributions (like exponential), specifying strict `from` and `to` bounds may be difficult. For this reason, if `strict_bounds=False`, the first bucket (index `0`) will represent values below `from`. Likewise the last bucket (index `count`) will represent values above `to`. (`strict_bounds` defaults to `True`.) It is up to you to chose reasonable and useful `from` and `to` bounds for discretization.

`labels` may be 
* unspecified, in which case values will be mapped to the (1-based) bucket index (0-based if `strict_bounds=False`)
* the string "lower_bound", in which case values will be mapped to the lower bound of the bucket (or `-Infinity` for the first bucket, if `strict_bounds=False`)
* the string "upper_bound", in which case values will be mapped to the upper bound of the bucket (or `+Infinity` for the last bucket, if `strict_bounds=False`)
* the string "bucket_range", in which case values will be mapped to a string of the format "[lower_bound] - [upper_bound]" for each bucket (`lower_bound` may be `-Infinity` and `upper_bound` may be `Infinity` if `strict_bounds=False`)
  * optionally specify the `bucket_range_separator` string that separates the upper and lower bucket bounds (default is " - ")
* the string "bucket_average", in which case values will be mapped to bucket middle or average (or `from` for the first bucket and `to` for the last bucket, if `strict_bounds=False`)
* a list of (string or numeric) bucket labels (the list must be equal in length to the number of buckets)

For all but the last option, you may optionally specify a `label_precision`, which is the number of digits bounds get rounded to. (Default is `4`.)

**Examples:**
```python
    synth_distribution_discretize_width_bucket(
        distribution=synth_distribution(class='...', type='...', ...),
        from=0.0, to=1.5, count=20, labels='lower_bound'
    )
```
```python
    synth_distribution_discretize_width_bucket(
        distribution=synth_distribution(class='...', type='...', ...),
        from=0.0, to=1.5, size=0.1
    )
```
```python
    synth_distribution_discretize_width_bucket(
        distribution=synth_distribution(class='...', type='...', ...),
        from=0.0, to=1.5, count=5, strict_bounds=False,
        labels=['< 0.0', '0.0 to 0.5', '0.5 to 1.0', '1.0 to 1.5', '> 1.5']
    )
```
</details>


## Constructing Complex Distributions
This package provides the following mechanisms for composing several distributions:

<details>
<summary><code>union</code></summary>

Generates values from several distributions with optional `weights`. If `weights` is omitted, each distribution is equally likely.
```python
    {{ synth_distribution_union(
        synth_distribution(class='...', type='...', ...),
        synth_distribution(class='...', type='...', ...),
        weights=[1, 2, ...]
    ) }}
```
Up to 10 distributions may be unioned. (Compose the macro to union more.)

For example, make a [bimodal distribution](https://en.wikipedia.org/wiki/Multimodal_distribution) as follows:
```python
{{ synth_table(
  rows = 100000,
  columns = [
    synth_column_distribution(name="continuous_bimodal",
        distribution=synth_distribution_union(
            synth_distribution(class='continuous', type='normal', mean=5.0, stddev=1.0),
            synth_distribution(class='continuous', type='normal', mean=8.0, stddev=1.0),
            weights=[1, 2]
        )
    ),
  ]
) }}
{{ config(post_hook=synth_get_post_hooks())}}
```
Here, values will come from the union of the two normal distributions, with the second distribution twice as likely as the first.

![Example of continuous bimodal distribution](/assets/continuous_bimodal.png)
**Above:** Histogram of a continuous bimodal distribution composed of the union of two normal distributions (1M values).

![Example of union of continuous normal distributions](/assets/continuous_union_normals.png)
**Above:** Histogram of the union of three continuous normal distributions (1M values).
</details>

<details>
<summary><code>average</code></summary>

Generates values from the (optionally weighted) average of values from several distributions. If `weights` is omitted, each distribution contributes equally to the average.
```python
    {{ synth_distribution_average(
        synth_distribution(class='...', type='...', ...),
        synth_distribution(class='...', type='...', ...),
        weights=[1, 2, ...]
    ) }}
```
Up to 10 distributions may be averaged. (Compose the macro to average more.)

![Example of continuous average distribution](/assets/continuous_average_exponential_normal.png)
**Above:** Histogram of a continuous average distribution composed of a normal and an exponential distribution (1M values).
</details>



# Column types
This package provides the following data types:


## Basic column types
Basic column types, which are quite performant.

<details>
<summary><code>boolean</code></summary>

Generates boolean values.
```python
{{ synth_column_boolean(name="is_complete", pct_true=0.2) }}
```
</details>

<details>
<summary><code>integer</code></summary>

Generates integer values.

For uniformly-distributed values, simply specify `min` and `max`:
```python
{{ synth_column_integer(name="event_year", min=2000, max=2020) }}
```

For non-uniformly-distributed values, specify a discretized distribution:
```python
{{ synth_column_distribution(name="event_year",
    distribution=synth_distribution_discretize_floor(
        distribution=synth_distribution_continuous_normal(mean=2010, stddev=2.5,)
    )
) }}
```
</details>

<details>
<summary><code>numeric</code></summary>

Generates numeric values.
```python
{{ synth_column_numeric(name="price", min=1.99, max=999.99, precision=2) }}
```

For non-uniformly-distributed values, specify a distribution rounded to the desired `precision`:
```python
{{ synth_column_distribution(name="event_year",
    distribution=synth_distribution_discretize_round(
        distribution=synth_distribution_continuous_normal(mean=500, stddev=180,),
        precision=2
    )
) }}
```
</details>

<details>
<summary><code>string</code></summary>

Generates random strings.
```python
{{ synth_column_string(name="password", min_length=10, max_length=20) }}
```
String characters will include `A-Z`, `a-z`, and `0-9`.
</details>

<details>
<summary><code>date</code></summary>

Generates date values.
```python
{{ synth_column_date(name="birth_date", min='1938-01-01', max='1994-12-31') }}
```
</details>

<details>
<summary><code>integer sequence</code></summary>

Generates an integer sequence (value is incremented at each row).
```python
{{ synth_column_integer_sequence(name="day_of_year", step=1, start=1) }}
```
</details>

<details>
<summary><code>date sequence</code></summary>

Generates a date sequence.
```python
{{ synth_column_date_sequence(name="calendar_date", start_date='2020-08-10', step=3)}}
```
</details>

<details>
<summary><code>primary key</code></summary>

Generates a primary key column. (Values are distinct hash strings.)
```python
{{ synth_column_primary_key(name="product_id") }}
```
</details>

<details>
<summary><code>value</code></summary>

Generates the same (single, static) value for every row.
```python
{{ synth_column_value(name="is_registered", value='Yes') }}
```
</details>

<details>
<summary><code>values</code></summary>

Generates values from a list of possible values, with optional probability weighting.
```python
{{ synth_column_values(name="academic_subject",
    values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'],
    probabilities=[0.2, 0.3, 0.15, 0.35]
) }}
```
If `probabilities` are omitted, every value is equally likely.

(Uses `synth_distribution_discrete_probabilities()` under the hood.)
</details>

<details>
<summary><code>expression</code></summary>

Generates values based on an expression (which may refer to other columns, or invoke SQL functions).
```python
{{ synth_column_expression(name='week_of_calendar_year',
    expression="DATE_PART('week', calendar_date)::int"
) }}
```
</details>

<details>
<summary><code>mapping</code></summary>

Generates values by mapping from an `expression` involving existing columns to values in a dictionary.
```python
{{ synth_column_mapping(name='day_type', expression='is_school_day',
    mapping=({ true:'Instructional day', false:'Non-instructional day' })
) }}
```
</details>


## Statistical column types
Statistical column types can be used to make advanced statistical relationships between tables and columns.

<details>
<summary><code>correlation</code></summary>

Generates two or more columns with correlated values.
```python
{% set birthyear_grade_correlations = ({
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
with
{{ synth_column_primary_key(name='k_student') }}
{{ synth_column_correlation(data=birthyear_grade_correlations, column='birth_year') }}
{{ synth_column_correlation(data=birthyear_grade_correlations, column='grade') }}
{{ synth_table(rows=var('num_students')) }}
select * from synth_table
```
To created correlated columns, you must specify a `data` object representing the correlation, which contains
* `columns` is a list of column names and possible values.
* `probabilities` is a hypercube, with dimension equal to the number of `columns`, the elements of which sum to `1.0`, indicating the probability of each possible combination of values for the `columns`. The outermost elements of the `probabilities` hypercube corresond to the values of the first column; the innermost elements of the hypercube correspond to the values of the last column. Each dimension of the hypercube must have the same size as the number of values for its corresponding column.

Constructing a `probabilities` hypercube of dimension more than two or three can be difficult &ndash; we recommend adding (temporary) comments and using indentation to keep track of columns, values, and dimensions.
</details>


## Reference column types
Column types which reference values in other columns of the same or different table.

<details>
<summary><code>foreign key</code></summary>

Generates values that are a primary key of another table.
```python
{{ synth_column_foreign_key(name='product_id', model_name='products', column='id') }}
```
</details>

<details>
<summary><code>lookup</code></summary>

Generates values based on looking up values from one column in another table..
```python
{{ synth_column_lookup(name='gender', model_name='synth_firstnames', value_cols='first_name', from_col='name', to_col='gender') }}
```
</details>

<details>
<summary><code>select</code></summary>

Generates values by selecting them from another table, optionally weighted using a specified column of the other table.
```python
{{ synth_column_select(
    name='random_ajective',
    model_name="synth_words",
    value_cols="word",
    distribution="weighted",
    weight_col="prevalence",
    filter="part_of_speech like '%ADJ%'"
) }}
```
The above will generate randomly-chosen adjectives (based on the specified `filter`), weighted by prevalence.
</details>


## Advanced column types
Advanced column types use real-world data which is maintained in the `seeds/` directory. Some effort has been made to make these data sets
* **Generalized**, rather than specific to a particular country, region, language, etc. For example, the *words* dictionary contains common words from many common languages, not just English.
* **Statistically rich**, with associated metadata which makes the data more useful by capturing various distributions embedded in the data. For example, the *countries* list includes the (approximate) population and land area of each country, which facilitates generating country lists weighted according to these features. Likewise, the *cities* list has the latitude and longitude coordinates for each city, which facilitates generating fairly realistic coordinates for synthetic addresses.

Advanced column types may all specify a `distribution="weighted"` and `weight_col="population"` (or similar) to skew value distributions. They may also specify `filter`, which is a SQL `where` expression narrowing down the pool of data values that will be used. Finally, they may specify a `filter_expressions` dictionary which allows dynamic filtering based on expressions which can involve row values from other columns. If, for example, we are creating a country column and pass `filter_expressions` as
```json
{
    "country_name": "INITCAP(my_country_col)",
    "geo_region_code": "my_geo_region_col"
}
```
then a `WHERE` clause like this will result:
```sql
synth_countries.country_name=INITCAP(my_country_col)
AND synth_countries.geo_region_code=my_geo_region_col
```
(`filter_expressions` and `filter` - if any - are combined via logical `AND`.)

<details>
<summary><code>city</code></summary>

Generates a city, selected from the `synth_cities` seed table.
```python
{{ synth_column_city(name='city', distribution="weighted", weight_col="population", filter="timezone like 'Europe/%'") }}
```
</details>

<details>
<summary><code>geo region</code></summary>

Generates a geo region (state, province, or territory), selected from the `synth_geo_regions` seed table.
```python
{{ synth_column_geo_region(name='geo_region', distribution="weighted", weight_col="population", filter="country='United States'") }}
```
</details>

<details>
<summary><code>country</code></summary>

Generates a country, selected from the `synth_countries` seed table.
```python
{{ synth_column_country(name='country', distribution="weighted", weight_col="population", filter="continent='Europe'") }}
```
</details>

<details>
<summary><code>first name</code></summary>

Generates a first name, selected from the `synth_firstnames` seed table.
```python
{{ synth_column_firstname(name='first_name', filter="gender='Male'") }}
```
</details>

<details>
<summary><code>last name</code></summary>

Generates a last name, selected from the `synth_lastnames` seed table.
```python
{{ synth_column_lastname(name='last_name') }}
```
</details>

<details>
<summary><code>word</code></summary>

Generates a single word, selected from the `synth_words` seed table.
```python
{{ synth_column_word(name='random_word', language_code="en", distribution="weighted", pos=["NOUN", "VERB"], filter="LENGTH(word)>3") }}
```
The above generates a randomly-selected English noun or verb, weighted according to frequency, of at least four characters.

Rather than `language_code` you may specify `language` (such as `language="English"`), but a language *must* be specified with one of these parameters. See [Words (Datasets)](#words) for a list of supported languages and parts of speech.
</details>

<details>
<summary><code>words</code></summary>

Generates several words, selected from the `synth_words` seed table.
```python
{{ synth_column_words(name='random_phrase', language_code="en", distribution="uniform", n=5) }}
```
The above generates a random string of five words, uniformly districbuted, with the first letter of each word capitalized.

Alternatively, you can generate words using format strings, for example
```python
{{ synth_column_words(name='course_title', language_code="en", distribution="uniform", format_strings=[
    "{ADV} learning for {ADJ} {NOUN}s",
    "{ADV} {VERB} {NOUN} course"
]) }}
```
This will generate sets of words according to one of the format strings you specify.

Note that this data type is constructed by separately generating a single word `n` times (or, for `format_string`s, the set union of all word instances from any `format_string`) and then concatenating them together, which can be slow if `n` is large (or you have many tokens in your `format_string`s).

Rather than `language_code` you may specify `language` (such as `language="English"`), but a language *must* be specified with one of these parameters. See [Words (Data Sets)](#words) for a list of supported languages and parts of speech.
</details>

<details>
<summary><code>language</code></summary>

Generates a spoken language (name or 2- or 3-letter code), selected from the `synth_languages` seed table.
```python
{{ synth_column_language(name='random_lang', type="name", distribution="weighted") }}
```
The optional `type` (which defaults to `name`) can take values `name` (the full English name of the language, e.g. *Spanish*), `code2` (the ISO 693-2 two-letter code for the langage, e.g. `es`), or `code3` (the ISO 693-3 three-letter code for the language, e.g. `spa`).
</details>


## Composite column types
Composite column types put together several other column types into a more complex data type.

<details>
<summary><code>address</code></summary>

Generates an address, based on `city`, `geo region`, `country`, `words`, and other values.

Creating a column `myaddress` using this macro will also create intermediate columns `myaddress__street_address`, `myaddress__city`, `myaddress__geo_region`, and `myaddress__postal_code` (or whatever `parts` you specify). You can then `add_update_hook()`s that reference these intermediate columns if you'd like. For example:
```python
{{ synth_column_primary_key(name='k_person') }}
{{ synth_column_firstname(name='first_name') }}
{{ synth_column_lastname(name='last_name') }}
{{ synth_column_address(name='home_address', countries=['United States'],
    parts=['street_address', 'city', 'geo_region', 'country', 'postal_code']) }}
{{ synth_column_expression(name='home_address_street', expression="home_address__street_address") }}
{{ synth_column_expression(name='home_address_city', expression="home_address__city") }}
{{ synth_column_expression(name='home_address_geo_region', expression="home_address__geo_region") }}
{{ synth_column_expression(name='home_address_country', expression="home_address__country") }}
{{ synth_column_expression(name='home_address_postal_code', expression="home_address__postal_code") }}

{{ synth_table(rows = 100) }}
{{ synth_add_cleanup_hook("alter table {{this}} drop column home_address") or "" }}
```

Alternatively, you may use something like

```python
{{ synth_column_primary_key(name='k_person') }}
{{ synth_column_firstname(name='first_name') }}
{{ synth_column_lastname(name='last_name') }}
{{ synth_column_address(name='home_address_street', countries=['United States'], parts=['street_address']) }}
{{ synth_column_address(name='home_address_city', countries=['United States'], parts=['city']) }}
{{ synth_column_address(name='home_address_geo_region', countries=['United States'], parts=['geo_region']) }}
{{ synth_column_address(name='home_address_country', countries=['United States'], parts=['country']) }}
{{ synth_column_address(name='home_address_postal_code', countries=['United States'], parts=['postal_code']) }}
{{ synth_table(rows = 100) }}
```
</details>

<details>
<summary><code>phone_number</code></summary>

Generates a phone number in the format `(123) 456-7890`.

```python
{{ synth_column_phone_number(name="phone_number") }}
```
</details>


# Advanced usage
Occasionally you may want to build up a more complex column's values from several simpler ones. This is easily done with an expression column, for example
```sql
{{ synth_column_primary_key(name="k_person") }}
{{ synth_column_firstname(name='first_name') }}
{{ synth_column_lastname(name='last_name') }}
{{ synth_column_expression(name='full_name', expression="first_name || ' ' || last_name") }}
{{ synth_remove(collection="final_fields", key="first_name") }}
{{ synth_remove(collection="final_fields", key="last_name") }}
{{ synth_table(rows = 100) }}
```
Note that you may want to "clean up" by supressing some of your intermediate columns, as shown with the `synth_remove()` calls in the example above.

You may also want to modify another table *only after this one is built*. This is also possible using cleanup hooks.

For example, suppose you want to create `products` and `orders`, but you want some `products` to be exponentially more popular (more `orders` for) than others. This is possible by
1. creating a `products` model with an extra popularity column:
    ```sql
    {{ synth_column_primary_key(name="k_product") }}
    {{ synth_column_string(name="name", min_length=10, max_length=20) }}
    {{ synth_column_distribution(name="popularity",
        distribution=synth_distribution(class='continuous', type='exponential', lambda=0.05)
    ) }}
    {{ synth_table(rows=50) }}
    ```
1. creating an `orders` model with a `synth_column_select()` to `products` using your popularity column, then use a cleanup hook to drop the `popularity` column:
    ```sql
    {{ synth_column_primary_key(name="k_order") }}
    {{ synth_column_select(name="k_product", lookup_table="products", 
        value_col="k_product", distribution="weighted", weight_col="popularity") }}
    {{ synth_column_distribution(name="status",
        distribution=synth_distribution(class='discrete', type='probabilities',
            probabilities={"New":0.2, "Shipped":0.5, "Returned":0.2, "Lost":0.1}
        )
    ) }}
    {{ synth_column_integer(name="num_ordered", min=1, max=10) }}

    {{ synth_add_cleanup_hook(
        'alter table {{target.database}}.{{target.schema}}.products drop column popularity'
    ) }}

    {{ synth_table(rows=5000) }}
    ```
    Note that the cleanup hook *must* go after any column definitions that rely on it, and before the `synth_table()` call.


# Datasets

## Words
The word list in `seeds/synth_words.csv` contains 70k words &ndash; the top 5k most common words from each of the following 14 languages:
* Bulgarian (`bg`)
* Czech (`cs`)
* Danish (`da`)
* Dutch (`nl`)
* English (`en`)
* Finnish (`fi`)
* French (`fr`)
* German (`de`)
* Hungarian (`hu`)
* Indonesian (`id`)
* Italian (`it`)
* Portuguese (`pt`)
* Slovenian (`sv`)
* Spanish (`es`)

With each word is associated a **frequency**, which is a value between 0 and 1 representing the frequency with which the word appears in common usage of the language, and a **part of speech** for the word, which is one of:
* ADJ: adjective
* ADP: adposition
* ADV: adverb
* AUX: auxiliary verb
* CONJ: coordinating conjunction
* DET: determiner
* INTJ: interjection
* NOUN: noun
* NUM: numeral
* PART: particle
* PRON: pronoun
* PROPN: proper noun
* PUNCT: punctuation
* SCONJ: subordinating conjunction
* SYM: symbol
* VERB: verb
* X: other

Some words may functionally belong to multiple parts of speech; this dataset uses only the single most common.

The dataset is constructed based on word lists and frequencies from [`wordfreq`](https://github.com/rspeer/wordfreq) and part-of-speech tagging from [`polyglot`](https://polyglot.readthedocs.io/en/latest/POS.html). Language availability is based on the set intersection of the languages supported by these two libraries.

You may run into an error when loading this data using `dbt seed` on SQLite - [an issue](https://github.com/codeforkjeff/dbt-sqlite/issues/35) has been raised with the `dbt-sqlite` adapter to solve this, in the meantime, you'd have to manually edit the seed batch size (make it smaller) to load `synth_words` in SQLite.

## Languages
The language list in `seeds/synth_languages.csv` contains 222 commonly-spoken (living) languages, with, for each, the ISO 693-2 and ISO 693-3 language codes, the approximate number of speakers, and a list of countries in which the language is predominantly spoken. Country names are consistent with those in the countries dataset at `seeds/synth_countries.csv`.

The dataset is assembled primarily from Wikipedia, including [this list of official languages by country](https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory), and the specific pages for each individual language.


# Performance
Here we provide benchmarks in Snowflake and AWS RDS Postgres for synthetic data generation, using the models found in `example_models/*.sql`..

## Snowflake
Using a single Xsmall warehouse:

| Model | Columns | Rows | Runtime | Data size |
| --- | --- | --- | --- | --- |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 10k | 11.48s | 646 KB |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 1M | 19.03s | 57.8 MB |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 100M | 64.61s | 5.7 GB |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 10B | 81 min | 614.7 GB |

| Model | Columns | Rows | Runtime | Data size |
| --- | --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 100 | 4.32s | 32.5 KB |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 50 | 1.28s | 16.0 KB |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 2 | 1.63s | 2.0 KB |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 1000 | 1.36s | 59.0 KB |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 100 | 1.46s | 22.0 KB |

| Model | Columns | Rows | Runtime | Data size |
| --- | --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 10k | 3.77s | 958.5 KB |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 5k | 2.14s | 267.5 KB |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 200 | 1.71s | 32.0 KB |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 100k | 3.60s | 5.3 MB |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 1M | 16.74s | 6.1 MB |

| Model | Columns | Rows | Runtime | Data size |
| --- | --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 1M | 21.43s | 53.0 MB |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 50k | 21.04s | 2.4 MB |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 20k | 1.85s | 1.3 MB |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 50M | 114 min | 1.0 GB |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 100M | 325 min | 2.5 GB |

(Note that **orders** and **inventory** are significantly slower at scale because they rely on **products** - and so many products are partitioned, slowing the query.)


## Postgres
Using an AWS RDS small instance:

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 10k | 19.71s |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 1M | 35.78s |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 100M | 17 min |

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 100 | 26.29s |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 50 | 22.35s |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 2 | 26.45s |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 1000 | 22.58s |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 100 | 22.12s |

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 10k | 18.77s |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 5k | 17.16s |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 200 | 19.82s |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 100k | 414.38s |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 1M | 396.10s |


## SQLite
Using a Lenovo laptop with Intel i-5 2.6GHz processor, 16GB RAM, and 500GB SSD:

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 10k | 0.16s |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 1M | 7.85s |
| [distributions](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/distributions.sql) | 15 | 100M | 15 min |

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 100 | 0.94s |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 50 | 0.67s |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 2 | 0.26s |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 1000 | 0.07s |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 100 | 4.91s |

| Model | Columns | Rows | Runtime |
| --- | --- | --- | --- |
| [customers](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/customers.sql) | 8 | 10k | 29.02s |
| [products](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/products.sql) | 3 | 5k | 13.59s |
| [stores](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/stores.sql) | 5 | 200 | 0.63s |
| [orders](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/orders.sql) | 4 | 100k | 157.2s |
| [inventory](https://github.com/edanalytics/dbt_synth_data/blob/main/example_models/inventory.sql) | 4 | 1M | 30 min |



# Changelog
Coming soon!



# Contributing
Bugfixes and new features (such as additional transformation operations) are gratefully accepted via pull requests here on GitHub.

## Contributions
* Cover image created with [DALL &bull; E mini](https://huggingface.co/spaces/dalle-mini/dalle-mini)



# License
Coming soon!



# Todo
- [ ] fix SQLite bug where joins seem to blow up rows (row counts in resulting table are potentially much larger than you specified) - strange issue, because the same joins all seem to work fine on Snowflake and Postgres...
- [ ] fix address so it selects a city, then uses the country (and geo_region) for that city, rather than a (different) random country (and geo_region)
- [ ] implement other [distributions](#distributions)... Poisson, Gamma, Power law/Pareto, Multinomial?
- [ ] flesh out more seeds, data columns, and composite columns