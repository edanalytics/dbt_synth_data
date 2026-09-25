{% macro synth_column_numeric_typed(name, min, max, precision=5, data_type='NUMERIC(38, 9)') -%}
    {# synth_column_numeric has no cast argument, so its expression always lands as the
       adapter's native float type (e.g. FLOAT64 on BigQuery) even when the target column
       is NUMERIC/DECIMAL. This wraps the identical distribution expression in an explicit
       CAST via synth_column_expression.

       data_type carries an explicit precision and scale because a bare NUMERIC means
       different things per adapter -- DECIMAL(18, 3) on DuckDB, NUMBER(38, 0) on
       Snowflake -- which would silently truncate below the macro's default precision=5.
       38 and 9 are BigQuery's NUMERIC limits (it caps precision - scale at 29) and are
       valid on DuckDB, Postgres, and Snowflake too. #}
    {% set expression %}
        CAST(
            {{ dbt_synth_data.synth_distribution_discretize_round(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min, max=max),
                precision=precision
            ) }}
        AS {{ data_type }})
    {% endset %}
    {{ dbt_synth_data.synth_column_expression(name=name, expression=expression) }}
{%- endmacro %}
