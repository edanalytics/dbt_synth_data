{% macro synth_column_group_values(name, group_expr, values, probabilities=None, draw_key=None) %}
    {# Same discrete-bucket approach as synth_column_values, but keyed by an arbitrary
       group_expr (e.g. a transaction id) instead of __row_number, so every row sharing
       that key draws the same value. String values only. draw_key defaults to name, so two
       group-level columns keyed off the same group_expr don't cross their thresholds at
       the same group ids. probabilities is optional and defaults to uniform, matching
       synth_column_values. #}
    {%- set probabilities = probabilities or [1.0 / (values | length)] * (values | length) -%}
    {%- set draw_key = draw_key or name -%}
    {%- set seed = adapter.dispatch('synth_column_group_values_seed', 'dbt_synth_data')(group_expr, draw_key) -%}
    {%- set cum = namespace(pct=0) -%}
    {%- set branches = [] -%}
    {%- for i in range(values | length - 1) -%}
        {%- set cum.pct = cum.pct + probabilities[i] -%}
        {%- set threshold = (cum.pct * 100000) | round(0, 'common') | int -%}
        {%- do branches.append("WHEN " ~ seed ~ " < " ~ threshold ~ " THEN '" ~ values[i] ~ "'") -%}
    {%- endfor -%}
    {%- set expression = "CASE " ~ (branches | join(' ')) ~ " ELSE '" ~ values[-1] ~ "' END" -%}
    {{ dbt_synth_data.synth_column_expression(name=name, expression=expression) }}
{% endmacro %}

{% macro default__synth_column_group_values_seed(group_expr, draw_key) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro bigquery__synth_column_group_values_seed(group_expr, draw_key) -%}
    MOD(ABS(FARM_FINGERPRINT(CONCAT('{{ draw_key }}', CAST({{ group_expr }} AS STRING)))), 100000)
{%- endmacro %}

{% macro duckdb__synth_column_group_values_seed(group_expr, draw_key) -%}
    (hash('{{ draw_key }}' || CAST({{ group_expr }} AS VARCHAR)) % 100000)
{%- endmacro %}

{% macro sqlite__synth_column_group_values_seed(group_expr, draw_key) -%}
    {# SQLite ships no built-in hash function, so this rolls its own: a base-31
       polynomial hash over the characters (same idea as Java's String.hashCode),
       then one multiplicative mix (Knuth's 32-bit Fibonacci constant) so that
       sequential keys -- e.g. group_expr = 0, 1, 2, 3, the common case -- don't
       land in near-sequential buckets. The polynomial alone fails this: two keys
       differing only in their last character produce hashes differing by ~1,
       which the mix step is there to scramble. Verified empirically, not derived
       from a known-good SQLite recipe. #}
    (((
        WITH RECURSIVE gv(i, acc, s) AS (
            SELECT 1, 0, '{{ draw_key }}' || CAST({{ group_expr }} AS TEXT)
            UNION ALL
            SELECT i + 1, (acc * 31 + unicode(substr(s, i, 1))) % 1000000007, s
            FROM gv WHERE i <= length(s)
        )
        SELECT acc FROM gv ORDER BY i DESC LIMIT 1
    ) * 2654435761) % 4294967296) % 100000
{%- endmacro %}

{% macro postgres__synth_column_group_values_seed(group_expr, draw_key) -%}
    {# hashtext() returns a signed int4; ABS(-2147483648) overflows int4 (no positive
       representation), so cast to bigint before taking ABS. #}
    MOD(ABS(hashtext('{{ draw_key }}' || CAST({{ group_expr }} AS TEXT))::bigint), 100000)
{%- endmacro %}

{% macro snowflake__synth_column_group_values_seed(group_expr, draw_key) -%}
    MOD(ABS(HASH('{{ draw_key }}' || CAST({{ group_expr }} AS STRING))), 100000)
{%- endmacro %}
