{% macro synth_distribution_discretize_floor(distribution) %}
    {{ return(adapter.dispatch('synth_distribution_discretize_floor')(distribution)) }}
{% endmacro %}

{% macro default__synth_distribution_discretize_floor(distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_discretize_floor(distribution) %}
    cast(floor( {{distribution}} ) as int)
{% endmacro %}

{% macro postgres__synth_distribution_discretize_floor(distribution) %}
    floor( {{distribution}} )
{% endmacro %}

{% macro snowflake__synth_distribution_discretize_floor(distribution) %}
    floor( {{distribution}} )
{% endmacro%}



{% macro synth_distribution_discretize_ceil(distribution) %}
    {{ return(adapter.dispatch('synth_distribution_discretize_ceil')(distribution)) }}
{% endmacro %}

{% macro default__synth_distribution_discretize_ceil(distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_discretize_ceil(distribution) %}
    cast(ceil( {{distribution}} ) as int)
{% endmacro %}

{% macro postgres__synth_distribution_discretize_ceil(distribution) %}
    ceil( {{distribution}} )
{% endmacro %}

{% macro snowflake__synth_distribution_discretize_ceil(distribution) %}
    ceil( {{distribution}} )
{% endmacro%}



{% macro synth_distribution_discretize_round(distribution, precision=0) %}
    {{ return(adapter.dispatch('synth_distribution_discretize_round')(distribution, precision)) }}
{% endmacro %}

{% macro default__synth_distribution_discretize_round(distribution, precision) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_discretize_round(distribution, precision) %}
    round( ( {{distribution}} ) , {{precision}})
{% endmacro %}

{% macro postgres__synth_distribution_discretize_round(distribution, precision) %}
    round( ( {{distribution}} )::numeric , {{precision}})
{% endmacro %}

{% macro snowflake__synth_distribution_discretize_round(distribution, precision) %}
    round( ( {{distribution}} )::numeric , {{precision}})
{% endmacro%}



{% macro synth_distribution_discretize_width_bucket(distribution, from=0.0, to=1.0, strict_bounds=True, count=None, size=None, labels=None, label_precision=4, bucket_range_separator=' - ') %}
    {# Either `size` or `count` must be specified #}
    {%- if size is none and count is none -%}
        {{ exceptions.raise_compiler_error("`either `count` (number of bins) or `size` (of each bin) must be specified for bin discretization") }}
    {%- endif -%}

    {# Set count from size if size is supplied #}
    {%- if size is not none -%}
        {%- set count = (to-from)/size -%}
    {%- elif count is not none -%}
        {%- if strict_bounds -%}
            {%- set size = (to-from)/count -%}
        {%- else -%}
            {%- set size = (to-from)/(count-2) -%}
        {%- endif -%}
    {%- endif -%}

    {%- if labels=='lower_bound' or labels=='upper_bound' or labels=='bucket_range' or labels=='bucket_average' -%}
        
        {%- if strict_bounds -%}{# assume no value is outside [from,to] #}
            case width_bucket({{distribution}}, {{from}}, {{to}}, {{count}})
                {% for i in range(1, count+1) %}
                when {{i}} then 
                    {% if labels=='lower_bound' -%}
                    round( {{from + ((i-1)*size)}}, {{label_precision}} )
                    {% elif labels=='upper_bound' -%}
                    round( {{from + (i*size)}}, {{label_precision}} )
                    {% elif labels=='bucket_average' -%}
                    round( {{from + ((i-0.5)*size)}}, {{label_precision}} )
                    {% elif labels=='bucket_range' -%}
                    concat(
                        round( {{from + ((i-1)*size)}}, {{label_precision}} ),
                        '{{bucket_range_separator}}', 
                        round( {{from + (i*size)}}, {{label_precision}} )
                    )
                    {%- endif %}
                {% endfor %}
            end

        {%- else -%}{# long-tail values could be outside [from,to] #}
            case width_bucket({{distribution}}, {{from}}, {{to}}, {{count-2}})
                
                when 0 then 
                    {% if labels=='lower_bound' -%}
                    '-Infinity'::float
                    {% elif labels=='upper_bound' -%}
                    round( {{from}}, {{label_precision}} )
                    {% elif labels=='bucket_average' -%}
                    round( {{from}}, {{label_precision}} )
                    {% elif labels=='bucket_range' -%}
                    concat(
                        '-Infinity'::varchar,
                        '{{bucket_range_separator}}',
                        round( {{from}}, {{label_precision}} )
                    )
                    {%- endif %}
                
                {% for i in range(1, count-1) %}
                when {{i}} then 
                    {% if labels=='lower_bound' -%}
                    round( {{from + ((i-1)*size)}}, {{label_precision}} )
                    {% elif labels=='upper_bound' -%}
                    round( {{from + (i*size)}}, {{label_precision}} )
                    {% elif labels=='bucket_average' -%}
                    round( {{from + ((i+0.5)*size)}}, {{label_precision}} )
                    {% elif labels=='bucket_range' -%}
                    concat(
                        round( {{from + ((i-1)*size)}}, {{label_precision}} ),
                        '{{bucket_range_separator}}',
                        round( {{from + (i*size)}}, {{label_precision}} )
                    )
                    {%- endif %}
                {% endfor %}
                
                when {{count-1}} then 
                    {% if labels=='lower_bound' -%}
                    round( {{to}}, {{label_precision}} )
                    {% elif labels=='upper_bound' -%}
                    'Infinity'::float
                    {% elif labels=='bucket_average' -%}
                    round( {{to}}, {{label_precision}} )
                    {% elif labels=='bucket_range' -%}
                    concat(
                        round( {{to}}, {{label_precision}} ),
                        '{{bucket_range_separator}}',
                        'Infinity'::varchar
                    )
                    {%- endif %}
            
            end
        {%- endif -%}
        
    {%- elif labels is none -%}
        {%- if strict_bounds -%}{# assume no value is outside [from,to] #}
            width_bucket({{distribution}}, {{from}}, {{to}}, {{count}})
        {%- else -%}{# long-tail values could be outside [from,to] #}
            width_bucket({{distribution}}, {{from}}, {{to}}, {{count-2}})
        {%- endif -%}

    {%- else -%}
        {%- if strict_bounds -%}{# assume no value is outside [from,to] #}
           case width_bucket({{distribution}}, {{from}}, {{to}}, {{count}})
                {% for i in range(1, count) %}
                when {{i}} then {{labels[i]}}
                {% endfor %}
            end
        {%- else -%}{# long-tail values could be outside [from,to] #}
            case width_bucket({{distribution}}, {{from}}, {{to}}, {{count-2}})
                {% for i in range(0, count) %}
                when {{i}} then {{labels[i]}}
                {% endfor %}
            end
        {%- endif -%}
        
    {%- endif -%}
{% endmacro %}
