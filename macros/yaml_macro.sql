{% macro yaml_to_macro(data) %}

    {#
        CASE A: var is a (potentially nested) mapping
            vars:
                student_teacher_ratio:
                    distribution_union():
                        distribution_continuous_normal():
                            mean: 15
                            stddev: 5
                        distribution_continuous_normal():
                            mean: 20
                            stddev: 5
                        weights: [1, 2]

        CASE B: var is a macro call with no params
            vars:
                student_teacher_ratio: distribution_continuous_normal() # default params

        CASE C: var is a fixed value (boolean, number, string) - no dynamic processing
            vars:
                student_teacher_ratio: 15
    #}

    {% if data is mapping and data|length==1 %}
        {% set first_key = data|first|string %}
        {% if first_key[-2:]!="()" %}
            {# just a mapping with 1 element, not macro yaml :( #}
            {{ return(data) }}
        {% endif %}
        {# CASE A #}
        {% set my_macro = first_key[0:-2] %}
        {% set my_params = {} %}
        {% for param, value in data[first_key]|items %}
            {% do my_params.update({param: yaml_to_macro(value)}) %}
        {% endfor %}
        {{ return(synth_call_macro(my_macro, my_params)) }}
    {% elif data is string and data[-2:]=="()" %}
        {# CASE B #}
        {% set my_macro = data[0:-2] %}
        {{ synth_call_macro(my_macro, {})() }}
    {% else %}
        {# CASE C #}
        {{ return(data) }}
    {% endif %}
{% endmacro %}

{% macro synth_call_macro(name, params) %}
    {% if 
           (name=='synth_distribution' and params.get('class', '')=='continuous' and params.get('type', '')=='uniform')
        or (name=='synth_distribution_continuous' and params.get('type', '')=='uniform')
        or (name=='synth_distribution_continuous_uniform')
    %}{{ return(dbt_synth_data.synth_distribution_continuous_uniform(min=params.get('min', 1), max=params.get('max', 1))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='continuous' and params.get('type', '')=='normal')
        or (name=='synth_distribution_continuous' and params.get('type', '')=='normal')
        or (name=='synth_distribution_continuous_normal')
    %}{{ return(dbt_synth_data.synth_distribution_continuous_normal(mean=params.get('mean', 0), stddev=params.get('stddev', 1))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='continuous' and params.get('type', '')=='exponential')
        or (name=='synth_distribution_continuous' and params.get('type', '')=='exponential')
        or (name=='synth_distribution_continuous_exponential')
    %}{{ return(dbt_synth_data.synth_distribution_continuous_exponential(lambda=params.get('lambda', 1.0))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='continuous' and params.get('type', '')=='laplace')
        or (name=='synth_distribution_continuous' and params.get('type', '')=='laplace')
        or (name=='synth_distribution_continuous_laplace')
    %}{{ return(dbt_synth_data.synth_distribution_continuous_laplace(mu=params.get('mu', 0.0),b=params.get('b', 1.0))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='continuous' and params.get('type', '')=='cauchy')
        or (name=='synth_distribution_continuous' and params.get('type', '')=='cauchy')
        or (name=='synth_distribution_continuous_cauchy')
    %}{{ return(dbt_synth_data.synth_distribution_continuous_cauchy(x0=params.get('x0', 0.0),gamma=params.get('gamma', 1.0))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='discrete' and params.get('type', '')=='bernoulli')
        or (name=='synth_distribution_discrete' and params.get('type', '')=='bernoulli')
        or (name=='synth_distribution_discrete_bernoulli')
    %}{{ return(dbt_synth_data.synth_distribution_discrete_bernoulli(p=params.get('p', 0.5))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='discrete' and params.get('type', '')=='binomial')
        or (name=='synth_distribution_discrete' and params.get('type', '')=='binomial')
        or (name=='synth_distribution_discrete_binomial')
    %}{{ return(dbt_synth_data.synth_distribution_discrete_binomial(n=params.get('n', 10), p=params.get('p', 0.5))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='discrete' and params.get('type', '')=='probabilities')
        or (name=='synth_distribution_discrete' and params.get('type', '')=='probabilities')
        or (name=='synth_distribution_discrete_probabilities')
    %}{{ return(dbt_synth_data.synth_distribution_discrete_probabilities(probabilities=params.get('probabilities', {}))) }}
    {% elif 
           (name=='synth_distribution' and params.get('class', '')=='discrete' and params.get('type', '')=='weights')
        or (name=='synth_distribution_discrete' and params.get('type', '')=='weights')
        or (name=='weights')
    %}{{ return(dbt_synth_data.synth_distribution_discrete_weights(values=params.get('values', []), weights=params.get('weights', []))) }}
    {% elif name=='synth_distribution_average'
    %}{{ return(dbt_synth_data.synth_distribution_average(
        d0=params.get('d0', ''),
        d1=params.get('d1', ''),
        d2=params.get('d2', None),
        d3=params.get('d3', None),
        d4=params.get('d4', None),
        d5=params.get('d5', None),
        d6=params.get('d6', None),
        d7=params.get('d7', None),
        d8=params.get('d8', None),
        d9=params.get('d9', None),
        weights=params.get('weights', None)
    )) }}
    {% elif name=='synth_distribution_union'
    %}{{ return(dbt_synth_data.synth_distribution_union(
        d0=params.get('d0', ''),
        d1=params.get('d1', ''),
        d2=params.get('d2', None),
        d3=params.get('d3', None),
        d4=params.get('d4', None),
        d5=params.get('d5', None),
        d6=params.get('d6', None),
        d7=params.get('d7', None),
        d8=params.get('d8', None),
        d9=params.get('d9', None),
        weights=params.get('weights', None)
    )) }}
    {% elif name=='synth_distribution_discretize_floor'
    %}{{ return(dbt_synth_data.synth_distribution_discretize_floor(distribution=params.get('distribution', ''))) }}
    {% elif name=='synth_distribution_discretize_ceil'
    %}{{ return(dbt_synth_data.synth_distribution_discretize_ceil(distribution=params.get('distribution', ''))) }}
    {% elif name=='synth_distribution_discretize_round'
    %}{{ return(dbt_synth_data.synth_distribution_discretize_round(
        distribution=params.get('distribution', ''),
        precision=params.get('precision', 0)
    )) }}
    {% elif name=='synth_distribution_discretize_width_bucket'
    %}{{ return(dbt_synth_data.synth_distribution_discretize_width_bucket(
        distribution=params.get('distribution', ''),
        from=params.get('from', 0.0),
        to=params.get('to', 1.0),
        strict_bounds=params.get('strict_bounds', True),
        count=params.get('count', None),
        size=params.get('size', None),
        labels=params.get('labels', None),
        label_precision=params.get('label_precision', 4),
        bucket_range_separator=params.get('bucket_range_separator', ' - ')
    )) }}
    {% elif name=='synth_expression'
    %}{{ return(
        params.get('expression', '')
        .replace("$0", params.get('p0', ''))
        .replace("$1", params.get('p1', ''))
        .replace("$2", params.get('p2', ''))
        .replace("$3", params.get('p3', ''))
        .replace("$4", params.get('p4', ''))
        .replace("$5", params.get('p5', ''))
        .replace("$6", params.get('p6', ''))
        .replace("$7", params.get('p7', ''))
        .replace("$8", params.get('p8', ''))
        .replace("$9", params.get('p9', ''))
    ) }}
    {% else%}{{ exceptions.raise_compiler_error("Unknown macro name: " + name) }}
    {% endif %}
{% endmacro %}