{% macro synth_distribution_discrete_bernoulli(p=0.5) -%}
    case
        when {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=1) }} < {{p}} then 0
        else 1
    end
{%- endmacro %}