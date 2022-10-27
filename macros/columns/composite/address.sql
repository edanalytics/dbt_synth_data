{#
    Creating a column `myaddress` using this macro will also create
    intermediate columns `myaddress__street_address`, `myaddress__city`,
    `myaddress__geo_region`, and `myaddress__postal_code` (or whatever
    `parts` you specify). You can then `add_update_hook()`s that reference
    these intermediate columns if you'd like. For example:

    {{ dbt_synth.table(
        rows = 100,
        columns = [
            dbt_synth.column_primary_key(name='k_person'),
            dbt_synth.column_firstname(name='first_name'),
            dbt_synth.column_lastname(name='last_name'),
            dbt_synth.column_address(name='home_address', countries=['United States of America'],
                parts=['street_address', 'city', 'geo_region', 'country', 'postal_code']),
            dbt_synth.column_expression(name='home_address_street', expression="home_address__street_address"),
            dbt_synth.column_expression(name='home_address_city', expression="home_address__city"),
            dbt_synth.column_expression(name='home_address_geo_region', expression="home_address__geo_region"),
            dbt_synth.column_expression(name='home_address_country', expression="home_address__country"),
            dbt_synth.column_expression(name='home_address_postal_code', expression="home_address__postal_code"),
        ]
    ) }}
    {{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column home_address") or "" }}
    {{ config(post_hook=dbt_synth.get_post_hooks())}}

    Alternatively, you may try something like

    {{ dbt_synth.table(
        rows = 100,
        columns = [
            dbt_synth.column_primary_key(name='k_person'),
            dbt_synth.column_firstname(name='first_name'),
            dbt_synth.column_lastname(name='last_name'),
            dbt_synth.column_address(name='home_address_street', countries=['United States of America'], parts=['street_address']),
            dbt_synth.column_address(name='home_address_city', countries=['United States of America'], parts=['city']),
            dbt_synth.column_address(name='home_address_geo_region', countries=['United States of America'], parts=['geo_region']),
            dbt_synth.column_address(name='home_address_country', countries=['United States of America'], parts=['country']),
            dbt_synth.column_address(name='home_address_postal_code', countries=['United States of America'], parts=['postal_code']),
        ]
    ) }}
    {{ config(post_hook=dbt_synth.get_post_hooks())}}

#}
{% macro column_address(
    name,
    address_types=['house','apartment','pobox'],
    street_types=['St.', 'Rd.', 'Dr.', 'Ln.', 'Ave.', 'Pl.', 'Blvd.', 'Ct.', 'Trl.', 'Pkwy.'],
    cities=[],
    geo_regions=[],
    countries=['United States'],
    postal_code_min=10,
    postal_code_max=99999,
    parts=['street_address', 'city', 'geo_region', 'postal_code']
) -%}
    {{ return(adapter.dispatch('column_address')(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max, parts)) }}
{%- endmacro %}

{% macro default__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max, parts) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}



{% macro postgres__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max, parts) %}
    {% if 'street_address' in parts %}
    {{ dbt_synth.add_update_hook(postgres__address_update_street_adjective(name)) or "" }}
    {{ dbt_synth.add_update_hook(postgres__address_update_street_noun(name)) or "" }}
    {{ dbt_synth.add_update_hook(postgres__address_update_street_address(name, address_types, street_types)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'address_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'number1')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_num_words')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_adjective_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_adjective')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_noun_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_noun')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'unit_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'number2')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'street_address')) or "" }}
    {% endif %}
    {% if 'city' in parts %}
    {{ dbt_synth.add_update_hook(postgres__address_update_city(name, cities, geo_regions)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'city_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'city')) or "" }}
    {% endif %}
    {% if 'geo_region' in parts %}
    {{ dbt_synth.add_update_hook(postgres__address_update_geo_region(name, geo_regions)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'geo_region_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'geo_region')) or "" }}
    {% endif %}
    {% if 'country' in parts %}
    {{ dbt_synth.add_update_hook(postgres__address_update_country(name, countries)) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'country_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'country')) or "" }}
    {% endif %}
    {% if 'postal_code' in parts %}
    {{ dbt_synth.add_cleanup_hook(postgres__address_cleanup(name, 'postal_code')) or "" }}
    {% endif %}
    {{ dbt_synth.add_update_hook(postgres__address_update(name, parts)) or "" }}

    {% if 'street_address' in parts %}
    floor(RANDOM() * {{address_types|length}} + 1) as {{name}}__address_type,
    floor(RANDOM() * 9989 + 10)::varchar as {{name}}__number1, {# 2-4 digit number #}
    floor(RANDOM() * 2 + 1)::int as {{name}}__street_num_words,
    floor(RANDOM() * (select count(*) from {{this.database}}.{{this.schema}}.synth_words where types like '%adjective%') + 1)::int as {{name}}__street_adjective_idx,
    ''::varchar as {{name}}__street_adjective,
    floor(RANDOM() * (select count(*) from {{this.database}}.{{this.schema}}.synth_words where types like '%noun%') + 1)::int as {{name}}__street_noun_idx,
    ''::varchar as {{name}}__street_noun,
    floor(RANDOM() * {{street_types|length}} + 1)::int as {{name}}__street_type,
    floor(RANDOM() * 2 + 1)::int as {{name}}__unit_type,
    floor(RANDOM() * 999 + 1)::varchar as {{name}}__number2, {# 1-3 digit number #}
    ''::varchar as {{name}}__street_address,
    {% endif %}
    {% if 'city' in parts %}
    floor(RANDOM() * 
        {%- if cities|length>0 %}{{cities|length}}
        {%- elif geo_regions|length>0 %}
            (select count(*) from {{this.database}}.{{this.schema}}.synth_cities
            where geo_region in ('{{geo_regions|join("','")}}') )
        {%- else %}(select count(*) from {{this.database}}.{{this.schema}}.synth_cities){% endif -%}
        + 1)::int as {{name}}__city_idx,
    ''::varchar as {{name}}__city,
    {% endif %}
    {% if 'geo_region' in parts %}
    floor(RANDOM() * 
        {%- if geo_regions|length>0 %}{{geo_regions|length}}
        {%- else %}(select count(*) from {{this.database}}.{{this.schema}}.synth_geo_regions){% endif -%}
        + 1)::int as {{name}}__geo_region_idx,
    ''::varchar as {{name}}__geo_region,
    {% endif %}
    {% if 'country' in parts %}
    floor(RANDOM() * 
        {%- if countries|length>0 %}{{countries|length}}
        {%- else %}(select count(*) from {{this.database}}.{{this.schema}}.synth_countries){% endif %}
        + 1)::int as {{name}}__country_idx,
    ''::varchar as {{name}}__country,
    {% endif %}
    {% if 'postal_code' in parts %}
    LPAD(floor(RANDOM() * ({{postal_code_max}}-{{postal_code_min}}) + {{postal_code_min}})::varchar, 5, '0') as {{name}}__postal_code,
    ''::varchar AS {{name}}
    {% endif %}
{% endmacro %}

{% macro postgres__address_update_street_adjective(name) %}
    update {{this}} set {{name}}__street_adjective=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%adjective%'
    ) as y where y.row_num={{name}}__street_adjective_idx
{% endmacro%}

{% macro postgres__address_update_street_noun(name) %}
    update {{this}} set {{name}}__street_noun=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%noun%'
    ) as y where y.row_num={{name}}__street_noun_idx
{% endmacro%}

{% macro postgres__address_update_street_address(name, address_types, street_types) %}
    update {{this}} set {{name}}__street_address=(
        case {{name}}__address_type
            {% for i in range(address_types|length) %}

            when {{i+1}} then
            {% if address_types[i]=='house' %}

                {{name}}__number1 || ' '
                || (case {{name}}__street_num_words
                    when 1 then {{name}}__street_noun
                    else {{name}}__street_adjective || ' ' || {{name}}__street_noun
                end)
                || ' '
                || (case {{name}}__street_type
                    {% for i in range(street_types|length) %}
                    when {{i+1}} THEN '{{street_types[i]}}'
                    {% endfor %}
                end)
            
            
            {% elif address_types[i]=='apartment' %}
            
                {{name}}__number1 || ' '
                || (case {{name}}__street_num_words
                    when 1 then {{name}}__street_noun
                    else {{name}}__street_adjective || ' ' || {{name}}__street_noun
                end)
                || ' '
                || (case {{name}}__street_type
                    {% for i in range(street_types|length) %}
                    when {{i+1}} THEN '{{street_types[i]}}'
                    {% endfor %}
                end)
                || ' ' {# unit #}
                || (case {{name}}__unit_type
                    when 1 then 'No. '
                    else '#'
                end)
                || {{name}}__number2
            
            {% elif address_types[i]=='pobox' %}
            
                (case {{name}}__unit_type
                    when 1 then 'PO'
                    else 'P.O. '
                end)
                || ' Box '
                || {{name}}__number2
            
            {% endif %}

            {% endfor %}
        end
    )
{% endmacro%}

{% macro postgres__address_update_city(name, cities, geo_regions) %}
    update {{this}} set {{name}}__city=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_cities
        {% if cities|length > 0 or geo_regions|length > 0 %}where {% endif -%}
        {%- if cities|length > 0 %}name in ('{{cities|join("','")}}') {% endif -%}
        {%- if cities|length > 0 and geo_regions|length > 0 %}and {% endif -%}
        {%- if geo_regions|length > 0 %}geo_region_abbr in ('{{geo_regions|join("','")}}'){% endif %}
    ) as y where y.row_num={{name}}__city_idx
{% endmacro%}

{% macro postgres__address_update_geo_region(name, geo_regions) %}
    update {{this}} set {{name}}__geo_region=y.abbr from (
        select abbr, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_geo_regions
        {% if geo_regions|length > 0 %}where abbr in ('{{geo_regions|join("','")}}'){% endif %}
    ) as y where y.row_num={{name}}__geo_region_idx
{% endmacro%}

{% macro postgres__address_update_country(name, countries) %}
    update {{this}} set {{name}}__country=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_countries
        {% if countries|length > 0 %}where name in ('{{countries|join("','")}}'){% endif %}
    ) as y where y.row_num={{name}}__country_idx
{% endmacro%}

{% macro postgres__address_update(name, parts) %}
    update {{this}} set {{name}}=(
        {% if 'street_address' in parts %}
            {{name}}__street_address
        {% endif %}

        {% if 'street_address' in parts and 'city' in parts %}
            || ', '
        {% endif %}

        {% if 'city' in parts %}
            {% if 'street_address' in parts %}||{% endif %}
            {{name}}__city
        {% endif %}

        {% if 'city' in parts and 'geo_region' in parts %}
            || ', '
        {% endif %}

        {% if 'geo_region' in parts %}
            {% if 'city' in parts or 'street_address' in parts %}||{% endif %}
            {{name}}__geo_region
        {% endif %}

        {% if 'geo_region' in parts and 'postal_code' in parts %}
            || ' '
        {% endif %}

        {% if 'postal_code' in parts %}
            {% if 'city' in parts or 'street_address' in parts or 'geo_region' in parts %}||{% endif %}
            {{name}}__postal_code
        {% endif %}

        {% if 'postal_code' in parts and 'country' in parts %}
            || ' '
        {% endif %}

        {% if 'country' in parts %}
            {% if 'city' in parts or 'street_address' in parts or 'geo_region' in parts or 'postal_code' in parts %}||{% endif %}
            {{name}}__country
        {% endif %}
    )
{% endmacro %}

{% macro postgres__address_cleanup(name, col) %}
alter table {{ this }} drop column {{name}}__{{col}}
{% endmacro %}






{% macro snowflake__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max, parts) %}
    {% if 'street_address' in parts %}
    {{ dbt_synth.add_update_hook(snowflake__address_update_street_noun(name)) or "" }}
    {{ dbt_synth.add_update_hook(snowflake__address_update_street_adjective(name)) or "" }}
    {{ dbt_synth.add_update_hook(snowflake__address_update_street_address(name, address_types, street_types)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'address_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'number1')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_num_words')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_adjective_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_adjective')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_noun_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_noun')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'unit_type')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'number2')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'street_address')) or "" }}
    {% endif %}
    {% if 'city' in parts %}
    {{ dbt_synth.add_update_hook(snowflake__address_update_city(name, cities, geo_regions)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'city_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'city')) or "" }}
    {% endif %}
    {% if 'geo_region' in parts %}
    {{ dbt_synth.add_update_hook(snowflake__address_update_geo_region(name, geo_regions)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'geo_region_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'geo_region')) or "" }}
    {% endif %}
    {% if 'country' in parts %}
    {{ dbt_synth.add_update_hook(snowflake__address_update_country(name, countries)) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'country_idx')) or "" }}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'country')) or "" }}
    {% endif %}
    {% if 'postal_code' in parts %}
    {{ dbt_synth.add_cleanup_hook(snowflake__address_cleanup(name, 'postal_code')) or "" }}
    {% endif %}
    {{ dbt_synth.add_update_hook(snowflake__address_update(name, parts)) or "" }}

    {% if 'street_address' in parts %}
    UNIFORM( 1, {{address_types|length}}, RANDOM({{get_randseed()}}))::int as {{name}}__address_type,
    UNIFORM(10, 9999, RANDOM({{get_randseed()}}))::varchar as {{name}}__number1, {# 2-4 digit number #}
    UNIFORM( 1,    2, RANDOM({{get_randseed()}}))::int as {{name}}__street_num_words,
    {# 1641 number comes from (select count(*) from synth_words where types like '%noun%'): #}
    UNIFORM( 1, 1641, RANDOM({{get_randseed()}}))::int as {{name}}__street_noun_idx,
    ''::varchar as {{name}}__street_noun,
    {# 610 number comes from (select count(*) from synth_words where types like '%adjective%''): #}
    UNIFORM( 1,  610, RANDOM({{get_randseed()}}))::int as {{name}}__street_adjective_idx,
    ''::varchar as {{name}}__street_adjective,
    UNIFORM( 1, {{street_types|length}}, RANDOM({{get_randseed()}}))::int as {{name}}__street_type,
    UNIFORM( 1,    2, RANDOM({{get_randseed()}}))::int as {{name}}__unit_type,
    UNIFORM( 1,  999, RANDOM({{get_randseed()}}))::varchar as {{name}}__number2, {# 1-3 digit number #}
    ''::varchar as {{name}}__street_address,
    {% endif %}
    {% if 'city' in parts %}
    {# 331 number comes from (select count(*) from synth_cities): #}
    UNIFORM( 1,  {% if cities|length>0 %}{{cities|length}}{% else %}331{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}__city_idx,
    ''::varchar as {{name}}__city,
    {% endif %}
    {% if 'geo_region' in parts %}
    {# 71 number comes from (select count(*) from synth_geo_regions): #}
    UNIFORM( 1,   {% if geo_regions|length>0 %}{{geo_regions|length}}{% else %}71{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}__geo_region_idx,
    ''::varchar as {{name}}__geo_region,
    {% endif %}
    {% if 'country' in parts %}
    {# 195 number comes from (select count(*) from synth_countries): #}
    UNIFORM( 1,  {% if countries|length>0 %}{{countries|length}}{% else %}195{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}__country_idx,
    ''::varchar as {{name}}__country,
    {% endif %}
    {% if 'postal_code' in parts %}
    LPAD(UNIFORM({{postal_code_min}}, {{postal_code_max}}, RANDOM({{get_randseed()}}) )::varchar, 5, '0') as {{name}}__postal_code,
    {% endif %}
    ''::varchar AS {{name}}
{% endmacro%}

{% macro snowflake__address_update_street_noun(name) %}
    update {{this}} x set x.{{name}}__street_noun=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%noun%'
    ) as y where y.row_num=x.{{name}}__street_noun_idx
{% endmacro%}

{% macro snowflake__address_update_street_adjective(name) %}
    update {{this}} x set x.{{name}}__street_adjective=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%adjective%'
    ) as y where y.row_num=x.{{name}}__street_adjective_idx
{% endmacro%}

{% macro snowflake__address_update_street_address(name, address_types, street_types) %}
    update {{this}} x set x.{{name}}__street_address=(
        case x.{{name}}__address_type
            {% for i in range(address_types|length) %}
            when {{i+1}} then 

                {% if address_types[i]=='house' %}

                    x.{{name}}__number1 || ' '
                    || (case x.{{name}}__street_num_words
                        when 1 then x.{{name}}__street_noun
                        else x.{{name}}__street_adjective || ' ' || x.{{name}}__street_noun
                    end)
                    || ' '
                    || (case x.{{name}}__street_type
                        {% for i in range(street_types|length) %}
                        when {{i+1}} THEN '{{street_types[i]}}'
                        {% endfor %}
                    end)
                
                {% elif address_types[i]=='apartment' %}
                
                    x.{{name}}__number1 || ' '
                    || (case x.{{name}}__street_num_words
                        when 1 then x.{{name}}__street_noun
                        else x.{{name}}__street_adjective || ' ' || x.{{name}}__street_noun
                    end)
                    || ' '
                    || (case x.{{name}}__street_type
                        {% for i in range(street_types|length) %}
                        when {{i+1}} THEN '{{street_types[i]}}'
                        {% endfor %}
                    end)
                    || ' ' {# unit #}
                    || (case x.{{name}}__unit_type
                        when 1 then 'No. '
                        else '#'
                    end)
                    || x.{{name}}__number2
                
                {% elif address_types[i]=='pobox' %}
                
                (case x.{{name}}__unit_type
                        when 1 then 'PO'
                        else 'P.O. '
                    end)
                    || ' Box '
                    || x.{{name}}__number2
                
                {% endif %}
            {% endfor %}
        end
    )
{% endmacro%}

{% macro snowflake__address_update_city(name, cities, geo_regions) %}
    update {{this}} x set x.{{name}}__city=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_cities
        {% if cities|length > 0 or geo_regions|length > 0 %}where {% endif %}
        {% if cities|length > 0 %}name in ('{{cities|join("','")}}') {% endif %}
        {% if cities|length > 0 and geo_regions|length > 0 %}and {% endif %}
        {% if geo_regions|length > 0 %}geo_region_abbr in ('{{geo_regions|join("','")}}') {% endif %}
    ) as y where y.row_num=x.{{name}}__city_idx
{% endmacro%}

{% macro snowflake__address_update_geo_region(name, geo_regions) %}
    update {{this}} x set x.{{name}}__geo_region=y.abbr from (
        select abbr, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_geo_regions
        {% if geo_regions|length > 0 %}where abbr in ('{{geo_regions|join("','")}}'){% endif %}
    ) as y where y.row_num=x.{{name}}__geo_region_idx
{% endmacro%}

{% macro snowflake__address_update_country(name, countries) %}
    update {{this}} x set x.{{name}}__country=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_countries
        {% if countries|length > 0 %}where name in ('{{countries|join("','")}}'){% endif %}
    ) as y where y.row_num=x.{{name}}__country_idx
{% endmacro%}

{% macro snowflake__address_update(name, parts) %}
    update {{this}} x set x.{{name}}=(
        {% if 'street_address' in parts %}
            x.{{name}}__street_address
        {% endif %}

        {% if 'street_address' in parts and 'city' in parts %}
            || ', '
        {% endif %}

        {% if 'city' in parts %}
            {% if 'street_address' in parts %}||{% endif %}
            x.{{name}}__city
        {% endif %}

        {% if 'city' in parts and 'geo_region' in parts %}
            || ', '
        {% endif %}

        {% if 'geo_region' in parts %}
            {% if 'city' in parts or 'street_address' in parts %}||{% endif %}
            x.{{name}}__geo_region
        {% endif %}

        {% if 'geo_region' in parts and 'postal_code' in parts %}
            || ' '
        {% endif %}

        {% if 'postal_code' in parts %}
            {% if 'city' in parts or 'street_address' in parts or 'geo_region' in parts %}||{% endif %}
            x.{{name}}__postal_code
        {% endif %}

        {% if 'postal_code' in parts and 'country' in parts %}
            || ' '
        {% endif %}

        {% if 'country' in parts %}
            {% if 'city' in parts or 'street_address' in parts or 'geo_region' in parts or 'postal_code' in parts %}||{% endif %}
            x.{{name}}__country
        {% endif %}
    )
{% endmacro %}

{% macro snowflake__address_cleanup(name, col) %}
alter table {{ this }} drop column {{name}}__{{col}}
{% endmacro %}
