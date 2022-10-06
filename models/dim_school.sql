-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('synth_words') }}
-- depends_on: {{ ref('synth_cities') }}

{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = var('num_schools'),
    columns = [
        dbt_synth.column_primary_key(name='k_school'),
        dbt_synth.column_foreign_key(name='k_lea', table='dim_lea', column='k_lea'),
        dbt_synth.column_lookup(name='tenant_code', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code'),
        dbt_synth.column_integer_sequence(name='school_id', step=1, start=1000),
        dbt_synth.column_value(name='school_name', value='SchoolNameComingSoon'),
        dbt_synth.column_value(name='school_short_name', value='SchoolShortNameComingSoon'),
        dbt_synth.column_lookup(name='lea_name', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_name'),
        dbt_synth.column_lookup(name='lea_id', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_id'),
        dbt_synth.column_value(name='school_category', value=None),
        dbt_synth.column_value(name='school_type', value='Regular'),
        dbt_synth.column_value(name='operational_status', value=None),
        dbt_synth.column_value(name='administrative_funding_control', value=None),
        dbt_synth.column_value(name='internet_access', value=None),
        dbt_synth.column_value(name='title_i_part_a_school_designation', value=None),
        dbt_synth.column_value(name='charter_status', value=None),
        dbt_synth.column_value(name='charter_approval_agency', value=None),
        dbt_synth.column_value(name='magnet_type', value=None),
        dbt_synth.column_value(name='website', value=None),
        dbt_synth.column_value(name='address_type', value='Physical'),
        dbt_synth.column_address(name='street_address', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['street_address']),
        dbt_synth.column_address(name='city', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['city']),
        dbt_synth.column_value(name='name_of_county', value=None),
        dbt_synth.column_address(name='state_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['geo_region']),
        dbt_synth.column_address(name='postal_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['postal_code']),
        dbt_synth.column_value(name='building_site_number', value=None),
        dbt_synth.column_value(name='locale', value=None),
        dbt_synth.column_value(name='congressional_district', value=None),
        dbt_synth.column_value(name='county_fips_code', value=None),
        dbt_synth.column_value(name='latitude', value=None),
        dbt_synth.column_value(name='longitude', value=None),
    ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
