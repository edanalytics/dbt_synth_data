{{ config(materialized='table') }}
select
    {{ synth_primary_key() }} as k_lea,
    {{ synth_value(value='c3203005af9e98e33a2cd94f030a2a89') }} as k_lea__parent,
    {{ synth_value(value='c3203005af9e98e33a2cd94f030a2a89') }} as k_sea,
    {{ synth_values(values=var('tenant_codes')) }} as tenant_code,
    {{ synth_integer_sequence(step=1, start=125) }} as lea_id,
    {{ synth_value(value='LEANameComingSoon') }} as lea_name,
    {{ synth_value(value='LEAShortNameComingSoon') }} as lea_short_name,
    {{ synth_value(value=None) }} as parent_lea_id,
    {{ synth_value(value='Independent') }} as lea_category,
    {{ synth_value(value=None) }} as education_service_center_id,
    {{ synth_value(value=None) }} as operational_status,
    {{ synth_value(value=None) }} as charter_status,
    {{ synth_value(value='Mailing') }} as address_type,
    {{ synth_address(
        name='street_address',
        countries=['United States'],
        geo_regions=[var('state_code')],
        address_types=['house'],
        parts=['street_address']
    ) }} as street_address,
    {{ synth_address(
        name='city',
        countries=['United States'],
        geo_regions=[var('state_code')],
        address_types=['house'],
        parts=['city']
    ) }} as city,
    {{ synth_value(value=None) }} as name_of_county,
    {{ synth_address(
        name='state_code',
        countries=['United States'],
        geo_regions=[var('state_code')],
        address_types=['house'],
        parts=['geo_region']
    ) }} as state_code,
    {{ synth_address(
        name='postal_code',
        countries=['United States'],
        geo_regions=[var('state_code')],
        address_types=['house'],
        parts=['postal_code']
    ) }} as postal_code,
    {{ synth_value(value=None) }} as building_site_number,
    {{ synth_value(value=None) }} as locale,
    {{ synth_value(value=None) }} as congressional_district,
    {{ synth_value(value=None) }} as county_fips_code,
    {{ synth_value(value=None) }} as latitude,
    {{ synth_value(value=None) }} as longitude
from {{ synth_table(rows=var('num_leas')) }}

{{ synth_add_update_hook("""
    insert into {{this}} (
        k_lea,
        k_lea__parent,
        k_sea,
        tenant_code,
        lea_id,
        lea_name,
        lea_short_name,
        parent_lea_id,
        lea_category,
        education_service_center_id,
        operational_status,
        charter_status,
        address_type,
        street_address,
        city,
        name_of_county,
        state_code,
        postal_code,
        building_site_number,
        locale,
        congressional_district,
        county_fips_code,
        latitude,
        longitude
    ) values (
        'c3203005af9e98e33a2cd94f030a2a89',
        NULL,
        NULL,
        '""" + var('tenant_codes')[0] + """',
        123,
        'Some State SEA',
        'SSSEA',
        NULL,
        'Independent',
        NULL,
        NULL,
        NULL,
        'Mailing',
        '123 Main St.',
        'Anytown',
        NULL,
        '""" + var('state_code') + """',
        12345,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    )
""") or "" }}

{{ config(post_hook=synth_get_post_hooks())}}
