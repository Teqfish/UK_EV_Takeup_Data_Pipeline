-- #### === QUARTERLY NEW VEHICLE RATIOS === ####
-- Pivot the long new-vehicle registrations table wide,
-- then calculate the plugin / fossil ratio.

with pivoted as (

    select
        quarter_date,

        max(case when fuel_group = 'total' then registered_licenses end) as total_registered_licenses,
        max(case when fuel_group = 'fossil' then registered_licenses end) as fossil_registered_licenses,
        max(case when fuel_group = 'plugin' then registered_licenses end) as plugin_registered_licenses

    from {{ ref('int_vehicle_registrations_new_quarterly') }}
    group by 1

)

select
    quarter_date,
    total_registered_licenses,
    fossil_registered_licenses,
    plugin_registered_licenses,

    -- Plugin / fossil ratio as a percentage
    safe_divide(plugin_registered_licenses, fossil_registered_licenses) * 100 as new_plugin_fossil_ratio

from pivoted
order by 1
