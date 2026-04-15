-- #### === QUARTERLY NEW VEHICLE REGISTRATIONS === ####
-- Filter to UK totals from VEH1153,
-- derive fossil as total minus plugin,
-- and reshape to a long fuel_group table for charting.

with filtered as (

    -- Keep only UK, total body type, total keepership, quarterly rows, 2015+
    select
        geography,
        date_interval,
        date_label,
        body_type,
        keepership,
        total,
        plugin
    from {{ ref('stg_dvla_veh1153') }}
    where geography = 'United Kingdom'
      and body_type = 'Total'
      and keepership = 'Total'
      and date_interval = 'Quarterly'
      and safe_cast(regexp_extract(date_label, r'(\d{4})') as int64) >= 2015

),

dated as (

    -- Extract year and quarter from the source label
    select
        geography,
        date_label,
        total,
        plugin,
        safe_cast(regexp_extract(date_label, r'(\d{4})') as int64) as year_num,
        concat('Q', regexp_extract(date_label, r'Q([1-4])')) as quarter_label
    from filtered

),

quarterized as (

    -- Build a true quarter date
    select
        date(
            year_num,
            case
                when quarter_label = 'Q1' then 1
                when quarter_label = 'Q2' then 4
                when quarter_label = 'Q3' then 7
                when quarter_label = 'Q4' then 10
            end,
            1
        ) as quarter_date,
        total,
        plugin,
        total - plugin as fossil
    from dated
    where year_num is not null
      and quarter_label in ('Q1', 'Q2', 'Q3', 'Q4')

),

longed as (

    -- Reshape to long fuel_group format
    select
        quarter_date,
        'total' as fuel_group,
        total as registered_licenses
    from quarterized

    union all

    select
        quarter_date,
        'fossil' as fuel_group,
        fossil as registered_licenses
    from quarterized

    union all

    select
        quarter_date,
        'plugin' as fuel_group,
        plugin as registered_licenses
    from quarterized

)

select
    quarter_date,
    fuel_group,
    registered_licenses
from longed
where registered_licenses is not null
order by 1, 2
