-- #### === QUARTERLY TRANSITION RATIOS === ####
-- Join the fossil / electricity price ratio to the
-- all-vehicle and new-vehicle plugin / fossil ratios.
-- Then calculate percentage change from the common baseline quarter:
-- 2020-Q3 = 2020-07-01.

with price_ratio as (

    select
        quarter_date,
        fossil_electricity_ratio
    from {{ ref('int_fossil_electricity_ratio_quarterly') }}

),

all_ratio as (

    select
        quarter_date,
        all_plugin_fossil_ratio
    from {{ ref('int_vehicle_ratios_all_quarterly') }}

),

new_ratio as (

    select
        quarter_date,
        new_plugin_fossil_ratio
    from {{ ref('int_vehicle_ratios_new_quarterly') }}

),

joined as (

    -- Keep only quarters present in all three series
    select
        p.quarter_date,
        p.fossil_electricity_ratio,
        a.all_plugin_fossil_ratio,
        n.new_plugin_fossil_ratio
    from price_ratio p
    inner join all_ratio a
        on p.quarter_date = a.quarter_date
    inner join new_ratio n
        on p.quarter_date = n.quarter_date

),

baseline as (

    -- Fix the common baseline at 2020-Q3
    select
        fossil_electricity_ratio as baseline_fossil_electricity_ratio,
        all_plugin_fossil_ratio as baseline_all_plugin_fossil_ratio,
        new_plugin_fossil_ratio as baseline_new_plugin_fossil_ratio
    from joined
    where quarter_date = date '2020-07-01'

)

select
    j.quarter_date,
    j.fossil_electricity_ratio,
    j.all_plugin_fossil_ratio,
    j.new_plugin_fossil_ratio,

    -- % change from 2020-Q3 baseline
    (safe_divide(j.fossil_electricity_ratio, b.baseline_fossil_electricity_ratio) - 1) * 100
        as fossil_electricity_ratio_pct_change,

    (safe_divide(j.all_plugin_fossil_ratio, b.baseline_all_plugin_fossil_ratio) - 1) * 100
        as all_plugin_fossil_ratio_pct_change,

    (safe_divide(j.new_plugin_fossil_ratio, b.baseline_new_plugin_fossil_ratio) - 1) * 100
        as new_plugin_fossil_ratio_pct_change

from joined j
cross join baseline b
where j.quarter_date between date '2020-07-01' and date '2025-07-01'
order by 1
