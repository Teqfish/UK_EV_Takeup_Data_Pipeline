-- #### === MONTHLY FX RATE === ####
-- Keep one FX observation per month.
-- We take the first available date in each month.

with monthly_ranked as (

    -- Rank daily rows within each month
    select
        date_trunc(date, month) as month_date,
        date,
        gbp_eur_rate,
        row_number() over (
            partition by date_trunc(date, month)
            order by date
        ) as month_row_num
    from {{ ref('stg_bank_of_england_eur_gbp_fx') }}

)

-- Keep only the first observed FX rate in each month
select
    month_date,
    date as source_date,
    gbp_eur_rate
from monthly_ranked
where month_row_num = 1
