-- #### === QUARTERLY PETROLEUM PRICES === ####
-- Clean the quarter label, build a quarter date,
-- and keep only the fuel price fields we need.

with base as (

    select
        year,
        quarter,
        premium_unleaded,
        diesel,
        crude_oil_index
    from {{ ref('stg_desnz_petroleum_products_prices') }}

),

quarter_mapped as (

    select
        year,
        quarter,
        case
            when quarter = 'Jan to Mar' then 1
            when quarter = 'Apr to Jun' then 4
            when quarter = 'Jul to Sep' then 7
            when quarter = 'Oct to Dec' then 10
        end as quarter_start_month,
        premium_unleaded,
        diesel,
        crude_oil_index
    from base

)

select
    date(year, quarter_start_month, 1) as quarter_date,
    premium_unleaded,
    diesel,
    crude_oil_index
from quarter_mapped
where quarter_start_month is not null
order by 1
