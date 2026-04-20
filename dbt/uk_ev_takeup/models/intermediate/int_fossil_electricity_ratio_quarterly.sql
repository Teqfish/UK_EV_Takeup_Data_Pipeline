-- #### === QUARTERLY FOSSIL / ELECTRICITY RATIO === ####
-- Join quarterly electricity and petroleum prices,
-- convert electricity from EUR/MWhe to GBP/MWhe using the monthly FX model later,
-- and calculate the fossil / electricity price ratio.
--
-- 1. join quarterly electricity prices to the monthly FX series via quarter
-- 2. convert electricity EUR -> GBP
-- 3. convert GBP/MWhe -> p/kWh
-- 4. build a weighted fossil average from petrol and diesel
-- 5. calculate the fossil / electricity ratio

with fx_monthly as (

    -- Keep the first FX observation in each month
    select
        month_date,
        gbp_eur_rate
    from {{ ref('int_eur_gbp_fx_monthly') }}

),

fx_quarterly as (

    -- Aggregate monthly FX to a quarterly average
    select
        date_trunc(month_date, quarter) as quarter_date,
        avg(gbp_eur_rate) as gbp_eur_rate
    from fx_monthly
    group by 1

),

electricity_quarterly as (

    -- Quarterly electricity prices in EUR/MWhe
    select
        quarter_date,
        electricity_price_eur_mwhe
    from {{ ref('int_electricity_prices_gb_quarterly') }}

),

petroleum_quarterly as (

    -- Quarterly petroleum prices
    select
        quarter_date,
        premium_unleaded,
        diesel,
        crude_oil_index
    from {{ ref('int_petroleum_prices_quarterly') }}

),

joined as (

    -- Join quarterly electricity, FX, and petroleum prices
    select
        e.quarter_date,
        e.electricity_price_eur_mwhe,
        f.gbp_eur_rate,
        p.premium_unleaded,
        p.diesel,
        p.crude_oil_index
    from electricity_quarterly e
    left join fx_quarterly f
        on e.quarter_date = f.quarter_date
    left join petroleum_quarterly p
        on e.quarter_date = p.quarter_date

)

select
    quarter_date,
    premium_unleaded,
    diesel,
    crude_oil_index,

    -- Convert electricity price from EUR/MWhe to GBP/MWhe
    electricity_price_eur_mwhe / gbp_eur_rate as electricity_price_gbp_mwhe,

    -- Convert GBP/MWhe to p/kWh
    (electricity_price_eur_mwhe / gbp_eur_rate) * 100 / 1000 as electricity_price_p_kwh,

    -- Weighted fossil average using your exploratory weights
    ((premium_unleaded * 0.8059) + (diesel * 0.1941)) as fossil_avg,

    -- Fossil / electricity price ratio
    ((premium_unleaded * 0.8059) + (diesel * 0.1941))
    / ((electricity_price_eur_mwhe / gbp_eur_rate) * 100 / 1000) as fossil_electricity_ratio

from joined
where gbp_eur_rate is not null
  and premium_unleaded is not null
  and diesel is not null
order by 1
