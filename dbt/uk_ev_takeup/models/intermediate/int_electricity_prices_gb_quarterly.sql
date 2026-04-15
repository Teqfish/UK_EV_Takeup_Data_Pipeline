-- #### === QUARTERLY GB ELECTRICITY PRICES === ####
-- Aggregate the monthly GB electricity series to quarter level.
-- We take the average monthly price within each quarter.

select
    date_trunc(month_date, quarter) as quarter_date,
    avg(price_eur_mwhe) as electricity_price_eur_mwhe
from {{ ref('int_electricity_prices_gb_monthly') }}
group by 1
order by 1
