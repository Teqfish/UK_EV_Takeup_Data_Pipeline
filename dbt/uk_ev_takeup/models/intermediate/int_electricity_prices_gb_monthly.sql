-- #### === MONTHLY GB ELECTRICITY PRICES === ####
-- Keep only Great Britain rows and align dates to month start.
-- This gives us a clean monthly electricity series in EUR/MWhe.

select
    date_trunc(cast(date as date), month) as month_date,
    country,
    iso3_code,
    price_eur_mwhe
from {{ ref('stg_european_wholesale_electricity_prices') }}
where iso3_code = 'GBR'
  and price_eur_mwhe is not null
