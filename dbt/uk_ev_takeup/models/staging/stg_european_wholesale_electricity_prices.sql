select
    cast(country as string) as country,
    cast(iso3_code as string) as iso3_code,
    cast(date as timestamp) as date,
    cast(price_eur_mwhe as numeric) as price_eur_mwhe
from {{ source('uk_ev_raw', 'raw_european_wholesale_electricity_prices') }}
where country is not null
  and iso3_code is not null
  and date is not null
