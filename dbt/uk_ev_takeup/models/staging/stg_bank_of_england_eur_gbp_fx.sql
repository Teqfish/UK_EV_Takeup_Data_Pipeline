select
    cast(date as date) as date,
    cast(gbp_eur_rate as numeric) as gbp_eur_rate
from {{ source('uk_ev_raw', 'raw_bank_of_england_eur_gbp_fx') }}
where date is not null
  and gbp_eur_rate is not null
