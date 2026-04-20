select
    cast(year as int64) as year,
    cast(quarter as string) as quarter,
    cast(premium_unleaded as numeric) as premium_unleaded,
    cast(diesel as numeric) as diesel,
    cast(crude_oil_acquired_by_refineries_2025_100_note_4_r as numeric) as crude_oil_index
from {{ source('uk_ev_raw', 'raw_desnz_petroleum_products_prices') }}
where year is not null
  and quarter is not null
