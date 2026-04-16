{{ config(
    materialized='table',
    partition_by={
      "field": "quarter_date",
      "data_type": "date",
      "granularity": "month"
    }
) }}

-- #### === MART: QUARTERLY ENERGY PRICES === ####
-- Expose the final quarterly energy price series for the dashboard.
-- This keeps the key electricity, fossil, and ratio fields in one place.

select
    quarter_date,
    premium_unleaded,
    diesel,
    crude_oil_index,
    electricity_price_gbp_mwhe,
    electricity_price_p_kwh,
    fossil_avg,
    fossil_electricity_ratio
from {{ ref('int_fossil_electricity_ratio_quarterly') }}
where quarter_date between date '2020-07-01' and date '2025-07-01'
