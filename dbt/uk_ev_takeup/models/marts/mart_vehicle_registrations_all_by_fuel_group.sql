{{ config(
    materialized='table',
    partition_by={
      "field": "quarter_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["fuel_group"]
) }}

-- #### === MART: QUARTERLY ALL VEHICLE REGISTRATIONS === ####
-- Expose the final all-vehicle registration series for the dashboard.
-- Keep the long fuel_group structure for easy charting in Streamlit.

select
    quarter_date,
    fuel_group,
    registered_licenses
from {{ ref('int_vehicle_registrations_all_quarterly') }}
where quarter_date between date '2020-07-01' and date '2025-07-01'
