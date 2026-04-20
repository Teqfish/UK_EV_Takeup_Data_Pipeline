{{ config(
    materialized='table',
    partition_by={
      "field": "quarter_date",
      "data_type": "date",
      "granularity": "month"
    }
) }}

-- #### === MART: QUARTERLY TRANSITION RATIOS === ####
-- Expose the final combined ratio series for the dashboard.
-- This includes the raw ratios and their percentage changes from 2020-Q3.

select
    quarter_date,
    fossil_electricity_ratio,
    fossil_electricity_ratio_pct_change,
    new_plugin_fossil_ratio,
    new_plugin_fossil_ratio_pct_change,
    all_plugin_fossil_ratio,
    all_plugin_fossil_ratio_pct_change
from {{ ref('int_transition_ratios_quarterly') }}
