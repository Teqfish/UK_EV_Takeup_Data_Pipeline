-- #### === MART: TRANSITION SCORECARDS === ####
-- Build one-row dashboard scorecards from the transition ratios mart.
-- We keep the latest quarter and calculate average % change over the 2020-Q3 to 2025-Q3 window.

select
    max(quarter_date) as as_of_quarter,
    avg(fossil_electricity_ratio_pct_change) as avg_pct_change_fossil_electricity_ratio_5y,
    avg(new_plugin_fossil_ratio_pct_change) as avg_pct_change_new_plugin_fossil_ratio_5y,
    avg(all_plugin_fossil_ratio_pct_change) as avg_pct_change_all_plugin_fossil_ratio_5y
from {{ ref('mart_transition_ratios_quarterly') }}
