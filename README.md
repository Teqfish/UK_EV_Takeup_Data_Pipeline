# UK_EV_Takeup_Data_Pipeline

DESNZ / BoE
	•	raw_bank_of_england_eur_gbp_fx
	•	raw_european_wholesale_electricity_prices
	•	raw_desnz_petroleum_products_prices

DVLA
	•	raw_dvla_veh1103
	•	raw_dvla_veh1153

GCS
  •	prepared_bank_of_england_eur_gbp_fx
	•	prepared_european_wholesale_electricity_prices
	•	prepared_desnz_petroleum_products_prices
	•	prepared_dvla_veh1103
	•	prepared_dvla_veh1153

BQ
	•	raw_bank_of_england_eur_gbp_fx
	•	raw_european_wholesale_electricity_prices
	•	raw_desnz_petroleum_products_prices
	•	raw_dvla_veh1103
	•	raw_dvla_veh1153

	•	stg_bank_of_england_eur_gbp_fx
	•	stg_european_wholesale_electricity_prices
	•	stg_desnz_petroleum_products_prices
	•	stg_dvla_veh1103
	•	stg_dvla_veh1153

	•	int_eur_gbp_fx_monthly
	•	int_electricity_prices_gb_monthly
	•	int_electricity_prices_gb_quarterly
	•	int_petroleum_prices_quarterly
	•	int_fossil_electricity_ratio_quarterly
	•	int_vehicle_registrations_all_quarterly
	•	int_vehicle_registrations_new_quarterly
	•	int_vehicle_ratios_all_quarterly
	•	int_vehicle_ratios_new_quarterly
	•	int_transition_ratios_quarterly

	•	mart_energy_prices_quarterly
    Columns:
    •	quarter_date
    •	premium_unleaded
    •	diesel
    •	crude_oil_index
    •	fossil_avg
    •	electricity_price_gbp_mwhe
    •	electricity_price_p_kwh
    •	fossil_electricity_ratio
    •	fossil_electricity_ratio_pct_change
	•	mart_vehicle_registrations_new_by_fuel_type
    Columns:
    •	quarter_date
    •	fuel_group
    •	registered_licenses
	•	mart_vehicle_registrations_all_by_fuel_type
    Columns:
    •	quarter_date
    •	fuel_group
    •	registered_licenses
	•	mart_transition_ratios_quarterly
    Columns:
    •	quarter_date
    •	fossil_electricity_ratio
    •	fossil_electricity_ratio_pct_change
    •	new_plugin_fossil_ratio
    •	new_plugin_fossil_ratio_pct_change
    •	all_plugin_fossil_ratio
    •	all_plugin_fossil_ratio_pct_change
	•	mart_transition_scorecards
  Columns:
    •	as_of_quarter
    •	avg_pct_change_fossil_electricity_ratio_5y
    •	avg_pct_change_new_plugin_fossil_ratio_5y
    •	avg_pct_change_all_plugin_fossil_ratio_5y


A data pipeline to compare the price of fuel with EV takeup in the UK

df_411
  - fuel prices
  - quarterly
  - 1989 - 2025
  - diesel/petrol = pence/ltr
  - crude oil index where 2010 = 100

  >> average the price of diesel and petrol, 2015-2024 Q4, quarterly

df_elec_gb
  - electricity prices
  - monthly
  - 2015 - 2025
  - GBP/MWhe

df_1111
  - licensed vehicles per year by year first registered by fuel
  - share of young vehicles by fuel type over time
  - quarterly
  - 1994 - 2024 inclusive

df_1103
  - Licensed vehicles at the end of the quarter by body type and fuel type
  - quarterly
  - 1994 - 2025 (from 2015 looks best)

df_1153
  - First time registered vehicles by fuel type
  - quarterly
  -
