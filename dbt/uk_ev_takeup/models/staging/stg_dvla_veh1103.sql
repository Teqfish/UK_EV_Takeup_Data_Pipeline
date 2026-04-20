select
    cast(Geography as string) as geography,
    cast(Date as string) as date_label,
    cast(Units as string) as units,
    cast(`Body Type` as string) as body_type,

    cast(Petrol as numeric) as petrol,
    cast(Diesel as numeric) as diesel,
    cast(`Hybrid electric - petrol` as numeric) as hybrid_electric_petrol,
    cast(`Hybrid electric - diesel` as numeric) as hybrid_electric_diesel,
    cast(`Plug-in hybrid electric - petrol` as numeric) as plugin_hybrid_electric_petrol,
    cast(`Plug-in hybrid electric - diesel` as numeric) as plugin_hybrid_electric_diesel,
    cast(`Battery electric` as numeric) as battery_electric,
    cast(`Range extended electric` as numeric) as range_extended_electric,
    cast(`Fuel cell electric` as numeric) as fuel_cell_electric,
    cast(Gas as numeric) as gas,
    cast(`Other fuel types` as numeric) as other_fuel_types,
    cast(Total as numeric) as total,
    cast(`Plug-in` as numeric) as plugin,
    cast(`Zero emission` as numeric) as zero_emission
from {{ source('uk_ev_raw', 'raw_dvla_veh1103') }}
where Geography is not null
  and Date is not null
