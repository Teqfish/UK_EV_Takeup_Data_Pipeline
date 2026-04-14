select
    cast(Geography as string) as geography,
    cast(`Date Interval` as string) as date_interval,
    cast(Date as string) as date_label,
    cast(Units as string) as units,
    cast(BodyType as string) as body_type,
    cast(Keepership as string) as keepership,

    cast(Petrol as numeric) as petrol,
    cast(Diesel as numeric) as diesel,
    cast(`Hybrid Electric - Petrol` as numeric) as hybrid_electric_petrol,
    cast(`Hybrid Electric - Diesel` as numeric) as hybrid_electric_diesel,
    cast(`Plug-in Hybrid Electric - Petrol` as numeric) as plugin_hybrid_electric_petrol,
    cast(`Plug-in Hybrid Electric - Diesel` as numeric) as plugin_hybrid_electric_diesel,
    cast(`Battery Electric` as numeric) as battery_electric,
    cast(`Range Extended Electric` as numeric) as range_extended_electric,
    cast(`Fuel Cell Electric` as numeric) as fuel_cell_electric,
    cast(Gas as numeric) as gas,
    cast(Others as numeric) as others,
    cast(Total as numeric) as total,
    cast(Plugin as numeric) as plugin,
    cast(ZEV as numeric) as zev
from {{ source('uk_ev_raw', 'raw_dvla_veh1153') }}
where Geography is not null
  and Date is not null
