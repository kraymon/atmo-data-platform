-- depends_on: {{ ref('mart_atmo_daily_departement') }}
-- depends_on: {{ ref('stg_meteo_daily') }}

select
    a.date_ech,
    a.code_departement,
    a.indice_moyen,
    a.polluants_dominants_departement,
    m.temperature_max,
    m.temperature_min,
    m.rain_sum,
    m.precipitation_sum,
    m.wind_speed_max,
    m.weathercode
from {{ ref('mart_atmo_daily_departement') }} a
left join {{ ref('stg_meteo_daily') }} m
    on  a.date_ech = m.date_ech
    and a.code_departement = m.code_departement