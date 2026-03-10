select
    a.date_ech,
    a.code_departement,
    a.indice_moyen,
    a.polluants_dominants_departement,
    m.temperature_max,
    m.precipitation_sum,
    m.wind_speed_max,
    m.weathercode          -- ensoleillé, nuageux, pluie...
from mart_atmo_daily_departement a
left join stg_meteo_daily m
    on a.date_ech = m.date_ech
    and a.code_departement = m.code_departement