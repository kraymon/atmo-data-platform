-- Distribution des weathercode selon l'indice
SELECT
    weathercode,
    round(avg(indice_moyen), 2)     as indice_moyen,
    count(*)                         as nb_observations,
    round(avg(temperature_max), 1)  as temp_max_moy,
    round(avg(wind_speed_max), 1)   as vent_moy
FROM mart_atmo_meteo_daily_departement
GROUP BY weathercode
ORDER BY indice_moyen DESC;

--Météo quand l'air est mauvais vs bon
SELECT
    CASE
        WHEN indice_moyen >= 3   THEN '🔴 Dégradé ou pire (>=3)'
        WHEN indice_moyen >= 2.5 THEN '🟡 Entre 2.5 et 3'
        ELSE                          '🟢 Bon (< 2.5)'
    END as qualite,
    round(avg(temperature_max), 1)  as temp_max_moy,
    round(avg(temperature_min), 1)  as temp_min_moy,
    round(avg(wind_speed_max), 1)   as vent_moy,
    round(avg(precipitation_sum), 1) as precipitation_moy,
    count(*)                         as nb_observations
FROM mart_atmo_meteo_daily_departement
GROUP BY qualite
ORDER BY qualite;

--PM10 selon la météo
SELECT
    weathercode,
    round(avg(indice_moyen), 2)     as indice_moyen,
    count(*)                         as nb_observations,
    round(avg(wind_speed_max), 1)   as vent_moy,
    round(avg(precipitation_sum), 1) as precipitation_moy
FROM mart_atmo_meteo_daily_departement
WHERE array_contains(polluants_dominants_departement, 'PM10')
GROUP BY weathercode
ORDER BY indice_moyen DESC;

--Corrélation vent × indice — la plus parlante
SELECT
    CASE
        WHEN wind_speed_max < 10  THEN '🌬️ Vent faible (<10)'
        WHEN wind_speed_max < 20  THEN '💨 Vent modéré (10-20)'
        WHEN wind_speed_max < 40  THEN '🌪️ Vent fort (20-40)'
        ELSE                           '🌪️ Très fort (>40)'
    END as categorie_vent,
    round(avg(indice_moyen), 2)     as indice_moyen,
    count(*)                         as nb_observations
FROM mart_atmo_meteo_daily_departement
GROUP BY categorie_vent
ORDER BY indice_moyen DESC;

--Corrélation température × O3 — effet canicule
SELECT
    CASE
        WHEN temperature_max < 10  THEN '❄️ Froid (<10°)'
        WHEN temperature_max < 20  THEN '🌤️ Tempéré (10-20°)'
        WHEN temperature_max < 30  THEN '☀️ Chaud (20-30°)'
        ELSE                            '🔥 Très chaud (>30°)'
    END as categorie_temp,
    round(avg(indice_moyen), 2)     as indice_moyen,
    count(*) filter (
        where array_contains(polluants_dominants_departement, 'O3')
    )                                as nb_fois_o3_dominant,
    count(*)                         as nb_observations
FROM mart_atmo_meteo_daily_departement
GROUP BY categorie_temp
ORDER BY indice_moyen DESC;