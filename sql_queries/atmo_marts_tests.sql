SELECT *
FROM mart_atmo_daily_departement
ORDER BY date_ech DESC;

select * from mart_atmo_daily_national;

select * from mart_atmo_commune;


-- top atmo communes
SELECT
    code_insee,
    nom_commune,
    nb_jours_mesures,
    indice_moyen,
    pct_jours_acceptable
FROM mart_atmo_commune
WHERE nb_jours_mesures >= 7  -- exclure les communes avec peu de données
ORDER BY indice_moyen ASC    -- meilleures en premier
LIMIT 20;

SELECT 
    min(code_no2) as min_no2, max(code_no2) as max_no2,
    min(code_o3)  as min_o3,  max(code_o3)  as max_o3,
    min(code_pm10) as min_pm10, max(code_pm10) as max_pm10,
    min(code_pm25) as min_pm25, max(code_pm25) as max_pm25,
    min(code_so2)  as min_so2,  max(code_so2)  as max_so2
FROM stg_atmo_daily;