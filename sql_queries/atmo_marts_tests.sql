--mart_atmo_daily_national
WITH aggregated as (
    select
        date_ech,
        count(*) as nb_communes,
        round(avg(code_qual), 2) as indice_moyen_national,
        count(*) filter (where code_qual = 1) as nb_bon,
        count(*) filter (where code_qual >= 4) as nb_mauvais_ou_pire,
        round(100.0 * count(*) filter (
            where array_contains(polluants_declencheurs, 'NO2')
        ) / count(*), 1) as pct_no2_declencheur,
        round(100.0 * count(*) filter (
            where array_contains(polluants_declencheurs, 'O3')
        ) / count(*), 1) as pct_o3_declencheur,
        round(100.0 * count(*) filter (
            where array_contains(polluants_declencheurs, 'PM10')
        ) / count(*), 1) as pct_pm10_declencheur,
        round(100.0 * count(*) filter (
            where array_contains(polluants_declencheurs, 'PM25')
        ) / count(*), 1) as pct_pm25_declencheur,
        round(100.0 * count(*) filter (
            where array_contains(polluants_declencheurs, 'SO2')
        ) / count(*), 1) as pct_so2_declencheur
    from stg_atmo_daily
    group by date_ech
),

with_dominant as (
    select *,
        case greatest(pct_no2_declencheur, pct_o3_declencheur, pct_pm10_declencheur, pct_pm25_declencheur, pct_so2_declencheur)
            when pct_no2_declencheur  then 'NO2'
            when pct_o3_declencheur   then 'O3'
            when pct_pm10_declencheur then 'PM10'
            when pct_pm25_declencheur then 'PM25'
            when pct_so2_declencheur  then 'SO2'
        end as polluant_dominant_national
    from aggregated
)

select 
    date_ech, nb_communes, 
    indice_moyen_national, 
    nb_bon,nb_mauvais_ou_pire, 
    polluant_dominant_national,
    pct_no2_declencheur,
    pct_o3_declencheur,
    pct_pm10_declencheur,
    pct_pm25_declencheur,
    pct_so2_declencheur 
from with_dominant
order by date_ech desc;

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

select * from mart_atmo_daily_national;