with base as (
    select * from {{ ref('stg_atmo_daily') }}
),

aggregated as (
    select
        code_insee,
        nom_commune,
        source_aasqa,
        latitude,
        longitude,

        -- Période couverte
        min(date_ech)                           as premiere_date,
        max(date_ech)                           as derniere_date,
        count(distinct date_ech)                as nb_jours_mesures,

        -- Distribution des qualificatifs
        count(*) filter (where code_qual = 1)   as nb_jours_bon,
        count(*) filter (where code_qual = 2)   as nb_jours_moyen,
        count(*) filter (where code_qual = 3)   as nb_jours_degrade,
        count(*) filter (where code_qual = 4)   as nb_jours_mauvais,
        count(*) filter (where code_qual = 5)   as nb_jours_tres_mauvais,
        count(*) filter (where code_qual = 6)   as nb_jours_extremement_mauvais,

        -- Indice moyen (tendance)
        round(avg(code_qual), 2)                as indice_moyen,

        -- Polluant le plus souvent déclencheur
        -- (le sous-indice qui égale code_qual le plus souvent)
        round(avg(code_no2),  2)                as indice_moyen_no2,
        round(avg(code_o3),   2)                as indice_moyen_o3,
        round(avg(code_pm10), 2)                as indice_moyen_pm10,
        round(avg(code_pm25), 2)                as indice_moyen_pm25,
        round(avg(code_so2),  2)                as indice_moyen_so2,

        -- % jours qualité acceptable (bon + moyen)
        round(
            100.0 * count(*) filter (where code_qual <= 2)
            / count(*),
        1)                                      as pct_jours_acceptable

    from base
    group by code_insee, nom_commune, source_aasqa, latitude, longitude
)

select * from aggregated