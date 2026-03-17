-- depends_on: {{ ref('stg_atmo_daily') }}

with base as (
    select * from {{ ref('stg_atmo_daily') }}
),

aggregated as (
    select
        date_ech,
        code_departement,
        count(distinct code_insee)                          as nb_communes,
        round(avg(code_qual), 2)                            as indice_moyen,
        count(*) filter (where code_qual = 1)               as nb_bon,
        count(*) filter (where code_qual = 2)               as nb_moyen,
        count(*) filter (where code_qual = 3)               as nb_degrade,
        count(*) filter (where code_qual = 4)               as nb_mauvais,
        count(*) filter (where code_qual = 5)               as nb_tres_mauvais,
        count(*) filter (where code_qual = 6)               as nb_extremement_mauvais,
        round(100.0 * count(*) filter (where code_qual <= 2)
            / count(*), 1)                                  as pct_acceptable,
        round(100.0 * count(*) filter (where array_contains(polluants_declencheurs, 'NO2'))
            / count(*), 1)                                  as pct_no2_declencheur,
        round(100.0 * count(*) filter (where array_contains(polluants_declencheurs, 'O3'))
            / count(*), 1)                                  as pct_o3_declencheur,
        round(100.0 * count(*) filter (where array_contains(polluants_declencheurs, 'PM10'))
            / count(*), 1)                                  as pct_pm10_declencheur,
        round(100.0 * count(*) filter (where array_contains(polluants_declencheurs, 'PM25'))
            / count(*), 1)                                  as pct_pm25_declencheur,
        round(100.0 * count(*) filter (where array_contains(polluants_declencheurs, 'SO2'))
            / count(*), 1)                                  as pct_so2_declencheur
    from base
    group by date_ech, code_departement
),

with_dominant as (
    select
        *,
        {{ polluant_dominant('pct', 'declencheur') }} as polluants_dominants_departement
    from aggregated
)

select * from with_dominant
order by date_ech desc, code_departement